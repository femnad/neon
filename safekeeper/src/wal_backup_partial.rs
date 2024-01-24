use std::sync::Arc;

use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use postgres_ffi::{v14::xlog_utils::XLogSegNoOffsetToRecPtr, XLogFileName, XLogSegNo, PG_TLI};
use remote_storage::RemotePath;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument};
use utils::lsn::Lsn;

use crate::{safekeeper::{Term, TermLsn}, timeline::Timeline, wal_backup::{self, Segment}, GlobalTimelines, SafeKeeperConf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UploadStatus {
    /// Upload is in progress
    InProgress,
    /// Upload is finished
    Uploaded,
    /// Deletion is in progress
    Deleting,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartialRemoteSegment {
    pub status: UploadStatus,
    pub name: String,
    pub commit_lsn: Lsn,
    pub flush_lsn: Lsn,
    pub term: Term,
}

// NB: these structures are a part of a control_file, you can't change them without
// changing the control file format version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct State {
    pub segments: Vec<PartialRemoteSegment>,
}

impl Default for State {
    fn default() -> Self {
        State { segments: vec![] }
    }
}

struct PartialBackup {
    wal_seg_size: usize,
    tli: Arc<Timeline>,
    commit_lsn_rx: tokio::sync::watch::Receiver<Lsn>,
    flush_lsn_rx: tokio::sync::watch::Receiver<TermLsn>,
    conf: SafeKeeperConf,
    local_prefix: Utf8PathBuf,
    remote_prefix: Utf8PathBuf,

    state: State,
}

// Read-only methods for getting segment names
impl PartialBackup {
    fn segment_name(&self, segno: u64) -> String {
        XLogFileName(PG_TLI, segno, self.wal_seg_size)
    }

    fn remote_segment_name(&self, segno: u64, flush_lsn: Lsn) -> String {
        format!(
            "{}_{:016X}_sk{}.partial",
            self.segment_name(segno),
            flush_lsn.0,
            self.conf.my_id.0,
        )
    }

    fn local_segment_name(&self, segno: u64) -> String {
        format!("{}.partial", self.segment_name(segno))
    }
}

impl PartialBackup {
    /// Takes a lock to read actual safekeeper state and returns a segment that should be uploaded.
    async fn prepare_upload(&self) -> PartialRemoteSegment {
        let sk_info = self.tli.get_safekeeper_info(&self.conf).await;
        let flush_lsn = Lsn(sk_info.flush_lsn);
        let commit_lsn = Lsn(sk_info.commit_lsn);
        let term = sk_info.term;
        let segno = flush_lsn.segment_number(self.wal_seg_size);

        let name = self.remote_segment_name(segno, flush_lsn);

        PartialRemoteSegment {
            status: UploadStatus::InProgress,
            name,
            commit_lsn,
            flush_lsn,
            term,
        }
    }

    /// Reads segment from disk and uploads it to the remote storage.
    async fn upload_segment(&mut self, prepared: PartialRemoteSegment) -> anyhow::Result<()> {
        let flush_lsn = prepared.flush_lsn;
        let segno = flush_lsn.segment_number(self.wal_seg_size);

        // We're going to backup bytes from the start of the segment up to flush_lsn.
        let backup_bytes = flush_lsn.segment_offset(self.wal_seg_size);
        
        let local_path = self.local_prefix.join(self.local_segment_name(segno));
        let remote_path = RemotePath::new(self.remote_prefix.join(&prepared.name).as_ref())?;

        // Upload first `backup_bytes` bytes of the segment to the remote storage.
        wal_backup::backup_object(&local_path, &remote_path, backup_bytes).await?;
        // TODO: metrics

        // We uploaded the segment, now let's verify that the data is still actual.
        // If the term changed, we cannot guarantee the validity of the uploaded data.
        // If the term is the same, we know the data is not corrupted.
        let sk_info = self.tli.get_safekeeper_info(&self.conf).await;
        if sk_info.term != prepared.term {
            anyhow::bail!("term changed during upload");
        }
        assert!(prepared.commit_lsn <= Lsn(sk_info.commit_lsn));
        assert!(prepared.flush_lsn <= Lsn(sk_info.flush_lsn));

        Ok(())
    }

    /// Write new state to disk. If in-memory and on-disk states diverged, returns an error.
    async fn commit_state(&mut self, new_state: State) -> anyhow::Result<()> {
        self.tli.map_control_file(|cf| {
            if cf.partial_backup != self.state {
                let memory = self.state.clone();
                self.state = cf.partial_backup.clone();
                anyhow::bail!("partial backup state diverged, memory={:?}, disk={:?}", memory, cf.partial_backup);
            }

            cf.partial_backup = new_state.clone();
            Ok(())
        }).await?;
        // update in-memory state
        self.state = new_state;
        Ok(())
    }

    /// Upload the latest version of the partial segment and garbage collect older versions.
    #[instrument(name = "upload", skip_all, fields(name = %prepared.name))]
    async fn do_upload(&mut self, prepared: &PartialRemoteSegment) -> anyhow::Result<()> {
        info!("starting upload {:?}", prepared);

        let state_0 = self.state.clone();
        let state_1 = {
            let mut state = state_0.clone();
            state.segments.push(prepared.clone());
            state
        };
        
        // we're going to upload a new segment, let's write it to disk to make GC later
        self.commit_state(state_1).await?;

        self.upload_segment(prepared.clone()).await?;

        let state_2 = {
            let mut state = state_0.clone();
            for seg in state.segments.iter_mut() {
                seg.status = UploadStatus::Deleting;
            }
            let mut actual_remote_segment = prepared.clone();
            actual_remote_segment.status = UploadStatus::Uploaded;
            state.segments.push(actual_remote_segment);
            state
        };
        
        // we've uploaded new segment, it's actual, all other segments should be GCed
        self.commit_state(state_2).await?;
        self.gc().await?;

        Ok(())
    }

    /// Delete all non-Uploaded segments from the remote storage. There should be only one
    /// Uploaded segment at a time.
    #[instrument(name = "gc", skip_all)]
    async fn gc(&mut self) -> anyhow::Result<()> {
        let mut segments_to_delete = vec![];

        let new_segments: Vec<PartialRemoteSegment> = self.state.segments.iter().filter_map(|seg| {
            if seg.status == UploadStatus::Uploaded {
                Some(seg.clone())
            } else {
                segments_to_delete.push(seg.name.clone());
                None
            }
        }).collect();

        info!("deleting objects: {:?}", segments_to_delete);
        let mut objects_to_delete = vec![];
        for seg in segments_to_delete.iter() {
            let remote_path = RemotePath::new(self.remote_prefix.join(seg).as_ref())?;
            objects_to_delete.push(remote_path);
        }

        // removing segments from remote storage
        wal_backup::delete_objects(&objects_to_delete).await?;

        // now we can update the state on disk
        let new_state = {
            let mut state = self.state.clone();
            state.segments = new_segments;
            state
        };
        self.commit_state(new_state).await?;

        Ok(())
    }
}

#[instrument(name = "Partial backup", skip_all, fields(ttid = %tli.ttid))]
pub async fn main_task(tli: Arc<Timeline>, conf: SafeKeeperConf) {
    debug!("started");

    let mut cancellation_rx = match tli.get_cancellation_rx() {
        Ok(rx) => rx,
        Err(_) => {
            info!("timeline canceled during task start");
            return;
        }
    };

    // TODO: sleep for random time to avoid thundering herd

    
    let (_, persistent_state) = tli.get_state().await;
    let commit_lsn_rx = tli.get_commit_lsn_watch_rx();
    let flush_lsn_rx = tli.get_term_flush_lsn_watch_rx();
    let wal_seg_size = tli.get_wal_seg_size().await;

    let local_prefix = tli.timeline_dir.clone();
    let remote_prefix = match tli.timeline_dir.strip_prefix(&conf.workdir) {
        Ok(path) => path.to_owned(),
        Err(e) => {
            info!("failed to strip workspace dir prefix: {:?}", e);
            return;
        }
    };

    let mut backup = PartialBackup {
        wal_seg_size,
        tli,
        state: persistent_state.partial_backup,
        commit_lsn_rx,
        flush_lsn_rx,
        conf,
        local_prefix,
        remote_prefix,
    };

    info!("state: {:?}", backup.state);

    // TODO: update to 15 minutes
    let upload_duration = std::time::Duration::from_secs(5);
    // let mut timer = UploadTimer::new(upload_duration);
    // tokio::pin!(timer);

    let mut interval = tokio::time::interval(upload_duration);

    loop {
        // if pending_seginfo.is_some() && pending_seginfo != uploaded_seginfo {
        //     timer.update_timer(true, pending_seginfo.as_ref().unwrap().segment_name.as_str());
        // } else {
        //     timer.update_timer(false, "");
        // }

        let mut should_upload = false;
        tokio::select! {
            _ = cancellation_rx.changed() => {
                info!("timeline canceled");
                return;
            }

            _ = interval.tick() => {
                should_upload = true;
            }
            _ = backup.commit_lsn_rx.changed() => {}
            _ = backup.flush_lsn_rx.changed() => {}
        }

        if !should_upload {
            continue;
        }

        // pending_seginfo = backup.get_pending_segment_info();
        // if should_upload && pending_seginfo.is_some() && pending_seginfo != uploaded_seginfo {
        //     let res = backup.upload_segment(pending_seginfo).await;
            
        // }
        let prepared = backup.prepare_upload().await;
        match backup.do_upload(&prepared).await {
            Ok(()) => {
                info!("uploaded {} up to flush_lsn {}", prepared.name, prepared.flush_lsn);
            }
            Err(e) => {
                info!("failed to upload {}: {:#}", prepared.name, e);
            }
        }
    }

    todo!()
}
