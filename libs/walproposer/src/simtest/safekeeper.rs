//! Safekeeper communication endpoint to WAL proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Result};
use bytes::{Bytes, BytesMut};
use hyper::Uri;
use log::info;
use safekeeper::{
    safekeeper::{
        ProposerAcceptorMessage, SafeKeeper, SafeKeeperState, ServerInfo, UNKNOWN_SERVER_VERSION,
    },
    simlib::{network::TCP, node_os::NodeOs, proto::AnyMessage, world::NodeEvent},
    timeline::TimelineError,
    SafeKeeperConf,
};
use tracing::debug;
use utils::{
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
};

use crate::simtest::storage::DiskStateStorage;

use super::{
    disk::{Disk, TimelineDisk},
    storage::DiskWALStorage,
};

struct ConnState {
    tcp: TCP,

    greeting: bool,
    ttid: TenantTimelineId,
    flush_pending: bool,
}

struct SharedState {
    sk: SafeKeeper<DiskStateStorage, DiskWALStorage>,
    disk: Arc<TimelineDisk>,
}

struct GlobalMap {
    timelines: HashMap<TenantTimelineId, SharedState>,
    conf: SafeKeeperConf,
    disk: Arc<Disk>,
}

impl GlobalMap {
    fn new(disk: Arc<Disk>, conf: SafeKeeperConf) -> Result<Self> {
        let mut timelines = HashMap::new();

        for (&ttid, disk) in disk.timelines.lock().iter() {
            debug!("loading timeline {}", ttid);
            let state = disk.state.lock().clone();

            if state.server.wal_seg_size == 0 {
                bail!(TimelineError::UninitializedWalSegSize(ttid));
            }

            if state.server.pg_version == UNKNOWN_SERVER_VERSION {
                bail!(TimelineError::UninitialinzedPgVersion(ttid));
            }

            if state.commit_lsn < state.local_start_lsn {
                bail!(
                    "commit_lsn {} is higher than local_start_lsn {}",
                    state.commit_lsn,
                    state.local_start_lsn
                );
            }

            let control_store = DiskStateStorage::new(disk.clone());
            let wal_store = DiskWALStorage::new(disk.clone(), &control_store)?;

            let sk = SafeKeeper::new(control_store, wal_store, conf.my_id)?;
            timelines.insert(
                ttid.clone(),
                SharedState {
                    sk,
                    disk: disk.clone(),
                },
            );
        }

        Ok(Self {
            timelines,
            conf,
            disk,
        })
    }

    fn create(&mut self, ttid: TenantTimelineId, server_info: ServerInfo) -> Result<()> {
        if self.timelines.contains_key(&ttid) {
            bail!("timeline {} already exists", ttid);
        }

        debug!("creating new timeline {}", ttid);

        let commit_lsn = Lsn::INVALID;
        let local_start_lsn = Lsn::INVALID;

        // TODO: load state from in-memory storage
        let state = SafeKeeperState::new(&ttid, server_info, vec![], commit_lsn, local_start_lsn);

        if state.server.wal_seg_size == 0 {
            bail!(TimelineError::UninitializedWalSegSize(ttid));
        }

        if state.server.pg_version == UNKNOWN_SERVER_VERSION {
            bail!(TimelineError::UninitialinzedPgVersion(ttid));
        }

        if state.commit_lsn < state.local_start_lsn {
            bail!(
                "commit_lsn {} is higher than local_start_lsn {}",
                state.commit_lsn,
                state.local_start_lsn
            );
        }

        let disk_timeline = self.disk.put_state(&ttid, state);
        let control_store = DiskStateStorage::new(disk_timeline.clone());
        let wal_store = DiskWALStorage::new(disk_timeline.clone(), &control_store)?;

        let sk = SafeKeeper::new(control_store, wal_store, self.conf.my_id)?;

        self.timelines.insert(
            ttid.clone(),
            SharedState {
                sk,
                disk: disk_timeline,
            },
        );
        Ok(())
    }

    fn get(&mut self, ttid: &TenantTimelineId) -> &mut SharedState {
        self.timelines.get_mut(ttid).expect("timeline must exist")
    }

    fn has_tli(&self, ttid: &TenantTimelineId) -> bool {
        self.timelines.contains_key(ttid)
    }
}

pub fn run_server(os: NodeOs, disk: Arc<Disk>) -> Result<()> {
    debug!("started server {}", os.id());
    let conf = SafeKeeperConf {
        workdir: PathBuf::from("."),
        my_id: NodeId(os.id() as u64),
        listen_pg_addr: String::new(),
        listen_http_addr: String::new(),
        no_sync: false,
        broker_endpoint: "/".parse::<Uri>().unwrap(),
        broker_keepalive_interval: Duration::from_secs(0),
        heartbeat_timeout: Duration::from_secs(0),
        remote_storage: None,
        max_offloader_lag_bytes: 0,
        backup_runtime_threads: None,
        wal_backup_enabled: false,
        auth: None,
    };

    let mut global = GlobalMap::new(disk, conf.clone())?;
    let mut conns: HashMap<i64, ConnState> = HashMap::new();

    let epoll = os.epoll();
    loop {
        // waiting for the next message
        let mut next_event = Some(epoll.recv());

        loop {
            let event = match next_event {
                Some(event) => event,
                None => break,
            };

            match event {
                NodeEvent::Accept(tcp) => {
                    conns.insert(
                        tcp.id(),
                        ConnState {
                            tcp,
                            greeting: false,
                            ttid: TenantTimelineId::empty(),
                            flush_pending: false,
                        },
                    );
                }
                NodeEvent::Message((msg, tcp)) => {
                    let conn = conns.get_mut(&tcp.id());
                    if let Some(conn) = conn {
                        let res = conn.process_any(msg, &mut global);
                        if res.is_err() {
                            debug!("conn {:?} error: {:#}", tcp, res.unwrap_err());
                            conns.remove(&tcp.id());
                        }
                    } else {
                        debug!("conn {:?} was closed, dropping msg {:?}", tcp, msg);
                    }
                }
                NodeEvent::Internal(_) => {}
                NodeEvent::Closed(_) => {}
                NodeEvent::WakeTimeout(_) => {}
            }

            // TODO: make simulator support multiple events per tick
            next_event = epoll.try_recv();
        }

        conns.retain(|_, conn| {
            let res = conn.flush(&mut global);
            if res.is_err() {
                debug!("conn {:?} error: {:?}", conn.tcp, res);
            }
            res.is_ok()
        });
    }
}

impl ConnState {
    fn process_any(&mut self, any: AnyMessage, global: &mut GlobalMap) -> Result<()> {
        if let AnyMessage::Bytes(copy_data) = any {
            let repl_prefix = b"START_REPLICATION ";
            if !self.greeting && copy_data.starts_with(repl_prefix) {
                self.process_start_replication(copy_data.slice(repl_prefix.len()..), global)?;
                bail!("finished processing START_REPLICATION")
            }

            let msg = ProposerAcceptorMessage::parse(copy_data)?;
            debug!("got msg: {:?}", msg);
            return self.process(msg, global);
        } else {
            bail!("unexpected message, expected AnyMessage::Bytes");
        }
    }

    fn process_start_replication(
        &mut self,
        copy_data: Bytes,
        global: &mut GlobalMap,
    ) -> Result<()> {
        // format is "<tenant_id> <timeline_id> <start_lsn> <end_lsn>"
        let str = String::from_utf8(copy_data.to_vec())?;

        let mut parts = str.split(' ');
        let tenant_id = parts.next().unwrap().parse::<TenantId>()?;
        let timeline_id = parts.next().unwrap().parse::<TimelineId>()?;
        let start_lsn = parts.next().unwrap().parse::<u64>()?;
        let end_lsn = parts.next().unwrap().parse::<u64>()?;

        let ttid = TenantTimelineId::new(tenant_id, timeline_id);
        let shared_state = global.get(&ttid);

        // read bytes from start_lsn to end_lsn
        let mut buf = vec![0; (end_lsn - start_lsn) as usize];
        shared_state.disk.wal.lock().read(start_lsn, &mut buf);

        // send bytes to the client
        self.tcp.send(AnyMessage::Bytes(Bytes::from(buf)));
        Ok(())
    }

    fn init_timeline(
        &mut self,
        ttid: TenantTimelineId,
        server_info: ServerInfo,
        global: &mut GlobalMap,
    ) -> Result<()> {
        self.ttid = ttid;
        if global.has_tli(&ttid) {
            return Ok(());
        }

        global.create(ttid, server_info)
    }

    fn process(&mut self, msg: ProposerAcceptorMessage, global: &mut GlobalMap) -> Result<()> {
        if !self.greeting {
            self.greeting = true;

            match msg {
                ProposerAcceptorMessage::Greeting(ref greeting) => {
                    debug!(
                        "start handshake with walproposer {:?}",
                        self.tcp,
                    );
                    let server_info = ServerInfo {
                        pg_version: greeting.pg_version,
                        system_id: greeting.system_id,
                        wal_seg_size: greeting.wal_seg_size,
                    };
                    let ttid = TenantTimelineId::new(greeting.tenant_id, greeting.timeline_id);
                    self.init_timeline(ttid, server_info, global)?
                }
                _ => {
                    bail!("unexpected message {msg:?} instead of greeting");
                }
            }
        }

        let tli = global.get(&self.ttid);

        match msg {
            ProposerAcceptorMessage::AppendRequest(append_request) => {
                self.flush_pending = true;
                self.process_sk_msg(
                    tli,
                    &ProposerAcceptorMessage::NoFlushAppendRequest(append_request),
                )?;
            }
            other => {
                self.process_sk_msg(tli, &other)?;
            }
        }

        Ok(())
    }

    /// Process FlushWAL if needed.
    // TODO: add extra flushes, to verify that extra flushes don't break anything
    fn flush(&mut self, global: &mut GlobalMap) -> Result<()> {
        if !self.flush_pending {
            return Ok(());
        }
        self.flush_pending = false;
        let shared_state = global.get(&self.ttid);
        self.process_sk_msg(shared_state, &ProposerAcceptorMessage::FlushWAL)
    }

    /// Make safekeeper process a message and send a reply to the TCP
    fn process_sk_msg(
        &mut self,
        shared_state: &mut SharedState,
        msg: &ProposerAcceptorMessage,
    ) -> Result<()> {
        let mut reply = shared_state.sk.process_msg(msg)?;
        if let Some(reply) = &mut reply {
            // // if this is AppendResponse, fill in proper hot standby feedback and disk consistent lsn
            // if let AcceptorProposerMessage::AppendResponse(ref mut resp) = reply {
            //     // TODO:
            // }

            let mut buf = BytesMut::with_capacity(128);
            reply.serialize(&mut buf)?;

            self.tcp.send(AnyMessage::Bytes(buf.into()));
        }
        Ok(())
    }
}

impl Drop for ConnState {
    fn drop(&mut self) {
        debug!("dropping conn: {:?}", self.tcp);
        if !std::thread::panicking() {
            self.tcp.close();
        }
        // TODO: clean up non-fsynced WAL
    }
}