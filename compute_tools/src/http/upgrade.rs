use std::{path::Path, sync::Arc};

use compute_api::{requests::UpgradeRequest, responses::ComputeStatus};
use hyper::{Body, Request, StatusCode};

use crate::compute::ComputeNode;

use anyhow::{anyhow, Error, Result};

const PG_UPGRADE: &str = "pg_upgrade";

fn _upgrade(compute: &Arc<ComputeNode>, _pg_version: &str) {
    let _pg_upgrade = Path::new(&compute.pgbin).join(PG_UPGRADE);
}

pub async fn handle(
    req: Request<Body>,
    compute: &Arc<ComputeNode>,
) -> Result<(), (Error, StatusCode)> {
    let body_bytes = hyper::body::to_bytes(req.into_body()).await.unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    let body = match serde_json::from_str::<UpgradeRequest>(&body_str) {
        Ok(r) => r,
        Err(e) => return Err((Into::into(e), StatusCode::BAD_REQUEST)),
    };

    // No sense in trying to upgrade to the same version.
    let curr_version = compute.pgversion.clone();
    let new_version = body.pg_version;
    if curr_version == new_version {
        return Err((
            anyhow!("cannot upgrade endpoint to the same version"),
            StatusCode::UNPROCESSABLE_ENTITY,
        ));
    }

    // Check that we are in the running state before trying to upgrade.
    if compute.get_status() != ComputeStatus::Running {
        return Err((
            anyhow!("expected compute to be in running state"),
            StatusCode::CONFLICT,
        ));
    }

    compute.set_status(ComputeStatus::Upgrading);

    let c = compute.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

        c.set_status(ComputeStatus::Running);
    });

    Ok(())
}
