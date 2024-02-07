use std::str::FromStr;

use anyhow::Context;
use clap::{Parser, Subcommand};
use hyper::{Method, StatusCode};
use pageserver_api::{
    models::{ShardParameters, TenantConfig, TenantCreateRequest},
    shard::TenantShardId,
};
use pageserver_client::mgmt_api::{self, ResponseErrorMessageExt};
use reqwest::Url;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use utils::id::{NodeId, TenantId};

// TODO: de-duplicate struct definitions

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub enum NodeAvailability {
    // Normal, happy state
    Active,
    // Offline: Tenants shouldn't try to attach here, but they may assume that their
    // secondary locations on this node still exist.  Newly added nodes are in this
    // state until we successfully contact them.
    Offline,
}

impl FromStr for NodeAvailability {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "offline" => Ok(Self::Offline),
            _ => Err(anyhow::anyhow!("Unknown availability state '{s}'")),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub enum NodeSchedulingPolicy {
    Active,
    Filling,
    Pause,
    Draining,
}

impl FromStr for NodeSchedulingPolicy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "filling" => Ok(Self::Filling),
            "pause" => Ok(Self::Pause),
            "draining" => Ok(Self::Draining),
            _ => Err(anyhow::anyhow!("Unknown scheduling state '{s}'")),
        }
    }
}

impl From<NodeSchedulingPolicy> for String {
    fn from(value: NodeSchedulingPolicy) -> String {
        use NodeSchedulingPolicy::*;
        match value {
            Active => "active",
            Filling => "filling",
            Pause => "pause",
            Draining => "draining",
        }
        .to_string()
    }
}

#[derive(Serialize, Deserialize)]
pub struct NodeRegisterRequest {
    pub node_id: NodeId,

    pub listen_pg_addr: String,
    pub listen_pg_port: u16,

    pub listen_http_addr: String,
    pub listen_http_port: u16,
}

#[derive(Serialize, Deserialize)]
pub struct NodeConfigureRequest {
    pub node_id: NodeId,

    pub availability: Option<NodeAvailability>,
    pub scheduling: Option<NodeSchedulingPolicy>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct NodePersistence {
    pub(crate) node_id: i64,
    pub(crate) scheduling_policy: String,
    pub(crate) listen_http_addr: String,
    pub(crate) listen_http_port: i32,
    pub(crate) listen_pg_addr: String,
    pub(crate) listen_pg_port: i32,
}

#[derive(Subcommand, Debug)]
enum Command {
    NodeRegister {
        #[arg(long)]
        node_id: NodeId,

        #[arg(long)]
        listen_pg_addr: String,
        #[arg(long)]
        listen_pg_port: u16,

        #[arg(long)]
        listen_http_addr: String,
        #[arg(long)]
        listen_http_port: u16,
    },
    NodeConfigure {
        #[arg(long)]
        node_id: NodeId,

        #[arg(long)]
        availability: Option<NodeAvailability>,
        #[arg(long)]
        scheduling: Option<NodeSchedulingPolicy>,
    },
    Nodes {},
    TenantCreate {
        #[arg(long)]
        tenant_id: TenantId,
    },
    TenantDelete {
        #[arg(long)]
        tenant_id: TenantId,
    },
    ServiceRegister {
        #[arg(long)]
        http_host: String,
        #[arg(long)]
        http_port: u16,
        #[arg(long)]
        region_id: String,
        #[arg(long)]
        availability_zone_id: String,
        // {
        //   "version": 1,
        //   "host": "${HOST}",
        //   "port": 6400,
        //   "region_id": "{{ console_region_id }}",
        //   "instance_id": "${INSTANCE_ID}",
        //   "http_host": "${HOST}",
        //   "http_port": 9898,
        //   "active": false,
        //   "availability_zone_id": "${AZ_ID}",
        //   "disk_size": ${DISK_SIZE},
        //   "instance_type": "${INSTANCE_TYPE}",
        //   "register_reason" : "New pageserver"
        // }
    },
}

/// Request format for control plane POST /management/api/v2/pageservers
///
/// This is how physical pageservers register, but in this context it is how we
/// register the storage controller with the control plane, as a "virtual pageserver"
#[derive(Serialize, Deserialize, Debug)]
struct ServiceRegisterRequest {
    version: u16,
    host: String,
    port: u16,
    /// This is the **Neon** region ID, which looks something like `aws-eu-west-1`
    region_id: String,
    /// This expects an EC2 instance ID, for bare metal pageservers.  But it can be any unique identifier.
    instance_id: String,
    http_host: String,
    http_port: u16,
    /// This is an EC2 AZ name (the ZoneName, not actually the AZ ID).  e.g. eu-west-1b
    availability_zone_id: String,
    disk_size: u64,
    /// EC2 instance type.  If it doesn't make sense, leave it blank.
    instance_type: String,
    /// Freeform memo describing the request
    register_reason: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ServiceRegisterResponse {
    // This is a partial representation of the management API's swagger `Pageserver` type.  Unused
    // fields are ignored.
    id: u64,
    node_id: u64,
    instance_id: String,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    #[arg(long)]
    api: Url,

    #[arg(long)]
    jwt: Option<String>,

    #[command(subcommand)]
    command: Command,
}

struct Client {
    base_url: Url,
    jwt_token: Option<String>,
    client: reqwest::Client,
}

impl Client {
    fn new(base_url: Url, jwt_token: Option<String>) -> Self {
        Self {
            base_url,
            jwt_token,
            client: reqwest::ClientBuilder::new()
                .build()
                .expect("Failed to construct http client"),
        }
    }

    /// Simple HTTP request wrapper for calling into attachment service
    async fn dispatch<RQ, RS>(
        &self,
        method: hyper::Method,
        path: String,
        body: Option<RQ>,
    ) -> mgmt_api::Result<RS>
    where
        RQ: Serialize + Sized,
        RS: DeserializeOwned + Sized,
    {
        // The configured URL has the /upcall path prefix for pageservers to use: we will strip that out
        // for general purpose API access.
        let url = Url::from_str(&format!(
            "http://{}:{}/{path}",
            self.base_url.host_str().unwrap(),
            self.base_url.port().unwrap()
        ))
        .unwrap();

        let mut builder = self.client.request(method, url);
        if let Some(body) = body {
            builder = builder.json(&body)
        }
        if let Some(jwt_token) = &self.jwt_token {
            builder = builder.header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {jwt_token}"),
            );
        }

        let response = builder.send().await.map_err(mgmt_api::Error::ReceiveBody)?;
        let response = response.error_from_body().await?;

        Ok(response
            .json()
            .await
            .map_err(pageserver_client::mgmt_api::Error::ReceiveBody)?)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let storcon_client = Client::new(cli.api.clone(), cli.jwt.clone());

    let mut trimmed = cli.api.to_string();
    trimmed.pop();
    let vps_client = mgmt_api::Client::new(trimmed, cli.jwt.as_ref().map(|s| s.as_str()));

    match cli.command {
        Command::NodeRegister {
            node_id,
            listen_pg_addr,
            listen_pg_port,
            listen_http_addr,
            listen_http_port,
        } => {
            storcon_client
                .dispatch::<_, ()>(
                    Method::POST,
                    "control/v1/node".to_string(),
                    Some(NodeRegisterRequest {
                        node_id,
                        listen_pg_addr,
                        listen_pg_port,
                        listen_http_addr,
                        listen_http_port,
                    }),
                )
                .await?;
        }
        Command::TenantCreate { tenant_id } => {
            vps_client
                .tenant_create(&TenantCreateRequest {
                    new_tenant_id: TenantShardId::unsharded(tenant_id),
                    generation: None,
                    shard_parameters: ShardParameters::default(),
                    config: TenantConfig::default(),
                })
                .await?;
        }
        Command::TenantDelete { tenant_id } => {
            let status = vps_client
                .tenant_delete(TenantShardId::unsharded(tenant_id))
                .await?;
            tracing::info!("Delete status: {}", status);
        }
        Command::Nodes {} => {
            let resp = storcon_client
                .dispatch::<(), Vec<NodePersistence>>(
                    Method::GET,
                    "control/v1/node".to_string(),
                    None,
                )
                .await?;
            println!("{}", serde_json::to_string(&resp)?);
        }
        Command::NodeConfigure {
            node_id,
            availability,
            scheduling,
        } => {
            let req = NodeConfigureRequest {
                node_id,
                availability,
                scheduling,
            };
            storcon_client
                .dispatch::<_, ()>(
                    Method::PUT,
                    format!("control/v1/node/{node_id}/config"),
                    Some(req),
                )
                .await?;
        }
        Command::ServiceRegister {
            http_host,
            http_port,
            region_id,
            availability_zone_id,
        } => {
            let instance_id = http_host.clone();
            let req = ServiceRegisterRequest {
                instance_id: instance_id.clone(),
                // We do not expose postgres protocol, but provide a valid-looking host/port for it
                host: http_host.clone(),
                port: 6400,
                http_host: http_host.clone(),
                http_port,
                version: 1,
                region_id,
                availability_zone_id: availability_zone_id,
                disk_size: 0,
                instance_type: "".to_string(),
                register_reason: "Storage Controller Virtual Pageserver".to_string(),
            };

            // curl -sf \
            //   -X POST \
            //   -H "Content-Type: application/json" \
            //   -H "Authorization: Bearer {{ controlplane_token.for_deploy | default('') }}" \
            //   {{ console_mgmt_base_url }}/management/api/v2/pageservers \
            //   -d@/tmp/payload \

            // let existing_pageserver = storcon_client
            //     .dispatch::<(), ServiceRegisterResponse>(
            //         Method::GET,
            //         format!("/management/api/v2/pageservers/{instance_id}"),
            //         None,
            //     )
            //     .await;
            // match existing_pageserver {
            //     Ok(existing) => {
            //         eprintln!(
            //             "Already registered {} with id={} node_id=>{}",
            //             instance_id, existing.id, existing.node_id
            //         );
            //         return Ok(());
            //     }
            //     Err(mgmt_api::Error::ApiError(StatusCode::NOT_FOUND, _)) => {
            //         eprintln!("Not already registered, registering now...");
            //     }
            //     Err(e) => return Err(e.into()),
            // }

            eprintln!("Body:\n{}\n", serde_json::to_string(&req).unwrap());

            let response = storcon_client
                .dispatch::<ServiceRegisterRequest, ServiceRegisterResponse>(
                    Method::POST,
                    "management/api/v2/pageservers".to_string(),
                    Some(req),
                )
                .await?;
            eprintln!(
                "Registered {} as id={} node_id={}",
                http_host, response.id, response.node_id
            );
        }
    }

    Ok(())
}
