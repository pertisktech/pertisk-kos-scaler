mod config;
mod controller;
mod mgmt;

use anyhow::{Context, Result};
use clap::Parser;
use config::Config;
use controller::Autoscaler;
use kube::config::{KubeConfigOptions, Kubeconfig};
use mgmt::MgmtClient;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(name = "kos-scaler", about = "Kubernetes worker-node autoscaler for Pertisk KOS")]
struct Args {
    #[arg(short, long, default_value = "/etc/kos-scaler/config.yaml")]
    config: String,
    #[arg(short, long, default_value = "info", env = "LOG_LEVEL")]
    log_level: String,
    #[arg(long, env = "KUBECONFIG")]
    kubeconfig: Option<String>,
    #[arg(long, env = "KUBE_CONTEXT")]
    kube_context: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level)))
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    let config = Config::load(&args.config)?;
    let kube = create_kube_client(&args).await?;
    let mut mgmt = MgmtClient::new(&config.mgmt_endpoint, &config.cluster_id)?;
    mgmt.authenticate().await?;
    info!(cluster_id = %config.cluster_id, endpoint = %config.mgmt_endpoint, "starting Pertisk KOS scaler");
    Autoscaler::new(kube, mgmt, config).run().await
}

async fn create_kube_client(args: &Args) -> Result<kube::Client> {
    if args.kubeconfig.is_none() && args.kube_context.is_none() {
        return kube::Client::try_default().await.context("creating Kubernetes client");
    }
    let options = KubeConfigOptions {
        context: args.kube_context.clone(),
        cluster: None,
        user: None,
    };
    let config = match &args.kubeconfig {
        Some(path) => {
            let kubeconfig = Kubeconfig::read_from(path)
                .with_context(|| format!("reading kubeconfig {path}"))?;
            kube::Config::from_custom_kubeconfig(kubeconfig, &options).await?
        }
        None => kube::Config::from_kubeconfig(&options).await?,
    };
    kube::Client::try_from(config).context("creating Kubernetes client")
}
