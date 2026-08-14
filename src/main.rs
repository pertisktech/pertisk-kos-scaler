mod api;
mod config;
mod controller;
mod mgmt;
mod state;

use anyhow::{Context, Result};
use clap::Parser;
use config::Config;
use controller::Autoscaler;
use kube::config::{KubeConfigOptions, Kubeconfig};
use mgmt::MgmtClient;
use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(
    name = "kos-scaler",
    about = "Kubernetes worker-node autoscaler for Pertisk KOS"
)]
struct Args {
    #[arg(short, long, default_value = "/etc/kos-scaler/config.yaml")]
    config: String,
    #[arg(short, long, default_value = "info", env = "LOG_LEVEL")]
    log_level: String,
    #[arg(long, env = "KUBECONFIG")]
    kubeconfig: Option<String>,
    #[arg(long, env = "KUBE_CONTEXT")]
    kube_context: Option<String>,
    #[arg(long, env = "HEALTH_ADDR")]
    listen_addr: Option<String>,
    #[arg(long, env = "KOS_SCALER_STATE_DIR")]
    state_dir: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level)))
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    let mut config = Config::load(&args.config)?;
    if let Some(listen_addr) = args.listen_addr.clone() {
        config.listen_addr = listen_addr;
    }
    config.state_dir = resolve_state_dir(args.state_dir.as_deref().unwrap_or(&config.state_dir))?;
    let listen_addr: SocketAddr = config
        .listen_addr
        .parse()
        .context("parsing listen address")?;

    let kube = create_kube_client(&args).await?;
    let mgmt = MgmtClient::new(&config.mgmt_endpoint, &config.cluster_id)?;
    mgmt.authenticate().await?;
    let ready = Arc::new(AtomicBool::new(false));
    let scaler = Autoscaler::new(kube, mgmt, config.clone(), ready);
    scaler.mark_ready();
    info!(
        cluster_id = %config.cluster_id,
        endpoint = %config.mgmt_endpoint,
        state_dir = %config.state_dir,
        listen = %listen_addr,
        "starting Pertisk KOS scaler"
    );
    tokio::select! {
        result = scaler.clone().run() => result,
        result = api::serve(listen_addr, scaler) => result,
    }
}

fn resolve_state_dir(configured: &str) -> Result<String> {
    let primary = PathBuf::from(configured);
    if is_writable_dir(&primary) {
        return Ok(primary.to_string_lossy().into_owned());
    }
    let fallback = PathBuf::from("/tmp/kos-scaler");
    warn!(
        configured = %primary.display(),
        fallback = %fallback.display(),
        "state directory is not writable; using fallback"
    );
    if !is_writable_dir(&fallback) {
        anyhow::bail!(
            "neither {} nor {} is writable",
            primary.display(),
            fallback.display()
        );
    }
    Ok(fallback.to_string_lossy().into_owned())
}

fn is_writable_dir(path: &Path) -> bool {
    if fs::create_dir_all(path).is_err() {
        return false;
    }
    let probe = path.join(".kos-scaler-state-check");
    match fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&probe)
    {
        Ok(_) => {
            let _ = fs::remove_file(probe);
            true
        }
        Err(_) => false,
    }
}

async fn create_kube_client(args: &Args) -> Result<kube::Client> {
    if args.kubeconfig.is_none() && args.kube_context.is_none() {
        return kube::Client::try_default()
            .await
            .context("creating Kubernetes client");
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
