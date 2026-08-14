use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::{fs, path::Path, time::Duration};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    /// Base URL of pertisk-mgmt, for example https://mgmt.example.com:8080.
    pub mgmt_endpoint: String,
    /// Pertisk management cluster UUID.
    pub cluster_id: String,
    #[serde(default = "default_sync_interval", with = "humantime_serde")]
    pub sync_interval: Duration,
    #[serde(default = "default_true")]
    pub disable_during_jobs: bool,
    /// How long to wait for PDB-aware drain before removing the worker anyway.
    #[serde(default = "default_drain_timeout", with = "humantime_serde")]
    pub drain_timeout: Duration,
    #[serde(default = "default_ignored_namespaces")]
    pub ignored_namespaces: Vec<String>,
    #[serde(default)]
    pub worker_pool: WorkerPoolConfig,
    #[serde(default)]
    pub cooldowns: CooldownConfig,
    #[serde(default = "default_state_dir")]
    pub state_dir: String,
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_max_events")]
    pub max_events: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPoolConfig {
    #[serde(default = "default_min_size")]
    pub min_size: i32,
    #[serde(default = "default_max_size")]
    pub max_size: i32,
    #[serde(default = "default_pending_pods")]
    pub scale_up_pending_pods: i32,
    #[serde(default = "default_target_utilization")]
    pub target_utilization: f64,
    #[serde(default)]
    pub scale_up_threshold: f64,
    #[serde(default)]
    pub scale_down_threshold: f64,
    #[serde(default = "default_safe_buffer")]
    pub safe_to_evict_buffer: f64,
    pub memory: Option<i64>,
    pub cores: Option<i64>,
    pub disk_gb: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CooldownConfig {
    #[serde(default = "default_up_cooldown", with = "humantime_serde")]
    pub scale_up: Duration,
    #[serde(default = "default_down_cooldown", with = "humantime_serde")]
    pub scale_down: Duration,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self {
            min_size: default_min_size(),
            max_size: default_max_size(),
            scale_up_pending_pods: default_pending_pods(),
            target_utilization: default_target_utilization(),
            scale_up_threshold: 0.0,
            scale_down_threshold: 0.0,
            safe_to_evict_buffer: default_safe_buffer(),
            memory: None,
            cores: None,
            disk_gb: None,
        }
    }
}

impl Default for CooldownConfig {
    fn default() -> Self {
        Self {
            scale_up: default_up_cooldown(),
            scale_down: default_down_cooldown(),
        }
    }
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let contents = fs::read_to_string(path.as_ref())
            .with_context(|| format!("reading configuration {}", path.as_ref().display()))?;
        let mut config: Self =
            serde_yaml::from_str(&contents).context("parsing configuration YAML")?;
        config.mgmt_endpoint = config.mgmt_endpoint.trim_end_matches('/').to_owned();
        if config.mgmt_endpoint.is_empty() || config.cluster_id.trim().is_empty() {
            bail!("mgmtEndpoint and clusterId must be set");
        }
        if config.worker_pool.max_size < config.worker_pool.min_size {
            bail!("workerPool.maxSize must be greater than or equal to workerPool.minSize");
        }
        if config.max_events == 0 {
            config.max_events = default_max_events();
        }
        if config.worker_pool.scale_up_threshold == 0.0 {
            config.worker_pool.scale_up_threshold = config.worker_pool.target_utilization + 0.15;
        }
        if config.worker_pool.scale_down_threshold == 0.0 {
            config.worker_pool.scale_down_threshold =
                (config.worker_pool.target_utilization - 0.15).max(0.0);
        }
        Ok(config)
    }
}

fn default_sync_interval() -> Duration {
    Duration::from_secs(30)
}
fn default_drain_timeout() -> Duration {
    Duration::from_secs(1800)
}
fn default_true() -> bool {
    true
}
fn default_min_size() -> i32 {
    1
}
fn default_max_size() -> i32 {
    10
}
fn default_pending_pods() -> i32 {
    1
}
fn default_target_utilization() -> f64 {
    0.5
}
fn default_safe_buffer() -> f64 {
    0.1
}
fn default_up_cooldown() -> Duration {
    Duration::from_secs(120)
}
fn default_down_cooldown() -> Duration {
    Duration::from_secs(600)
}
fn default_state_dir() -> String {
    "/var/lib/kos-scaler".into()
}
fn default_listen_addr() -> String {
    "0.0.0.0:8080".into()
}
fn default_max_events() -> usize {
    100
}
fn default_ignored_namespaces() -> Vec<String> {
    ["kube-system", "kube-public", "kube-node-lease"]
        .into_iter()
        .map(str::to_owned)
        .collect()
}
