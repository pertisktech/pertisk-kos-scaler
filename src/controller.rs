use crate::{config::Config, mgmt::{MgmtClient, Node as MgmtNode}};
use anyhow::{Context, Result};
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{Api, ResourceExt, api::{ListParams, Patch, PatchParams}};
use std::{collections::{HashMap, HashSet}, sync::Arc, time::Instant};
use tokio::time::interval;
use tracing::{info, warn};

pub struct Autoscaler {
    kube: kube::Client,
    mgmt: MgmtClient,
    config: Config,
    last_up: tokio::sync::Mutex<Option<Instant>>,
    last_down: tokio::sync::Mutex<Option<Instant>>,
    draining: tokio::sync::Mutex<HashSet<String>>,
}

#[derive(Default)]
struct ClusterLoad {
    pending_pods: i32,
    max_utilization: f64,
    node_utilization: HashMap<String, f64>,
    evictable_pods: HashMap<String, i32>,
}

impl Autoscaler {
    pub fn new(kube: kube::Client, mgmt: MgmtClient, config: Config) -> Arc<Self> {
        Arc::new(Self {
            kube, mgmt, config,
            last_up: tokio::sync::Mutex::new(None),
            last_down: tokio::sync::Mutex::new(None),
            draining: tokio::sync::Mutex::new(HashSet::new()),
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let mut ticker = interval(self.config.sync_interval);
        loop {
            ticker.tick().await;
            if let Err(error) = self.reconcile().await {
                warn!(%error, "reconciliation failed");
            }
        }
    }

    async fn reconcile(&self) -> Result<()> {
        if self.config.disable_during_jobs && self.mgmt.has_active_lifecycle_job().await? {
            info!("Pertisk management lifecycle job is active; skipping reconciliation");
            return Ok(());
        }
        let load = self.cluster_load().await?;
        let workers = self.mgmt.workers().await.context("listing Pertisk workers")?;
        let current = workers.len() as i32;
        let ready = workers.iter().filter(|worker| worker.is_ready()).count() as i32;
        let pool = &self.config.worker_pool;

        if current > ready {
            info!(current, ready, "worker pool is still converging; skipping scale actions");
            return Ok(());
        }

        if current < pool.min_size {
            self.scale_up(pool.min_size - current, "worker count below minimum").await?;
            return Ok(());
        }

        let needs_capacity = load.pending_pods >= pool.scale_up_pending_pods
            || load.max_utilization >= pool.scale_up_threshold;
        if needs_capacity && current < pool.max_size && self.can_scale_up().await {
            let requested = if load.pending_pods > 0 {
                (load.pending_pods + pool.scale_up_pending_pods - 1) / pool.scale_up_pending_pods
            } else {
                ((current as f64 * load.max_utilization / pool.target_utilization).ceil() as i32 - current).max(1)
            };
            let count = requested.min(pool.max_size - current);
            self.scale_up(count, "pending pods or request utilization requires capacity").await?;
            return Ok(());
        }

        if current <= pool.min_size || load.pending_pods > 0
            || load.max_utilization >= pool.scale_down_threshold || !self.can_scale_down().await {
            return Ok(());
        }

        self.try_scale_down(&workers, &load).await
    }

    async fn scale_up(&self, count: i32, reason: &str) -> Result<()> {
        if count <= 0 {
            return Ok(());
        }
        info!(count, %reason, "requesting worker scale-up");
        let pool = &self.config.worker_pool;
        self.mgmt.add_workers(count, pool.memory, pool.cores, pool.disk_gb).await?;
        *self.last_up.lock().await = Some(Instant::now());
        Ok(())
    }

    async fn try_scale_down(&self, workers: &[MgmtNode], load: &ClusterLoad) -> Result<()> {
        let Some((node_name, utilization)) = load.node_utilization.iter()
            .filter(|(name, _)| load.evictable_pods.get(*name).copied().unwrap_or(0) == 0)
            .min_by(|(_, left), (_, right)| left.total_cmp(right)) else {
                return Ok(());
            };

        // A node is only removed after it is unschedulable and has no evictable pods.
        // This deliberately avoids forcing eviction or bypassing PodDisruptionBudgets.
        let candidate = workers.iter().find(|worker| worker.name == *node_name);
        let Some(candidate) = candidate else {
            warn!(node = %node_name, "Kubernetes worker is not yet mapped to pertisk-mgmt inventory");
            return Ok(());
        };
        if !candidate.is_ready() {
            return Ok(());
        }
        let capacity_after_removal = 1.0 - self.config.worker_pool.safe_to_evict_buffer;
        if *utilization > capacity_after_removal {
            return Ok(());
        }
        if !self.draining.lock().await.contains(node_name) {
            self.cordon(node_name).await?;
            self.draining.lock().await.insert(node_name.clone());
            info!(node = %node_name, "cordoned empty worker; removal will occur next reconciliation");
            return Ok(());
        }
        info!(node = %node_name, "requesting worker removal");
        self.mgmt.remove_worker(&candidate.id).await?;
        self.draining.lock().await.remove(node_name);
        *self.last_down.lock().await = Some(Instant::now());
        Ok(())
    }

    async fn can_scale_up(&self) -> bool {
        self.last_up.lock().await.map(|time| time.elapsed() >= self.config.cooldowns.scale_up).unwrap_or(true)
    }

    async fn can_scale_down(&self) -> bool {
        let last_up = *self.last_up.lock().await;
        let last_down = *self.last_down.lock().await;
        last_up.map(|time| time.elapsed() >= self.config.cooldowns.scale_down).unwrap_or(true)
            && last_down.map(|time| time.elapsed() >= self.config.cooldowns.scale_down).unwrap_or(true)
    }

    async fn cordon(&self, name: &str) -> Result<()> {
        let nodes: Api<Node> = Api::all(self.kube.clone());
        nodes.patch(name, &PatchParams::default(), &Patch::Merge(
            serde_json::json!({"spec": {"unschedulable": true}}),
        )).await.context("cordoning worker")?;
        Ok(())
    }

    async fn cluster_load(&self) -> Result<ClusterLoad> {
        let nodes: Api<Node> = Api::all(self.kube.clone());
        let pods: Api<Pod> = Api::all(self.kube.clone());
        let workers: Vec<Node> = nodes.list(&ListParams::default()).await?
            .items.into_iter().filter(|node| !is_control_plane(node)).collect();
        let capacity: HashMap<String, (i64, i64)> = workers.iter().filter_map(|node| {
            let alloc = node.status.as_ref()?.allocatable.as_ref()?;
            Some((node.name_any(), (
                alloc.get("cpu").map(quantity_cpu).unwrap_or(0),
                alloc.get("memory").map(quantity_memory).unwrap_or(0),
            )))
        }).collect();
        let mut cpu: HashMap<String, i64> = HashMap::new();
        let mut memory: HashMap<String, i64> = HashMap::new();
        let mut evictable: HashMap<String, i32> = HashMap::new();
        let mut pending = 0;
        for pod in pods.list(&ListParams::default()).await?.items {
            if self.config.ignored_namespaces.iter().any(|namespace| pod.namespace().as_deref() == Some(namespace)) {
                continue;
            }
            if is_unschedulable_pending(&pod) {
                pending += 1;
            }
            let Some(node) = pod.spec.as_ref().and_then(|spec| spec.node_name.clone()) else { continue };
            if !capacity.contains_key(&node) { continue; }
            if pod.status.as_ref().and_then(|status| status.phase.as_deref()) == Some("Running") {
                let request = pod.spec.as_ref().map(pod_requests).unwrap_or_default();
                *cpu.entry(node.clone()).or_default() += request.0;
                *memory.entry(node.clone()).or_default() += request.1;
                if !is_daemon_set(&pod) { *evictable.entry(node).or_default() += 1; }
            }
        }
        let mut result = ClusterLoad { pending_pods: pending, evictable_pods: evictable, ..Default::default() };
        for (name, (cpu_capacity, memory_capacity)) in capacity {
            let utilization = (cpu.get(&name).copied().unwrap_or(0) as f64 / cpu_capacity.max(1) as f64)
                .max(memory.get(&name).copied().unwrap_or(0) as f64 / memory_capacity.max(1) as f64);
            result.max_utilization = result.max_utilization.max(utilization);
            result.node_utilization.insert(name, utilization);
        }
        Ok(result)
    }
}

fn is_control_plane(node: &Node) -> bool {
    node.metadata.labels.as_ref().is_some_and(|labels|
        labels.contains_key("node-role.kubernetes.io/control-plane") || labels.contains_key("node-role.kubernetes.io/master"))
}
fn is_daemon_set(pod: &Pod) -> bool {
    pod.metadata.owner_references.as_ref().is_some_and(|owners| owners.iter().any(|owner| owner.kind == "DaemonSet"))
}
fn is_unschedulable_pending(pod: &Pod) -> bool {
    pod.status.as_ref().is_some_and(|status| {
        status.phase.as_deref() == Some("Pending")
            && status.conditions.as_ref().is_some_and(|conditions| conditions.iter().any(|condition|
                condition.type_ == "PodScheduled"
                    && condition.status == "False"
                    && condition.reason.as_deref() == Some("Unschedulable")))
    })
}
fn pod_requests(pod: &k8s_openapi::api::core::v1::PodSpec) -> (i64, i64) {
    pod.containers.iter().fold((0, 0), |(cpu, memory), container| {
        let requests = container.resources.as_ref().and_then(|resources| resources.requests.as_ref());
        (cpu + requests.and_then(|values| values.get("cpu")).map(quantity_cpu).unwrap_or(0),
         memory + requests.and_then(|values| values.get("memory")).map(quantity_memory).unwrap_or(0))
    })
}
fn quantity_cpu(quantity: &k8s_openapi::apimachinery::pkg::api::resource::Quantity) -> i64 {
    let raw = quantity.0.trim();
    raw.strip_suffix('m').and_then(|value| value.parse().ok())
        .or_else(|| raw.parse::<f64>().ok().map(|value| (value * 1000.0) as i64)).unwrap_or(0)
}
fn quantity_memory(quantity: &k8s_openapi::apimachinery::pkg::api::resource::Quantity) -> i64 {
    let raw = quantity.0.trim();
    let units = [("Ki", 1024_f64), ("Mi", 1024_f64.powi(2)), ("Gi", 1024_f64.powi(3)),
        ("Ti", 1024_f64.powi(4)), ("K", 1_000_f64), ("M", 1_000_000_f64), ("G", 1_000_000_000_f64)];
    units.iter().find_map(|(unit, multiplier)| raw.strip_suffix(unit).and_then(|value| value.parse::<f64>().ok().map(|value| (value * multiplier) as i64)))
        .or_else(|| raw.parse().ok()).unwrap_or(0)
}
