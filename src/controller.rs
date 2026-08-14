use crate::{
    config::Config,
    mgmt::{MgmtClient, Node as MgmtNode},
    state::{
        ClusterSnapshot, ConfigSnapshot, DRAIN_STARTED_ANNOTATION, DRAINING_ANNOTATION,
        DrainSnapshot, NodeSnapshot, PendingPodSnapshot, PersistedState, ScaleDirection,
        ScaleEvent, ScaleResult, StatusSnapshot, WorkerPoolSnapshot, cooldown_elapsed,
        drain_timed_out, parse_drain_started, should_record_deferred, state_file,
    },
};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    Api, ResourceExt,
    api::{DeleteParams, EvictParams, ListParams, Patch, PatchParams},
};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::time::interval;
use tracing::{info, warn};

pub struct Autoscaler {
    kube: kube::Client,
    mgmt: MgmtClient,
    config: Config,
    state: tokio::sync::Mutex<PersistedState>,
    state_path: PathBuf,
    last_deferred: tokio::sync::Mutex<HashMap<String, DateTime<Utc>>>,
    last_error: tokio::sync::Mutex<Option<String>>,
    started_at: DateTime<Utc>,
    ready: Arc<AtomicBool>,
}

#[derive(Default, Clone)]
struct ClusterLoad {
    pending_pods: i32,
    pending: Vec<PendingPodSnapshot>,
    max_utilization: f64,
    cpu_utilization: f64,
    memory_utilization: f64,
    node_utilization: HashMap<String, f64>,
    nodes: Vec<NodeSnapshot>,
    control_plane_count: i32,
}

impl Autoscaler {
    pub fn new(
        kube: kube::Client,
        mgmt: MgmtClient,
        config: Config,
        ready: Arc<AtomicBool>,
    ) -> Arc<Self> {
        let state_path = state_file(&config.state_dir);
        let state = PersistedState::load(&state_path, config.max_events);
        Arc::new(Self {
            kube,
            mgmt,
            config,
            state: tokio::sync::Mutex::new(state),
            state_path,
            last_deferred: tokio::sync::Mutex::new(HashMap::new()),
            last_error: tokio::sync::Mutex::new(None),
            started_at: Utc::now(),
            ready,
        })
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::SeqCst);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }

    pub async fn run(self: Arc<Self>) -> Result<()> {
        let mut ticker = interval(self.config.sync_interval);
        loop {
            ticker.tick().await;
            if let Err(error) = self.reconcile().await {
                let message = error.to_string();
                *self.last_error.lock().await = Some(message.clone());
                warn!(%error, "reconciliation failed");
            } else {
                *self.last_error.lock().await = None;
            }
        }
    }

    async fn reconcile(&self) -> Result<()> {
        self.adopt_annotated_drains().await?;

        if self.config.disable_during_jobs && self.mgmt.has_active_lifecycle_job().await? {
            self.record_deferred(
                "lifecycle-job",
                0,
                0,
                "Pertisk management lifecycle job is active",
            )
            .await;
            info!("Pertisk management lifecycle job is active; skipping reconciliation");
            return Ok(());
        }
        let load = self.cluster_load().await?;
        let workers = self
            .mgmt
            .workers()
            .await
            .context("listing Pertisk workers")?;
        let current = workers.len() as i32;
        let ready = workers.iter().filter(|worker| worker.is_ready()).count() as i32;
        let pool = &self.config.worker_pool;

        if current > ready {
            self.record_deferred(
                "converging",
                current,
                current,
                "worker pool is still converging",
            )
            .await;
            info!(
                current,
                ready, "worker pool is still converging; skipping scale actions"
            );
            return Ok(());
        }

        if current < pool.min_size {
            self.recover_cordoned_workers().await?;
            self.scale_up(
                current,
                pool.min_size - current,
                "worker count below minimum",
            )
            .await?;
            return Ok(());
        }

        let needs_capacity = load.pending_pods >= pool.scale_up_pending_pods
            || load.max_utilization >= pool.scale_up_threshold;
        if needs_capacity && current < pool.max_size {
            if !self.can_scale_up().await {
                self.record_deferred(
                    "scale-up-cooldown",
                    current,
                    current,
                    "scale-up is on cooldown",
                )
                .await;
            } else {
                let requested = if load.pending_pods > 0 {
                    (load.pending_pods + pool.scale_up_pending_pods - 1)
                        / pool.scale_up_pending_pods
                } else {
                    ((current as f64 * load.max_utilization / pool.target_utilization).ceil()
                        as i32
                        - current)
                        .max(1)
                };
                let count = requested.min(pool.max_size - current);
                self.scale_up(
                    current,
                    count,
                    "pending pods or request utilization requires capacity",
                )
                .await?;
                return Ok(());
            }
        }

        if current <= pool.min_size {
            self.recover_cordoned_workers().await?;
            return Ok(());
        }

        if self.complete_in_flight_drain(&workers, current).await? {
            return Ok(());
        }

        if load.pending_pods > 0 {
            self.record_deferred(
                "pending-pods",
                current,
                current,
                "pending pods block scale-down",
            )
            .await;
            return Ok(());
        }
        if load.max_utilization >= pool.scale_down_threshold {
            return Ok(());
        }
        if !self.can_scale_down().await {
            self.record_deferred(
                "scale-down-cooldown",
                current,
                current,
                "scale-down is on cooldown",
            )
            .await;
            return Ok(());
        }

        self.try_scale_down(&workers, &load, current).await
    }

    async fn scale_up(&self, current: i32, count: i32, reason: &str) -> Result<()> {
        if count <= 0 {
            return Ok(());
        }
        info!(count, %reason, "requesting worker scale-up");
        let pool = &self.config.worker_pool;
        match self
            .mgmt
            .add_workers(count, pool.memory, pool.cores, pool.disk_gb)
            .await
        {
            Ok(()) => {
                self.mutate_state(|state| {
                    state.last_scale_up = Some(Utc::now());
                    state.record_event(
                        self.config.max_events,
                        ScaleEvent {
                            time: Utc::now(),
                            pool: "workers".into(),
                            direction: ScaleDirection::Up,
                            from: current,
                            to: current + count,
                            result: ScaleResult::Success,
                            message: reason.into(),
                        },
                    );
                })
                .await;
                Ok(())
            }
            Err(error) => {
                self.mutate_state(|state| {
                    state.record_event(
                        self.config.max_events,
                        ScaleEvent {
                            time: Utc::now(),
                            pool: "workers".into(),
                            direction: ScaleDirection::Up,
                            from: current,
                            to: current + count,
                            result: ScaleResult::Failed,
                            message: error.to_string(),
                        },
                    );
                })
                .await;
                Err(error)
            }
        }
    }

    async fn try_scale_down(
        &self,
        workers: &[MgmtNode],
        load: &ClusterLoad,
        current: i32,
    ) -> Result<()> {
        let draining = {
            let state = self.state.lock().await;
            state.draining.clone()
        };
        let Some((node_name, utilization)) = load
            .node_utilization
            .iter()
            .filter(|(name, _)| !draining.contains_key(*name))
            .min_by(|(_, left), (_, right)| left.total_cmp(right))
            .map(|(name, utilization)| (name.clone(), *utilization))
        else {
            return Ok(());
        };

        let candidate = workers.iter().find(|worker| worker.name == node_name);
        let Some(candidate) = candidate else {
            warn!(node = %node_name, "Kubernetes worker is not yet mapped to pertisk-mgmt inventory");
            return Ok(());
        };
        if !candidate.is_ready() {
            return Ok(());
        }
        let capacity_after_removal = 1.0 - self.config.worker_pool.safe_to_evict_buffer;
        if utilization > capacity_after_removal {
            return Ok(());
        }

        let started = Utc::now();
        self.cordon(&node_name, started).await?;
        self.drain_node(&node_name).await?;
        self.mutate_state(|state| {
            state.draining.insert(node_name.clone(), started);
            state.record_event(
                self.config.max_events,
                ScaleEvent {
                    time: started,
                    pool: "workers".into(),
                    direction: ScaleDirection::Down,
                    from: current,
                    to: current - 1,
                    result: ScaleResult::Success,
                    message: format!("cordoned {node_name} and started PDB-aware drain"),
                },
            );
        })
        .await;
        info!(node = %node_name, "cordoned worker and started PDB-aware drain");
        Ok(())
    }

    async fn complete_in_flight_drain(&self, workers: &[MgmtNode], current: i32) -> Result<bool> {
        let tracked = {
            let state = self.state.lock().await;
            state
                .draining
                .iter()
                .map(|(name, started)| (name.clone(), *started))
                .collect::<Vec<_>>()
        };
        let Some((node_name, started)) = tracked.into_iter().min_by_key(|(_, started)| *started)
        else {
            return Ok(false);
        };

        let drained = self.is_node_drained(&node_name).await?;
        if !drained {
            if drain_timed_out(started, self.config.drain_timeout, Utc::now()) {
                warn!(
                    node = %node_name,
                    timeout = ?self.config.drain_timeout,
                    "drain timeout reached; removing worker while remaining pods may still be terminating"
                );
            } else {
                self.drain_node(&node_name).await?;
                info!(
                    node = %node_name,
                    elapsed = ?Utc::now().signed_duration_since(started),
                    "waiting for PDB-aware drain to finish"
                );
                return Ok(true);
            }
        }

        let Some(candidate) = workers.iter().find(|worker| worker.name == node_name) else {
            warn!(node = %node_name, "draining worker disappeared from pertisk-mgmt inventory");
            let _ = self.clear_drain_annotations(&node_name).await;
            self.mutate_state(|state| {
                state.draining.remove(&node_name);
            })
            .await;
            return Ok(true);
        };

        info!(node = %node_name, "requesting worker removal after drain");
        match self.mgmt.remove_worker(&candidate.id).await {
            Ok(()) => {
                let _ = self.clear_drain_annotations(&node_name).await;
                self.mutate_state(|state| {
                    state.draining.remove(&node_name);
                    state.last_scale_down = Some(Utc::now());
                    state.record_event(
                        self.config.max_events,
                        ScaleEvent {
                            time: Utc::now(),
                            pool: "workers".into(),
                            direction: ScaleDirection::Down,
                            from: current,
                            to: current - 1,
                            result: ScaleResult::Success,
                            message: format!("removed drained worker {node_name}"),
                        },
                    );
                })
                .await;
                Ok(true)
            }
            Err(error) => {
                self.mutate_state(|state| {
                    state.record_event(
                        self.config.max_events,
                        ScaleEvent {
                            time: Utc::now(),
                            pool: "workers".into(),
                            direction: ScaleDirection::Down,
                            from: current,
                            to: current - 1,
                            result: ScaleResult::Failed,
                            message: error.to_string(),
                        },
                    );
                })
                .await;
                Err(error)
            }
        }
    }

    async fn adopt_annotated_drains(&self) -> Result<()> {
        let nodes: Api<Node> = Api::all(self.kube.clone());
        let listed = nodes.list(&ListParams::default()).await?;
        let mut live = HashSet::new();
        self.mutate_state(|state| {
            for node in listed.items {
                if is_control_plane(&node) {
                    continue;
                }
                let name = node.name_any();
                live.insert(name.clone());
                let unschedulable = node.spec.as_ref().and_then(|spec| spec.unschedulable).unwrap_or(false);
                if let Some(started) = parse_drain_started(node.metadata.annotations.as_ref()) {
                    if state.draining.insert(name.clone(), started).is_none() {
                        info!(node = %name, started_at = %started, "rehydrated drain from node annotation");
                    }
                } else if !unschedulable {
                    state.draining.remove(&name);
                }
            }
            state.draining.retain(|name, _| live.contains(name));
        }).await;
        Ok(())
    }

    async fn recover_cordoned_workers(&self) -> Result<()> {
        let nodes: Api<Node> = Api::all(self.kube.clone());
        for node in nodes.list(&ListParams::default()).await?.items {
            if is_control_plane(&node) {
                continue;
            }
            let name = node.name_any();
            let unschedulable = node
                .spec
                .as_ref()
                .and_then(|spec| spec.unschedulable)
                .unwrap_or(false);
            if !unschedulable {
                continue;
            }
            info!(node = %name, "uncordoning worker because pool is at or below min size");
            self.uncordon(&name).await?;
            self.mutate_state(|state| {
                state.draining.remove(&name);
            })
            .await;
        }
        Ok(())
    }

    async fn can_scale_up(&self) -> bool {
        let state = self.state.lock().await;
        cooldown_elapsed(
            state.last_scale_up,
            self.config.cooldowns.scale_up,
            Utc::now(),
        )
    }

    async fn can_scale_down(&self) -> bool {
        let state = self.state.lock().await;
        cooldown_elapsed(
            state.last_scale_up,
            self.config.cooldowns.scale_down,
            Utc::now(),
        ) && cooldown_elapsed(
            state.last_scale_down,
            self.config.cooldowns.scale_down,
            Utc::now(),
        )
    }

    async fn cordon(&self, name: &str, started: DateTime<Utc>) -> Result<()> {
        let nodes: Api<Node> = Api::all(self.kube.clone());
        nodes
            .patch(
                name,
                &PatchParams::default(),
                &Patch::Merge(serde_json::json!({
                    "metadata": {
                        "annotations": {
                            DRAINING_ANNOTATION: "true",
                            DRAIN_STARTED_ANNOTATION: started.to_rfc3339(),
                        }
                    },
                    "spec": { "unschedulable": true }
                })),
            )
            .await
            .context("cordoning worker")?;
        Ok(())
    }

    async fn uncordon(&self, name: &str) -> Result<()> {
        let nodes: Api<Node> = Api::all(self.kube.clone());
        nodes
            .patch(
                name,
                &PatchParams::default(),
                &Patch::Merge(serde_json::json!({
                    "metadata": {
                        "annotations": {
                            DRAINING_ANNOTATION: serde_json::Value::Null,
                            DRAIN_STARTED_ANNOTATION: serde_json::Value::Null,
                        }
                    },
                    "spec": { "unschedulable": false }
                })),
            )
            .await
            .context("uncordoning worker")?;
        Ok(())
    }

    async fn clear_drain_annotations(&self, name: &str) -> Result<()> {
        let nodes: Api<Node> = Api::all(self.kube.clone());
        if nodes.get(name).await.is_err() {
            return Ok(());
        }
        nodes
            .patch(
                name,
                &PatchParams::default(),
                &Patch::Merge(serde_json::json!({
                    "metadata": {
                        "annotations": {
                            DRAINING_ANNOTATION: serde_json::Value::Null,
                            DRAIN_STARTED_ANNOTATION: serde_json::Value::Null,
                        }
                    }
                })),
            )
            .await
            .context("clearing drain annotations")?;
        Ok(())
    }

    async fn drain_node(&self, node_name: &str) -> Result<()> {
        let pods: Api<Pod> = Api::all(self.kube.clone());
        let list = pods
            .list(&ListParams::default().fields(&format!("spec.nodeName={node_name}")))
            .await
            .context("listing pods for drain")?;
        for pod in list {
            if should_skip_drain(&pod) {
                continue;
            }
            let name = pod.name_any();
            let namespace = pod.namespace().unwrap_or_else(|| "default".into());
            let params = EvictParams {
                delete_options: Some(DeleteParams {
                    grace_period_seconds: Some(30),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let namespaced: Api<Pod> = Api::namespaced(self.kube.clone(), &namespace);
            if let Err(error) = namespaced.evict(&name, &params).await {
                warn!(pod = %name, %namespace, %error, "pod eviction deferred (likely PDB or API conflict)");
            }
        }
        Ok(())
    }

    async fn is_node_drained(&self, node_name: &str) -> Result<bool> {
        let pods: Api<Pod> = Api::all(self.kube.clone());
        let list = pods
            .list(&ListParams::default().fields(&format!("spec.nodeName={node_name}")))
            .await
            .context("listing pods while checking drain")?;
        Ok(list.items.iter().all(should_skip_drain))
    }

    pub async fn dashboard_snapshot(&self) -> Result<StatusSnapshot> {
        let load = self.cluster_load().await?;
        let workers = self.mgmt.workers().await.unwrap_or_default();
        let state = self.state.lock().await;
        let error = self.last_error.lock().await.clone();
        let now = Utc::now();
        let mut errors = Vec::new();
        if let Some(error) = error {
            errors.push(error);
        }
        Ok(StatusSnapshot {
            timestamp: now,
            started_at: self.started_at,
            uptime_sec: now
                .signed_duration_since(self.started_at)
                .num_seconds()
                .max(0) as f64,
            cluster: ClusterSnapshot {
                worker_count: workers.len() as i32,
                worker_ready: workers.iter().filter(|worker| worker.is_ready()).count() as i32,
                control_plane_count: load.control_plane_count,
                pending_pod_count: load.pending_pods,
                cpu_utilization_pct: load.cpu_utilization * 100.0,
                memory_utilization_pct: load.memory_utilization * 100.0,
                max_utilization_pct: load.max_utilization * 100.0,
            },
            worker_pool: WorkerPoolSnapshot {
                name: "workers".into(),
                current_count: workers.len() as i32,
                min_size: self.config.worker_pool.min_size,
                max_size: self.config.worker_pool.max_size,
                target_utilization_pct: self.config.worker_pool.target_utilization * 100.0,
                scale_up_threshold_pct: self.config.worker_pool.scale_up_threshold * 100.0,
                scale_down_threshold_pct: self.config.worker_pool.scale_down_threshold * 100.0,
                last_scale_up: state.last_scale_up,
                last_scale_down: state.last_scale_down,
            },
            nodes: load.nodes,
            draining_nodes: state
                .draining
                .iter()
                .map(|(name, started)| DrainSnapshot {
                    node_name: name.clone(),
                    since_sec: now.signed_duration_since(*started).num_seconds().max(0) as f64,
                    started_at: *started,
                })
                .collect(),
            pending_pods: load.pending,
            events: {
                let mut events = state.events.clone();
                events.sort_by(|left, right| right.time.cmp(&left.time));
                events
            },
            config: ConfigSnapshot {
                cluster_id: self.config.cluster_id.clone(),
                mgmt_endpoint: self.config.mgmt_endpoint.clone(),
                sync_interval: humantime::format_duration(self.config.sync_interval).to_string(),
                drain_timeout: humantime::format_duration(self.config.drain_timeout).to_string(),
                ignored_namespaces: self.config.ignored_namespaces.clone(),
                disable_during_jobs: self.config.disable_during_jobs,
            },
            errors,
        })
    }

    async fn cluster_load(&self) -> Result<ClusterLoad> {
        let nodes: Api<Node> = Api::all(self.kube.clone());
        let pods: Api<Pod> = Api::all(self.kube.clone());
        let all_nodes = nodes.list(&ListParams::default()).await?.items;
        let control_plane_count = all_nodes
            .iter()
            .filter(|node| is_control_plane(node))
            .count() as i32;
        let mut capacity: HashMap<String, (i64, i64, bool)> = HashMap::new();
        for node in &all_nodes {
            let alloc = node
                .status
                .as_ref()
                .and_then(|status| status.allocatable.as_ref());
            let cpu = alloc
                .and_then(|values| values.get("cpu"))
                .map(quantity_cpu)
                .unwrap_or(0);
            let memory = alloc
                .and_then(|values| values.get("memory"))
                .map(quantity_memory)
                .unwrap_or(0);
            capacity.insert(node.name_any(), (cpu, memory, is_control_plane(node)));
        }
        let mut cpu: HashMap<String, i64> = HashMap::new();
        let mut memory: HashMap<String, i64> = HashMap::new();
        let mut pod_count: HashMap<String, i32> = HashMap::new();
        let mut pending = Vec::new();
        let ignored: HashSet<&str> = self
            .config
            .ignored_namespaces
            .iter()
            .map(String::as_str)
            .collect();
        for pod in pods.list(&ListParams::default()).await?.items {
            if pod
                .namespace()
                .as_deref()
                .is_some_and(|namespace| ignored.contains(namespace))
            {
                continue;
            }
            if is_unschedulable_pending(&pod) {
                pending.push(PendingPodSnapshot {
                    namespace: pod.namespace().unwrap_or_else(|| "default".into()),
                    name: pod.name_any(),
                    reason: "Unschedulable".into(),
                });
            }
            let Some(node) = pod.spec.as_ref().and_then(|spec| spec.node_name.clone()) else {
                continue;
            };
            if !capacity.contains_key(&node) {
                continue;
            }
            *pod_count.entry(node.clone()).or_default() += 1;
            if pod
                .status
                .as_ref()
                .and_then(|status| status.phase.as_deref())
                == Some("Running")
            {
                let request = pod.spec.as_ref().map(pod_requests).unwrap_or_default();
                *cpu.entry(node.clone()).or_default() += request.0;
                *memory.entry(node).or_default() += request.1;
            }
        }
        let mut total_cpu = 0i64;
        let mut total_cpu_cap = 0i64;
        let mut total_mem = 0i64;
        let mut total_mem_cap = 0i64;
        let mut result = ClusterLoad {
            pending_pods: pending.len() as i32,
            pending,
            control_plane_count,
            ..Default::default()
        };
        for node in &all_nodes {
            let name = node.name_any();
            let (cpu_capacity, memory_capacity, control_plane) =
                capacity.get(&name).copied().unwrap_or((0, 0, false));
            let cpu_used = cpu.get(&name).copied().unwrap_or(0);
            let mem_used = memory.get(&name).copied().unwrap_or(0);
            let cpu_util = cpu_used as f64 / cpu_capacity.max(1) as f64;
            let mem_util = mem_used as f64 / memory_capacity.max(1) as f64;
            let utilization = cpu_util.max(mem_util);
            if !control_plane {
                total_cpu += cpu_used;
                total_cpu_cap += cpu_capacity.max(1);
                total_mem += mem_used;
                total_mem_cap += memory_capacity.max(1);
                result.max_utilization = result.max_utilization.max(utilization);
                result.node_utilization.insert(name.clone(), utilization);
            }
            result.nodes.push(NodeSnapshot {
                name,
                role: if control_plane {
                    "control-plane"
                } else {
                    "worker"
                }
                .into(),
                unschedulable: node
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.unschedulable)
                    .unwrap_or(false),
                pod_count: pod_count.get(&node.name_any()).copied().unwrap_or(0),
                cpu_requested_cores: cpu_used as f64 / 1000.0,
                cpu_capacity_cores: cpu_capacity as f64 / 1000.0,
                cpu_utilization_pct: cpu_util * 100.0,
                memory_requested_gib: mem_used as f64 / 1024.0 / 1024.0 / 1024.0,
                memory_capacity_gib: memory_capacity as f64 / 1024.0 / 1024.0 / 1024.0,
                memory_utilization_pct: mem_util * 100.0,
                max_utilization_pct: utilization * 100.0,
            });
        }
        result.cpu_utilization = total_cpu as f64 / total_cpu_cap.max(1) as f64;
        result.memory_utilization = total_mem as f64 / total_mem_cap.max(1) as f64;
        Ok(result)
    }

    async fn mutate_state(&self, mutate: impl FnOnce(&mut PersistedState)) {
        let mut state = self.state.lock().await;
        mutate(&mut state);
        if let Err(error) = state.save(&self.state_path) {
            warn!(path = %self.state_path.display(), %error, "failed to persist scaler state");
        }
    }

    async fn record_deferred(&self, key: &str, from: i32, to: i32, message: &str) {
        let now = Utc::now();
        let should_record = {
            let mut last = self.last_deferred.lock().await;
            should_record_deferred(&mut last, key, now)
        };
        if !should_record {
            return;
        }
        self.mutate_state(|state| {
            state.record_event(
                self.config.max_events,
                ScaleEvent {
                    time: now,
                    pool: "workers".into(),
                    direction: ScaleDirection::Skip,
                    from,
                    to,
                    result: ScaleResult::Deferred,
                    message: message.into(),
                },
            );
        })
        .await;
    }
}

fn should_skip_drain(pod: &Pod) -> bool {
    if pod.metadata.deletion_timestamp.is_some() {
        return true;
    }
    if pod
        .metadata
        .annotations
        .as_ref()
        .is_some_and(|annotations| annotations.contains_key("kubernetes.io/config.mirror"))
    {
        return true;
    }
    is_daemon_set(pod)
}

fn is_control_plane(node: &Node) -> bool {
    node.metadata.labels.as_ref().is_some_and(|labels| {
        labels.contains_key("node-role.kubernetes.io/control-plane")
            || labels.contains_key("node-role.kubernetes.io/master")
    })
}
fn is_daemon_set(pod: &Pod) -> bool {
    pod.metadata
        .owner_references
        .as_ref()
        .is_some_and(|owners| owners.iter().any(|owner| owner.kind == "DaemonSet"))
}
fn is_unschedulable_pending(pod: &Pod) -> bool {
    pod.status.as_ref().is_some_and(|status| {
        status.phase.as_deref() == Some("Pending")
            && status.conditions.as_ref().is_some_and(|conditions| {
                conditions.iter().any(|condition| {
                    condition.type_ == "PodScheduled"
                        && condition.status == "False"
                        && condition.reason.as_deref() == Some("Unschedulable")
                })
            })
    })
}
fn pod_requests(pod: &k8s_openapi::api::core::v1::PodSpec) -> (i64, i64) {
    pod.containers
        .iter()
        .fold((0, 0), |(cpu, memory), container| {
            let requests = container
                .resources
                .as_ref()
                .and_then(|resources| resources.requests.as_ref());
            (
                cpu + requests
                    .and_then(|values| values.get("cpu"))
                    .map(quantity_cpu)
                    .unwrap_or(0),
                memory
                    + requests
                        .and_then(|values| values.get("memory"))
                        .map(quantity_memory)
                        .unwrap_or(0),
            )
        })
}
fn quantity_cpu(quantity: &k8s_openapi::apimachinery::pkg::api::resource::Quantity) -> i64 {
    let raw = quantity.0.trim();
    raw.strip_suffix('m')
        .and_then(|value| value.parse().ok())
        .or_else(|| raw.parse::<f64>().ok().map(|value| (value * 1000.0) as i64))
        .unwrap_or(0)
}
fn quantity_memory(quantity: &k8s_openapi::apimachinery::pkg::api::resource::Quantity) -> i64 {
    let raw = quantity.0.trim();
    let units = [
        ("Ki", 1024_f64),
        ("Mi", 1024_f64.powi(2)),
        ("Gi", 1024_f64.powi(3)),
        ("Ti", 1024_f64.powi(4)),
        ("K", 1_000_f64),
        ("M", 1_000_000_f64),
        ("G", 1_000_000_000_f64),
    ];
    units
        .iter()
        .find_map(|(unit, multiplier)| {
            raw.strip_suffix(unit).and_then(|value| {
                value
                    .parse::<f64>()
                    .ok()
                    .map(|value| (value * multiplier) as i64)
            })
        })
        .or_else(|| raw.parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    #[test]
    fn oldest_drain_is_preferred() {
        let older = DateTime::parse_from_rfc3339("2026-08-14T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let newer = DateTime::parse_from_rfc3339("2026-08-14T10:10:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut draining: BTreeMap<String, DateTime<Utc>> = BTreeMap::new();
        draining.insert("newer".into(), newer);
        draining.insert("older".into(), older);
        let selected = draining
            .iter()
            .min_by_key(|(_, started)| *started)
            .map(|(name, _)| name.as_str());
        assert_eq!(selected, Some("older"));
        assert!(!drain_timed_out(older, Duration::from_secs(1800), newer));
    }
}
