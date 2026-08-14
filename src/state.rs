use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
    time::Duration,
};
use tracing::{info, warn};

pub const STATE_VERSION: u32 = 1;
pub const DRAINING_ANNOTATION: &str = "kos-scaler.pertisk.io/draining";
pub const DRAIN_STARTED_ANNOTATION: &str = "kos-scaler.pertisk.io/drain-started-at";
pub const DEFERRED_EVENT_THROTTLE: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScaleDirection {
    Up,
    Down,
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScaleResult {
    Success,
    Failed,
    Deferred,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScaleEvent {
    pub time: DateTime<Utc>,
    pub pool: String,
    pub direction: ScaleDirection,
    pub from: i32,
    pub to: i32,
    pub result: ScaleResult,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PersistedState {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub last_scale_up: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_scale_down: Option<DateTime<Utc>>,
    #[serde(default)]
    pub draining: BTreeMap<String, DateTime<Utc>>,
    #[serde(default)]
    pub events: Vec<ScaleEvent>,
}

fn default_version() -> u32 {
    STATE_VERSION
}

impl Default for PersistedState {
    fn default() -> Self {
        Self {
            version: STATE_VERSION,
            last_scale_up: None,
            last_scale_down: None,
            draining: BTreeMap::new(),
            events: Vec::new(),
        }
    }
}

impl PersistedState {
    pub fn load(path: &Path, max_events: usize) -> Self {
        let content = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Self::default();
            }
            Err(error) => {
                warn!(path = %path.display(), %error, "failed to read scaler state; starting empty");
                return Self::default();
            }
        };
        match serde_json::from_str::<Self>(&content) {
            Ok(mut state) => {
                state.version = STATE_VERSION;
                state.trim_events(max_events);
                info!(path = %path.display(), events = state.events.len(), draining = state.draining.len(), "loaded scaler state");
                state
            }
            Err(error) => {
                warn!(path = %path.display(), %error, "scaler state is corrupt; starting empty");
                Self::default()
            }
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating state directory {}", parent.display()))?;
        }
        let serialized = serde_json::to_vec_pretty(self).context("serializing scaler state")?;
        let tmp = tmp_path(path);
        fs::write(&tmp, serialized).with_context(|| format!("writing {}", tmp.display()))?;
        fs::rename(&tmp, path).with_context(|| format!("replacing {}", path.display()))?;
        Ok(())
    }

    pub fn record_event(&mut self, max_events: usize, event: ScaleEvent) {
        self.events.push(event);
        self.trim_events(max_events);
    }

    fn trim_events(&mut self, max_events: usize) {
        if self.events.len() > max_events {
            let excess = self.events.len() - max_events;
            self.events.drain(0..excess);
        }
    }
}

pub fn state_file(dir: impl AsRef<Path>) -> PathBuf {
    dir.as_ref().join("kos-scaler-state.json")
}

fn tmp_path(path: &Path) -> PathBuf {
    path.with_extension("json.tmp")
}

pub fn parse_drain_started(
    annotations: Option<&BTreeMap<String, String>>,
) -> Option<DateTime<Utc>> {
    let annotations = annotations?;
    if annotations.get(DRAINING_ANNOTATION).map(String::as_str) != Some("true") {
        return None;
    }
    annotations
        .get(DRAIN_STARTED_ANNOTATION)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

pub fn drain_timed_out(started: DateTime<Utc>, timeout: Duration, now: DateTime<Utc>) -> bool {
    now.signed_duration_since(started)
        .to_std()
        .map(|elapsed| elapsed > timeout)
        .unwrap_or(false)
}

pub fn cooldown_elapsed(
    last: Option<DateTime<Utc>>,
    cooldown: Duration,
    now: DateTime<Utc>,
) -> bool {
    match last {
        Some(time) => now
            .signed_duration_since(time)
            .to_std()
            .map(|elapsed| elapsed >= cooldown)
            .unwrap_or(false),
        None => true,
    }
}

pub fn should_record_deferred(
    last: &mut HashMap<String, DateTime<Utc>>,
    key: &str,
    now: DateTime<Utc>,
) -> bool {
    match last.get(key) {
        Some(previous)
            if now
                .signed_duration_since(*previous)
                .to_std()
                .map(|elapsed| elapsed < DEFERRED_EVENT_THROTTLE)
                .unwrap_or(false) =>
        {
            false
        }
        _ => {
            last.insert(key.to_owned(), now);
            true
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusSnapshot {
    pub timestamp: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub uptime_sec: f64,
    pub cluster: ClusterSnapshot,
    pub worker_pool: WorkerPoolSnapshot,
    pub nodes: Vec<NodeSnapshot>,
    pub draining_nodes: Vec<DrainSnapshot>,
    pub pending_pods: Vec<PendingPodSnapshot>,
    pub events: Vec<ScaleEvent>,
    pub config: ConfigSnapshot,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterSnapshot {
    pub worker_count: i32,
    pub worker_ready: i32,
    pub control_plane_count: i32,
    pub pending_pod_count: i32,
    pub cpu_utilization_pct: f64,
    pub memory_utilization_pct: f64,
    pub max_utilization_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPoolSnapshot {
    pub name: String,
    pub current_count: i32,
    pub min_size: i32,
    pub max_size: i32,
    pub target_utilization_pct: f64,
    pub scale_up_threshold_pct: f64,
    pub scale_down_threshold_pct: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_scale_up: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_scale_down: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSnapshot {
    pub name: String,
    pub role: String,
    pub unschedulable: bool,
    pub pod_count: i32,
    pub cpu_requested_cores: f64,
    pub cpu_capacity_cores: f64,
    pub cpu_utilization_pct: f64,
    #[serde(rename = "memoryRequestedGiB")]
    pub memory_requested_gib: f64,
    #[serde(rename = "memoryCapacityGiB")]
    pub memory_capacity_gib: f64,
    pub memory_utilization_pct: f64,
    pub max_utilization_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainSnapshot {
    pub node_name: String,
    pub since_sec: f64,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingPodSnapshot {
    pub namespace: String,
    pub name: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigSnapshot {
    pub cluster_id: String,
    pub mgmt_endpoint: String,
    pub sync_interval: String,
    pub drain_timeout: String,
    pub ignored_namespaces: Vec<String>,
    pub disable_during_jobs: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn temp_file(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kos-scaler-state-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn save_and_load_round_trip() {
        let path = temp_file("round-trip.json");
        let mut state = PersistedState::default();
        let started = Utc::now();
        state.draining.insert("worker-a".into(), started);
        state.last_scale_up = Some(started);
        state.record_event(
            100,
            ScaleEvent {
                time: started,
                pool: "workers".into(),
                direction: ScaleDirection::Up,
                from: 2,
                to: 3,
                result: ScaleResult::Success,
                message: "pending pods".into(),
            },
        );
        state.save(&path).unwrap();
        let loaded = PersistedState::load(&path, 100);
        assert_eq!(
            loaded.draining.get("worker-a").map(|time| time.timestamp()),
            Some(started.timestamp())
        );
        assert_eq!(loaded.events.len(), 1);
        assert_eq!(loaded.events[0].message, "pending pods");
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn corrupt_state_starts_empty() {
        let path = temp_file("corrupt.json");
        fs::write(&path, "{not-json").unwrap();
        let loaded = PersistedState::load(&path, 100);
        assert_eq!(loaded, PersistedState::default());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn missing_state_starts_empty() {
        let path = temp_file("missing.json");
        let _ = fs::remove_file(&path);
        let loaded = PersistedState::load(&path, 100);
        assert_eq!(loaded, PersistedState::default());
    }

    #[test]
    fn event_ring_is_bounded() {
        let mut state = PersistedState::default();
        for index in 0..5 {
            state.record_event(
                3,
                ScaleEvent {
                    time: Utc::now(),
                    pool: "workers".into(),
                    direction: ScaleDirection::Skip,
                    from: index,
                    to: index,
                    result: ScaleResult::Deferred,
                    message: index.to_string(),
                },
            );
        }
        assert_eq!(state.events.len(), 3);
        assert_eq!(state.events[0].message, "2");
        assert_eq!(state.events[2].message, "4");
    }

    #[test]
    fn drain_timeout_uses_wall_clock() {
        let started = DateTime::parse_from_rfc3339("2026-08-14T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let now = DateTime::parse_from_rfc3339("2026-08-14T00:29:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(!drain_timed_out(started, Duration::from_secs(1800), now));
        let later = DateTime::parse_from_rfc3339("2026-08-14T00:31:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(drain_timed_out(started, Duration::from_secs(1800), later));
    }

    #[test]
    fn parse_drain_annotation() {
        let mut annotations = BTreeMap::new();
        annotations.insert(DRAINING_ANNOTATION.into(), "true".into());
        annotations.insert(
            DRAIN_STARTED_ANNOTATION.into(),
            "2026-08-14T12:00:00Z".into(),
        );
        let started = parse_drain_started(Some(&annotations)).unwrap();
        assert_eq!(started.to_rfc3339(), "2026-08-14T12:00:00+00:00");
        annotations.insert(DRAINING_ANNOTATION.into(), "false".into());
        assert!(parse_drain_started(Some(&annotations)).is_none());
    }

    #[test]
    fn deferred_events_are_throttled() {
        let mut last = HashMap::new();
        let now = Utc::now();
        assert!(should_record_deferred(&mut last, "cooldown", now));
        assert!(!should_record_deferred(&mut last, "cooldown", now));
        assert!(should_record_deferred(&mut last, "jobs", now));
    }

    #[test]
    fn snapshot_serializes_camel_case() {
        let snapshot = StatusSnapshot {
            timestamp: DateTime::parse_from_rfc3339("2026-08-14T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            started_at: DateTime::parse_from_rfc3339("2026-08-14T11:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            uptime_sec: 3600.0,
            cluster: ClusterSnapshot {
                worker_count: 3,
                worker_ready: 3,
                control_plane_count: 1,
                pending_pod_count: 0,
                cpu_utilization_pct: 40.0,
                memory_utilization_pct: 30.0,
                max_utilization_pct: 40.0,
            },
            worker_pool: WorkerPoolSnapshot {
                name: "workers".into(),
                current_count: 3,
                min_size: 2,
                max_size: 10,
                target_utilization_pct: 50.0,
                scale_up_threshold_pct: 65.0,
                scale_down_threshold_pct: 35.0,
                last_scale_up: None,
                last_scale_down: None,
            },
            nodes: Vec::new(),
            draining_nodes: Vec::new(),
            pending_pods: Vec::new(),
            events: Vec::new(),
            config: ConfigSnapshot {
                cluster_id: "abc".into(),
                mgmt_endpoint: "https://mgmt.example.com".into(),
                sync_interval: "30s".into(),
                drain_timeout: "30m".into(),
                ignored_namespaces: vec!["kube-system".into()],
                disable_during_jobs: true,
            },
            errors: Vec::new(),
        };
        let json = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(json["workerPool"]["minSize"], 2);
        assert_eq!(json["cluster"]["pendingPodCount"], 0);
        assert_eq!(json["config"]["clusterId"], "abc");
    }
}
