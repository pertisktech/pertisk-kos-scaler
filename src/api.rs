use crate::controller::Autoscaler;
use anyhow::Result;
use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::get,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

#[derive(Clone)]
pub struct HttpState {
    pub autoscaler: Arc<Autoscaler>,
}

pub fn ready_response(ready: bool) -> (StatusCode, &'static str) {
    if ready {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

pub async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

pub async fn readyz(State(state): State<HttpState>) -> impl IntoResponse {
    ready_response(state.autoscaler.is_ready())
}

pub async fn status(State(state): State<HttpState>) -> impl IntoResponse {
    match state.autoscaler.dashboard_snapshot().await {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(error) => {
            error!(%error, "failed to build status snapshot");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to build status snapshot: {error}"),
            )
                .into_response()
        }
    }
}

pub async fn index() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

pub fn router(autoscaler: Arc<Autoscaler>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/api/status", get(status))
        .layer(TraceLayer::new_for_http())
        .with_state(HttpState { autoscaler })
}

pub async fn serve(addr: SocketAddr, autoscaler: Arc<Autoscaler>) -> Result<()> {
    let app = router(autoscaler);
    info!(%addr, "starting dashboard and health server");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>KOS Scaler</title>
  <style>
    :root {
      --bg: #0c0d18; --card: #131421; --border: #2c2f4c; --text: #e6e7f0;
      --muted: #7778a0; --primary: #9a7bf7; --ok: #4ade9b; --warn: #fbbf24; --danger: #ff5c56;
    }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: Inter, -apple-system, sans-serif; color: var(--text); background: var(--bg); }
    .shell { max-width: 1280px; margin: 0 auto; padding: 28px; }
    h1 { margin: 0 0 8px; font-size: 2.2rem; letter-spacing: -0.04em; }
    .lede, .sub, .empty { color: #a8aac6; }
    .meta { display: flex; gap: 10px; flex-wrap: wrap; margin: 16px 0 22px; }
    .pill { padding: 8px 12px; border-radius: 999px; border: 1px solid color-mix(in srgb, var(--primary) 40%, transparent); font-size: 13px; color: #a8aac6; }
    .stats { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 16px; margin-bottom: 22px; }
    .stat, .panel { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 16px; }
    .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }
    .value { margin-top: 8px; font-size: 28px; font-weight: 700; }
    .section-head { display: flex; justify-content: space-between; margin: 18px 0 10px; }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { padding: 12px 14px; text-align: left; border-bottom: 1px solid color-mix(in srgb, var(--border) 65%, transparent); }
    th { color: var(--primary); font-size: 12px; text-transform: uppercase; }
    .ok { color: var(--ok); font-weight: 700; }
    .warn { color: var(--warn); font-weight: 700; }
    .danger { color: var(--danger); font-weight: 700; }
    .error-box { display: none; margin: 12px 0; padding: 12px; border-radius: 10px; background: color-mix(in srgb, var(--danger) 15%, transparent); color: #ffc7c4; }
    .table-wrap { overflow-x: auto; }
    @media (max-width: 900px) { .stats { grid-template-columns: 1fr 1fr; } .shell { padding: 16px; } }
  </style>
</head>
<body>
  <div class="shell">
    <h1 id="title">KOS Scaler</h1>
    <p class="lede" id="endpoint"></p>
    <div class="meta">
      <div class="pill" id="sync">Sync</div>
      <div class="pill" id="uptime">Uptime</div>
      <div class="pill" id="updated">Updated</div>
      <div class="pill" id="jobs">Jobs lock</div>
    </div>
    <div class="error-box" id="errors"></div>
    <section class="stats" id="stats"></section>
    <section class="panel">
      <div class="section-head"><h2>Worker pool</h2></div>
      <div class="table-wrap"><table><thead><tr><th>Name</th><th>Current</th><th>Bounds</th><th>Target</th><th>Scale up</th><th>Scale down</th><th>Last up</th><th>Last down</th></tr></thead><tbody id="pool"></tbody></table></div>
    </section>
    <section class="panel">
      <div class="section-head"><h2>Nodes</h2></div>
      <div class="table-wrap"><table><thead><tr><th>Name</th><th>Role</th><th>Status</th><th>Pods</th><th>CPU</th><th>Memory</th><th>Peak</th></tr></thead><tbody id="nodes"></tbody></table></div>
    </section>
    <section class="panel">
      <div class="section-head"><h2>Draining</h2></div>
      <div class="table-wrap"><table><thead><tr><th>Node</th><th>Age</th><th>Started</th></tr></thead><tbody id="draining"></tbody></table><div class="empty" id="drainingEmpty">No nodes are draining.</div></div>
    </section>
    <section class="panel">
      <div class="section-head"><h2>Pending pods</h2></div>
      <div class="table-wrap"><table><thead><tr><th>Pod</th><th>Reason</th></tr></thead><tbody id="pending"></tbody></table><div class="empty" id="pendingEmpty">No unschedulable pending pods.</div></div>
    </section>
    <section class="panel">
      <div class="section-head"><h2>Recent events <span class="sub" id="eventsCount">(0)</span></h2></div>
      <div class="table-wrap"><table><thead><tr><th>Time</th><th>Action</th><th>Result</th><th>Message</th></tr></thead><tbody id="events"></tbody></table><div class="empty" id="eventsEmpty">No scale events recorded yet.</div></div>
    </section>
  </div>
  <script>
    const fmt = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
    const dtf = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'medium' });
    function esc(value) {
      return String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }
    function pct(value) { return fmt.format(value || 0) + '%'; }
    function num(value) { return fmt.format(value || 0); }
    function age(seconds) {
      const s = Math.round(seconds || 0);
      if (s < 60) return s + 's';
      const m = Math.floor(s / 60);
      if (m < 60) return m + 'm';
      return Math.floor(m / 60) + 'h ' + (m % 60) + 'm';
    }
    function ts(value) {
      if (!value) return 'never';
      const d = new Date(value);
      return Number.isNaN(d.getTime()) ? 'never' : dtf.format(d);
    }
    function setRows(bodyId, emptyId, rows) {
      const body = document.getElementById(bodyId);
      const empty = emptyId ? document.getElementById(emptyId) : null;
      body.innerHTML = rows.join('');
      if (empty) empty.style.display = rows.length ? 'none' : 'block';
    }
    function render(data) {
      document.getElementById('title').textContent = 'KOS ' + (data.config.clusterId || 'scaler');
      document.getElementById('endpoint').textContent = data.config.mgmtEndpoint || '';
      document.getElementById('sync').textContent = 'Sync ' + (data.config.syncInterval || 'n/a');
      document.getElementById('uptime').textContent = 'Uptime ' + age(data.uptimeSec);
      document.getElementById('updated').textContent = 'Updated ' + ts(data.timestamp);
      document.getElementById('jobs').textContent = 'Jobs lock ' + (data.config.disableDuringJobs ? 'on' : 'off');
      const errors = data.errors || [];
      const box = document.getElementById('errors');
      box.style.display = errors.length ? 'block' : 'none';
      box.textContent = errors.join(' | ');
      document.getElementById('stats').innerHTML = [
        ['Workers', num(data.cluster.workerCount) + ' / ' + num(data.workerPool.maxSize)],
        ['Ready', num(data.cluster.workerReady)],
        ['Pending pods', num(data.cluster.pendingPodCount)],
        ['Peak util', pct(data.cluster.maxUtilizationPct)]
      ].map(([label, value]) => '<div class="stat"><div class="label">' + esc(label) + '</div><div class="value">' + esc(value) + '</div></div>').join('');
      const pool = data.workerPool || {};
      setRows('pool', null, ['<tr><td><strong>' + esc(pool.name) + '</strong></td><td>' + esc(pool.currentCount) + '</td><td>' + esc(pool.minSize) + ' - ' + esc(pool.maxSize) + '</td><td>' + pct(pool.targetUtilizationPct) + '</td><td>' + pct(pool.scaleUpThresholdPct) + '</td><td>' + pct(pool.scaleDownThresholdPct) + '</td><td>' + esc(ts(pool.lastScaleUp)) + '</td><td>' + esc(ts(pool.lastScaleDown)) + '</td></tr>']);
      setRows('nodes', null, (data.nodes || []).map(node =>
        '<tr><td><strong>' + esc(node.name) + '</strong></td><td>' + esc(node.role) + '</td><td class="' + (node.unschedulable ? 'warn' : 'ok') + '">' + (node.unschedulable ? 'cordoned' : 'running') + '</td><td>' + esc(node.podCount) + '</td><td>' + esc(num(node.cpuRequestedCores) + ' / ' + num(node.cpuCapacityCores) + ' (' + pct(node.cpuUtilizationPct) + ')') + '</td><td>' + esc(num(node.memoryRequestedGiB) + ' / ' + num(node.memoryCapacityGiB) + ' GiB (' + pct(node.memoryUtilizationPct) + ')') + '</td><td>' + pct(node.maxUtilizationPct) + '</td></tr>'
      ));
      setRows('draining', 'drainingEmpty', (data.drainingNodes || []).map(item =>
        '<tr><td><strong>' + esc(item.nodeName) + '</strong></td><td>' + esc(age(item.sinceSec)) + '</td><td>' + esc(ts(item.startedAt)) + '</td></tr>'
      ));
      setRows('pending', 'pendingEmpty', (data.pendingPods || []).map(pod =>
        '<tr><td><strong>' + esc(pod.namespace + '/' + pod.name) + '</strong></td><td>' + esc(pod.reason) + '</td></tr>'
      ));
      setRows('events', 'eventsEmpty', (data.events || []).map(event =>
        '<tr><td>' + esc(ts(event.time)) + '</td><td>' + esc(event.direction + ' ' + event.from + ' → ' + event.to) + '</td><td class="' + (event.result === 'success' ? 'ok' : event.result === 'deferred' ? 'warn' : 'danger') + '">' + esc(event.result) + '</td><td>' + esc(event.message) + '</td></tr>'
      ));
      document.getElementById('eventsCount').textContent = '(' + ((data.events || []).length) + ')';
    }
    async function refresh() {
      try {
        const res = await fetch('/api/status', { cache: 'no-store' });
        if (!res.ok) return;
        render(await res.json());
      } catch (_) {}
    }
    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>"#;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[test]
    fn ready_response_semantics() {
        assert_eq!(ready_response(true), (StatusCode::OK, "ok"));
        assert_eq!(
            ready_response(false),
            (StatusCode::SERVICE_UNAVAILABLE, "not ready")
        );
    }

    #[tokio::test]
    async fn healthz_ok() {
        let app = Router::new().route("/healthz", get(healthz));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"ok");
    }
}
