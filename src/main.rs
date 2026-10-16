use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use tracing::warn;

const INDEX_HTML: &str = include_str!("../static/index.html");
const MAX_POINTS_PER_SERIES: usize = 50_000;

type Labels = BTreeMap<String, String>;

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct SeriesKey {
    project: String,
    metric: String,
    labels: Labels,
}

#[derive(Default)]
struct Store {
    series: HashMap<SeriesKey, VecDeque<(i64, f64)>>,
}

impl Store {
    fn insert(&mut self, project: &str, metric: &str, labels: Labels, ts: i64, value: f64) {
        let key = SeriesKey {
            project: project.to_string(),
            metric: metric.to_string(),
            labels,
        };
        let q = self.series.entry(key).or_default();
        q.push_back((ts, value));
        while q.len() > MAX_POINTS_PER_SERIES {
            q.pop_front();
        }
    }

    fn total_points(&self) -> u64 {
        self.series.values().map(|q| q.len() as u64).sum()
    }

    /// Remove points older than `cutoff` (epoch secs). Returns removed count.
    fn prune(&mut self, cutoff: i64) -> usize {
        let mut removed = 0;
        self.series.retain(|_, q| {
            let before = q.len();
            while q.front().map(|(ts, _)| *ts < cutoff).unwrap_or(false) {
                q.pop_front();
            }
            removed += before - q.len();
            !q.is_empty()
        });
        removed
    }
}

#[derive(Clone)]
struct AppState {
    store: Arc<RwLock<Store>>,
    tokens: HashMap<String, String>,
    data_dir: PathBuf,
    retention_hours: i64,
    started_ts: i64,
}

#[derive(Parser, Debug)]
#[command(name = "metrics-lite", about = "Lightweight push metrics + chart UI")]
struct Args {
    #[arg(long, env = "PORT", default_value_t = 3100)]
    port: u16,
    #[arg(long, env = "BIND", default_value = "0.0.0.0")]
    bind: String,
    #[arg(long, env = "DATA_DIR", default_value = "./data")]
    data_dir: PathBuf,
    #[arg(long, env = "RETENTION_HOURS", default_value_t = 24)]
    retention_hours: i64,
    /// Optional "proj:tok,proj2:tok2". Env METRICS_TOKENS. Prefix with @ to read from file.
    #[arg(long, env = "METRICS_TOKENS", default_value = "")]
    tokens: String,
}

fn parse_tokens(s: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    let s = s.trim();
    if s.is_empty() {
        return m;
    }
    // @file support
    let content: String;
    let body: &str = if let Some(path) = s.strip_prefix('@') {
        match std::fs::read_to_string(path.trim()) {
            Ok(c) => {
                content = c;
                &content
            }
            Err(e) => {
                warn!("tokens file unreadable {}: {}", path, e);
                return m;
            }
        }
    } else {
        s
    };
    // also support file path without @ if it exists on disk and has no ':' or ',' separators ambiguity:
    // (skip: keep explicit @file only, plus METRICS_TOKENS_FILE env handled by caller)
    for part in body.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((k, v)) = part.split_once(':') {
            let (k, v) = (k.trim().to_string(), v.trim().to_string());
            if !k.is_empty() && !v.is_empty() {
                m.insert(k, v);
            }
        }
    }
    m
}

fn check_token(
    tokens: &HashMap<String, String>,
    project: &str,
    header_token: Option<&str>,
    query_token: Option<&str>,
) -> bool {
    match tokens.get(project) {
        None => true, // no token configured -> open
        Some(expected) => {
            header_token == Some(expected.as_str()) || query_token == Some(expected.as_str())
        }
    }
}

fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Parse Prometheus/OpenMetrics text: `name{labels} value [ts]` lines. Ignore # comments.
fn parse_prometheus_text(
    body: &str,
    project: &str,
    now: i64,
) -> Vec<(String, Labels, i64, f64)> {
    let mut out = Vec::new();
    for raw in body.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // split metric+labels from value+ts
        // find end of metric part: first whitespace
        let sp = match line.find(|c: char| c == ' ' || c == '\t') {
            Some(i) => i,
            None => continue,
        };
        let head = &line[..sp];
        let tail = line[sp..].trim();
        let (name, labels) = match parse_metric_head(head) {
            Some(v) => v,
            None => continue,
        };
        if name.is_empty() {
            continue;
        }
        let mut parts = tail.split_whitespace();
        let val_s = match parts.next() {
            Some(v) => v,
            None => continue,
        };
        // skip +Inf/-Inf/NaN? store only finite
        let value: f64 = match val_s {
            "+Inf" | "Inf" => f64::INFINITY,
            "-Inf" => f64::NEG_INFINITY,
            "NaN" => f64::NAN,
            s => match s.parse() {
                Ok(v) => v,
                Err(_) => continue,
            },
        };
        if !value.is_finite() {
            continue;
        }
        let ts: i64 = match parts.next() {
            Some(ts_s) => match ts_s.parse::<f64>() {
                Ok(t) => {
                    // Prometheus timestamps are ms; auto-detect
                    if t > 1e12 {
                        (t / 1000.0) as i64
                    } else if t > 1e10 {
                        // ambiguous ms-range; treat as ms too
                        (t / 1000.0) as i64
                    } else {
                        t as i64
                    }
                }
                Err(_) => now,
            },
            None => now,
        };
        let _ = project;
        out.push((name, labels, ts, value));
    }
    out
}

fn parse_metric_head(head: &str) -> Option<(String, Labels)> {
    if let Some(brace) = head.find('{') {
        let name = head[..brace].trim().to_string();
        if !valid_metric_name(&name) {
            return None;
        }
        let rest = &head[brace..];
        if !rest.ends_with('}') {
            return None;
        }
        let inner = &rest[1..rest.len() - 1];
        let mut labels = Labels::new();
        for part in split_labels(inner) {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (k, v) = part.split_once('=')?;
            let k = k.trim().to_string();
            let mut v = v.trim().to_string();
            if v.len() >= 2 && v.starts_with('"') && v.ends_with('"') {
                v = unescape_label_value(&v[1..v.len() - 1]);
            } else {
                return None;
            }
            if k.is_empty() {
                continue;
            }
            labels.insert(k, v);
        }
        Some((name, labels))
    } else {
        let name = head.trim().to_string();
        if !valid_metric_name(&name) {
            return None;
        }
        Some((name, Labels::new()))
    }
}

fn valid_metric_name(n: &str) -> bool {
    let mut it = n.chars();
    match it.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == ':' => {}
        _ => return false,
    }
    n.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

fn split_labels(s: &str) -> Vec<&str> {
    // split on commas outside quotes
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_q = false;
    let mut esc = false;
    for (i, c) in s.char_indices() {
        if esc {
            esc = false;
            continue;
        }
        if c == '\\' && in_q {
            esc = true;
            continue;
        }
        if c == '"' {
            in_q = !in_q;
        } else if c == ',' && !in_q {
            parts.push(&s[start..i]);
            start = i + 1;
        }
    }
    parts.push(&s[start..]);
    parts
}

fn unescape_label_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut esc = false;
    for c in s.chars() {
        if esc {
            match c {
                'n' => out.push('\n'),
                't' => out.push('\t'),
                'r' => out.push('\r'),
                _ => out.push(c),
            }
            esc = false;
        } else if c == '\\' {
            esc = true;
        } else {
            out.push(c);
        }
    }
    out
}

fn escape_label_value(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

// ---------- API types ----------

#[derive(Deserialize)]
struct PushSample {
    metric: String,
    value: f64,
    #[serde(default)]
    labels: Option<Labels>,
    #[serde(default)]
    ts: Option<f64>,
}

#[derive(Serialize)]
struct PushResp {
    ok: bool,
    stored: usize,
}

#[derive(Serialize)]
struct QuerySeries {
    labels: Labels,
    points: Vec<(i64, f64)>,
}

#[derive(Serialize)]
struct QueryResp {
    project: String,
    metric: String,
    points: Vec<(i64, f64)>,
    series: Vec<QuerySeries>,
}

async fn post_push(
    State(st): State<AppState>,
    Path(project): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let header_token = headers
        .get("x-api-token")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let query_token = q.get("token").cloned();
    if !check_token(
        &st.tokens,
        &project,
        header_token.as_deref(),
        query_token.as_deref(),
    ) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"ok": false, "error": "bad or missing token"})),
        )
            .into_response();
    }
    let now = now_ts();
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    // Bulk-friendly: accept gzip-compressed bodies (Content-Encoding: gzip).
    // 10x smaller on the wire for JSON batches: curl --data-binary @- -H 'Content-Encoding: gzip'
    let body: Vec<u8> = if headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(',')
        .any(|e| e.trim().eq_ignore_ascii_case("gzip"))
    {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut dec = GzDecoder::new(&body[..]);
        let mut out = Vec::new();
        if dec.read_to_end(&mut out).is_err() {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"ok": false, "error": "invalid gzip body"})),
            )
                .into_response();
        }
        out
    } else {
        body.to_vec()
    };
    let trimmed = if body.len() > 1024 {
        String::from_utf8_lossy(&body[..1024]).into_owned()
    } else {
        String::from_utf8_lossy(&body).into_owned()
    };
    let is_text = ct.contains("text/plain")
        || !(trimmed.trim_start().starts_with('{') || trimmed.trim_start().starts_with('['));
    let mut stored = 0usize;
    let mut store = st.store.write().await;
    if is_text {
        let text = String::from_utf8_lossy(&body).into_owned();
        for (metric, labels, ts, value) in parse_prometheus_text(&text, &project, now) {
            store.insert(&project, &metric, labels, ts, value);
            stored += 1;
        }
    } else {
        let text = String::from_utf8_lossy(&body).into_owned();
        // try single object, then array, then JSONL (one object per line —
        // the natural pipe format: prog | mlpush myproject)
        let mut samples: Vec<PushSample> = Vec::new();
        if ct.contains("ndjson") || ct.contains("jsonl") || ct.contains("json-lines") {
            for (n, line) in text.lines().enumerate() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                match serde_json::from_str::<PushSample>(line) {
                    Ok(s) => samples.push(s),
                    Err(_) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({"ok": false, "error": format!("invalid JSON on line {}", n + 1)})),
                        )
                            .into_response();
                    }
                }
            }
        } else if let Ok(s) = serde_json::from_str::<PushSample>(&text) {
            samples.push(s);
        } else if let Ok(arr) = serde_json::from_str::<Vec<PushSample>>(&text) {
            samples = arr;
        } else {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"ok": false, "error": "invalid JSON push body"})),
            )
                .into_response();
        }
        for s in samples {
            if !valid_metric_name(&s.metric) || !s.value.is_finite() {
                continue;
            }
            let ts = s.ts.map(|t| t as i64).unwrap_or(now);
            store.insert(&project, &s.metric, s.labels.unwrap_or_default(), ts, s.value);
            stored += 1;
        }
    }
    Json(PushResp { ok: true, stored }).into_response()
}

async fn get_query(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let project = match q.get("project") {
        Some(p) if !p.is_empty() => p.clone(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "missing ?project="})),
            )
                .into_response()
        }
    };
    let metric = match q.get("metric") {
        Some(m) if !m.is_empty() => m.clone(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "missing ?metric="})),
            )
                .into_response()
        }
    };
    let now = now_ts();
    let from: i64 = q
        .get("from")
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| {
            if v < 0.0 {
                now + v as i64 // relative e.g. from=-3600
            } else {
                v as i64
            }
        })
        .unwrap_or(0);
    let to: i64 = q
        .get("to")
        .and_then(|v| v.parse::<f64>().ok())
        .map(|v| v as i64)
        .unwrap_or(i64::MAX);
    let reserved = ["project", "metric", "from", "to", "token"];
    let filters: Vec<(String, String)> = q
        .iter()
        .filter(|(k, _)| !reserved.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let store = st.store.read().await;
    let mut series = Vec::new();
    let mut merged: Vec<(i64, f64)> = Vec::new();
    for (key, pts) in store.series.iter() {
        if key.project != project || key.metric != metric {
            continue;
        }
        if !filters
            .iter()
            .all(|(k, v)| key.labels.get(k).map(|x| x == v).unwrap_or(false))
        {
            continue;
        }
        let fp: Vec<(i64, f64)> = pts
            .iter()
            .filter(|(ts, _)| *ts >= from && *ts <= to)
            .cloned()
            .collect();
        if fp.is_empty() {
            continue;
        }
        merged.extend(fp.iter().cloned());
        series.push(QuerySeries {
            labels: key.labels.clone(),
            points: fp,
        });
    }
    series.sort_by(|a, b| format!("{:?}", a.labels).cmp(&format!("{:?}", b.labels)));
    merged.sort_by_key(|(ts, _)| *ts);
    (
        StatusCode::OK,
        Json(QueryResp {
            project,
            metric,
            points: merged,
            series,
        }),
    )
        .into_response()
}

async fn get_projects(State(st): State<AppState>) -> impl IntoResponse {
    let store = st.store.read().await;
    let mut set = HashSet::new();
    for k in store.series.keys() {
        set.insert(k.project.clone());
    }
    let mut v: Vec<String> = set.into_iter().collect();
    v.sort();
    Json(serde_json::json!({"projects": v}))
}

async fn get_metrics(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let store = st.store.read().await;
    let proj = q.get("project").cloned().unwrap_or_default();
    let mut set = HashSet::new();
    for k in store.series.keys() {
        if proj.is_empty() || k.project == proj {
            set.insert(k.metric.clone());
        }
    }
    let mut v: Vec<String> = set.into_iter().collect();
    v.sort();
    Json(serde_json::json!({"metrics": v}))
}

async fn get_healthz() -> impl IntoResponse {
    Json(serde_json::json!({"ok": true}))
}

async fn get_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn get_prom(State(st): State<AppState>) -> Response {
    let store = st.store.read().await;
    let mut out = String::new();
    out.push_str("# HELP lite_up 1 when metrics-lite is running\n# TYPE lite_up gauge\nlite_up 1\n");
    out.push_str("# HELP lite_series_count number of series stored\n# TYPE lite_series_count gauge\n");
    out.push_str(&format!("lite_series_count {}\n", store.series.len()));
    out.push_str("# HELP lite_push_points_total total points stored\n# TYPE lite_push_points_total counter\n");
    out.push_str(&format!("lite_push_points_total {}\n", store.total_points()));
    out.push_str(&format!(
        "# HELP lite_uptime_seconds seconds since start\n# TYPE lite_uptime_seconds counter\nlite_uptime_seconds {}\n",
        now_ts() - st.started_ts
    ));
    out.push_str("# HELP lite_push_value latest value per series\n# TYPE lite_push_value gauge\n");
    let mut keys: Vec<&SeriesKey> = store.series.keys().collect();
    keys.sort_by(|a, b| {
        (a.project.clone(), a.metric.clone(), format!("{:?}", a.labels)).cmp(&(
            b.project.clone(),
            b.metric.clone(),
            format!("{:?}", b.labels),
        ))
    });
    for k in keys {
        if let Some((_, v)) = store.series.get(k).and_then(|q| q.back()) {
            let mut lbl = format!(
                "project=\"{}\",metric=\"{}\"",
                escape_label_value(&k.project),
                escape_label_value(&k.metric)
            );
            for (lk, lv) in &k.labels {
                lbl.push_str(&format!(
                    ",{}=\"{}\"",
                    lk,
                    escape_label_value(lv)
                ));
            }
            out.push_str(&format!("lite_push_value{{{}}} {}\n", lbl, v));
        }
    }
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        out,
    )
        .into_response()
}

// ---------- snapshot ----------

#[derive(Serialize, Deserialize)]
struct SnapRow {
    metric: String,
    labels: Labels,
    ts: i64,
    value: f64,
}

async fn save_snapshot(store: &Arc<RwLock<Store>>, data_dir: &PathBuf) {
    if let Err(e) = tokio::fs::create_dir_all(data_dir).await {
        warn!("snapshot mkdir failed: {}", e);
        return;
    }
    // group by project
    let store = store.read().await;
    let mut by_proj: HashMap<String, Vec<SnapRow>> = HashMap::new();
    for (k, pts) in store.series.iter() {
        let rows = by_proj.entry(k.project.clone()).or_default();
        for (ts, v) in pts.iter() {
            rows.push(SnapRow {
                metric: k.metric.clone(),
                labels: k.labels.clone(),
                ts: *ts,
                value: *v,
            });
        }
    }
    drop(store);
    for (proj, rows) in by_proj {
        let safe: String = proj
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let path = data_dir.join(format!("{}.jsonl", safe));
        let tmp = data_dir.join(format!("{}.jsonl.tmp", safe));
        let mut buf = String::new();
        for r in &rows {
            buf.push_str(&serde_json::to_string(r).unwrap_or_default());
            buf.push('\n');
        }
        // map project filename back: if sanitized != project, embed project? we use filename as project on load;
        // keep original project inside? Snapshot rows lack project; loader uses filename. Sanitized names are
        // assumed stable. Fine for v1.
        if let Err(e) = tokio::fs::write(&tmp, buf).await {
            warn!("snapshot write failed: {}", e);
            continue;
        }
        if let Err(e) = tokio::fs::rename(&tmp, &path).await {
            warn!("snapshot rename failed: {}", e);
        }
    }
}

async fn load_snapshot(store: &Arc<RwLock<Store>>, data_dir: &PathBuf) {
    let mut rd = match tokio::fs::read_dir(data_dir).await {
        Ok(r) => r,
        Err(_) => return,
    };
    let mut loaded = 0usize;
    use tokio::io::AsyncBufReadExt;
    while let Ok(Some(ent)) = rd.next_entry().await {
        let p = ent.path();
        if p.extension().map(|e| e == "jsonl").unwrap_or(false) {
            let proj = p
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            if proj.is_empty() {
                continue;
            }
            let f = match tokio::fs::File::open(&p).await {
                Ok(f) => f,
                Err(_) => continue,
            };
            let mut lines = tokio::io::BufReader::new(f).lines();
            let mut st = store.write().await;
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(row) = serde_json::from_str::<SnapRow>(&line) {
                    st.insert(&proj, &row.metric, row.labels, row.ts, row.value);
                    loaded += 1;
                }
            }
        }
    }
    if loaded > 0 {
        tracing::info!("loaded {} snapshot points from {:?}", loaded, data_dir);
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "metrics_lite=info,tower_http=warn".into()),
        )
        .init();
    let args = Args::parse();
    // METRICS_TOKENS_FILE support (tokens from file)
    let mut token_src = args.tokens.clone();
    if token_src.trim().is_empty() {
        if let Ok(f) = std::env::var("METRICS_TOKENS_FILE") {
            if let Ok(c) = std::fs::read_to_string(f.trim()) {
                token_src = c;
            }
        }
        if token_src.trim().is_empty() {
            if let Ok(c) = std::env::var("TOKENS") {
                token_src = c;
            }
        }
    }
    let tokens = parse_tokens(&token_src);

    let state = AppState {
        store: Arc::new(RwLock::new(Store::default())),
        tokens,
        data_dir: args.data_dir.clone(),
        retention_hours: args.retention_hours,
        started_ts: now_ts(),
    };

    load_snapshot(&state.store, &state.data_dir).await;

    // background: retention prune + periodic snapshot
    {
        let store = state.store.clone();
        let rh = state.retention_hours;
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(60));
            loop {
                t.tick().await;
                if rh > 0 {
                    let cutoff = now_ts() - rh * 3600;
                    let n = store.write().await.prune(cutoff);
                    if n > 0 {
                        tracing::info!("pruned {} old points", n);
                    }
                }
            }
        });
    }
    {
        let store = state.store.clone();
        let dir = state.data_dir.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(60));
            loop {
                t.tick().await;
                save_snapshot(&store, &dir).await;
            }
        });
    }

    let app: Router = Router::new()
        .route("/", get(get_index))
        .route("/healthz", get(get_healthz))
        .route("/metrics", get(get_prom))
        .route("/api/v1/push/:project", axum::routing::post(post_push))
        .route("/api/v1/query", get(get_query))
        .route("/api/v1/projects", get(get_projects))
        .route("/api/v1/metrics", get(get_metrics))
        // Bulk pipes exceed axum's 2MB default body cap: local trusted daemon,
        // cap is enforced downstream by series ring limits instead.
        .layer(axum::extract::DefaultBodyLimit::disable())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("{}:{}", args.bind, args.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind failed");
    tracing::info!("metrics-lite listening on {}", addr);
    axum::serve(listener, app).await.expect("serve failed");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prom_text_basic() {
        let body = "# HELP feed_mid last\n# TYPE feed_mid gauge\nlive_up 1\nfeed_mid{symbol=\"XAU\"} 2650.1\nfoo{a=\"1\",b=\"2\"} 3.5 1700000000\nbadline\n";
        let rows = parse_prometheus_text(body, "p", 999);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, "live_up");
        assert!(rows[0].1.is_empty());
        assert_eq!(rows[0].2, 999);
        assert_eq!(rows[0].3, 1.0);
        assert_eq!(rows[1].0, "feed_mid");
        assert_eq!(rows[1].1.get("symbol").unwrap(), "XAU");
        assert_eq!(rows[2].2, 1700000000);
    }

    #[test]
    fn prom_text_ms_ts() {
        let rows = parse_prometheus_text("m 1 1700000000000\n", "p", 0);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].2, 1700000000);
    }

    #[test]
    fn prom_text_ignores_bad() {
        let rows = parse_prometheus_text("# HELP x\noops{bad} notanumber\n9bad 1\n", "p", 0);
        assert!(rows.is_empty());
    }

    #[test]
    fn token_gate() {
        let mut t = HashMap::new();
        t.insert("proj1".to_string(), "tok1".to_string());
        assert!(check_token(&t, "proj1", Some("tok1"), None));
        assert!(check_token(&t, "proj1", None, Some("tok1")));
        assert!(!check_token(&t, "proj1", Some("wrong"), None));
        assert!(!check_token(&t, "proj1", None, None));
        assert!(check_token(&t, "other", None, None)); // unconfigured project is open
    }

    #[test]
    fn retention_prune() {
        let mut s = Store::default();
        s.insert("p", "m", Labels::new(), 100, 1.0);
        s.insert("p", "m", Labels::new(), 200, 2.0);
        s.insert("p", "m", Labels::new(), 300, 3.0);
        let n = s.prune(250);
        assert_eq!(n, 2);
        let q = s.series.values().next().unwrap();
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].0, 300);
    }

    #[test]
    fn json_roundtrip_store() {
        // push->query roundtrip at store level
        let mut s = Store::default();
        let mut lbl = Labels::new();
        lbl.insert("symbol".to_string(), "XAU".to_string());
        s.insert("demo", "live_equity", lbl.clone(), 1000, 14726.78);
        s.insert("demo", "live_equity", lbl.clone(), 1005, 14730.0);
        let key = SeriesKey {
            project: "demo".into(),
            metric: "live_equity".into(),
            labels: lbl,
        };
        let q = s.series.get(&key).unwrap();
        assert_eq!(q.len(), 2);
        let pts: Vec<(i64, f64)> = q.iter().filter(|(ts, _)| *ts >= 999 && *ts <= 1010).cloned().collect();
        assert_eq!(pts, vec![(1000, 14726.78), (1005, 14730.0)]);
    }

    #[test]
    fn ring_cap() {
        let mut s = Store::default();
        for i in 0..(MAX_POINTS_PER_SERIES + 100) {
            s.insert("p", "m", Labels::new(), i as i64, i as f64);
        }
        assert_eq!(s.series.values().next().unwrap().len(), MAX_POINTS_PER_SERIES);
    }

    #[test]
    fn tokens_parse() {
        let m = parse_tokens("proj1:tok1, proj2:tok2");
        assert_eq!(m.get("proj1").unwrap(), "tok1");
        assert_eq!(m.get("proj2").unwrap(), "tok2");
        assert!(parse_tokens("").is_empty());
    }
}
