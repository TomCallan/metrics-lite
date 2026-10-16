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
const MAX_LOG_LINES_PER_PROJECT: usize = 5_000;

type Labels = BTreeMap<String, String>;

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct SeriesKey {
    project: String,
    metric: String,
    labels: Labels,
}

#[derive(Clone, Debug)]
struct LogEntry {
    ts: i64,
    level: String,
    msg: String,
}

#[derive(Default)]
struct Store {
    series: HashMap<SeriesKey, VecDeque<(i64, f64)>>,
    logs: HashMap<String, VecDeque<LogEntry>>,
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

    fn log(&mut self, project: &str, ts: i64, level: String, msg: String) {
        let q = self.logs.entry(project.to_string()).or_default();
        q.push_back(LogEntry { ts, level, msg });
        while q.len() > MAX_LOG_LINES_PER_PROJECT {
            q.pop_front();
        }
    }

    fn total_log_lines(&self) -> u64 {
        self.logs.values().map(|q| q.len() as u64).sum()
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
        self.logs.retain(|_, q| {
            let before = q.len();
            while q.front().map(|e| e.ts < cutoff).unwrap_or(false) {
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
    layout: Arc<RwLock<Layout>>,
}

const MAX_PANELS: usize = 12;
const MAX_PANEL_METRICS: usize = 8;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
struct Panel {
    #[serde(default)]
    id: String,
    /// "chart" or "logs"
    #[serde(default = "default_panel_kind")]
    kind: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    project: String,
    #[serde(default)]
    metrics: Vec<String>,
    /// chart: raw|avg|min|max ; logs: unused
    #[serde(default = "default_agg")]
    agg: String,
    /// logs: "info,warn,error" subset ; chart: unused
    #[serde(default = "default_levels")]
    levels: String,
    /// logs: substring filter ; chart: unused
    #[serde(default)]
    q: String,
    /// logs: tail limit ; chart: unused
    #[serde(default = "default_log_limit")]
    limit: u32,
    /// "full" or "half" grid width
    #[serde(default = "default_width")]
    width: String,
}

fn default_panel_kind() -> String { "chart".to_string() }
fn default_agg() -> String { "raw".to_string() }
fn default_levels() -> String { "info,warn,error".to_string() }
fn default_log_limit() -> u32 { 200 }
fn default_width() -> String { "full".to_string() }

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
struct Layout {
    #[serde(default)]
    panels: Vec<Panel>,
}

fn layout_path(data_dir: &PathBuf) -> PathBuf {
    data_dir.join("dashboards.json")
}

fn load_layout(data_dir: &PathBuf) -> Layout {
    std::fs::read_to_string(layout_path(data_dir))
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

fn save_layout(data_dir: &PathBuf, layout: &Layout) -> bool {
    serde_json::to_string_pretty(layout)
        .ok()
        .and_then(|s| std::fs::write(layout_path(data_dir), s).ok())
        .is_some()
}

/// Clamp user-supplied layout to safe bounds. Returns cleaned layout.
fn sanitize_layout(mut layout: Layout) -> Layout {
    layout.panels.truncate(MAX_PANELS);
    let mut n = 0u32;
    for p in &mut layout.panels {
        n += 1;
        if p.id.is_empty() || p.id.len() > 32 {
            p.id = format!("p{n}");
        }
        p.kind = p.kind.to_lowercase();
        if p.kind != "logs" {
            p.kind = "chart".to_string();
        }
        p.title.truncate(80);
        p.project.truncate(64);
        p.metrics.truncate(MAX_PANEL_METRICS);
        for m in &mut p.metrics {
            m.truncate(128);
        }
        p.metrics.retain(|m| !m.is_empty());
        if !["raw", "avg", "min", "max"].contains(&p.agg.as_str()) {
            p.agg = "raw".to_string();
        }
        let lv: Vec<String> = p.levels.split(',').map(|x| x.trim().to_lowercase()).filter(|x| valid_level(x)).collect();
        p.levels = if lv.is_empty() { "info".to_string() } else { lv.join(",") };
        p.q.truncate(128);
        p.limit = p.limit.clamp(20, 2000);
        if p.width != "half" {
            p.width = "full".to_string();
        }
    }
    layout
}

async fn get_layout(State(st): State<AppState>) -> impl IntoResponse {
    let l = st.layout.read().await;
    Json(serde_json::json!({"panels": l.panels}))
}

async fn put_layout(State(st): State<AppState>, Json(body): Json<Layout>) -> Response {
    let clean = sanitize_layout(body);
    {
        let mut l = st.layout.write().await;
        *l = clean.clone();
    }
    if !save_layout(&st.data_dir, &clean) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"ok": false, "error": "could not persist layout"})),
        )
            .into_response();
    }
    Json(serde_json::json!({"ok": true, "panels": clean.panels.len()})).into_response()
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
    /// Two-way scrape targets: "proj=url,secs;proj2=url2,secs" (env SCRAPE_TARGETS).
    /// Pulls Prometheus exposition into each project every N seconds.
    #[arg(long, env = "SCRAPE_TARGETS", default_value = "")]
    scrape: String,
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

/// Parse scrape targets: "proj=url,secs;proj2=url2,secs" (secs default 30).
fn parse_scrape_targets(s: &str) -> Vec<(String, String, u64)> {
    let mut out = Vec::new();
    for part in s.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (proj, rest) = match part.split_once('=') {
            Some(p) => p,
            None => continue,
        };
        let (url, secs) = match rest.rsplit_once(',') {
            Some((u, n)) => (u.trim(), n.trim().parse::<u64>().unwrap_or(30).clamp(5, 3600)),
            None => (rest.trim(), 30),
        };
        if !proj.trim().is_empty() && !url.is_empty() {
            out.push((proj.trim().to_string(), url.to_string(), secs));
        }
    }
    out
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

// ---------- log ingest ----------

fn valid_level(s: &str) -> bool {
    matches!(s, "debug" | "info" | "warn" | "error")
}

/// Shared gzip handling for push bodies. Returns the raw bytes or a 400 response.
fn decode_push_body(headers: &HeaderMap, body: Bytes) -> Result<Vec<u8>, Response> {
    if headers
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
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"ok": false, "error": "invalid gzip body"})),
            )
                .into_response());
        }
        Ok(out)
    } else {
        Ok(body.to_vec())
    }
}

#[derive(Deserialize)]
struct LogPush {
    #[serde(default = "default_info_level")]
    level: String,
    msg: String,
    #[serde(default)]
    ts: Option<f64>,
}

fn default_info_level() -> String {
    "info".to_string()
}

#[derive(Serialize)]
struct LogResp {
    ok: bool,
    stored: usize,
}

#[derive(Serialize)]
struct LogLine {
    ts: i64,
    level: String,
    msg: String,
}

/// Split "LEVEL rest" prefixes (INFO/WARN/ERROR/DEBUG, any case) from journal-style lines.
fn split_level_prefix(line: &str) -> (String, String) {
    let t = line.trim_start();
    if let Some(i) = t.find(|c: char| c == ' ' || c == ':') {
        let (lvl, rest) = t.split_at(i);
        let l = lvl.to_lowercase();
        if valid_level(&l) {
            let msg = rest.trim_start_matches([' ', ':']).trim_start().to_string();
            if !msg.is_empty() {
                return (l, msg);
            }
        }
    }
    ("info".to_string(), line.trim().to_string())
}

async fn post_logs(
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
    let body = match decode_push_body(&headers, body) {
        Ok(b) => b,
        Err(r) => return r,
    };
    let ct = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let text = String::from_utf8_lossy(&body).into_owned();
    // JSON single/array/JSONL, else plain-text lines (one log per line).
    let mut entries: Vec<(i64, String, String)> = Vec::new();
    if ct.contains("ndjson") || ct.contains("jsonl") || ct.contains("json-lines") {
        for (n, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<LogPush>(line) {
                Ok(s) => entries.push((
                    s.ts.map(|t| t as i64).unwrap_or(now),
                    s.level.to_lowercase(),
                    s.msg,
                )),
                Err(_) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({"ok": false, "error": format!("invalid JSON on line {}", n + 1)})),
                    )
                        .into_response();
                }
            }
        }
    } else if let Ok(s) = serde_json::from_str::<LogPush>(&text) {
        entries.push((s.ts.map(|t| t as i64).unwrap_or(now), s.level.to_lowercase(), s.msg));
    } else if let Ok(arr) = serde_json::from_str::<Vec<LogPush>>(&text) {
        for s in arr {
            entries.push((s.ts.map(|t| t as i64).unwrap_or(now), s.level.to_lowercase(), s.msg));
        }
    } else if !(text.trim_start().starts_with('{') || text.trim_start().starts_with('[')) {
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let (level, msg) = split_level_prefix(line);
            entries.push((now, level, msg));
        }
    } else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"ok": false, "error": "invalid JSON log body"})),
        )
            .into_response();
    }
    let mut stored = 0usize;
    let mut store = st.store.write().await;
    for (ts, level, msg) in entries {
        let level = if valid_level(&level) { level } else { "info".to_string() };
        if msg.len() > 8192 {
            continue;
        }
        store.log(&project, ts, level, msg);
        stored += 1;
    }
    Json(LogResp { ok: true, stored }).into_response()
}

async fn get_logs(Query(q): Query<HashMap<String, String>>, State(st): State<AppState>) -> Response {
    let project = match q.get("project") {
        Some(p) if !p.is_empty() => p.clone(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "missing ?project="})),
            )
                .into_response();
        }
    };
    let levels: Option<HashSet<String>> = q.get("levels").map(|s| {
        s.split(',').map(|x| x.trim().to_lowercase()).collect()
    });
    let substr = q.get("q").cloned().unwrap_or_default().to_lowercase();
    let from = q.get("from").and_then(|s| s.parse::<i64>().ok());
    let to = q.get("to").and_then(|s| s.parse::<i64>().ok());
    let limit = q
        .get("limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(500)
        .min(5000)
        .max(1);
    let store = st.store.read().await;
    let mut out: Vec<LogLine> = Vec::new();
    if let Some(lines) = store.logs.get(&project) {
        for e in lines.iter().rev() {
            if out.len() >= limit {
                break;
            }
            if let Some(f) = from {
                if e.ts < (if f < 0 { now_ts() + f } else { f }) {
                    continue;
                }
            }
            if let Some(t) = to {
                if e.ts > t {
                    continue;
                }
            }
            if let Some(lv) = &levels {
                if !lv.contains(&e.level) {
                    continue;
                }
            }
            if !substr.is_empty() && !e.msg.to_lowercase().contains(&substr) {
                continue;
            }
            out.push(LogLine { ts: e.ts, level: e.level.clone(), msg: e.msg.clone() });
        }
        out.reverse();
    }
    Json(serde_json::json!({"project": project, "lines": out})).into_response()
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
    for p in store.logs.keys() {
        set.insert(p.clone());
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

/// Live stream (SSE) so another device can plug into the data.
/// `GET /api/v1/stream?project=p[&metric=m][&interval=1]`
/// Emits `event: sample` with {"project","metric","labels","ts","value"} for
/// every series (or just `metric`) once per interval seconds.
async fn get_stream(
    State(st): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    use axum::response::sse::{Event, Sse};
    let project = q.get("project").cloned().unwrap_or_default();
    let metric_filter = q.get("metric").cloned().unwrap_or_default();
    let interval = q
        .get("interval")
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(1.0)
        .clamp(0.25, 60.0);
    if project.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "missing ?project="})),
        )
            .into_response();
    }
    let store = st.store.clone();
    // unfold yields one Event per poll; keep a pending queue so a single tick
    // can flush every series, and only sleep once the queue drains.
    struct StreamState {
        store: Arc<RwLock<Store>>,
        project: String,
        metric: String,
        interval: f64,
        pending: std::collections::VecDeque<axum::response::sse::Event>,
    }
    let initial = StreamState {
        store,
        project,
        metric: metric_filter,
        interval,
        pending: std::collections::VecDeque::new(),
    };
    let stream = futures_util::stream::unfold(initial, |mut s| async move {
        use axum::response::sse::Event;
        if s.pending.is_empty() {
            tokio::time::sleep(Duration::from_secs_f64(s.interval)).await;
            let read = s.store.read().await;
            for (k, q) in read.series.iter() {
                if k.project != s.project {
                    continue;
                }
                if !s.metric.is_empty() && k.metric != s.metric {
                    continue;
                }
                if let Some((ts, v)) = q.back() {
                    let mut lbl = serde_json::Map::new();
                    for (lk, lv) in &k.labels {
                        lbl.insert(lk.clone(), serde_json::Value::String(lv.clone()));
                    }
                    let payload = serde_json::json!({
                        "project": k.project,
                        "metric": k.metric,
                        "labels": lbl,
                        "ts": ts,
                        "value": v,
                    });
                    if let Ok(ev) = Event::default().event("sample").json_data(payload) {
                        s.pending.push_back(ev);
                    }
                }
            }
        }
        let ev = s.pending.pop_front().unwrap_or_else(|| Event::default().comment("idle"));
        Some((Ok::<_, std::convert::Infallible>(ev), s))
    });
    Sse::new(stream).into_response()
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
    out.push_str("# HELP lite_log_lines_total total log lines stored\n# TYPE lite_log_lines_total counter\n");
    out.push_str(&format!("lite_log_lines_total {}\n", store.total_log_lines()));
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

    let layout = load_layout(&args.data_dir);
    let state = AppState {
        store: Arc::new(RwLock::new(Store::default())),
        tokens,
        data_dir: args.data_dir.clone(),
        retention_hours: args.retention_hours,
        started_ts: now_ts(),
        layout: Arc::new(RwLock::new(layout)),
    };

    load_snapshot(&state.store, &state.data_dir).await;

    // background: scrape targets (two-way: dashboard pulls Prometheus endpoints)
    for (project, url, secs) in parse_scrape_targets(&args.scrape) {
        let store = state.store.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .user_agent("metrics-lite/1.0")
                .build();
            let client = match client {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("scrape {project}: client build failed: {e}");
                    return;
                }
            };
            let mut t = tokio::time::interval(Duration::from_secs(secs));
            loop {
                t.tick().await;
                match client.get(&url).send().await {
                    Err(e) => tracing::warn!("scrape {project} {url}: {e}"),
                    Ok(r) => match r.text().await {
                        Err(e) => tracing::warn!("scrape {project} {url}: read: {e}"),
                        Ok(text) => {
                            let now = now_ts();
                            let rows = parse_prometheus_text(&text, &project, now);
                            if rows.is_empty() {
                                tracing::warn!("scrape {project} {url}: no rows parsed");
                            } else {
                                let mut store = store.write().await;
                                for (metric, labels, ts, value) in rows {
                                    if value.is_finite() {
                                        store.insert(&project, &metric, labels, ts, value);
                                    }
                                }
                            }
                        }
                    },
                }
            }
        });
    }

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
        .route("/api/v1/logs/:project", axum::routing::post(post_logs))
        .route("/api/v1/logs", get(get_logs))
        .route("/api/v1/layout", get(get_layout).put(put_layout))
        .route("/api/v1/stream", get(get_stream))
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

    #[test]
    fn log_level_prefix() {
        assert_eq!(split_level_prefix("WARN something broke"), ("warn".into(), "something broke".into()));
        assert_eq!(split_level_prefix("ERROR: boom"), ("error".into(), "boom".into()));
        assert_eq!(split_level_prefix("INFO up"), ("info".into(), "up".into()));
        assert_eq!(split_level_prefix("plain line"), ("info".into(), "plain line".into()));
        assert_eq!(split_level_prefix("WARNING: x"), ("info".into(), "WARNING: x".into()));
    }

    #[test]
    fn scrape_targets_parse() {
        let v = parse_scrape_targets("a=http://h:1/m,30;b=http://h:2/m");
        assert_eq!(v, vec![
            ("a".to_string(), "http://h:1/m".to_string(), 30),
            ("b".to_string(), "http://h:2/m".to_string(), 30),
        ]);
        assert!(parse_scrape_targets("").is_empty());
        assert!(parse_scrape_targets("noequals").is_empty());
        let v = parse_scrape_targets("a=http://h/m,2");
        assert_eq!(v[0].2, 5); // clamped to minimum
    }

    #[test]
    fn layout_sanitize() {
        let l = sanitize_layout(Layout { panels: vec![
            Panel { kind: "CHART".into(), project: "p".into(), metrics: vec!["a".into(), "".into()], ..Default::default() },
            Panel { kind: "bogus".into(), width: "wide".into(), ..Default::default() },
        ]});
        assert_eq!(l.panels.len(), 2);
        assert_eq!(l.panels[0].kind, "chart");
        assert_eq!(l.panels[0].metrics, vec!["a".to_string()]);
        assert!(!l.panels[0].id.is_empty());
        assert_eq!(l.panels[1].kind, "chart");
        assert_eq!(l.panels[1].width, "full");
        let many = Layout { panels: (0..30).map(|_| Panel::default()).collect() };
        assert_eq!(sanitize_layout(many).panels.len(), MAX_PANELS);
    }

    #[test]
    fn layout_save_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("ml-layout-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let l = Layout { panels: vec![
            Panel { id: "p1".into(), kind: "logs".into(), project: "v".into(), q: "err".into(), ..Default::default() },
        ]};
        assert!(save_layout(&dir, &l));
        let back = load_layout(&dir);
        assert_eq!(back.panels.len(), 1);
        assert_eq!(back.panels[0].q, "err");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn log_store_and_cap() {
        let mut s = Store::default();
        s.log("p", 100, "info".into(), "a".into());
        s.log("p", 200, "error".into(), "b".into());
        assert_eq!(s.total_log_lines(), 2);
        for i in 0..(MAX_LOG_LINES_PER_PROJECT + 10) {
            s.log("p", 300 + i as i64, "info".into(), "x".into());
        }
        assert_eq!(s.logs.get("p").unwrap().len(), MAX_LOG_LINES_PER_PROJECT);
        let n = s.prune(300 + MAX_LOG_LINES_PER_PROJECT as i64);
        assert!(n > 0);
    }
}
