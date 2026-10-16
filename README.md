# metrics-lite

Single-binary, lightweight Grafana replacement (v1: push + charts + scrape).
Multi-project push ingest, Prometheus scrape compat, and a zero-config web chart UI.
Far lighter than a Prometheus + Grafana pair on a small box:
**~5 MB RSS idle, 4.3 MB binary.**

## Build

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo build --release        # -> target/release/metrics-lite (4.3 MB)
cargo test                   # 8 unit tests
```

## Run

```bash
./target/release/metrics-lite --port 3100 --bind 0.0.0.0 --data-dir ./data --retention-hours 24
# env equivalents: PORT BIND DATA_DIR RETENTION_HOURS METRICS_TOKENS
# tokens: --tokens "proj1:tok1,proj2:tok2"  (or METRICS_TOKENS; @/path/to/file; METRICS_TOKENS_FILE)
# scrape (two-way): --scrape "proj=http://127.0.0.1:9103/metrics,30;other=http://127.0.0.1:9200/metrics,60"
#   pulls Prometheus exposition into a project every N seconds (or SCRAPE_TARGETS)
```

Open **http://host:3100** → a customizable board of panels.

- **Panels**: add `+ chart`, `+ logs`, or `+ both` (chart above, log tail below).
  Each panel has its own project, multi-metric overlay, raw/min/max/avg
  aggregation, log level/filter/limit, title, and full/half width.
- **Always-visible controls**: the top bar edits the *selected* panel (click a
  panel to select). Project, metrics, aggregation, and time range are one click
  away — no edit mode to find.
- **Drag & drop**: drag a panel header to reorder.
- **Themes**: light/dark toggle (persisted; `?theme=light` also works).
- **Persistence**: layout is stored server-side (`data/dashboards.json`) and
  appears on every device; log lines persist across restarts too.
- Click a metric in the multi-select with ctrl/cmd for overlays.

## Remote access from another device

The server binds `0.0.0.0:3100`, so any device that can route to it (same LAN,
VPN/tailnet) can read and write. Use the host's routable address:

```bash
export ML_HOST=http://<host-address>:3100        # e.g. a tailnet IP
scripts/mlplug projects
scripts/mlplug query myproject cpu_percent 900
scripts/mlplug stream myproject cpu_percent      # live SSE tail
scripts/mlplug logs myproject warn,error 100
scripts/mlplug push myproject cpu_percent 42
```

Raw endpoints for your own clients:

```bash
curl "$ML_HOST/api/v1/query?project=myproject&metric=cpu_percent&from=-900"
curl -N "$ML_HOST/api/v1/stream?project=myproject&interval=1"   # SSE live
```

Set `--tokens "proj:secret,..."` (or `METRICS_TOKENS`) to require
`X-API-Token` / `?token=` per project when exposing beyond a trusted network.

## Push examples

```bash
# JSON push (single sample; array of samples also accepted)
curl -X POST http://host:3100/api/v1/push/demo \
  -H 'Content-Type: application/json' \
  -d '{"metric":"live_equity","value":14726.78,"labels":{"symbol":"XAU"}}'
# -> {"ok":true,"stored":1}

# with optional ts (epoch secs, default now)
curl -X POST http://host:3100/api/v1/push/demo \
  -H 'Content-Type: application/json' \
  -d '{"metric":"live_equity","value":14726.78,"labels":{"symbol":"XAU"},"ts":1720000000}'

# Prometheus-text push (Content-Type: text/plain; ignores # HELP/TYPE)
printf 'live_up 1\nfeed_mid{symbol="XAU"} 2650.1\n' | \
  curl -X POST http://host:3100/api/v1/push/demo \
    -H 'Content-Type: text/plain' --data-binary @-
# -> {"ok":true,"stored":2}

# with per-project token (only required if token configured for that project)
curl -X POST 'http://host:3100/api/v1/push/sec?token=tok123' \
  -H 'Content-Type: application/json' -d '{"metric":"m","value":1}'
curl -X POST http://host:3100/api/v1/push/sec \
  -H 'Content-Type: application/json' -H 'X-API-Token: tok123' \
  -d '{"metric":"m","value":1}'
```

## Bulk pipe (large data)

```bash
# easiest: pipe anything through scripts/mlpush (auto format + auto gzip)
some-producer | ./scripts/mlpush myproject
mlpush myproject --format ndjson < bulk.jsonl

# raw forms: JSONL (one object per line) or gzip of any body
cat bulk.jsonl | curl -X POST http://host:3100/api/v1/push/demo \
  -H 'Content-Type: application/x-ndjson' --data-binary @-
gzip -c batch.json | curl -X POST http://host:3100/api/v1/push/demo \
  -H 'Content-Type: application/json' -H 'Content-Encoding: gzip' --data-binary @-
```

Notes: no body-size cap on `/api/v1/push` (local daemon); gzip typically 10x
(measured: 100k points, 8.3MB -> 564KB, stored in 0.26s). Per-series ring
holds the latest 50k points; retention prunes past 24h.

## Logs

```bash
# JSON / JSONL ({"level":"info|warn|error|debug","msg":"...","ts"?}) or plain text
curl -X POST http://host:3100/api/v1/logs/demo \
  -H 'Content-Type: application/json' -d '{"level":"error","msg":"order rejected"}'
printf 'WARN tick gap\nplain line\n' | curl -X POST http://host:3100/api/v1/logs/demo --data-binary @-
# gzip + tokens work the same as /api/v1/push

# query: levels=info,warn,error q=<substring> from/to limit=<1..5000>
curl 'http://host:3100/api/v1/logs?project=demo&levels=warn,error&q=gap&limit=200'
```

UI: `charts | logs` tabs — level checkboxes, text filter, pause, tail view.
`scripts/push-journal-logs <unit> <project>` forwards a systemd unit's journal
(cursor-based, 30s timer pattern; see `apps-velotrade-logs.timer`). Logs are
in-memory only (5k lines/project, same 24h prune) — not in snapshots.

## Query examples

```bash
curl 'http://host:3100/api/v1/query?project=demo&metric=live_equity&from=-3600'
# from/to: epoch secs; negative from = relative (now-3600). Extra params filter labels:
curl 'http://host:3100/api/v1/query?project=demo&metric=feed_mid&symbol=XAU'
# -> {"project":..,"metric":..,"points":[[ts,val],...],"series":[{"labels":{..},"points":[...]}]}

curl http://host:3100/api/v1/projects
curl 'http://host:3100/api/v1/metrics?project=demo'
curl http://host:3100/healthz   # {"ok":true}
curl http://host:3100/metrics   # Prometheus exposition (see below)
```

## API reference

| Endpoint | Method | Notes |
|---|---|---|
| `/` | GET | embedded panel board UI (single HTML, ECharts CDN + offline fallback) |
| `/healthz` | GET | `{"ok":true}` |
| `/metrics` | GET | self + federated: `lite_up`, `lite_series_count`, `lite_push_points_total`, `lite_uptime_seconds`, `lite_push_value{project,metric,...}` |
| `/api/v1/push/:project` | POST | JSON or Prometheus-text (auto-detect; text/plain forces text parse). Token via `X-API-Token` header or `?token=` if configured |
| `/api/v1/query` | GET | `?project=&metric=&from=&to=` + label filters; `series` per label-set + merged `points` |
| `/api/v1/projects` | GET | `{"projects":[...]}` |
| `/api/v1/metrics?project=` | GET | `{"metrics":[...]}` |
| `/api/v1/logs/:project` | POST | log lines: JSON/JSONL/plain text (level-prefix sniffed). Same gzip/token handling |
| `/api/v1/logs` | GET | `?project=&levels=warn,error&q=<substr>&from=&to=&limit=` |
| `/api/v1/layout` | GET/PUT | dashboard panel layout (`{"panels":[...]}`), persisted to `$DATA_DIR/dashboards.json` |
| `/api/v1/stream` | GET | SSE live latest values: `?project=&metric=&interval=` |

Storage: in-memory `RwLock<HashMap<series, VecDeque<(ts,val)>>>`, cap 50k points/series
(oldest evicted), retention prune every 60 s (`--retention-hours`, 0 = off),
periodic JSONL snapshot to `$DATA_DIR/<project>.jsonl` every 60 s, loaded on start.

## systemd unit

```ini
[Unit]
Description=metrics-lite
After=network.target

[Service]
ExecStart=/opt/apps/metrics-lite/target/release/metrics-lite --port 3100 --bind 0.0.0.0 --data-dir /opt/apps/metrics-lite/data
Environment=METRICS_TOKENS=proj1:tok1,proj2:tok2
Restart=always
WorkingDirectory=/opt/apps/metrics-lite

[Install]
WantedBy=multi-user.target
```

## Customizable dashboards

The UI is a board of panels (chart or logs), persisted server-side:
`GET/PUT /api/v1/layout`. Each panel has its own project, metrics (multi),
aggregation, width (full/half), and order. Layout survives restarts and shows
up on every device that opens the UI. Bad input is clamped server-side
(max 12 panels, 8 metrics/panel, valid levels only).

## Replacing Prometheus/Grafana

Point push clients at metrics-lite instead of running a Prometheus client
port per app:

```bash
curl -X POST http://host:3100/api/v1/push/myapp \
  -H 'Content-Type: application/json' \
  -d '{"metric":"feed_mid","value":2650.1,"labels":{"symbol":"XAU"}}'
```

Two-way options per source — pick one:

- **Push** (shown above, plus `scripts/mlpush` for pipes and
  `scripts/push-journal-logs` for journals).
- **Scrape**: point metrics-lite at an existing Prometheus `/metrics`
  endpoint and it pulls on an interval (see `--scrape` under Run).

During migration, existing Prometheus setups can scrape metrics-lite itself —
it re-exposes every stored latest value as
`lite_push_value{project="..",metric="..",...}`.

## Memory comparison

| Stack | Idle RSS |
|---|---|
| prometheus (limit) | 512 MB |
| grafana (limit) | 512 MB |
| **metrics-lite (release, measured)** | **~5 MB** |

Cap is per-series (50k points ≈ <2 MB worst case per hot series); retention prune
(24 h default) bounds idle growth. No embedded DB, no JS build step.
