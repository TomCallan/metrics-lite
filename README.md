# metrics-lite

Single-binary, lightweight Grafana replacement (v1: push + charts).
Multi-project push ingest, Prometheus scrape compat, and a zero-config web chart UI.
Far lighter than prometheus (512 MB limit) + grafana (512 MB limit):
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
```

Open **http://host:3100** → select project + metric → chart. No config needed.

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
| `/` | GET | embedded chart UI (single HTML, Chart.js CDN + offline fallback) |
| `/healthz` | GET | `{"ok":true}` |
| `/metrics` | GET | self + federated: `lite_up`, `lite_series_count`, `lite_push_points_total`, `lite_uptime_seconds`, `lite_push_value{project,metric,...}` |
| `/api/v1/push/:project` | POST | JSON or Prometheus-text (auto-detect; text/plain forces text parse). Token via `X-API-Token` header or `?token=` if configured |
| `/api/v1/query` | GET | `?project=&metric=&from=&to=` + label filters; `series` per label-set + merged `points` |
| `/api/v1/projects` | GET | `{"projects":[...]}` |
| `/api/v1/metrics?project=` | GET | `{"metrics":[...]}` |

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

## Migration notes

Point push clients (e.g. `velotrade-rs`, `drivecost`) at it instead of running a
Prometheus client port per app:

```bash
# instead of :9101/metrics scrape only, also push live values:
curl -X POST http://host:3100/api/v1/push/velotrade-bot \
  -H 'Content-Type: application/json' \
  -d '{"metric":"feed_mid","value":2650.1,"labels":{"symbol":"XAU"}}'
```

During migration, keep the existing Prometheus scraping working by adding a scrape
target for metrics-lite — it re-exposes every stored latest value:

```yaml
# /opt/infra/monitoring/targets/metrics-lite.json
[{"targets": ["host.docker.internal:3100"], "labels": {"app": "metrics-lite"}}]
```

`lite_push_value{project="velotrade-bot",metric="feed_mid",symbol="XAU"}` then shows
up in Prometheus/Grafana alongside native `feed_*` series until you cut over.

Port table:

| Port | Service |
|---|---|
| 3000 | grafana (legacy, to retire) |
| 9090 | prometheus (legacy, scrape during migration) |
| 91xx | per-app `/metrics` (e.g. 9101 velotrade-bot `feed_*`) |
| **3100** | **metrics-lite UI + push API + `/metrics` for scraping** |

## Memory comparison

| Stack | Idle RSS |
|---|---|
| prometheus (limit) | 512 MB |
| grafana (limit) | 512 MB |
| **metrics-lite (release, measured)** | **~5 MB** |

Cap is per-series (50k points ≈ <2 MB worst case per hot series); retention prune
(24 h default) bounds idle growth. No embedded DB, no JS build step.
