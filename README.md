# MeshBug

A diagnostic dashboard for a [MeshCore](https://meshcore.dev) LoRa mesh
network. Subscribes to one or more MQTT brokers carrying observer node
publishes, persists every observation to Postgres, decodes the MeshCore
packet headers, and surfaces the mesh's health in a real-time web UI.

Written in Go, no client-side JS framework. Reactivity comes from
[Datastar](https://data-star.dev) over Server-Sent Events; the UI chrome is
[Tabler](https://tabler.io); charts and the topology graph are
server-rendered SVG. Designed to deploy as a single binary or a Helm chart
against an existing Kubernetes cluster.

## What it shows

- **Overview** — observers online, packets/min and unique packets/min,
  channel utilization per observer.
- **Observers** — per-observer health: packets/min sparkline, noise floor,
  uptime, time since last packet.
- **Observer detail** — neighbors heard with RSSI min/avg/max + SNR, packet
  type breakdown, flood vs direct split, sequence-gap detection.
- **Neighbors** — global view of every node heard in the last 24h, with
  RSSI color coding and "heard by N observers".
- **Topology** — server-rendered SVG graph of observers and the sources
  they hear from, edge weight = packets, color = RSSI.
- **Live feed** — Datastar SSE stream of every packet as it arrives.
- **Anomalies** — rule-based detectors for silent observers, RSSI drops,
  `score != 1000`, and packet-type traffic spikes.

## Architecture at a glance

```
MQTT broker(s) ─┐
                ├─► paho.mqtt.golang ─► ingest (batch insert via pgx.CopyFrom) ─┐
                └─►                                                              ├─► Postgres
                                                                                 │     • observers / observer_status
                                                                                 │     • packet_observations (monthly partitions)
                                                                                 │     • packets_unique
                                                                                 │     • rollup_observer_{1m,1h}, rollup_neighbor_1m
                                                                                 │     • anomalies
                                                                                 ▼
                                       background workers:  rollup • anomaly • partition maintainer
                                                                                 ▼
              browser ◄── Datastar SSE patches ◄── chi + templ ◄── sse.Hub ◄─────┘
```

- Observer messages are decoded just enough to extract route, payload type,
  transport codes, and source/destination hash prefixes — no payloads are
  decrypted.
- Cross-observer dedup uses the `packet_hash` published by the observer.
  `packets_unique` carries the canonical version; `packet_observations`
  keeps every per-observer copy.
- Server time is the canonical `ts` everywhere. Observer-published
  timestamps are ignored to avoid clock-skew issues; rollups stay coherent.
- Retention: forever. The packets table is monthly-partitioned so storage
  growth doesn't degrade query performance.

## Three services (event-sourced)

MeshBug ships as one binary with three subcommands forming a write-side ⇒
read-side pipeline. They communicate only through Postgres.

```
MQTT brokers ──► [ingest] ──► raw_events  ──►  [project] ──► observers
                              (append-only)                    observer_status
                                                               packet_observations
                                                               packets_unique
                                                               rollups + anomalies
                                                                          │
                                                                  pg_notify
                                                                          ▼
                                                                       [web] ──► browser (SSE)
```

| Subcommand        | Reads MQTT | Writes raw_events | Writes derived tables | Runs HTTP | Replicas |
| ----------------- | :--------: | :---------------: | :-------------------: | :-------: | -------- |
| `meshbug ingest`  | ✓          | ✓                 |                       |           | 1+       |
| `meshbug project` |            |                   | ✓ (rollup + anomaly)  |           | **exactly 1** |
| `meshbug web`     |            |                   |                       | ✓         | 1+       |

`raw_events` is the immutable source of truth. The projector reads from it
in order, decodes each MQTT payload, and writes the derived tables. If the
projector logic ever changes (new fields, new derivations, decoder bug fix),
`meshbug project --reset` truncates every derived table and rebuilds from
`raw_events` end-to-end. The raw bytes are never modified.

For local UI development against your cluster's data, run only `mise run web`
and point `MESHBUG_DATABASE_URL` at your cluster's Postgres. Ingest and
project stay deployed and continue to advance state.

## Quickstart (local development)

Requirements: [mise](https://mise.jdx.dev), Docker, and a Bash-compatible
shell. Everything else (`go`, `templ`, `golang-migrate`, `helm`,
`golangci-lint`) is installed by `mise install` from `.mise/config.toml`.

```sh
mise install              # toolchain
mise run init-local       # scaffolds .mise/config.local.toml (gitignored)
# edit .mise/config.local.toml — fill in MESHBUG_DATABASE_URL plus your
# MQTT_BROKER / MQTT_USER / MQTT_PASSWORD if you're going to run ingest
# locally as well.

mise run pg-up            # ephemeral Postgres in Docker (skip if using a remote DB)
mise run migrate-up       # applies all scopes (common, ingest, project)

# In one terminal:
mise run ingest           # MQTT → raw_events (no derivation)

# In another terminal:
mise run project          # raw_events → derived tables + rollup + anomaly

# In a third terminal:
mise run web              # serves http://localhost:8080

# (If you ever need to rebuild every derived table from raw_events:)
mise run project-reset
```

**Developing UI against a remote Postgres:** point `MESHBUG_DATABASE_URL` at
your cluster's database (via a `kubectl port-forward` or similar) and only
run `mise run web`. Leave ingest in the cluster.

### Useful tasks

| Task | What it does |
| ---- | ------------ |
| `mise run build`         | `templ generate` + `go build -o bin/meshbug` |
| `mise run test`          | `go test ./...` |
| `mise run lint`          | `golangci-lint run` |
| `mise run fetch-assets`  | Pull pinned Tabler + Datastar files into `internal/web/static/` |
| `mise run clean-assets`  | Remove fetched assets (force re-download) |
| `mise run pg-up` / `pg-down` | Spin a local Postgres in Docker |
| `mise run psql`          | psql shell against `$MESHBUG_DATABASE_URL` |
| `mise run docker`        | `docker build -t meshbug:dev .` |
| `mise run helm-lint` / `helm-template` | Lint / render the Helm chart |

`build`, `test`, `lint`, and `run` automatically run `fetch-assets` and
`generate` first, so a fresh checkout needs nothing extra.

## Deploying to Kubernetes

Released container images and Helm charts are published to GHCR:

- Container — `ghcr.io/jleight/meshbug:<version>`
- Chart     — `oci://ghcr.io/jleight/charts/meshbug` (OCI registry)

Install:

```sh
# 1. Create the Postgres DSN secret out-of-band:
kubectl create secret generic meshbug-postgres \
  --from-literal=dsn='postgres://meshbug:pw@db.host:5432/meshbug?sslmode=require'

# 2. Create one secret per MQTT broker:
kubectl create secret generic meshbug-broker-home \
  --from-literal=username=core-scope \
  --from-literal=password=...

# 3. Install the chart from the OCI registry:
helm install meshbug oci://ghcr.io/jleight/charts/meshbug \
  --version 0.1.0 \
  --namespace meshbug --create-namespace \
  --set 'ingress.host=meshbug.example.com' \
  --set 'brokers[0].name=home' \
  --set 'brokers[0].url=wss://mqtt.example.com:443' \
  --set 'brokers[0].existingSecret=meshbug-broker-home'
```

Each chart release defaults `image.tag` to its own `appVersion`, so bumping
the chart version pulls the matching container image. [Renovate][renovate]
tracks both the OCI chart and the container via its `helmv3` and `docker`
managers, so a single config in your gitops repo keeps everything current.

The chart deploys a single replica — rollup workers assume one writer.
Scale horizontally only by sharding observers across instances.

[renovate]: https://docs.renovatebot.com/modules/manager/helmv3/

### Releasing

There is **no manual versioning**. The `release` workflow computes a
date-based, auto-incrementing version of the form `{YEAR}.{MONTH}.{BUILD}`:

- Every push to `main` publishes a release: container `:{version}` + `:latest`,
  packaged Helm chart pushed to `oci://ghcr.io/jleight/charts/meshbug`,
  and a GitHub Release with the chart `.tgz` attached.
- Every pull request from this repo's own branches publishes a container
  tagged `:{version}-beta` for testing in-cluster (no chart, no release).
  Pull requests from forks build the container but do not push it (forks
  don't have GHCR write access).

`{BUILD}` is the Nth run of the release workflow this calendar month,
computed by querying the GitHub Actions API at runtime — so it resets to 1
on the first build of each new month. Example progression:
`2026.5.41-beta` (PR) → `2026.5.42` (merge to main) → `2026.6.1` (first build in June).

## Configuration

All knobs are environment variables:

| Variable | Used by | Purpose |
| -------- | ------- | ------- |
| `MESHBUG_DATABASE_URL` | both    | Postgres DSN. Required. |
| `MESHBUG_LOG_LEVEL`    | both    | `debug` / `info` / `warn` / `error`. Default `info`. |
| `MESHBUG_HTTP_ADDR`    | web     | Listen address. Default `:8080`. |
| `MESHBUG_AUTO_MIGRATE` | ingest  | Apply embedded migrations at startup. Default `true`. |
| `MESHBUG_BROKERS_JSON` | ingest  | Inline JSON broker list — see `internal/config/config.go`. |
| `MESHBUG_BROKERS_CONFIG` | ingest | Path to a JSON broker file (used by the Helm ConfigMap). |
| `MESHBUG_BROKER_<NAME>_USERNAME` / `_PASSWORD` | ingest | Per-broker credentials, merged into the structural list at startup. |
| `MQTT_BROKER` / `MQTT_USER` / `MQTT_PASSWORD` | ingest | Convenience: defines a single broker named `default` if no JSON is provided. |

## Project layout

```
cmd/meshbug/                # entrypoint: dispatches `ingest` and `web` subcommands
internal/
  config/                   # env-driven config loader
  mqtt/                     # paho MQTT manager (1..N brokers)
  meshcore/                 # MeshCore wire-format header decoder
  ingest/                   # MQTT → raw_events (append-only, no decoding)
  project/                  # raw_events → derived tables; cursor + reset support
  notify/                   # cross-process pub/sub via Postgres LISTEN/NOTIFY
  store/                    # pgx pool, queries, embedded migrations, partitions
  rollup/                   # 1m and 1h aggregation workers
  anomaly/                  # rule-based detectors
  sse/                      # in-process pub/sub hub
  web/                      # chi router, handlers, SSE writer
    templates/              # templ templates (compiled at build time)
    static/                 # vendored Tabler + Datastar (fetched, not committed)
deploy/helm/meshbug/        # Helm chart
.mise/                      # tools, env, and tasks (incl. file task fetch-assets)
```

## License

Source-available under the [Elastic License 2.0](./LICENSE). You may run,
modify, and distribute MeshBug, but you may not provide it as a hosted or
managed service that gives third parties access to a substantial set of
its features.

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](./CONTRIBUTING.md)
and the [Contributor License Agreement](./CLA.md). A bot will ask you to
sign the CLA on your first PR by replying to its comment — **once per
contributor**, not per PR or per commit. Subsequent PRs from the same
GitHub account auto-pass.
