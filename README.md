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

## Quickstart (local development)

Requirements: [mise](https://mise.jdx.dev), Docker, and a Bash-compatible
shell. Everything else (`go`, `templ`, `golang-migrate`, `helm`,
`golangci-lint`) is installed by `mise install` from `.mise/config.toml`.

```sh
mise install              # toolchain
mise run init-local       # scaffolds .mise/config.local.toml (gitignored)
# edit .mise/config.local.toml — fill in your broker URL, user, password,
# and a MESHBUG_DATABASE_URL.

mise run pg-up            # ephemeral Postgres in Docker
mise run migrate-up
mise run run              # builds + runs against your live broker
```

Open <http://localhost:8080>. New observer status rows should appear within
~30 seconds and packets shortly after.

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

| Variable | Purpose |
| -------- | ------- |
| `MESHBUG_HTTP_ADDR` | Listen address. Default `:8080`. |
| `MESHBUG_DATABASE_URL` | Postgres DSN. Required. |
| `MESHBUG_AUTO_MIGRATE` | Apply embedded migrations at startup. Default `true`. |
| `MESHBUG_LOG_LEVEL` | `debug` / `info` / `warn` / `error`. Default `info`. |
| `MESHBUG_BROKERS_JSON` | Inline JSON broker list — see `internal/config/config.go`. |
| `MESHBUG_BROKERS_CONFIG` | Path to a JSON broker file (used by the Helm ConfigMap). |
| `MESHBUG_BROKER_<NAME>_USERNAME` / `_PASSWORD` | Per-broker credentials. Merged into the structural list at startup. |
| `MQTT_BROKER` / `MQTT_USER` / `MQTT_PASSWORD` | Convenience: defines a single broker named `default` if no JSON is provided. |

## Project layout

```
cmd/meshbug/                # entrypoint
internal/
  config/                   # env-driven config loader
  mqtt/                     # paho MQTT manager (1..N brokers)
  meshcore/                 # MeshCore wire-format header decoder
  ingest/                   # MQTT → typed rows → batched pgx.CopyFrom
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
