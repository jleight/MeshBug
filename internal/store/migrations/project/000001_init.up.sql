-- Projector (read-model) schema. All tables here are derived from
-- `ingest.raw_events`; `meshbug project --reset` truncates and rebuilds them.
--
-- We also create the `web` schema here even though no tables live there yet —
-- web-owned state (sessions, user prefs, etc.) will go there in future
-- migrations, and we want the schema present from day one so deploys don't
-- have to coordinate schema creation with feature rollouts.

CREATE SCHEMA IF NOT EXISTS project;
CREATE SCHEMA IF NOT EXISTS web;

-- Partition state for project-owned partitioned tables (packet_observations).
CREATE TABLE IF NOT EXISTS project._partition_state (
  partition_name text        PRIMARY KEY,
  range_start    timestamptz NOT NULL,
  range_end      timestamptz NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now()
);

-- Projector cursor (highest ingest.raw_events.id this projection has processed).
CREATE TABLE IF NOT EXISTS project.projector_state (
  name           text        PRIMARY KEY,
  last_event_id  bigint      NOT NULL DEFAULT 0,
  updated_at     timestamptz NOT NULL DEFAULT now()
);
INSERT INTO project.projector_state(name) VALUES ('default')
  ON CONFLICT DO NOTHING;

-- Physical observer nodes that publish to MQTT.
CREATE TABLE IF NOT EXISTS project.observers (
  id                bytea       PRIMARY KEY,
  origin_name       text        NOT NULL,
  region            text,
  model             text,
  firmware_version  text,
  client_version    text,
  source            text,
  radio_freq_khz    int,
  radio_bw_khz      numeric(10,3),
  radio_sf          int,
  radio_cr          int,
  first_seen        timestamptz NOT NULL,
  last_seen         timestamptz NOT NULL
);

-- /status time series.
CREATE TABLE IF NOT EXISTS project.observer_status (
  observer_id   bytea       NOT NULL REFERENCES project.observers(id) ON DELETE CASCADE,
  ts            timestamptz NOT NULL,
  status        text        NOT NULL,
  uptime_secs   bigint,
  battery_mv    int,
  queue_len     int,
  noise_floor   int,
  tx_air_secs   bigint,
  rx_air_secs   bigint,
  recv_errors   bigint,
  last_rssi     int,
  last_snr      numeric(6,2),
  debug_flags   bigint,
  PRIMARY KEY (observer_id, ts)
);

CREATE INDEX IF NOT EXISTS observer_status_ts_idx
  ON project.observer_status (ts DESC);

-- packet_observations: one row per (observer, packet) — monthly-partitioned.
CREATE SEQUENCE IF NOT EXISTS project.packet_observations_id_seq AS bigint;

CREATE TABLE IF NOT EXISTS project.packet_observations (
  ts              timestamptz NOT NULL,
  observation_id  bigint      NOT NULL DEFAULT nextval('project.packet_observations_id_seq'),
  observer_id     bytea       NOT NULL,
  packet_hash     bytea,
  direction       text        NOT NULL,
  packet_type     text,
  route           text,
  len             int,
  payload_len     int,
  rssi            int,
  snr             numeric(6,2),
  score           int,
  duration_ms     int,
  raw             bytea,
  decoded_type    smallint,
  source_prefix   bytea,
  dest_prefix     bytea,
  transport_codes bytea,
  PRIMARY KEY (ts, observation_id)
) PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS packet_observations_observer_ts_idx
  ON project.packet_observations (observer_id, ts DESC);
CREATE INDEX IF NOT EXISTS packet_observations_hash_idx
  ON project.packet_observations (packet_hash) WHERE packet_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS packet_observations_source_ts_idx
  ON project.packet_observations (source_prefix, ts DESC) WHERE source_prefix IS NOT NULL;

CREATE TABLE IF NOT EXISTS project.packet_observations_default
  PARTITION OF project.packet_observations DEFAULT;

-- One row per unique packet hash across the mesh.
CREATE TABLE IF NOT EXISTS project.packets_unique (
  packet_hash    bytea       PRIMARY KEY,
  first_seen     timestamptz NOT NULL,
  last_seen      timestamptz NOT NULL,
  packet_type    text,
  route          text,
  decoded_type   smallint,
  source_prefix  bytea,
  observer_count int         NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS packets_unique_last_seen_idx
  ON project.packets_unique (last_seen DESC);
CREATE INDEX IF NOT EXISTS packets_unique_source_idx
  ON project.packets_unique (source_prefix, last_seen DESC) WHERE source_prefix IS NOT NULL;

-- Rollups.
CREATE TABLE IF NOT EXISTS project.rollup_observer_1m (
  observer_id  bytea       NOT NULL,
  bucket       timestamptz NOT NULL,
  packets      int         NOT NULL,
  unique_pkts  int         NOT NULL,
  flood_pkts   int         NOT NULL,
  direct_pkts  int         NOT NULL,
  avg_rssi     numeric(6,2),
  min_rssi     int,
  max_rssi     int,
  avg_snr      numeric(6,2),
  noise_floor  int,
  PRIMARY KEY (observer_id, bucket)
);
CREATE INDEX IF NOT EXISTS rollup_observer_1m_bucket_idx
  ON project.rollup_observer_1m (bucket DESC);

CREATE TABLE IF NOT EXISTS project.rollup_observer_1h (
  observer_id  bytea       NOT NULL,
  bucket       timestamptz NOT NULL,
  packets      int         NOT NULL,
  unique_pkts  int         NOT NULL,
  flood_pkts   int         NOT NULL,
  direct_pkts  int         NOT NULL,
  avg_rssi     numeric(6,2),
  min_rssi     int,
  max_rssi     int,
  avg_snr      numeric(6,2),
  noise_floor  int,
  PRIMARY KEY (observer_id, bucket)
);
CREATE INDEX IF NOT EXISTS rollup_observer_1h_bucket_idx
  ON project.rollup_observer_1h (bucket DESC);

CREATE TABLE IF NOT EXISTS project.rollup_neighbor_1m (
  observer_id   bytea       NOT NULL,
  source_prefix bytea       NOT NULL,
  bucket        timestamptz NOT NULL,
  packets       int         NOT NULL,
  avg_rssi      numeric(6,2),
  min_rssi      int,
  max_rssi      int,
  avg_snr       numeric(6,2),
  PRIMARY KEY (observer_id, source_prefix, bucket)
);
CREATE INDEX IF NOT EXISTS rollup_neighbor_1m_bucket_idx
  ON project.rollup_neighbor_1m (bucket DESC);
CREATE INDEX IF NOT EXISTS rollup_neighbor_1m_source_idx
  ON project.rollup_neighbor_1m (source_prefix, bucket DESC);

-- Anomaly events surfaced in the UI.
CREATE TABLE IF NOT EXISTS project.anomalies (
  id          bigserial   PRIMARY KEY,
  ts          timestamptz NOT NULL,
  kind        text        NOT NULL,
  subject_id  bytea,
  severity    text        NOT NULL,
  details     jsonb       NOT NULL,
  resolved_at timestamptz
);
CREATE INDEX IF NOT EXISTS anomalies_ts_idx
  ON project.anomalies (ts DESC);
CREATE INDEX IF NOT EXISTS anomalies_kind_subject_idx
  ON project.anomalies (kind, subject_id, ts DESC);
