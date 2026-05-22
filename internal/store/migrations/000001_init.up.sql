-- observers: physical MQTT-publishing observer nodes
CREATE TABLE observers (
  id                bytea       PRIMARY KEY,                  -- sha256(32 bytes)
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

-- /status time series
CREATE TABLE observer_status (
  observer_id   bytea       NOT NULL REFERENCES observers(id) ON DELETE CASCADE,
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

CREATE INDEX observer_status_ts_idx ON observer_status (ts DESC);

-- packet_observations: one row per (observer, packet). Range-partitioned by ts (monthly).
CREATE TABLE packet_observations (
  ts              timestamptz NOT NULL,
  observation_id  bigint      NOT NULL,
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
  duration_ms    int,
  raw             bytea,
  decoded_type    smallint,
  source_prefix   bytea,
  dest_prefix     bytea,
  transport_codes bytea,
  PRIMARY KEY (ts, observation_id)
) PARTITION BY RANGE (ts);

CREATE SEQUENCE packet_observations_id_seq AS bigint;
ALTER TABLE packet_observations
  ALTER COLUMN observation_id SET DEFAULT nextval('packet_observations_id_seq');

CREATE INDEX packet_observations_observer_ts_idx
  ON packet_observations (observer_id, ts DESC);
CREATE INDEX packet_observations_hash_idx
  ON packet_observations (packet_hash) WHERE packet_hash IS NOT NULL;
CREATE INDEX packet_observations_source_ts_idx
  ON packet_observations (source_prefix, ts DESC) WHERE source_prefix IS NOT NULL;

-- default partition catches any out-of-range writes (e.g. clock skew); reconciled into
-- the proper monthly partition later if needed.
CREATE TABLE packet_observations_default
  PARTITION OF packet_observations DEFAULT;

-- one row per unique packet hash across the whole mesh
CREATE TABLE packets_unique (
  packet_hash    bytea       PRIMARY KEY,
  first_seen     timestamptz NOT NULL,
  last_seen      timestamptz NOT NULL,
  packet_type    text,
  route          text,
  decoded_type   smallint,
  source_prefix  bytea,
  observer_count int         NOT NULL DEFAULT 1
);

CREATE INDEX packets_unique_last_seen_idx ON packets_unique (last_seen DESC);
CREATE INDEX packets_unique_source_idx
  ON packets_unique (source_prefix, last_seen DESC) WHERE source_prefix IS NOT NULL;

-- rollups
CREATE TABLE rollup_observer_1m (
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
CREATE INDEX rollup_observer_1m_bucket_idx ON rollup_observer_1m (bucket DESC);

CREATE TABLE rollup_observer_1h (
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
CREATE INDEX rollup_observer_1h_bucket_idx ON rollup_observer_1h (bucket DESC);

CREATE TABLE rollup_neighbor_1m (
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
CREATE INDEX rollup_neighbor_1m_bucket_idx ON rollup_neighbor_1m (bucket DESC);
CREATE INDEX rollup_neighbor_1m_source_idx ON rollup_neighbor_1m (source_prefix, bucket DESC);

-- anomalies
CREATE TABLE anomalies (
  id          bigserial   PRIMARY KEY,
  ts          timestamptz NOT NULL,
  kind        text        NOT NULL,
  subject_id  bytea,
  severity    text        NOT NULL,
  details     jsonb       NOT NULL,
  resolved_at timestamptz
);
CREATE INDEX anomalies_ts_idx ON anomalies (ts DESC);
CREATE INDEX anomalies_kind_subject_idx ON anomalies (kind, subject_id, ts DESC);

-- tracking which monthly partitions exist
CREATE TABLE _partition_state (
  partition_name text        PRIMARY KEY,
  range_start    timestamptz NOT NULL,
  range_end      timestamptz NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now()
);
