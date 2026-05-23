-- Ingest schema: the append-only event log captured from MQTT. Every other
-- table in MeshBug is derived from `ingest.raw_events` and can be rebuilt
-- from scratch.
--
-- `ingest._partition_state` is consulted by the in-process partition manager
-- in the ingest service to maintain monthly partitions on raw_events.

CREATE SCHEMA IF NOT EXISTS ingest;

CREATE TABLE IF NOT EXISTS ingest._partition_state (
  partition_name text        PRIMARY KEY,
  range_start    timestamptz NOT NULL,
  range_end      timestamptz NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS ingest.raw_events_id_seq AS bigint;

-- received_at is stamped at the MQTT callback instant by the ingester; we
-- don't want a column default silently masking a missing value at INSERT.
CREATE TABLE IF NOT EXISTS ingest.raw_events (
  id          bigint      NOT NULL DEFAULT nextval('ingest.raw_events_id_seq'),
  received_at timestamptz NOT NULL,
  broker      text        NOT NULL,
  topic       text        NOT NULL,
  payload     bytea       NOT NULL,
  PRIMARY KEY (received_at, id)
) PARTITION BY RANGE (received_at);

CREATE INDEX IF NOT EXISTS raw_events_id_idx
  ON ingest.raw_events (id);
CREATE INDEX IF NOT EXISTS raw_events_topic_idx
  ON ingest.raw_events (topic, received_at DESC);

CREATE TABLE IF NOT EXISTS ingest.raw_events_default
  PARTITION OF ingest.raw_events DEFAULT;
