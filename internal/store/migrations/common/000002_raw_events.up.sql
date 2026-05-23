-- Append-only event log: every MQTT message we receive, exactly as the broker
-- delivered it. raw_events is the source of truth; every other table is
-- derived from it by the projector and can be rebuilt from scratch.
--
-- Common scope because ingest writes it and the partition manager (in both
-- services) maintains its monthly partitions.

CREATE SEQUENCE IF NOT EXISTS raw_events_id_seq AS bigint;

CREATE TABLE IF NOT EXISTS raw_events (
  id          bigint      NOT NULL DEFAULT nextval('raw_events_id_seq'),
  received_at timestamptz NOT NULL DEFAULT now(),
  broker      text        NOT NULL,
  topic       text        NOT NULL,
  payload     bytea       NOT NULL,
  PRIMARY KEY (received_at, id)
) PARTITION BY RANGE (received_at);

CREATE INDEX IF NOT EXISTS raw_events_id_idx ON raw_events (id);
CREATE INDEX IF NOT EXISTS raw_events_topic_idx ON raw_events (topic, received_at DESC);

CREATE TABLE IF NOT EXISTS raw_events_default PARTITION OF raw_events DEFAULT;
