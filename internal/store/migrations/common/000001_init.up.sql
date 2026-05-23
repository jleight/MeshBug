-- Tables that both the ingest and project services touch.
--
-- _partition_state is consulted by the in-process partition manager which
-- runs in both ingest (maintaining raw_events monthly partitions) and project
-- (maintaining packet_observations monthly partitions).
CREATE TABLE IF NOT EXISTS _partition_state (
  partition_name text        PRIMARY KEY,
  range_start    timestamptz NOT NULL,
  range_end      timestamptz NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now()
);
