-- The ingester now stamps received_at at the MQTT callback instant rather
-- than relying on the column default at INSERT time (which was the batch
-- flush time, off by up to ~250 ms from actual MQTT delivery). Drop the
-- default so a missing value becomes an error rather than silently using
-- now().

ALTER TABLE raw_events ALTER COLUMN received_at DROP DEFAULT;
