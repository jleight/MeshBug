-- advert_received_at: the earliest received_at across every observation
-- of the advert with timestamp = nodes.advert_timestamp. Updated
-- whenever a fresher advert lands (reset to that observation's ts) or
-- another observer re-reports the current advert (kept at the minimum).

ALTER TABLE project.nodes
  ADD COLUMN IF NOT EXISTS advert_received_at timestamptz;
