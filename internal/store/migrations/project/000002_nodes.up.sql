-- Nodes seen on the mesh, derived from ADVERT packets. One row per
-- ed25519 public key. Updated by the Nodes projector stage every time it
-- sees a new advert with a stricter timestamp than the one already on
-- file. Stale rows are retained — last_seen tells you when we last heard
-- from the node.

CREATE TABLE IF NOT EXISTS project.nodes (
  public_key       bytea       PRIMARY KEY,
  prefix1          bytea       NOT NULL,
  prefix4          bytea       NOT NULL,
  node_type        text        NOT NULL,
  name             text        NOT NULL DEFAULT '',
  has_lat_lon      boolean     NOT NULL DEFAULT false,
  lat_e6           int,
  lon_e6           int,
  has_feat1        boolean     NOT NULL DEFAULT false,
  feat1            int,
  has_feat2        boolean     NOT NULL DEFAULT false,
  feat2            int,
  advert_timestamp timestamptz,
  advert_count     bigint      NOT NULL DEFAULT 0,
  first_seen       timestamptz NOT NULL,
  last_seen        timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS nodes_prefix1_idx
  ON project.nodes (prefix1);
CREATE INDEX IF NOT EXISTS nodes_prefix4_idx
  ON project.nodes (prefix4);
CREATE INDEX IF NOT EXISTS nodes_last_seen_idx
  ON project.nodes (last_seen DESC);
CREATE INDEX IF NOT EXISTS nodes_type_idx
  ON project.nodes (node_type);
CREATE INDEX IF NOT EXISTS nodes_name_idx
  ON project.nodes (lower(name));
