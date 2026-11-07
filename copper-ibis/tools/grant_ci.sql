-- Unity Catalog grants for the CI service principal.
--
-- The SQL equivalent of terraform/grants.tf. Replace :ci_principal below with the
-- service principal's APPLICATION ID (the UUID printed by tools/setup_oidc.sh), then run:
--
--   python tools/run_sql.py --profile wind --warehouse <id> --file tools/grant_ci.sql
--
-- Note the privilege names use underscores (CREATE_TABLE, not "CREATE TABLE").
--
-- Why per-schema rather than one catalog-level grant: a catalog grant does not imply
-- create rights on each schema, and a missing schema grant surfaces as a confusing
-- "dataset does not exist" error rather than a permission error.

-- Catalog: traversal only. CREATE_SCHEMA is deliberately withheld so a typo'd schema
-- name in a dataset fails loudly instead of quietly creating a new schema.
GRANT USE_CATALOG ON CATALOG wind_dev TO `:ci_principal`;

-- Schemas the pipeline publishes into.
GRANT USE_SCHEMA, CREATE_TABLE, CREATE_MATERIALIZED_VIEW, MODIFY, SELECT
  ON SCHEMA wind_dev.bronze TO `:ci_principal`;

GRANT USE_SCHEMA, CREATE_TABLE, CREATE_MATERIALIZED_VIEW, MODIFY, SELECT
  ON SCHEMA wind_dev.silver TO `:ci_principal`;

GRANT USE_SCHEMA, CREATE_TABLE, CREATE_MATERIALIZED_VIEW, MODIFY, SELECT
  ON SCHEMA wind_dev.gold TO `:ci_principal`;

-- The pipeline event log is published here.
GRANT USE_SCHEMA, CREATE_TABLE, CREATE_MATERIALIZED_VIEW, MODIFY, SELECT
  ON SCHEMA wind_dev.ops TO `:ci_principal`;

-- The raw schema holds the landing Volume; the pipeline reads from it but never
-- creates tables there.
GRANT USE_SCHEMA ON SCHEMA wind_dev.raw TO `:ci_principal`;

-- CI writes seed files into the Volume for the integration test, so it needs write
-- as well as read.
GRANT READ_VOLUME, WRITE_VOLUME ON VOLUME wind_dev.raw.landing TO `:ci_principal`;
