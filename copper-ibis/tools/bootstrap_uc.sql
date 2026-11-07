-- Unity Catalog bootstrap for one environment.
--
-- This is the CLI/SQL equivalent of what terraform/catalog.tf declares. It exists because
-- this workspace metastore uses Databricks Default Storage, which the `databricks catalogs
-- create` CLI command cannot request -- SQL CREATE CATALOG handles it natively.
--
-- `${catalog}` is substituted by tools/run_sql.py:
--
--   python tools/run_sql.py --profile wind --warehouse <id> \
--       --file tools/bootstrap_uc.sql --set catalog=wind_dev
--
-- Idempotent: safe to re-run. To move these objects under Terraform later, use
-- `terraform import` rather than re-creating them.

CREATE CATALOG IF NOT EXISTS ${catalog}
  COMMENT 'Wind turbine telemetry platform. Managed manually; to be codified in Terraform.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.raw
  COMMENT 'Landing zone. Holds the Volume that Auto Loader reads source files from.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze
  COMMENT 'Raw ingested telemetry and turbine change events, as landed.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.silver
  COMMENT 'Validated long/narrow sensor readings, the quarantine table, and the SCD2 turbine dimension.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.gold
  COMMENT 'Serving datasets: enriched facts, window aggregates, threshold alerts, current state.';

CREATE SCHEMA IF NOT EXISTS ${catalog}.ops
  COMMENT 'Operational metadata, including the pipeline event log.';

-- Auto Loader source. A UC Volume keeps the pipeline cloud-agnostic: it reads
-- /Volumes/<catalog>/raw/landing/... regardless of the storage underneath.
CREATE VOLUME IF NOT EXISTS ${catalog}.raw.landing
  COMMENT 'Landing zone for sensor telemetry (sensors/) and turbine master data (turbines/).';
