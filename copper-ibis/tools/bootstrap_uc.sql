-- Unity Catalog bootstrap for the dev environment.
--
-- This is the CLI/SQL equivalent of what terraform/catalog.tf declares. It exists because
-- the workspace metastore uses Databricks Default Storage, which the `databricks catalogs
-- create` CLI command cannot request -- SQL CREATE CATALOG handles it natively.
--
-- Idempotent: safe to re-run. To move this under Terraform later, `terraform import`
-- each object rather than re-creating it.

CREATE CATALOG IF NOT EXISTS wind_dev
  COMMENT 'Wind turbine telemetry platform (dev). Managed manually; to be codified in Terraform.';

CREATE SCHEMA IF NOT EXISTS wind_dev.raw
  COMMENT 'Landing zone. Holds the Volume that Auto Loader reads source files from.';

CREATE SCHEMA IF NOT EXISTS wind_dev.bronze
  COMMENT 'Raw ingested telemetry and turbine change events, as landed.';

CREATE SCHEMA IF NOT EXISTS wind_dev.silver
  COMMENT 'Validated long/narrow sensor readings, the quarantine table, and the SCD2 turbine dimension.';

CREATE SCHEMA IF NOT EXISTS wind_dev.gold
  COMMENT 'Serving datasets: enriched facts, window aggregates, threshold alerts, current state.';

CREATE SCHEMA IF NOT EXISTS wind_dev.ops
  COMMENT 'Operational metadata, including the pipeline event log.';

-- Auto Loader source. A UC Volume keeps the pipeline cloud-agnostic: it reads
-- /Volumes/wind_dev/raw/landing/... regardless of the storage underneath.
CREATE VOLUME IF NOT EXISTS wind_dev.raw.landing
  COMMENT 'Landing zone for sensor telemetry (sensors/) and turbine master data (turbines/).';
