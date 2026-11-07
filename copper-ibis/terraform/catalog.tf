# Unity Catalog containers for the pipeline. Terraform owns these; the bundle owns the
# pipeline that fills them. Keeping the catalog here means `bundle destroy` can never take
# the data with it.

locals {
  # Medallion schemas plus `raw` (the landing volume) and `ops` (the pipeline event log).
  schemas = {
    raw = {
      comment = "Landing zone. Holds the Volume that Auto Loader reads source files from."
    }
    bronze = {
      comment = "Raw ingested telemetry and turbine change events, as landed."
    }
    silver = {
      comment = "Validated long/narrow sensor readings, the quarantine table, and the SCD2 turbine dimension."
    }
    gold = {
      comment = "Serving datasets: enriched facts, window aggregates, threshold alerts, current state."
    }
    ops = {
      comment = "Operational metadata, including the pipeline event log."
    }
  }
}

resource "databricks_catalog" "wind" {
  provider     = databricks.workspace
  name         = var.catalog_name
  comment      = "Wind turbine telemetry platform (${var.environment}). Managed by Terraform."
  storage_root = var.storage_root != "" ? var.storage_root : null

  properties = {
    environment = var.environment
    managed_by  = "terraform"
    project     = "wind_turbine_telemetry"
  }

  # Deliberately NOT force_destroy. Removing a catalog holding production telemetry should
  # require a human to go and do it on purpose.
}

resource "databricks_schema" "layer" {
  provider     = databricks.workspace
  for_each     = local.schemas
  catalog_name = databricks_catalog.wind.name
  name         = each.key
  comment      = each.value.comment

  properties = {
    layer      = each.key
    managed_by = "terraform"
  }
}

# Auto Loader source. A UC Volume is what keeps this configuration cloud-agnostic: the
# pipeline reads /Volumes/<catalog>/raw/landing/... regardless of the storage underneath.
resource "databricks_volume" "landing" {
  provider         = databricks.workspace
  name             = "landing"
  catalog_name     = databricks_catalog.wind.name
  schema_name      = databricks_schema.layer["raw"].name
  volume_type      = "MANAGED"
  comment          = "Landing zone for sensor telemetry (sensors/) and turbine master data (turbines/)."
}
