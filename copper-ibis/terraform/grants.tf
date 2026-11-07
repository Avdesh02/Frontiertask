# Grants for the CI service principal and human groups.
#
# `databricks_grant` (singular) is authoritative for ONE principal on a securable, which
# is what we want here -- it leaves grants made to other principals alone. The plural
# `databricks_grants` would take ownership of the full ACL and silently revoke anything
# granted outside Terraform.
#
# Privilege names use underscores: CREATE_TABLE, not "CREATE TABLE".

# --- CI service principal ----------------------------------------------------------

resource "databricks_grant" "ci_catalog" {
  provider  = databricks.workspace
  catalog   = databricks_catalog.wind.name
  principal = databricks_service_principal.ci.application_id

  # Catalog level only. CREATE_SCHEMA is deliberately NOT granted: the schemas are
  # pre-created by Terraform above, and withholding it means a typo'd schema name in a
  # dataset fails loudly instead of quietly conjuring a new schema.
  privileges = [
    "USE_CATALOG",
  ]
}

# The pipeline publishes to bronze, silver, and gold via fully-qualified names, so it
# needs create rights on EACH of those schemas -- a catalog-level grant does not imply
# them. A missing grant here surfaces as a confusing "dataset does not exist" error
# rather than a permission error, so these are worth getting right.
resource "databricks_grant" "ci_dataset_schemas" {
  provider  = databricks.workspace
  for_each  = toset(["bronze", "silver", "gold", "ops"])
  schema    = databricks_schema.layer[each.key].id
  principal = databricks_service_principal.ci.application_id

  privileges = [
    "USE_SCHEMA",
    "CREATE_TABLE",             # streaming tables
    "CREATE_MATERIALIZED_VIEW", # materialized views
    "MODIFY",
    "SELECT",
  ]
}

# The raw schema holds the landing Volume; the pipeline reads from it but never creates
# tables there.
resource "databricks_grant" "ci_raw_schema" {
  provider  = databricks.workspace
  schema    = databricks_schema.layer["raw"].id
  principal = databricks_service_principal.ci.application_id

  privileges = [
    "USE_SCHEMA",
  ]
}

# Writing seed/test files into the landing volume is how CI gives the pipeline something
# to ingest, so it needs WRITE_VOLUME as well as READ_VOLUME.
resource "databricks_grant" "ci_volume" {
  provider  = databricks.workspace
  volume    = databricks_volume.landing.id
  principal = databricks_service_principal.ci.application_id

  privileges = [
    "READ_VOLUME",
    "WRITE_VOLUME",
  ]
}

# --- Human groups -------------------------------------------------------------------

resource "databricks_grant" "admins" {
  provider  = databricks.workspace
  for_each  = toset(var.pipeline_admin_groups)
  catalog   = databricks_catalog.wind.name
  principal = each.value

  privileges = [
    "ALL_PRIVILEGES",
  ]
}

# Analysts get read-only access to gold alone -- not to bronze, and not to the quarantine
# table, which can contain malformed payloads.
resource "databricks_grant" "readers_catalog" {
  provider  = databricks.workspace
  for_each  = toset(var.pipeline_reader_groups)
  catalog   = databricks_catalog.wind.name
  principal = each.value

  privileges = ["USE_CATALOG"]
}

resource "databricks_grant" "readers_gold" {
  provider  = databricks.workspace
  for_each  = toset(var.pipeline_reader_groups)
  schema    = databricks_schema.layer["gold"].id
  principal = each.value

  privileges = [
    "USE_SCHEMA",
    "SELECT",
  ]
}
