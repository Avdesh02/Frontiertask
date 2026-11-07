terraform {
  required_version = ">= 1.6"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.141"
    }
  }

  # TODO: point this at a remote backend before anyone else runs it. Local state means
  # two people applying concurrently will clobber each other, and the state file contains
  # resource IDs you do not want only on one laptop. Pick the one matching your cloud:
  #
  #   backend "s3"    { bucket = "..." key = "wind/terraform.tfstate" region = "..." use_lockfile = true }
  #   backend "azurerm" { resource_group_name = "..." storage_account_name = "..." container_name = "tfstate" key = "wind.tfstate" }
  #   backend "gcs"   { bucket = "..." prefix = "wind" }
  #
  # Use a separate key/prefix per environment so dev and prod states never mix.
}

# Workspace-level provider: Unity Catalog objects (catalog, schemas, volume, grants).
provider "databricks" {
  alias = "workspace"
  host  = var.databricks_host

  # Auth is taken from the environment so no credential is ever written to a file:
  #   - local bootstrap: DATABRICKS_TOKEN, or a `databricks auth login` profile
  #   - CI:              DATABRICKS_AUTH_TYPE=github-oidc + DATABRICKS_CLIENT_ID
}

# Account-level provider: the CI service principal and its OIDC federation policies live
# at the account, not in the workspace. This needs account-admin credentials.
provider "databricks" {
  alias      = "account"
  host       = var.databricks_account_host
  account_id = var.databricks_account_id
}
