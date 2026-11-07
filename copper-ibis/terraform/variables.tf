variable "databricks_host" {
  type        = string
  description = "Workspace URL, e.g. https://adb-1234567890.1.azuredatabricks.net"
}

variable "databricks_account_host" {
  type        = string
  description = "Account console URL. accounts.cloud.databricks.com (AWS), accounts.azuredatabricks.net (Azure), or accounts.gcp.databricks.com (GCP)."
  default     = "https://accounts.cloud.databricks.com"
}

variable "databricks_account_id" {
  type        = string
  description = "Databricks account UUID. Found in the account console. Also used as the default OIDC audience."
}

variable "environment" {
  type        = string
  description = "Environment name. Drives catalog naming and the OIDC subject."

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "catalog_name" {
  type        = string
  description = "Unity Catalog catalog for this environment."
}

variable "storage_root" {
  type        = string
  description = <<-EOT
    Optional managed storage location for the catalog (an external location URL such as
    abfss://... or s3://...). Leave empty to inherit the metastore's root storage, which
    is the simplest option and keeps this config cloud-agnostic.
  EOT
  default     = ""
}

variable "github_repository" {
  type        = string
  description = <<-EOT
    GitHub repo in owner/name form, used to build the OIDC subject claim.

    WARNING: repos created, renamed, or transferred on or after 2026-07-15 use immutable
    numeric IDs in the subject (owner@123456/repo@456789). If auth fails with a subject
    mismatch, read the actual `sub` from the failing workflow run and set
    github_subject_override instead of guessing.
  EOT
}

variable "github_subject_override" {
  type        = string
  description = "Exact OIDC subject claim, bypassing the constructed value. Federation subjects are exact-match with no wildcards."
  default     = ""
}

variable "pipeline_admin_groups" {
  type        = list(string)
  description = "Groups granted full access to the catalog, e.g. data engineers."
  default     = []
}

variable "pipeline_reader_groups" {
  type        = list(string)
  description = "Groups granted read-only access to the gold schema, e.g. analysts."
  default     = []
}
