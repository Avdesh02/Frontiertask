output "catalog_name" {
  value       = databricks_catalog.wind.name
  description = "Catalog the pipeline publishes into. Set as the bundle's `catalog` variable."
}

output "landing_volume_path" {
  value       = "/Volumes/${databricks_catalog.wind.name}/raw/landing"
  description = "Auto Loader source root. Sensor files go in sensors/, turbine master data in turbines/."
}

output "ci_service_principal_application_id" {
  value       = databricks_service_principal.ci.application_id
  description = <<-EOT
    Set this as the GitHub repository variable DATABRICKS_CLIENT_ID.

    It is a client ID, not a secret -- a repository variable is the right place for it,
    not a repository secret. Authentication still requires a GitHub-minted OIDC token
    whose subject matches the federation policy.
  EOT
}

output "oidc_subject" {
  value       = local.oidc_subject
  description = <<-EOT
    The exact subject claim this environment's federation policy trusts.

    If a workflow fails to authenticate, compare this against the `sub` in the token the
    run actually presented -- a repo rename silently changes it.
  EOT
}

output "github_variables_to_set" {
  value = {
    DATABRICKS_HOST      = var.databricks_host
    DATABRICKS_CLIENT_ID = databricks_service_principal.ci.application_id
  }
  description = "Copy these into GitHub repository variables (Settings > Secrets and variables > Actions > Variables)."
}
