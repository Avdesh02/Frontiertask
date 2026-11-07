# The CI identity and its OIDC trust with GitHub Actions.
#
# This is what removes stored passwords from CI: GitHub mints a short-lived OIDC token for
# each workflow run, Databricks validates it against the federation policy below, and the
# run gets an access token for the service principal. No PAT, nothing to rotate, nothing
# to leak from repo secrets.

locals {
  # Federation subjects are EXACT MATCH -- no wildcards -- so each GitHub environment
  # needs its own policy. Scoping by environment (rather than by branch or by pull_request)
  # gives a stable subject across push, workflow_dispatch, and tag triggers.
  oidc_subject = (
    var.github_subject_override != ""
    ? var.github_subject_override
    : "repo:${var.github_repository}:environment:${var.environment}"
  )
}

resource "databricks_service_principal" "ci" {
  provider     = databricks.account
  display_name = "wind-turbine-ci-${var.environment}"

  # A deployment identity: it needs workspace access to run the pipeline, but no
  # ability to spin up its own clusters -- the pipeline is serverless.
  workspace_access     = true
  allow_cluster_create = false
}

resource "databricks_service_principal_federation_policy" "github" {
  provider             = databricks.account
  service_principal_id = databricks_service_principal.ci.id
  policy_id            = "github-actions-${var.environment}"
  description          = "Trusts GitHub Actions running in the ${var.environment} environment of ${var.github_repository}."

  # Note the `=`: oidc_policy is an attribute-style block in the Databricks provider, not
  # a bare HCL block. Writing it without the equals sign is a confusing parse error.
  oidc_policy = {
    issuer        = "https://token.actions.githubusercontent.com"
    audiences     = [var.databricks_account_id]
    subject_claim = "sub"
    subject       = local.oidc_subject
  }
}

# The service principal must exist in the workspace, not just the account, before it can
# own a pipeline or write to Unity Catalog.
resource "databricks_service_principal" "ci_workspace" {
  provider       = databricks.workspace
  application_id = databricks_service_principal.ci.application_id
  display_name   = databricks_service_principal.ci.display_name
}
