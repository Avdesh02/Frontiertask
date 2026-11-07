# Development environment.
#
# Credentials are NOT set here -- they come from the environment (DATABRICKS_TOKEN or a
# CLI profile locally, OIDC in CI), so this file is safe to commit.

databricks_host         = "https://dbc-0090b005-ba2d.cloud.databricks.com"
databricks_account_host = "https://accounts.cloud.databricks.com"
databricks_account_id   = "1afab785-e196-4789-a670-538c352ff12d"

environment  = "dev"
catalog_name = "wind_dev"

github_repository = "Avdesh02/Frontiertask"

# Groups that exist in your workspace. Leave empty until you know the names.
pipeline_admin_groups  = []
pipeline_reader_groups = []
