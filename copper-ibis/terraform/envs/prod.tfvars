# Production environment.
#
# Use a SEPARATE Terraform state for this environment (different backend key/prefix) so a
# dev apply can never touch production catalogs.

databricks_host         = "https://dbc-0090b005-ba2d.cloud.databricks.com"
databricks_account_host = "https://accounts.cloud.databricks.com"
databricks_account_id   = "1afab785-e196-4789-a670-538c352ff12d"

environment  = "prod"
catalog_name = "wind_prod"

github_repository = "Avdesh02/Frontiertask"

pipeline_admin_groups  = []
pipeline_reader_groups = []
