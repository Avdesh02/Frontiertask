#!/usr/bin/env bash
#
# Create the CI service principal and its GitHub OIDC federation policies.
#
# This is the CLI equivalent of terraform/identity.tf, for workspaces where you would
# rather not adopt Terraform state yet. It is idempotent: re-running reuses an existing
# service principal and skips policies that already exist.
#
# What it creates:
#   1. An account-level service principal (the CI identity)
#   2. One federation policy per GitHub environment -- federation subjects are
#      exact-match with no wildcards, so each environment needs its own
#   3. The workspace assignment, so the principal can actually use the workspace
#
# After running, grants are applied separately by tools/grant_ci.sql.
#
# Usage:
#   bash tools/setup_oidc.sh
#
# Requires: an ACCOUNT-level CLI profile (see ACCOUNT_PROFILE) whose user is an account
# admin. A workspace-level profile cannot create account objects.

set -euo pipefail

CLI="${CLI:-$HOME/bin/databricks.exe}"
ACCOUNT_PROFILE="${ACCOUNT_PROFILE:-wind-account}"
WORKSPACE_PROFILE="${WORKSPACE_PROFILE:-wind}"

ACCOUNT_ID="${ACCOUNT_ID:-1afab785-e196-4789-a670-538c352ff12d}"
WORKSPACE_ID="${WORKSPACE_ID:-7474659581094621}"
GITHUB_REPO="${GITHUB_REPO:-Avdesh02/Frontiertask}"
SP_NAME="${SP_NAME:-wind-turbine-ci}"
ENVIRONMENTS="${ENVIRONMENTS:-dev prod}"

# Git Bash rewrites a leading /api/... into a filesystem path, turning every call into
# a 404. This disables that rewriting for the whole script.
export MSYS_NO_PATHCONV=1

say() { printf '\n== %s\n' "$*"; }

api() {
  # api <method> <path> [json-file]
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    "$CLI" api "$method" "$path" --profile "$ACCOUNT_PROFILE" --json "@$body"
  else
    "$CLI" api "$method" "$path" --profile "$ACCOUNT_PROFILE"
  fi
}

json_field() { python -c "
import json,sys
raw = sys.stdin.read()
start = min((i for i in (raw.find('{'), raw.find('[')) if i >= 0), default=-1)
if start < 0:
    sys.exit(0)
data = json.loads(raw[start:])
print(data.get('$1', '') if isinstance(data, dict) else '')
"; }

say "Checking account access as profile '$ACCOUNT_PROFILE'"
if ! api get "/api/2.0/accounts/$ACCOUNT_ID/scim/v2/ServicePrincipals" >/dev/null 2>&1; then
  cat >&2 <<EOF
Cannot reach the account API with profile '$ACCOUNT_PROFILE'.

Log in at the account level first (this is separate from the workspace login):

  $CLI auth login --host https://accounts.cloud.databricks.com \\
      --account-id $ACCOUNT_ID --profile $ACCOUNT_PROFILE

This requires an ACCOUNT admin, which is a different role from workspace admin.
EOF
  exit 1
fi

say "Looking for an existing service principal named '$SP_NAME'"
EXISTING=$(api get "/api/2.0/accounts/$ACCOUNT_ID/scim/v2/ServicePrincipals" | python -c "
import json,sys
raw=sys.stdin.read(); b=raw.find('{')
if b<0: raise SystemExit
for r in json.loads(raw[b:]).get('Resources', []):
    if r.get('displayName') == '$SP_NAME':
        print(r.get('id', ''), r.get('applicationId', ''))
        break
")

if [[ -n "$EXISTING" ]]; then
  SP_ID=$(echo "$EXISTING" | cut -d' ' -f1)
  APP_ID=$(echo "$EXISTING" | cut -d' ' -f2)
  echo "Reusing existing service principal (numeric id $SP_ID)"
else
  say "Creating service principal '$SP_NAME'"
  cat > /tmp/sp.json <<EOF
{"displayName": "$SP_NAME", "active": true}
EOF
  RESPONSE=$(api post "/api/2.0/accounts/$ACCOUNT_ID/scim/v2/ServicePrincipals" /tmp/sp.json)
  SP_ID=$(echo "$RESPONSE" | json_field id)
  APP_ID=$(echo "$RESPONSE" | json_field applicationId)
  rm -f /tmp/sp.json
fi

if [[ -z "${SP_ID:-}" || -z "${APP_ID:-}" ]]; then
  echo "Failed to determine the service principal id. Aborting." >&2
  exit 1
fi

echo "  numeric id:     $SP_ID    (used by the federation policy API)"
echo "  application id: $APP_ID   (used by GitHub as DATABRICKS_CLIENT_ID)"

for ENV_NAME in $ENVIRONMENTS; do
  SUBJECT="repo:${GITHUB_REPO}:environment:${ENV_NAME}"
  POLICY_ID="github-actions-${ENV_NAME}"

  say "Federation policy '$POLICY_ID' for subject $SUBJECT"
  cat > /tmp/policy.json <<EOF
{
  "policy_id": "$POLICY_ID",
  "description": "Trusts GitHub Actions in the $ENV_NAME environment of $GITHUB_REPO",
  "oidc_policy": {
    "issuer": "https://token.actions.githubusercontent.com",
    "audiences": ["$ACCOUNT_ID"],
    "subject_claim": "sub",
    "subject": "$SUBJECT"
  }
}
EOF
  if api post \
      "/api/2.0/accounts/$ACCOUNT_ID/servicePrincipals/$SP_ID/federationPolicies" \
      /tmp/policy.json 2>/tmp/policy.err; then
    echo "  created"
  else
    if grep -qi "already exists\|ALREADY_EXISTS" /tmp/policy.err; then
      echo "  already exists, leaving as is"
    else
      echo "  FAILED:" >&2
      cat /tmp/policy.err >&2
    fi
  fi
  rm -f /tmp/policy.json /tmp/policy.err
done

say "Assigning the service principal to workspace $WORKSPACE_ID"
cat > /tmp/assign.json <<EOF
{"principal_id": $SP_ID, "permissions": ["ADMIN"]}
EOF
if api put \
    "/api/2.0/accounts/$ACCOUNT_ID/workspaces/$WORKSPACE_ID/permissionassignments/principals/$SP_ID" \
    /tmp/assign.json 2>/tmp/assign.err; then
  echo "  assigned"
else
  echo "  note: $(cat /tmp/assign.err)"
fi
rm -f /tmp/assign.json /tmp/assign.err

cat <<EOF

================================================================================
Done. Set these as GitHub repository VARIABLES (not secrets -- neither is
sensitive, and authentication still requires a GitHub-minted OIDC token):

  Settings > Secrets and variables > Actions > Variables

  DATABRICKS_HOST       https://dbc-0090b005-ba2d.cloud.databricks.com
  DATABRICKS_CLIENT_ID  $APP_ID

Then create the GitHub environments the policies trust:

  Settings > Environments > New environment
    - dev
    - prod   (add required reviewers here)

The environment names must match exactly -- federation subjects are not wildcards.

Finally, grant the principal access to Unity Catalog:

  python tools/run_sql.py --profile $WORKSPACE_PROFILE --warehouse <id> \\
      --file tools/grant_ci.sql
================================================================================
EOF
