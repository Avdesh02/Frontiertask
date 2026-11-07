# Wind Turbine Telemetry — Lakeflow Declarative Pipeline

Batch turbine master data and streaming sensor telemetry, ingested into Unity Catalog by a
single **Lakeflow Spark Declarative Pipeline**, deployed from GitHub with **Terraform**
(Unity Catalog + identity) and a **Declarative Automation Bundle** (pipeline + jobs),
authenticated entirely by **OIDC** — no stored passwords.

---

## Architecture

```
                 ┌──────────────────────────────────────────────┐
  turbine CDC ──►│  /Volumes/<catalog>/raw/landing/turbines/     │
  sensor JSON ──►│  /Volumes/<catalog>/raw/landing/sensors/      │
                 └───────────────────┬──────────────────────────┘
                                     │  Auto Loader (cloudFiles)
                 ┌───────────────────▼──────────────────────────┐
     BRONZE      │  bronze_sensor_raw      bronze_turbine_cdc   │  as landed, + provenance
                 └───────────────────┬──────────────────────────┘
                                     │  explode nested tags, apply rules once
                 ┌───────────────────▼──────────────────────────┐
     SILVER      │  v_silver_candidates  (temporary view)       │
                 │        ├── silver_sensor_readings   (valid)  │  long/narrow
                 │        └── silver_sensor_quarantine (bad)    │  + violation_reasons
                 │  dim_turbine  ◄── AUTO CDC, SCD Type 2       │  __START_AT/__END_AT
                 └───────────────────┬──────────────────────────┘
                                     │
                 ┌───────────────────▼──────────────────────────┐
     GOLD        │  gold_reading_enriched      (as-of join)     │
                 │  gold_tag_window_agg        (tumbling stats) │
                 │  gold_threshold_alerts      (WARN/CRITICAL)  │
                 │  gold_turbine_current_state (live status)    │
                 └──────────────────────────────────────────────┘
```

**Why long/narrow silver:** one row per `(turbine_id, tag, event_ts)`. A new sensor tag
flows through to silver with zero code change; only the gold pivot needs touching to give
it a named column.

**Why SCD Type 2 on the dimension:** a reading taken before a turbine was re-rated joins
to the rating that was in force *at the time*, not today's. That is what
`gold_reading_enriched` does with its as-of join.

**Why quarantine instead of drop:** bad rows go to `silver_sensor_quarantine` carrying a
`violation_reasons` array naming each rule they failed. Nothing is lost silently.

---

## Repository layout

```
├── databricks.yml                  bundle root: dev/prod targets, variables
├── resources/
│   ├── turbine_pipeline.pipeline.yml   the pipeline (serverless, UC, thresholds)
│   ├── turbine_pipeline.job.yml        continuous job that drives it
│   └── data_generator.job.yml          synthetic telemetry generator job
├── src/turbine_pipeline/
│   ├── lib/                        PURE PySpark — no pipeline imports, unit testable
│   └── transformations/            thin @dp-decorated wrappers
├── tools/generate_sensor_data.py   synthetic data generator (local or job)
├── tests/unit/                     pytest + local SparkSession, no workspace needed
├── tests/integration/              databricks-connect assertions on real UC tables
└── terraform/                      catalog, schemas, volume, SP, OIDC policy, grants
```

Workflows live at the **repository root**, not here, because GitHub only runs
`.github/workflows/` from the root:

```
<repo root>/.github/workflows/
├── copper-ibis-ci.yml
├── copper-ibis-cd-dev.yml
└── copper-ibis-cd-prod.yml
```

They are name-prefixed and `paths:`-scoped to `copper-ibis/**`, so they ignore changes to
the sibling `jade-marmot/` project and leave the workflow namespace free for it.

### Why every dataset name is fully qualified

One pipeline publishes into three schemas (`bronze`, `silver`, `gold`). That is supported
— but a pipeline has only one *default* schema, and **bare names resolve against that
default, not against the sibling dataset's actual schema**. So this silently breaks:

```python
# WRONG: resolves to <default schema>.silver_sensor_readings, which does not exist
spark.readStream.table("silver_sensor_readings")
```

`lib/naming.py` builds every name as `catalog.schema.table` from a catalog injected via
pipeline configuration. That also makes splitting into per-layer pipelines later a config
change rather than a rewrite, since qualified names don't depend on any pipeline's
defaults.

The one exception is temporary views, which **must** be single-part — qualifying one is a
hard error, because a temp view doesn't live in a schema.

### The `lib` / `transformations` split

Lakeflow features cannot run locally. So every transform lives in `lib/` as a plain
function taking and returning a DataFrame, and `transformations/` holds only decorated
wrappers:

```python
# lib/silver.py — testable with plain pytest
def valid_readings(df): return df.where(F.col("is_valid")).drop(...)

# transformations/03_silver_readings.py — needs a pipeline runtime
@dp.table(name="silver_sensor_readings")
def silver_sensor_readings():
    return deduplicate_readings(valid_readings(spark.readStream.table("v_silver_candidates")))
```

### Tool boundary

| Terraform | Declarative Automation Bundle |
| --- | --- |
| Catalog, schemas, Volume | The pipeline |
| Service principal + OIDC policy | The jobs |
| Grants | Pipeline configuration (thresholds, paths) |

This follows Databricks' own IaC guidance. It also means `terraform destroy` cannot drop
your datasets and `bundle destroy` cannot drop your catalog.

---

## One-time setup

### 1. Bootstrap credentials (local, never committed)

```bash
databricks auth login --host https://<your-workspace> --profile wind
```

### 2. Create the Unity Catalog objects

Two options. **Terraform is the designed path**:

```bash
cd terraform
terraform init
terraform apply -var-file=envs/dev.tfvars
```

> **Set a remote backend first if more than one person will run this.** `providers.tf`
> ships with local state and a TODO listing the S3/azurerm/gcs stanzas. Use a separate
> state key per environment.

**Or bootstrap with SQL** (what this workspace actually used):

```bash
python tools/run_sql.py --profile wind --warehouse <warehouse-id> \
  --file tools/bootstrap_uc.sql
```

This exists because the `databricks catalogs create` CLI command cannot request
**Default Storage**, which metastores without their own storage root require — it fails
with *"Metastore storage root URL does not exist"*. SQL `CREATE CATALOG` handles it
natively. The script is idempotent; to adopt these objects into Terraform later, use
`terraform import` rather than re-creating them.

### 4. Wire up GitHub

```bash
terraform output github_variables_to_set
```

Set both as repository **variables** (not secrets — neither is sensitive):

| Variable | Value |
| --- | --- |
| `DATABRICKS_HOST` | your workspace URL |
| `DATABRICKS_CLIENT_ID` | the service principal's application UUID |

Then create GitHub environments `dev` and `prod`, with required reviewers on `prod`.

### 5. Deploy

```bash
databricks bundle deploy --target dev
```

---

## How OIDC authentication works

GitHub mints a short-lived token per workflow run; Databricks validates it against the
federation policy and issues an access token for the service principal. Three env vars,
no secret:

```yaml
permissions:
  id-token: write        # without this, no token is minted at all
env:
  DATABRICKS_AUTH_TYPE: github-oidc
  DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
  DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}
```

**Federation subjects are exact-match — no wildcards.** Each GitHub environment needs its
own policy, which is why Terraform creates one per environment and every workflow job
declares `environment:`.

### When auth fails

> ⚠️ Repositories created, renamed, or transferred **on or after 2026-07-15** use
> immutable numeric IDs in the subject:
> `repo:octo-org@123456/octo-repo@456789:environment:prod`.
> **A later rename silently breaks authentication.**

To fix: read the actual `sub` from the failing run's token, then set
`github_subject_override` in your tfvars and re-apply. Compare against:

```bash
terraform output oidc_subject
```

---

## Running it

### Watch data arrive in real time

```bash
# Terminal 1 — the pipeline is already running continuously in prod; in dev, trigger it:
databricks bundle run turbine_pipeline --target dev

# Terminal 2 — drip files into the Volume
python tools/generate_sensor_data.py \
  --volume /Volumes/wind_dev/raw/landing/sensors \
  --with-dimension /Volumes/wind_dev/raw/landing/turbines \
  --turbines 20 --interval 5 --duration 300
```

Or run it inside the workspace, which is more convincing because there is no upload lag:

```bash
databricks bundle run turbine_data_generator --target dev
```

Then watch the counts climb:

```sql
SELECT count(*) FROM wind_dev.silver.silver_sensor_readings;

SELECT * FROM wind_dev.gold.gold_turbine_current_state
ORDER BY last_seen_ts DESC LIMIT 20;

SELECT severity, tag, count(*)
FROM wind_dev.gold.gold_threshold_alerts
GROUP BY severity, tag ORDER BY severity;

-- Quarantine should be non-empty: the generator corrupts ~7% of readings on purpose.
SELECT violation_reasons, count(*)
FROM wind_dev.silver.silver_sensor_quarantine
GROUP BY violation_reasons;

-- SCD2 history: a closed row (__END_AT not null) proves versioning works.
SELECT turbine_id, rated_power_kw, __START_AT, __END_AT
FROM wind_dev.silver.dim_turbine
WHERE turbine_id = 'WTG-0001' ORDER BY __START_AT;
```

### Pipeline health

The event log is published to Unity Catalog, so failures are queryable:

```sql
SELECT timestamp, event_type, message
FROM wind_dev.ops.turbine_pipeline_event_log
WHERE level IN ('ERROR', 'WARN')
ORDER BY timestamp DESC LIMIT 50;

-- Data quality metrics from the expectations
SELECT timestamp, details:flow_progress.data_quality.expectations
FROM wind_dev.ops.turbine_pipeline_event_log
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC LIMIT 20;
```

---

## Testing

| Layer | Command | Needs a workspace |
| --- | --- | --- |
| Unit | `pytest tests/unit` | no |
| Bundle | `databricks bundle validate -t dev` | yes (auth only) |
| Integration | `pytest tests/integration -m integration_test` | yes |

```bash
pip install -e ".[dev]"          # unit tests (installs pyspark)
pip install -e ".[integration]"  # integration tests (installs databricks-connect)
```

> **Never install both extras into one environment.** `databricks-connect` ships its own
> `pyspark` module and shadows the real one. That is why they are separate extras and
> separate CI steps.

> **Known local issue on Windows:** PySpark's Python workers can fail with
> `SocketException: Software caused connection abort`. It is an environment problem, not a
> code one — the same tests pass on the Linux CI runners. If you hit it, run the unit
> tests in WSL or rely on CI.

---

## CI/CD

| Workflow | Trigger | Does |
| --- | --- | --- |
| `copper-ibis-ci.yml` | pull request | ruff, unit tests, `bundle validate` (dev **and** prod), `terraform plan` |
| `copper-ibis-cd-dev.yml` | push to `main` | deploy dev → seed the Volume → run the pipeline → integration tests |
| `copper-ibis-cd-prod.yml` | manual / `copper-ibis-v*` tag | optional `terraform apply` → deploy prod → start the continuous job |

`copper-ibis-cd-dev.yml` is the real proof: it seeds files, runs the pipeline to
completion, and asserts on the resulting Unity Catalog tables. Green means data actually
flowed.

Every Databricks-touching job is guarded by `if: vars.DATABRICKS_CLIENT_ID != ''`, so
until you create the OIDC service principal those jobs skip rather than fail. Lint and
unit tests run regardless.

---

## Current API notes

This code targets the current API. If you are adapting older examples, these all changed:

| Old | Current |
| --- | --- |
| `import dlt`, `@dlt.table` | `from pyspark import pipelines as dp`, `@dp.table` |
| `dlt.apply_changes()` | `dp.create_auto_cdc_flow()` |
| `LIVE.my_table` | bare or fully-qualified table names |
| pipeline `target:` | pipeline `schema:` |
| pipeline `continuous:` | a job with a continuous trigger |
| `bundle.engine: terraform` | removed in CLI 1.20.0; `direct` only |
| `DATABRICKS_BUNDLE_VAR_*` | `BUNDLE_VAR_*` |
| `cloud_files()` | `read_files()` |

Also note: `pivot()`, `collect()`, `count()`, and `toPandas()` are **not permitted** in
pipeline dataset code. `lib/gold.py` pivots with conditional aggregation
(`max_by(when(tag = 'x', value), ...)`) for this reason.

## Two gotchas found by actually running this

Both cost a failed update before being fixed, and neither is obvious from the docs.

**1. AUTO CDC expectations are evaluated against the *target* schema.** Declaring
`expect_all_or_drop=TURBINE_CDC_RULES` on `dim_turbine` failed with
`UNRESOLVED_COLUMN: change_seq`, because `except_column_list` strips that column before
the expectation runs. CDC control columns can only be validated **upstream**, on
`bronze_turbine_cdc`, where they still exist.

**2. `__START_AT` / `__END_AT` inherit the type of `sequence_by`.** Sequencing by the
BIGINT `change_seq` made the SCD2 validity columns BIGINT, and the gold as-of join then
failed with `BINARY_OP_DIFF_TYPES` comparing a TIMESTAMP `event_ts` against them. The fix
is to sequence by the timestamp `changed_at`.

The trade-off in #2: `change_seq` would be immune to two changes landing in the same
millisecond. If that matters more than a clean as-of join, sequence by `change_seq` and
cast inside `enrich_with_dimension` instead.
