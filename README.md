# Lead Data Engineer – Take-Home Exercise

## Solution Overview

This repository contains a batch analytics pipeline for processing platform fees data on Google Cloud Platform (GCP). The solution uses:
- **GCS** as a data lake for raw file storage
- **BigQuery** for data warehouse (raw, staging, reporting layers)
- **Python** for data ingestion pipeline
- **Terraform** for infrastructure provisioning
- **dbt** for SQL-based data transformations

### Architecture

```
CSV File ──▶ GCS Data Lake ──▶ BigQuery (raw) ──▶ BigQuery (staging) ──▶ BigQuery (reporting)
             (versioned)       Python ingestion    dbt (incremental)     dbt (full refresh)
```

### Components

- **GCS Bucket** — Data lake - stores raw CSV files with versioning
- **Python Ingestion** — Loads CSV from GCS to BigQuery native table
- **BigQuery Native Tables** — Raw, staging, and reporting layers
- **Terraform** — Infrastructure as code for all GCP resources
- **dbt** — SQL-based data transformations with testing

---

## Why GCS + Python Ingestion?

### Data Lake Pattern Benefits

- **File preservation** — Original CSV files retained in GCS with versioning
- **Idempotent ingestion** — Python script uses WRITE_TRUNCATE for safe reruns
- **Flexibility** — Same files accessible by other tools (Spark, Dataflow, etc.)
- **Cloud Function ready** — Ingestion script can be deployed to Cloud Function
- **Lifecycle management** — Automatic transition to Nearline/Coldline for older files

**Data lake benefits** (GCS):
- **Immutable archive** — original CSV files retained as-is, never modified by downstream processing
- **Versioning** — accidental deletes or overwrites are recoverable; provides file-level audit trail
- **Lifecycle management** — automatic transition to cheaper storage tiers (Nearline at 90d, Coldline at 365d)
- **Tool-agnostic access** — same files readable by Spark, Dataflow, or any GCS-compatible tool

**Native table benefits** (BigQuery, vs. external tables):
- **Query performance** — data stored in BigQuery's columnar format with automatic caching; external tables read from GCS on every query
- **Partition pruning** — `raw.fees` partitioned by `fee_date`; queries filtering on date scan only relevant partitions
- **Cost predictability** — cached queries hit storage, not GCS egress; external tables incur GCS read costs per query
- **Statistics and optimiser** — BigQuery collects table statistics for native tables, enabling better query plans

---

## Data Model

### Layers - Medallion Architecture

- **Bronze** (`raw`) — Ingested source data (from GCS via Python)
- **Silver** (`staging`) — Cleaned, deduplicated, validated
- **Gold** (`reporting`) — Business-ready analytics models

### Models

```
raw.fees ──▶ staging(dedup) ──▶ reporting(aggregated models)
(Python)     (dbt)        (dbt)           (dbt)

```

### Adviser Attribution Strategy

**First-touch attribution**: The adviser associated with a client's **first fee payment** receives credit for that client's entire lifetime value.

**Rationale:**
- Rewards client acquisition effort

**Implementation:** `dim_clients.first_adviser_id` captures the adviser from the client's earliest fee record (the first adviser where client actually paid fees), used in `rpt_adviser_ltv`.

---

## Key Assumptions

1. **Duplicates** = same `(client_id, adviser_id, fee_date, fee_amount)` — Exact duplicates are removed; allows same client to have multiple distinct fees on same date
2. **LTV windows** = calendar months from first fee date — `DATE_ADD(first_fee_date, INTERVAL N MONTH)` rather than rolling 30-day periods
3. **Cohort** = month of first fee — Format `YYYY-MM` based on `first_fee_date`
4. **Negative fees = refunds/corrections** — Negative fee amounts are included in LTV calculations to reflect true net value
5. **client_nino is PII** — Has BigQuery policy tag for column-level security
6. **Single adviser per fee** — Each fee record has one adviser; client-adviser relationship inferred from fees

---

## Tradeoffs

- **GCS + Python ingestion** (vs. external tables) — True data ingestion, better query performance, idempotent
- **Native tables for all layers** (vs. external/views) — Consistent performance, easier to manage
- **dbt for transforms** (vs. stored procedures, Dataform) — Industry standard, better testing, documentation
- **Incremental staging only** (vs. all models incremental) — Staging is the largest table and benefits from partial reprocessing; reporting models are small aggregations that need full rebuild for correctness
- **First-touch attribution** (vs. time-weighted, fee-split) — Simpler, auditable, common industry practice
- **EU data location** (vs. US/multi-region) — UK fintech data residency requirements
- **GCS versioning enabled** (vs. no versioning) — Audit trail, accidental deletion recovery

---

## Running the Pipeline

### Prerequisites

1. **GCP Project** with BigQuery and GCS APIs enabled
2. **gcloud CLI** installed and authenticated
3. **dbt** installed with BigQuery adapter (`pip install dbt-bigquery`)
4. **Terraform** installed
5. **GCP Service Account** with the following IAM roles:
   - `roles/bigquery.dataEditor` — create/write BigQuery tables and datasets
   - `roles/bigquery.jobUser` — run BigQuery queries and dbt models
   - `roles/storage.objectAdmin` — read/write GCS data lake and Terraform state
   - `roles/storage.admin` — manage GCS bucket (Terraform)

### Setup

```bash
# 1. Set service account credentials
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# 2. Deploy infrastructure (GCS bucket + BigQuery datasets)
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your project ID
terraform init
terraform apply

# 3. Install dbt packages
source venv/bin/activate
cd ../dbt
dbt deps
```

### Run Pipeline

```bash
# Set credentials (required before running)
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Full pipeline (upload to GCS + dbt transform + test)
./scripts/run_pipeline.sh

# Transform only (skip ingestion)
./scripts/run_pipeline.sh --skip-ingest

# Force full refresh of all models
./scripts/run_pipeline.sh --full-refresh
```

## Production Evolution

This section describes how to evolve this solution into a production-grade, trustworthy fintech data platform.

### 1. Production Readiness

**Orchestration:**

- **Scheduler** — Cloud Composer (Airflow) or Cloud Workflows for daily/hourly runs
- **Event-driven** — GCS upload triggers Cloud Function OR GCS Airflow Sensors → Python ingestion → dbt run
- **Alerting** — Cloud Monitoring alerts on pipeline failures;
- **Retry logic** — Exponential backoff in Airflow DAGs; dead-letter handling for bad files

**Data Governance and Catalog:**

- **Dataplex** — Central governance layer — auto-discovers BigQuery tables and GCS assets, organises them into business domains
- **Data Catalog tags** — Tag every table with business metadata: `owner`, `domain`, `sensitivity` (public/internal/confidential/restricted), `pii_columns`, `refresh_frequency`. Searchable org-wide
- **PII classification** — Use Dataplex or DLP API to auto-scan for PII (NINOs, names, emails) and apply policy tags; ensures new PII columns are caught even if not manually tagged
- **Data domains** — Organise assets by business domain — enables domain-level ownership, access policies, and quality SLAs
- **Business glossary** — Define standard terms (LTV, cohort, first-touch attribution) in Data Catalog so all teams use consistent definitions
- **dbt docs** — `dbt docs generate` produces browsable documentation with column descriptions, model DAG, and test coverage — serves as the technical catalog alongside GCP's business catalog

**CI/CD:**

- **Branch protection** — All changes via PR; require approval from data team before merge to `main`
- **Pre-commit hooks** — SQLFluff for SQL linting/formatting standards; dbt-bouncer for enforcing dbt project conventions (naming, materialization, test coverage)
- **dbt CI** — On PR: `dbt build --select state:modified+` against a dev dataset; compare row counts and aggregates vs. production
- **Automated testing** — All dbt tests run in CI before deploy; block merge on test failure
- **Environment separation** — `dev`, `ci`, `prod` BigQuery datasets; dbt profiles per environment

**IAM and access control:**

- **Principle of least privilege** — Service accounts scoped per component: ingestion SA (write `raw` only), dbt SA (read `raw`, write `staging`/`reporting`)
- **Dataset-level IAM** — `raw` → data engineers only; `staging` → engineers + senior analysts; `reporting` → all analysts and BI tools
- **Column-level ACLs** — BigQuery policy tags on `client_nino` — only authorised roles can query PII
- **Data masking** — BigQuery data masking policies for analyst access
- **VPC Service Controls** — Isolate GCS and BigQuery in a VPC perimeter; prevent data exfiltration
- **CMEK encryption** — Customer-managed encryption keys for GCS and BigQuery
- **Audit logging** — BigQuery audit logs exported to separate dataset — who queried what, when

**Monitoring and Observability - Leverage Cloud Monitoring on GCP and DBT Elementary**

- **Pipeline runtime** — Log execution times; alert on anomalies
- **Row counts** — Track row counts per layer; alert on unexpected changes
- **GCS metrics** — Monitor object count, storage size, access patterns
- **Data freshness** — Monitor latest file timestamp in GCS
- **Test failures** — Alert on dbt test failures

### 2. Auditing, Lineage and Reconciliation

- **GCS versioning** — Already enabled — provides file-level audit trail
- **Query audit logs** — Enable BigQuery audit logs; export to separate dataset for analysis
- **dbt lineage** — Store `manifest.json` and `run_results.json` in GCS; `dbt docs generate` produces a browsable DAG showing model dependencies and column descriptions
- **Dataplex** — GCP's data governance layer — auto-discovers BigQuery tables and GCS assets, enforces data quality rules, and provides a unified metadata catalog across the platform
- **Data Catalog** — Tag all datasets and tables with business metadata (owner, domain, sensitivity level, PII classification); searchable by analysts across the organisation
- **BigQuery column-level lineage** — Native lineage in BigQuery tracks how data flows between columns across tables — complements dbt's model-level lineage with query-level tracing
- **Row count reconciliation** — `assert_row_count_reconciliation.sql` — validates `stg_fees` never exceeds `raw.fees` and no more than 5% of rows are lost to dedup/null filtering
- **Financial reconciliation** — `assert_financial_reconciliation.sql` — validates `SUM(fee_amount)` in `stg_fees` matches `SUM(ltv_total)` in `rpt_client_ltv` (tolerance: 0.01)
- **Cross-layer traceability** — Any reporting number can be traced back through staging → raw → original CSV in GCS

### 3. Backfills and Reprocessing

- **Incremental staging** — `stg_fees` uses incremental materialization with a 3-day lookback window on `fee_date`, merging on `fee_id` to upsert reprocessed rows
- **Full-refresh reporting** — Reporting models (`dim_clients`, `rpt_client_ltv`, `rpt_cohort_ltv`, `rpt_adviser_ltv`) rebuild fully from staging each run — they are small aggregations where correctness outweighs reprocessing cost
- **Selective refresh** — Add date range parameters to pipeline scripts
- **Historical corrections** — Use `dbt run --full-refresh -s stg_fees+` to rebuild all layers from raw

### 4. Schema Changes

- **dbt contracts** — Define `contract: {enforced: true}` in YAML with explicit column names, types, and constraints. dbt will **fail the build** if the upstream data doesn't match the contract — catching schema drift before it reaches reporting
- **Schema evolution (additive)** — New optional columns: add to Python ingestion schema (`define_schema()`) + dbt staging model + YAML contract. BigQuery native tables support additive changes without rebuild
- **Schema evolution (breaking)** — Type changes or column renames: version the GCS path (`/raw/fees_v2/`), update ingestion script, update dbt contract, run `--full-refresh` to rebuild all layers
- **Alerting on drift** — If source data adds/removes columns without updating the contract, dbt contract enforcement fails the build and blocks deployment. No silent schema drift.
- **Why not auto-detect?** — BigQuery supports `autodetect` for schema inference, but in fintech this is risky — an unexpected column type change (e.g., `fee_amount` arriving as STRING) would silently pass and corrupt downstream calculations. Explicit contracts are safer.
- **CI validation** — `dbt build --select state:modified+` in CI tests contract + model changes against a dev dataset before merging to production

### 5. Data Quality Checks

**Implemented tests:**

- **`assert_ltv_matches_raw.sql`** — Independently recalculates LTV from `stg_fees` and compares against `rpt_client_ltv`; fails if any mismatch > 0.01
- **`assert_client_data.sql`** — Verifies each `client_id` maps to exactly one `client_nino`; catches upstream data quality issues or hash collisions
- **`assert_row_count_reconciliation.sql`** — Validates `stg_fees` row count never exceeds `raw.fees` and no more than 5% lost to dedup/null filtering
- **`assert_financial_reconciliation.sql`** — Validates `SUM(fee_amount)` is preserved from staging through to reporting (tolerance: 0.01)
- **dbt column tests** — `unique` and `not_null` on all primary keys and required fields across every model

**Future enhancements:**

- **Freshness** — Monitor GCS object timestamps; dbt source freshness checks
- **Anomaly detection or moving averages** — Statistical bounds checks (e.g., fee_amount within expected range, row count within N% of previous run)
- **Dataplex Auto Data Quality** — GCP-native data quality rules defined declaratively on BigQuery tables — runs null checks, range validations, uniqueness, and custom SQL rules on a schedule without dbt. Complements dbt tests by catching issues at the platform level (e.g., raw data quality before dbt even runs)

### 6. Cost Management

- **GCS lifecycle** — Auto-transition to Nearline (90d) and Coldline (365d)
- **Partitioning** — Already implemented: `raw.fees` and `stg_fees` partitioned by `fee_date`
- **Clustering** — Cluster native tables by frequently filtered columns
- **Slot reservations** — Use BigQuery reservations for predictable costs
