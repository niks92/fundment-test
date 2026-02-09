#!/bin/bash
# =============================================================================
# Pipeline Orchestration Script
# =============================================================================
#
# Runs the complete data pipeline:
#   1. Ingest CSV to BigQuery (raw layer)
#   2. Run dbt transformations (staging + reporting)
#   3. Run dbt tests (data quality validation)
#
# Usage:
#   ./scripts/run_pipeline.sh
#
# =============================================================================

set -euo pipefail

# Parse arguments
SKIP_INGEST=false
SKIP_TESTS=false
FULL_REFRESH=""

for arg in "$@"; do
    case $arg in
        --skip-ingest)
            SKIP_INGEST=true
            shift
            ;;
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        --full-refresh)
            FULL_REFRESH="--full-refresh"
            shift
            ;;
    esac
done

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"
DBT_DIR="${PROJECT_ROOT}/dbt"

echo "=============================================="
echo "Fundment Data Pipeline"
echo "=============================================="
echo "Project root: ${PROJECT_ROOT}"
echo "Start time:   $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# -----------------------------------------------------------------------------
# Step 1: Ingestion (Python pipeline)
# -----------------------------------------------------------------------------
if [[ "${SKIP_INGEST}" == "false" ]]; then
    echo ">>> Step 1/3: Ingesting CSV to BigQuery"
    echo "----------------------------------------------"

    # Set default service account key path if not already set
    if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
        echo "ERROR: GOOGLE_APPLICATION_CREDENTIALS environment variable is not set"
        echo "Usage: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json"
        exit 1
    fi
    echo "Using credentials: ${GOOGLE_APPLICATION_CREDENTIALS}"

    python "${SCRIPT_DIR}/ingest_fees.py"
    echo ""
else
    echo ">>> Step 1/3: Ingestion SKIPPED (--skip-ingest)"
    echo ""
fi

# -----------------------------------------------------------------------------
# Step 2: dbt Transformations
# -----------------------------------------------------------------------------
echo ">>> Step 2/3: Running dbt transformations"
echo "----------------------------------------------"

cd "${DBT_DIR}"

# Install dbt packages if needed
if [[ ! -d "dbt_packages" ]]; then
    echo "Installing dbt packages..."
    dbt deps --profiles-dir "${DBT_DIR}"
    echo ""
fi

# Run dbt models
echo "Running dbt models..."
dbt run --profiles-dir "${DBT_DIR}" ${FULL_REFRESH}
echo ""

# -----------------------------------------------------------------------------
# Step 3: dbt Tests
# -----------------------------------------------------------------------------
if [[ "${SKIP_TESTS}" == "false" ]]; then
    echo ">>> Step 3/3: Running dbt tests"
    echo "----------------------------------------------"
    dbt test --profiles-dir "${DBT_DIR}"
    echo ""
else
    echo ">>> Step 3/3: Tests SKIPPED (--skip-tests)"
    echo ""
fi

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo "=============================================="
echo "Pipeline Complete"
echo "=============================================="
echo "End time: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
echo "Tables created/updated:"
echo "  - raw.fees                 (source data)"
echo "  - staging.stg_fees         (cleaned/deduplicated)"
echo "  - reporting.dim_clients    (client dimension)"
echo "  - reporting.rpt_client_ltv (client LTV)"
echo "  - reporting.rpt_cohort_ltv (cohort LTV)"
echo "  - reporting.rpt_adviser_ltv (adviser attribution)"
echo ""
echo "To query results, run:"
echo "  bq query --nouse_legacy_sql 'SELECT * FROM reporting.rpt_adviser_ltv ORDER BY rank_by_ltv_6_month LIMIT 10'"
