"""
Ingestion Pipeline: Load raw fees CSV from GCS to BigQuery

This script loads the raw fees CSV from Cloud Storage into a native BigQuery table.
It replaces the external table approach with actual data ingestion.

Features:
- Idempotent: Safe to rerun (uses WRITE_TRUNCATE)
- Validates row counts after load
- Can be deployed to Cloud Function later

Usage:
    python ingest_fees.py

Environment Variables:
    GCP_PROJECT_ID: GCP project ID (default: fundment-test-486411)
    GCS_BUCKET: Source bucket name (default: fundment-test-486411-data-lake)
    BQ_DATASET: Target dataset (default: raw)
"""

import os
import logging
from google.cloud import bigquery
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'fundment-test-486411')
GCS_BUCKET = os.getenv('GCS_BUCKET', 'fundment-test-486411-data-lake')
BQ_DATASET = os.getenv('BQ_DATASET', 'raw')
BQ_TABLE = 'fees'
SERVICE_ACCOUNT_KEY = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)

# Source and destination
GCS_URI = f'gs://{GCS_BUCKET}/raw/fees/*.csv'
TABLE_ID = f'{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}'


def get_bq_client() -> bigquery.Client:
    """Create BigQuery client with service account credentials."""
    if SERVICE_ACCOUNT_KEY:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_KEY
        )
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    return bigquery.Client(project=PROJECT_ID)


def define_schema() -> list[bigquery.SchemaField]:
    """Define the BigQuery table schema."""
    return [
        bigquery.SchemaField('client_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('client_nino', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('adviser_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('fee_date', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('fee_amount', 'FLOAT64', mode='REQUIRED'),
    ]


def load_csv_to_bigquery(client: bigquery.Client) -> bigquery.LoadJob:
    """
    Load CSV from GCS to BigQuery.

    Uses WRITE_TRUNCATE for idempotency - safe to rerun.
    """
    job_config = bigquery.LoadJobConfig(
        schema=define_schema(),
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Idempotent
        allow_quoted_newlines=True,
        allow_jagged_rows=False,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='fee_date',  # Partition by fee date for efficient scans
        ),
    )

    logger.info(f'Loading data from {GCS_URI} to {TABLE_ID}')

    load_job = client.load_table_from_uri(
        GCS_URI,
        TABLE_ID,
        job_config=job_config
    )

    # Wait for job to complete
    load_job.result()

    return load_job


def validate_load(client: bigquery.Client) -> int:
    """Validate the load by checking row count."""
    table = client.get_table(TABLE_ID)
    row_count = table.num_rows

    if row_count == 0:
        raise ValueError(f'Table {TABLE_ID} has 0 rows after load')

    return row_count


def main():
    """Main ingestion pipeline."""
    logger.info('Starting fees ingestion pipeline')
    logger.info(f'Project: {PROJECT_ID}')
    logger.info(f'Source: {GCS_URI}')
    logger.info(f'Destination: {TABLE_ID}')

    client = get_bq_client()

    try:
        # Load data
        load_job = load_csv_to_bigquery(client)
        logger.info(f'Load job completed: {load_job.job_id}')

        # Validate
        row_count = validate_load(client)
        logger.info(f'Successfully loaded {row_count:,} rows to {TABLE_ID}')

        # Log job statistics
        logger.info(f'Bytes processed: {load_job.output_bytes:,}')

    except Exception as e:
        logger.error(f'Ingestion failed: {e}')
        raise


# Cloud Function entry point
def ingest_fees(event=None, context=None):
    """
    Cloud Function entry point.

    Can be triggered by:
    - Cloud Scheduler (HTTP)
    - GCS file upload (event)
    - Manual invocation
    """
    main()
    return {'status': 'success', 'table': TABLE_ID}


if __name__ == '__main__':
    main()
