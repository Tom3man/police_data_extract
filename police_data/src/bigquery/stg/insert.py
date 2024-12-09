import logging
from pathlib import Path

from google.cloud import bigquery

from police_data.src.bigquery.common.configs import get_bq_config
from police_data.src.bigquery.common.table_checks import \
    create_table_if_not_exists

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


def load_data_from_gcs(client, dataset_id, table_id, gcs_uri):
    """
    Loads data from GCS into a BigQuery table.

    Args:
        client (bigquery.Client): BigQuery client.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        gcs_uri (str): GCS URI of the source data.

    Returns:
        None
    """
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Assumes the first row contains headers
        autodetect=False      # Use the predefined schema
    )

    # Load data into BigQuery
    logging.info(f"Starting load job for GCS URI: {gcs_uri}")
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    logging.info(f"Data from {gcs_uri} loaded into '{table_id}'.")


if __name__ == "__main__":

    gcs_uri = "gs://police-street-crime-data/lancashire/year=2023/month=09/09.csv"

    # Set up client
    client = bigquery.Client()

    # Load configuration from JSON

    # Get global bigquery config
    config = get_bq_config()

    # Extract bucket name and local folder path
    dataset_id = config.get('dataset_id')
    staging_table_id = config.get('staging_table_id')

    script_dir = Path(__file__).resolve().parent
    schema_path = f"{script_dir}/staging_schema.json"

    # Create staging table if it doesn't exist
    create_table_if_not_exists(
        client=client,
        dataset_id=dataset_id,
        table_id=staging_table_id,
        schema_file=schema_path,
    )

    # Load data into staging table
    try:
        load_data_from_gcs(client, dataset_id, staging_table_id, gcs_uri)
    except Exception as e:
        logging.error(f"Failed to load data from GCS to staging table: {e}")
