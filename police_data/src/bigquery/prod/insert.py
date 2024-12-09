import logging
from pathlib import Path

from google.api_core.exceptions import NotFound
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


def load_staging_to_production(client, staging_table_ref, production_table_ref):
    """
    Inserts data from the staging table into the production table with transformations.

    Args:
        client (bigquery.Client): BigQuery client.
        staging_table_ref (bigquery.TableReference): Reference to the staging table.
        production_table_ref (bigquery.TableReference): Reference to the production table.
    """
    query = f"""
        MERGE INTO
            `{production_table_ref.project}.{production_table_ref.dataset_id}.{production_table_ref.table_id}`
        AS prod
        USING (
            SELECT
                crime_id,
                PARSE_DATE('%Y-%m', month) AS month_year,
                falls_within AS police_force,
                longitude,
                latitude,
                lsoa_code,
                crime_type,
                location AS location_description,
                ST_GEOGPOINT(longitude, latitude) AS geo_point
            FROM `{staging_table_ref.project}.{staging_table_ref.dataset_id}.{staging_table_ref.table_id}`
            WHERE longitude IS NOT NULL
            AND crime_id IS NOT NULL
        ) AS stage
        ON prod.crime_id = stage.crime_id
        AND prod.month_year = stage.month_year
        WHEN NOT MATCHED THEN
        INSERT (
            crime_id,
            month_year,
            police_force,
            longitude,
            latitude,
            lsoa_code,
            crime_type,
            location_description,
            geo_point
        )
        VALUES (
            stage.crime_id,
            stage.month_year,
            stage.police_force,
            stage.longitude,
            stage.latitude,
            stage.lsoa_code,
            stage.crime_type,
            stage.location_description,
            stage.geo_point
        );
    """
    logging.info("Starting query job to load data from staging to production...")
    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete
    logging.info("Data successfully loaded into production table.")


def delete_staging_table(client, table_ref):
    """
    Deletes the staging table.

    Args:
        client (bigquery.Client): BigQuery client.
        table_ref (bigquery.TableReference): Reference to the table to delete.
    """
    try:
        client.delete_table(table_ref)
        logging.info(f"Staging table '{table_ref.table_id}' successfully deleted.")
    except NotFound:
        logging.warning(f"Staging table '{table_ref.table_id}' not found; nothing to delete.")
    except Exception as e:
        logging.error(f"Failed to delete staging table '{table_ref.table_id}': {e}")


if __name__ == "__main__":

    # Initialise BigQuery client
    client = bigquery.Client()

    # Get global bigquery config
    config = get_bq_config()

    # Extract bucket name and local folder path
    dataset_id = config.get('dataset_id')
    staging_table_id = config.get('staging_table_id')
    production_table_id = config.get('production_table_id')

    # References to tables
    staging_table_ref = client.dataset(dataset_id).table(staging_table_id)
    production_table_ref = client.dataset(dataset_id).table(production_table_id)

    script_dir = Path(__file__).resolve().parent
    schema_path = f"{script_dir}/prod_schema.json"

    # Create staging table if it doesn't exist
    create_table_if_not_exists(
        client=client,
        dataset_id=dataset_id,
        table_id=staging_table_id,
        schema_file=schema_path,
    )

    # Load data from staging to production
    try:
        load_staging_to_production(client, staging_table_ref, production_table_ref)

        # Delete staging table after successful load
        delete_staging_table(client, staging_table_ref)

    except Exception as e:
        logging.error(f"Failed to load data from staging to production: {e}")
