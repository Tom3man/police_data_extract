import json
import logging

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def load_schema_from_file(schema_file_path: str) -> list[bigquery.SchemaField]:
    """
    Loads BigQuery schema from a JSON file.

    Args:
        schema_file_path (str): Path to the JSON file containing the schema.

    Returns:
        list[bigquery.SchemaField]: A list of BigQuery schema fields.
    """
    try:
        with open(schema_file_path, "r") as f:
            schema_data = json.load(f)
            return [
                bigquery.SchemaField(
                    name=field["name"],
                    field_type=field["type"],
                    mode=field["mode"]
                )
                for field in schema_data
            ]
    except FileNotFoundError:
        logging.error(f"Schema file '{schema_file_path}' not found.")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing schema file '{schema_file_path}': {e}")
        raise


def create_table_if_not_exists(client: bigquery.Client, dataset_id: str, table_id: str, schema_file: str):
    """
    Checks if a BigQuery table exists and creates it if it doesn't.

    Args:
        client (bigquery.Client): BigQuery client.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        schema_file (str): Path to the JSON file containing the table schema.

    Returns:
        None
    """
    table_ref = client.dataset(dataset_id).table(table_id)

    # Load schema
    schema = load_schema_from_file(schema_file)

    try:
        client.get_table(table_ref)  # Try to get the table
        logging.info(f"Table '{table_id}' already exists in dataset '{dataset_id}'.")
    except NotFound:
        # Table doesn't exist; create it
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logging.info(f"Table '{table_id}' created in dataset '{dataset_id}'.")
