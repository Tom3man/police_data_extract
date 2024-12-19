import json
import logging
import os
from datetime import datetime
from typing import List

import psycopg2
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def load_db_config(config_file="db_config.json"):
    """
    Loads the database configuration from a JSON file.
    """
    with open(config_file, 'r') as f:
        return json.load(f)


def extract_relevant_csv_files(
        data_folder: str, start_date: str, end_date: str
) -> List[str]:
    """
    Extracts a list of CSV file paths from a given folder that match a date range.

    The function generates a range of months between `start_date` and `end_date` in "YYYY-MM" format
    and identifies CSV files in the directory whose paths contain matching year and month folders.

    Args:
        data_folder (str): Path to the root folder containing CSV files organized by year and month.
        start_date (str): Start date in "YYYY-MM" format.
        end_date (str): End date in "YYYY-MM" format.

    Returns:
        List[str]: A list of file paths to relevant CSV files within the date range.

    Raises:
        ValueError: If `start_date` or `end_date` is not in the correct format.
        FileNotFoundError: If `data_folder` does not exist.
    """
    # Validate folder path
    if not os.path.exists(data_folder):
        raise FileNotFoundError(f"The specified data folder does not exist: {data_folder}")

    # Parse dates
    try:
        start = datetime.strptime(start_date, "%Y-%m")
        end = datetime.strptime(end_date, "%Y-%m")
    except ValueError:
        raise ValueError("Start date and end date must be in 'YYYY-MM' format.")

    # Generate date range in "YYYY-MM" format
    date_range = []
    current = start.replace(day=1)  # Start from the first day of the month
    while current <= end:
        date_range.append(current.strftime("%Y-%m"))
        # Move to the next month
        next_month = current.month + 1
        current = current.replace(
            year=current.year + (next_month // 13),  # Increment year if needed
            month=(next_month % 12) or 12  # Ensure month wraps around
        )

    # Collect relevant CSV file paths
    csv_files = []
    for root, _, files in os.walk(data_folder):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)

                # Extract month-year from file path (assuming folder structure includes YYYY/MM)
                try:
                    month_year = f"{file_path.split('/')[-2]}-{file_path.split('/')[-1].replace('.csv', '')}"
                    if month_year in date_range:
                        csv_files.append(file_path)
                except (IndexError, ValueError):
                    # Skip files that don't match the expected folder structure
                    continue

    return csv_files


def write_csv_to_staging(file_path, db_config):
    """
    Loads a CSV file into the staging table.
    """
    connection = None
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        with open(file_path, 'r') as f:
            cursor.copy_expert("""
                COPY crimes_staging (
                    crime_id,
                    month_year,
                    police_force,
                    longitude,
                    latitude,
                    lsoa_code,
                    crime_type,
                    location_description
                )
                FROM STDIN
                WITH CSV HEADER DELIMITER ',';
            """, f)
        connection.commit()
    except Exception as e:
        log.error(f"Error loading CSV: {e}")
    finally:
        if connection:
            connection.close()


def upload_staging_to_prod(db_config):
    """
    Moves data from the staging table to the production table.
    """
    connection = None
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("""
            INSERT INTO crimes_prod (
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
            SELECT
                crime_id,
                month_year,
                police_force,
                longitude,
                latitude,
                lsoa_code,
                crime_type,
                location_description,
                ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geo_point
            FROM crimes_staging;
        """)
        connection.commit()
    except Exception as e:
        log.error(f"Error uploading to production: {e}")
    finally:
        if connection:
            connection.close()


def truncate_staging(db_config):
    """
    Clears all data from the staging table.
    """
    connection = None
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE crimes_staging;")
        connection.commit()
    except Exception as e:
        log.error(f"Error truncating staging table: {e}")
    finally:
        if connection:
            connection.close()


def main():

    # Load the database configuration from the config file
    CONFIG = load_db_config()

    DB_CONFIG = CONFIG['db_config']
    RUN_CONFIG = CONFIG['run_config']

    data_folder = RUN_CONFIG['data_path']
    start_date = RUN_CONFIG['start_date']
    end_date = RUN_CONFIG['end_date']

    files = extract_relevant_csv_files(
        data_folder=data_folder, start_date=start_date, end_date=end_date)

    file_count = len(files)

    log.info(f"Preparing to ingest {file_count} files to staging and production.")

    loading_count = 0

    # Add tqdm to display progress
    for file_path in tqdm(files, desc="Processing files", unit="file"):

        # Write a CSV to the staging table
        write_csv_to_staging(file_path=file_path, db_config=DB_CONFIG)

        loading_count += 1

        if loading_count == 5:
            # Upload from staging to production
            upload_staging_to_prod(db_config=DB_CONFIG)

            # Truncate the staging table
            truncate_staging(db_config=DB_CONFIG)

            loading_count = 0


if __name__ == "__main__":

    main()
