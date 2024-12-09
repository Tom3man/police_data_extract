import json
import logging
import os
import time
import zipfile
from datetime import datetime
from typing import List, Optional, Tuple, Union

import pandas as pd

from police_data import DATA_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def extract_json_file(json_file_path: str) -> Union[Tuple[Tuple[str, str], List[str]], None]:
    """
    Reads a JSON file and extracts the start and end dates and IDs of all entries with 'active': true.

    Args:
        json_file_path (str): Path to the JSON file.

    Returns:
        Union[Tuple[Tuple[str, str], List[str]], None]:
            - A tuple containing:
                - A tuple of the start and end dates (str, str).
                - A list of active force IDs (List[str]).
            - None if the file could not be read or the JSON is invalid.
    """
    try:
        # Open and load the JSON file
        with open(json_file_path, 'r') as file:
            data = json.load(file)

        # Extract IDs where 'active' is True
        active_ids = [
            force['id'] for force in data.get('forces', []) if force.get('active', False)
        ]

        # Extract start and end dates
        dates = data.get('dates', {})
        start_date = dates.get('start_date')
        end_date = dates.get('end_date')

        # Validate required fields
        if start_date is None or end_date is None:
            raise KeyError("Missing 'start_date' or 'end_date' in 'dates'.")

        return (start_date, end_date), active_ids

    except FileNotFoundError:
        log.error(f"Error: File '{json_file_path}' not found.")
    except json.JSONDecodeError:
        log.error(f"Error: Invalid JSON format in file '{json_file_path}'.")
    except KeyError as e:
        log.error(f"Error: Missing key in JSON - {e}.")
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}.")

    return None


def rename_latest_file(download_dir: str, new_name: str, timeout: Optional[int] = 60):
    """
    Renames the most recently downloaded file in a directory.

    Args:
        download_dir (str): The directory where the file is downloaded.
        new_name (str): The new name for the file.
        timeout (int): Maximum time to wait for the file to appear (in seconds).

    Raises:
        FileNotFoundError: If no file is found within the timeout period.
    """
    start_time = time.time()

    while True:
        # List all files in the directory sorted by modification time
        files = sorted(
            [os.path.join(download_dir, f) for f in os.listdir(download_dir)],
            key=os.path.getmtime,
        )

        if files:
            latest_file = files[-1]  # Get the most recently modified file
            if not latest_file.endswith(".crdownload"):  # Ensure it's not incomplete
                new_file_path = os.path.join(download_dir, new_name)
                os.rename(latest_file, new_file_path)
                log.info(f"File renamed to '{new_file_path}'.")
                return

        if time.time() - start_time > timeout:
            raise FileNotFoundError(f"No completed file found in '{download_dir}' within the timeout period.")
        time.sleep(1)  # Poll every second


def clean_and_reorganise_data(source_folder: str, output_folder: str) -> None:
    """
    Cleans, reorganises, and deletes old police data from the source folder
    after processing to the output folder. Data is organized by region/year,
    with files named as the month (e.g., 07.csv, 08.csv).

    Args:
        source_folder (str): Path to the source folder containing raw data (year-month structure).
        output_folder (str): Path to the output folder where cleaned data will be saved.

    Returns:
        None
    """

    # Identify all year-month directories in the source folder
    year_month_folders = [
        os.path.join(source_folder, folder)
        for folder in os.listdir(source_folder)
        if os.path.isdir(os.path.join(source_folder, folder))
    ]

    log.info(f"Found {len(year_month_folders)} year-month folders to process.")

    # Process each year-month folder
    for year_month_folder in year_month_folders:
        year_month = os.path.basename(year_month_folder)
        log.info(f"Processing folder: {year_month_folder}")

        # Extract year and month from the folder name (e.g., '2022-07')
        try:
            year, month = year_month.split("-")
        except ValueError as e:
            log.error(f"Invalid folder name format: {year_month}. Expected 'YYYY-MM'. Error: {e}")
            continue

        # Iterate over all files in the year-month folder
        for region_file in os.listdir(year_month_folder):
            original_path = os.path.join(year_month_folder, region_file)

            # Extract region name by cleaning the file name
            try:
                region = (
                    region_file
                    .replace(f"{year_month}-", "")
                    .replace("-street", "")
                    .replace(".csv", "")
                )
            except Exception as e:
                log.error(f"Error extracting region name from {region_file}: {e}")
                continue

            # Create the region/year folder in the output directory
            new_region_folder = os.path.join(output_folder, region, year)
            os.makedirs(new_region_folder, exist_ok=True)

            # Define the new file path (rename to month.csv)
            new_region_file = os.path.join(new_region_folder, f"{month}.csv")

            # Read, clean, save, and delete the processed file
            try:
                # Read and process the data
                df = pd.read_csv(original_path)
                df.columns = [col.replace(" ", "_").lower() for col in df.columns]

                # Add a load date column for tracking
                df['load_date'] = datetime.today().strftime('%Y-%m-%d')

                # Save the cleaned data
                df.to_csv(new_region_file, index=False)

                log.info(f"Processed and saved: {new_region_file}")

                # Delete the original file after successful processing
                os.remove(original_path)
                log.info(f"Deleted original file: {original_path}")

            except Exception as e:
                log.error(f"Error processing file {original_path}: {e}")

        # Delete the now-empty year-month folder
        try:
            os.rmdir(year_month_folder)
            log.info(f"Deleted empty folder: {year_month_folder}")
        except OSError as e:
            log.error(f"Error deleting folder {year_month_folder}: {e}")


def extract_and_cleanup_zip(
        zip_file_path: str,
        extract_to: Optional[str] = f"{DATA_PATH}/police_data_extracted",
        delete_zip: Optional[bool] = False
) -> str:
    """
    Extracts a zip file to a specified directory and optionally deletes the zip file afterward.

    Args:
        zip_file_path (str): The path to the zip file to be extracted.
        extract_to (str): The directory where the zip contents will be extracted.
        delete_zip (bool): Whether to delete the zip file after extraction. Default is False.

    Returns:
        None
    """
    try:
        # Ensure the extraction directory exists
        os.makedirs(extract_to, exist_ok=True)

        # Open and extract the zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            log.info(f"Extracted '{zip_file_path}' to '{extract_to}'")

        # Optionally delete the zip file
        if delete_zip:
            os.remove(zip_file_path)
            log.info(f"Deleted zip file: {zip_file_path}")

        return extract_to

    except FileNotFoundError:
        log.error(f"Error: The file '{zip_file_path}' does not exist.")
    except zipfile.BadZipFile:
        log.error(f"Error: The file '{zip_file_path}' is not a valid zip file.")
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}")
