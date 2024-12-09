import json
import logging
import os

from google.cloud import storage

from police_data import DATA_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def upload_file_to_gcs(
        region: str, year: str, month: str, local_folder: str, bucket_name: str, skip_existing: bool = True
):
    """
    Uploads a specific file to a GCS bucket, organised as partitioned year/month.

    Args:
        region (str): The region name (e.g., "lancashire").
        year (str): The year (e.g., "2023").
        month (str): The month (e.g., "09").
        local_folder (str): Path to the local folder containing the file.
        bucket_name (str): Name of the GCS bucket.
        skip_existing (bool): Whether to skip uploading files that already exist in GCS.

    Returns:
        None
    """
    # Build the local file path
    local_file = os.path.join(local_folder, region, year, f"{month}.csv")

    if not os.path.exists(local_file):
        logging.warning(f"File not found: {local_file}")
        return

    # Construct the GCS path
    gcs_path = os.path.join(region, f"year={year}", f"month={month}", f"{month}.csv").replace("\\", "/")

    # Initialize GCS client and bucket
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    # Check if file exists in GCS
    if skip_existing and blob.exists():
        logging.info(f"Skipped existing file: gs://{bucket_name}/{gcs_path}")
        return

    # Upload the file
    try:
        blob.upload_from_filename(local_file)
        logging.info(f"Uploaded {local_file} to gs://{bucket_name}/{gcs_path}")
    except Exception as e:
        logging.error(f"Error uploading {local_file} to gs://{bucket_name}/{gcs_path}: {e}")


def upload_folder(local_folder: str, bucket_name: str):
    """
    Uploads all CSV files from a local folder to a GCS bucket, maintaining the regional structure.

    Args:
        local_folder (str): Path to the local folder containing regional data.
        bucket_name (str): Name of the GCS bucket.
    """
    # Get top-level region folders
    regional_folder_list = [
        os.path.join(local_folder, dir_name)
        for dir_name in os.listdir(local_folder)
        if os.path.isdir(os.path.join(local_folder, dir_name))
    ]

    for regional_folder in regional_folder_list:
        region = os.path.basename(regional_folder)

        # Get year folders within each region
        year_folder_list = [
            os.path.join(regional_folder, dir_name)
            for dir_name in os.listdir(regional_folder)
            if os.path.isdir(os.path.join(regional_folder, dir_name))
        ]

        for year_folder in year_folder_list:
            year = os.path.basename(year_folder)

            # Get CSV files within each year folder
            files = [f for f in os.listdir(year_folder) if os.path.isfile(os.path.join(year_folder, f))]

            for file in files:
                month = file.replace('.csv', '')
                logging.info(f"Preparing to upload: Region={region}, Year={year}, Month={month}")

                # Upload each file
                upload_file_to_gcs(
                    region=region,
                    year=year,
                    month=month,
                    local_folder=local_folder,
                    bucket_name=bucket_name
                )


if __name__ == "__main__":

    # Define paths and configurations
    data_path = f"{DATA_PATH}/police_data_raw"

    # Load configuration from JSON
    try:
        with open('upload.json', 'r') as file:
            config = json.load(file)
    except FileNotFoundError:
        logging.error("Configuration file 'upload.json' not found.")
        exit(1)

    # Extract bucket name and local folder path
    bucket_name = config.get('bucket_name')
    local_folder_path = f"{DATA_PATH}/{config.get('local_folder_path')}"

    if not bucket_name or not os.path.exists(local_folder_path):
        logging.error("Invalid configuration: Ensure 'bucket_name' and 'local_folder_path' are set correctly.")
        exit(1)

    # Begin uploading files
    logging.info("Starting upload process...")
    upload_folder(local_folder=local_folder_path, bucket_name=bucket_name)
    logging.info("Upload process completed.")
