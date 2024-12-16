import logging
import os
import time
from typing import List, Union

from orb.spinner.utils import find_element_with_retry
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support.ui import Select

from police_data import DATA_PATH, MODULE_PATH
from police_data.src.extract.common import (clean_and_reorganise_data,
                                            extract_and_cleanup_zip,
                                            extract_json_file,
                                            rename_latest_file)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


class PoliceDataDownloader:
    """
    A class to automate the download of police data using Selenium.

    Attributes:
        data_path (str): The local path where downloaded files are saved.
        start_date (str): The start date in YYYY-MM format for the data range.
        end_date (str): The end date in YYYY-MM format for the data range.
        driver (WebDriver): The Selenium WebDriver instance.
    """

    BASE_URL: str = "https://data.police.uk/data/"

    def __init__(self, data_path: str):
        """
        Initialises the PoliceDataDownloader class.

        Args:
            data_path (str): Local path for saving downloaded data.
        """
        self.data_path = data_path

        self.driver = None

    def configure_web_driver(self) -> Union[WebDriver, None]:
        """
        Configures the Selenium WebDriver with download preferences and initialises it.
        """
        try:
            log.info("Configuring WebDriver...")
            prefs = {
                "download.default_directory": self.data_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
            }

            options = webdriver.ChromeOptions()
            options.add_experimental_option("prefs", prefs)

            self.driver = webdriver.Chrome(options=options)
            self.driver.get(self.BASE_URL)
            log.info("WebDriver configured successfully.")

            return self.driver

        except WebDriverException as e:
            log.error(f"Error configuring WebDriver: {e}")
            raise

    @staticmethod
    def select_dates(driver: WebDriver, start_date: str, end_date: str):
        """
        Selects the start and end dates for the data range.

        Args:
            start_date (str): Start date in YYYY-MM format.
            end_date (str): End date in YYYY-MM format.
        """
        log.info("Selecting dates...")
        try:
            start_date_element = find_element_with_retry(
                driver=driver, locator=(By.ID, "id_date_from"))
            start_date_dropdown = Select(start_date_element)
            start_date_dropdown.select_by_value(start_date)

            end_date_element = find_element_with_retry(
                driver=driver, locator=(By.ID, "id_date_to"))
            end_date_dropdown = Select(end_date_element)
            end_date_dropdown.select_by_value(end_date)

            log.info("Dates selected successfully.")
        except Exception as e:
            log.error(f"Error selecting dates: {e}")
            raise

    @staticmethod
    def select_forces(driver: WebDriver, force_ids: List[str]) -> None:
        """
        Selects multiple police forces based on their IDs.

        Args:
            force_ids (List[str]): List of force IDs to select.

        Raises:
            Exception: If a force cannot be selected after retries.
        """
        for force_id in force_ids:
            try:
                log.debug(f"Attempting to select force with ID: {force_id}")

                # Locate the force checkbox or element
                force_element = find_element_with_retry(
                    driver=driver, locator=(By.ID, force_id))

                # Click the element to select it
                force_element.click()
                log.info(f"Successfully selected force: {force_id}")

            except Exception as e:
                log.error(f"Error selecting force '{force_id}': {e}")
                raise

    @staticmethod
    def generate_file(driver: WebDriver):
        """
        Clicks the 'Generate File' button to initiate data generation.
        """
        log.info("Generating file...")
        try:
            generate_file_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
            generate_file_button.click()
            log.info("File generation initiated.")
        except Exception as e:
            log.error(f"Error generating file: {e}")
            raise

    def download_to_local(self, driver: WebDriver, button_timeout: int = 30, download_timeout: int = 60):
        """
        Waits for the 'download now' button to appear, clicks it, and waits for a .zip file to be fully downloaded.

        Args:
            button_timeout (int): Maximum time to wait for the download button to appear (in seconds).
            download_timeout (int): Maximum time to wait for the download to complete (in seconds).

        Raises:
            TimeoutError: If the button does not appear or the download does not complete within the timeout period.
        """
        log.info("Starting download process...")

        # Monitor for the 'download now' button
        button_start_time = time.time()
        download_button = None

        while True:
            try:
                download_button = find_element_with_retry(
                    driver=driver, locator=(By.CLASS_NAME, "button"), wait_time=5)
                download_button_text = download_button.text.lower().strip()
                log.debug(f"Button text: {download_button_text}")

                if download_button_text == 'download now':
                    log.info("Download button is ready.")
                    break
            except Exception as e:
                log.warning(f"Waiting for download button: {e}")

            if time.time() - button_start_time > button_timeout:
                raise TimeoutError("Download button did not appear within the timeout period.")
            time.sleep(1)  # Poll every second

        # Click the download button
        try:
            log.info("Clicking the download button...")
            download_button.click()
            time.sleep(5)
        except Exception as e:
            log.error(f"Failed to click the download button: {e}")
            raise

        # Monitor the download folder for the .zip file
        download_start_time = time.time()

        try:
            while True:
                # List files in the download directory
                files = os.listdir(self.data_path)
                if files:
                    # Check if any temporary download files are still present
                    if not any(file.endswith(".crdownload") or file.endswith(".tmp") for file in files):
                        log.info(f"Downloaded files: {files}")
                        return
                # Check timeout
                if time.time() - download_start_time > download_timeout:
                    raise TimeoutError("Download did not complete within the timeout period.")
                time.sleep(1)  # Poll every second
        except Exception as e:
            log.error(f"Error during file download: {e}")
            raise


if __name__ == "__main__":

    # Define the paths for raw data and configuration
    data_path = f"{DATA_PATH}/police_data_raw"

    # Extract start/end dates and force IDs from the JSON configuration file
    json_info = extract_json_file(
        json_file_path=f'{MODULE_PATH}/src/extract/extract.json'
    )

    if json_info:
        # Unpack extracted data and log the details
        (start_date, end_date), force_ids = json_info
        log.info(f"Start Date: {start_date}, End Date: {end_date}")
        log.info(f"Active Force IDs: {force_ids}")
    else:
        # Log an error if JSON extraction fails
        log.error("Failed to extract data.")
        exit(1)  # Exit the program since the required data is missing

    # Initialise the PoliceDataDownloader with the raw data path
    street_data_downloader = PoliceDataDownloader(data_path=data_path)

    try:
        # Start the download process
        log.info("Starting download process...")

        # Configure the Selenium WebDriver
        web_driver = street_data_downloader.configure_web_driver()

        # Select the date range for downloading data
        street_data_downloader.select_dates(
            driver=web_driver, start_date=start_date, end_date=end_date
        )

        # Select the police forces based on the active IDs
        street_data_downloader.select_forces(
            driver=web_driver, force_ids=force_ids
        )

        # Generate the file on the website
        street_data_downloader.generate_file(driver=web_driver)

        # Download the generated file to the local machine
        street_data_downloader.download_to_local(driver=web_driver)

        log.info("Download process completed successfully.")
    except Exception as e:
        # Log any errors encountered during the process
        log.error(f"Process failed: {e}")
    finally:
        # Ensure the WebDriver is closed properly
        if street_data_downloader.driver:
            street_data_downloader.driver.quit()
            log.info("WebDriver closed.")

    # Rename the downloaded file to a standard name for further processing
    rename_latest_file(
        download_dir=data_path, new_name='street.zip'
    )

    # Define paths for the zip file and cleaned data folder
    zip_folder = f"{data_path}/street.zip"
    cleaned_folder = f"{DATA_PATH}/police_data_cleaned"

    # Extract and clean up the zip file
    output_folder = extract_and_cleanup_zip(
        zip_file_path=zip_folder, delete_zip=True
    )

    # Reorganize the extracted data into the cleaned folder
    clean_and_reorganise_data(
        source_folder=output_folder, output_folder=cleaned_folder
    )
