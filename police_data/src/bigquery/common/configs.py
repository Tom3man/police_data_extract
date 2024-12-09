import json
import logging
from pathlib import Path
from typing import Dict

from police_data import MODULE_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def get_bq_config() -> Dict[str, str]:
    """
    Loads BigQuery configuration from a JSON file.

    Returns:
        dict: Configuration as a dictionary.

    Raises:
        SystemExit: If the configuration file is not found or is invalid.
    """
    config_file = Path(MODULE_PATH) / "src/bigquery/bq_config.json"

    try:
        with config_file.open("r") as file:
            return json.load(file)
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_file}")
        exit(1)
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing configuration file '{config_file}': {e}")
        exit(1)
