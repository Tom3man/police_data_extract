import pandas as pd
from datetime import datetime

# Columns to select and format in the DataFrame
SELECT_COLS = [
    'crime_id',
    'month',
    'falls_within',
    'longitude',
    'latitude',
    'lsoa_code',
    'crime_type',
    'location'
]

RENAMED_COLUMNS = [
    'crime_id',
    'month_year',
    'police_force',
    'longitude',
    'latitude',
    'lsoa_code',
    'crime_type',
    'location_description'
]


def format_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and formats a given DataFrame for ingestion.

    This function performs the following transformations:
        1. Normalises column names by replacing spaces with underscores and converting to lowercase.
        2. Filters the DataFrame to include only the required columns (`SELECT_COLS`).
        3. Adds a `load_date` column to track when the data was processed.

    Args:
        df (pd.DataFrame): The input DataFrame to be cleaned and formatted.

    Returns:
        pd.DataFrame: A cleaned and formatted DataFrame.

    Raises:
        KeyError: If any of the required columns in `SELECT_COLS` are missing.
        ValueError: If the input DataFrame is empty or invalid.
    """
    if df.empty:
        raise ValueError("Input DataFrame is empty.")

    # Normalise column names
    df.columns = [col.replace(" ", "_").lower() for col in df.columns]

    # Validate required columns
    missing_cols = [col for col in SELECT_COLS if col not in df.columns]
    if missing_cols:
        raise KeyError(f"The following required columns are missing from the input DataFrame: {missing_cols}")

    # Select only required columns
    df = df[SELECT_COLS]

    # Rename columns
    df.columns = RENAMED_COLUMNS

    # Drop any rows without longitude or latitude
    df = df[df['longitude'].notna() & df['latitude'].notna()]

    # Drop any rows without a crime ID
    df = df[df['crime_id'].notna()]

    # Drop any duplicate rows
    df = df.drop_duplicates()

    # # Add a load date column for tracking
    # df['load_date'] = datetime.today().strftime('%Y-%m-%d')

    return df
