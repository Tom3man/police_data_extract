import psycopg2
from psycopg2 import sql, OperationalError
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)


def create_table_if_not_exists(connection_params, table_name, create_table_query):
    """
    Create a table in PostgreSQL if it does not already exist.

    Args:
        connection_params (dict): Connection parameters for PostgreSQL.
                                  Example:
                                  {
                                      "dbname": "police_data",
                                      "user": "postgres",
                                      "password": "freehily123",
                                      "host": "34.147.240.114",
                                      "port": "5432"
                                  }
        table_name (str): The name of the table to create.
        create_table_query (str): SQL query to create the table.

    Returns:
        None
    """
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(**connection_params)
        cursor = connection.cursor()

        # Check if the table exists
        cursor.execute(
            sql.SQL(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
                """
            ),
            [table_name]
        )
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            logging.info(f"Table '{table_name}' does not exist. Creating it...")
            cursor.execute(create_table_query)
            connection.commit()
            logging.info(f"Table '{table_name}' created successfully.")
        else:
            logging.info(f"Table '{table_name}' already exists. Skipping creation.")

    except OperationalError as e:
        logging.error(f"Failed to connect to PostgreSQL server: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        # Close the database connection
        if connection:
            cursor.close()
            connection.close()
            logging.info("PostgreSQL connection closed.")


if __name__ == "__main__":

    # Connection parameters
    connection_params = {
        "dbname": "police_data",
        "user": "postgres",
        "password": "freehily123",
        "host": "34.147.240.114",  # Replace with your Cloud SQL instance IP
        "port": "5432"
    }

    # Table name
    table_name = "crimes"

    # Create table query
    create_table_query = """
    CREATE TABLE crimes (
        crime_id TEXT NOT NULL PRIMARY KEY,
        month_year DATE NOT NULL,
        police_force TEXT NOT NULL,
        longitude FLOAT NOT NULL,
        latitude FLOAT NOT NULL,
        lsoa_code TEXT,
        crime_type TEXT,
        location_description TEXT,
        geo_point GEOGRAPHY NOT NULL
    );
    """

    # Call the function
    create_table_if_not_exists(connection_params, table_name, create_table_query)
