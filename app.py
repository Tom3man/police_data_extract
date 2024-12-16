import psycopg2
from fastapi import FastAPI, HTTPException, Query
from psycopg2.extras import RealDictCursor

# Database configuration
DATABASE_CONFIG = {
    "dbname": "police_data",
    "user": "postgres",
    "password": "freehily123",
    "host": "127.0.0.1",
    "port": 5433,
}


def get_connection():
    """
    Returns a new database connection.
    """
    return psycopg2.connect(**DATABASE_CONFIG)


app = FastAPI()


@app.get("/")
def root():
    return {"message": "Welcome to the Police Data API!"}


@app.get("/crimes/")
def get_crimes(longitude: float, latitude: float, radius: float = Query(1000, gt=0)):
    """
    Retrieves crimes within a specified radius from a given longitude and latitude.

    Args:
        longitude (float): Longitude of the search point.
        latitude (float): Latitude of the search point.
        radius (float): Search radius in meters (default: 1000 meters).

    Returns:
        List of crimes within the specified radius.
    """
    query = """
        SELECT
            crime_id,
            month_year,
            police_force,
            longitude,
            latitude,
            lsoa_code,
            crime_type,
            location_description
        FROM crimes_prod
        WHERE ST_DWithin(
            geo_point,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326),
            %s
        );
    """
    try:
        conn = get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (longitude, latitude, radius))
            results = cursor.fetchall()
            if not results:
                raise HTTPException(status_code=404, detail="No crimes found in the given area.")
            return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
