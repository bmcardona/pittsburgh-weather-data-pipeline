"""
Weather data extraction from Open-Meteo API for Pittsburgh neighborhoods
"""
import requests
import json
import psycopg2
from datetime import datetime
import os
from typing import Dict, Optional


def get_db_connection():
    """
    Create database connection using environment variables.
    
    Returns:
        psycopg2.connection: Active database connection
    """
    return psycopg2.connect(
        host=os.getenv('WEATHER_DB_HOST'),
        port=os.getenv('WEATHER_DB_PORT'),
        database=os.getenv('WEATHER_DB_NAME'),
        user=os.getenv('WEATHER_DB_USER'),
        password=os.getenv('WEATHER_DB_PASSWORD')
    )


def get_weather_data(latitude: float, longitude: float) -> Optional[Dict]:
    """
    Fetch current weather data from Open-Meteo API.
    
    Args:
        latitude: Latitude coordinate in decimal degrees
        longitude: Longitude coordinate in decimal degrees
    
    Returns:
        Optional[Dict]: JSON response containing weather data, or None if request fails
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": [
            "temperature_2m", 
            "relative_humidity_2m", 
            "apparent_temperature",
            "is_day", 
            "wind_speed_10m", 
            "wind_direction_10m", 
            "wind_gusts_10m",
            "precipitation", 
            "rain", 
            "showers", 
            "snowfall", 
            "weather_code",
            "cloud_cover", 
            "pressure_msl", 
            "surface_pressure"
        ],
        "timezone": "America/New_York",
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request error for lat={latitude}, lon={longitude}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON decode error for lat={latitude}, lon={longitude}: {e}")
        return None


def load_coordinates(coordinates_path: str = '/opt/airflow/etl/coordinates.json') -> list:
    """
    Load neighborhood coordinates from JSON file.
    
    Args:
        coordinates_path: Path to the JSON file containing coordinate data
    
    Returns:
        list: List of neighborhoods with name, latitude, longitude
    
    Raises:
        FileNotFoundError: If the coordinates file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
        KeyError: If required fields are missing
    """
    with open(coordinates_path, 'r') as f:
        data = json.load(f)
    
    if 'neighborhoods' not in data:
        raise KeyError("coordinates.json must contain 'neighborhoods' key")
    
    return data['neighborhoods']


def get_or_create_date(cursor, observation_time: datetime, schema: str) -> int:
    """
    Get or create date dimension record and return date_id.
    
    Args:
        cursor: Active database cursor
        observation_time: DateTime object for the weather observation
        schema: Database schema name
    
    Returns:
        int: The date_id (primary key) for the date dimension record
    """
    date_only = observation_time.date()
    
    # Try to get existing date_id
    cursor.execute(f"""
        SELECT date_id FROM {schema}.dim_date WHERE date = %s
    """, (date_only,))
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Create new date dimension record
    cursor.execute(f"""
        INSERT INTO {schema}.dim_date (
            date, year, month, day, day_of_week, day_name, 
            week_of_year, quarter, is_weekend
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING date_id
    """, (
        date_only,
        date_only.year,
        date_only.month,
        date_only.day,
        date_only.weekday(),
        date_only.strftime('%A'),
        date_only.isocalendar()[1],
        (date_only.month - 1) // 3 + 1,
        date_only.weekday() >= 5
    ))
    
    return cursor.fetchone()[0]


def insert_or_update_location(cursor, neighborhood_name: str, latitude: float, 
                              longitude: float, schema: str) -> int:
    """
    Insert or update location dimension record and return location_id.
    
    Args:
        cursor: Active database cursor
        neighborhood_name: Name of the neighborhood (e.g., "Shadyside")
        latitude: Latitude coordinate in decimal degrees
        longitude: Longitude coordinate in decimal degrees
        schema: Database schema name
    
    Returns:
        int: The location_id (primary key) for the location dimension record
    """
    cursor.execute(f"""
        INSERT INTO {schema}.dim_location (neighborhood_name, latitude, longitude)
        VALUES (%s, %s, %s)
        ON CONFLICT (latitude, longitude) 
        DO UPDATE SET 
            neighborhood_name = EXCLUDED.neighborhood_name,
            updated_at = CURRENT_TIMESTAMP
        RETURNING location_id
    """, (neighborhood_name, latitude, longitude))
    return cursor.fetchone()[0]


def insert_weather_observation(cursor, location_id: int, weather_data: Dict, schema: str) -> None:
    """
    Insert or update current weather observation in the fact table.
    
    Args:
        cursor: Active database cursor
        location_id: Foreign key reference to dim_location
        weather_data: Raw JSON response from Open-Meteo API containing current weather
        schema: Database schema name
    
    Raises:
        ValueError: If the weather data doesn't contain a time field
    """
    current = weather_data.get('current', {})
    time_str = current.get('time')
    
    if not time_str:
        raise ValueError("No time field in weather data")
    
    # Parse the ISO format timestamp
    if 'T' in time_str:
        observation_time = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    else:
        observation_time = datetime.fromisoformat(time_str)
    
    # Get or create date_id
    date_id = get_or_create_date(cursor, observation_time, schema)
    
    cursor.execute(f"""
        INSERT INTO {schema}.fact_current_weather (
            location_id, date_id, observation_time,
            temperature_2m, apparent_temperature, relative_humidity_2m,
            wind_speed_10m, wind_direction_10m, wind_gusts_10m,
            precipitation, rain, showers, snowfall,
            weather_code, cloud_cover, is_day,
            pressure_msl, surface_pressure
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (location_id, observation_time) 
        DO UPDATE SET
            date_id = EXCLUDED.date_id,
            temperature_2m = EXCLUDED.temperature_2m,
            apparent_temperature = EXCLUDED.apparent_temperature,
            relative_humidity_2m = EXCLUDED.relative_humidity_2m,
            wind_speed_10m = EXCLUDED.wind_speed_10m,
            wind_direction_10m = EXCLUDED.wind_direction_10m,
            wind_gusts_10m = EXCLUDED.wind_gusts_10m,
            precipitation = EXCLUDED.precipitation,
            rain = EXCLUDED.rain,
            showers = EXCLUDED.showers,
            snowfall = EXCLUDED.snowfall,
            weather_code = EXCLUDED.weather_code,
            cloud_cover = EXCLUDED.cloud_cover,
            is_day = EXCLUDED.is_day,
            pressure_msl = EXCLUDED.pressure_msl,
            surface_pressure = EXCLUDED.surface_pressure
    """, (
        location_id, date_id, observation_time,
        current.get('temperature_2m'), current.get('apparent_temperature'),
        current.get('relative_humidity_2m'), current.get('wind_speed_10m'),
        current.get('wind_direction_10m'), current.get('wind_gusts_10m'),
        current.get('precipitation'), current.get('rain'),
        current.get('showers'), current.get('snowfall'),
        current.get('weather_code'), current.get('cloud_cover'),
        current.get('is_day'), current.get('pressure_msl'),
        current.get('surface_pressure')
    ))


def main():
    """
    Main ETL process for extracting weather data from Open-Meteo API.
    
    Loads Pittsburgh neighborhood coordinates, fetches current weather for each,
    and stores in the database.
    """
    # Get schema from environment variable (defaults to 'pittsburgh')
    schema = os.getenv('WEATHER_DB_SCHEMA', 'pittsburgh')
    print(f"Using schema: {schema}")
    
    # Load coordinates
    try:
        neighborhoods = load_coordinates()
        print(f"Loaded {len(neighborhoods)} Pittsburgh neighborhoods")
    except FileNotFoundError:
        print("ERROR: coordinates.json file not found")
        return
    except json.JSONDecodeError:
        print("ERROR: Invalid JSON in coordinates file")
        return
    except KeyError as e:
        print(f"ERROR: {e}")
        return
    
    # Connect to database
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
    except Exception as e:
        print(f"ERROR: Database connection failed: {e}")
        return
    
    # Process each neighborhood
    success_count = 0
    error_count = 0
    
    for neighborhood in neighborhoods:
        try:
            name = neighborhood['name']
            lat = neighborhood['latitude']
            lon = neighborhood['longitude']
            
            # Fetch weather data
            weather_data = get_weather_data(lat, lon)
            if not weather_data:
                error_count += 1
                continue
            
            # Insert/update location
            location_id = insert_or_update_location(cursor, name, lat, lon, schema)
            
            # Insert weather observation
            insert_weather_observation(cursor, location_id, weather_data, schema)
            
            # Commit after each successful insert
            conn.commit()
            success_count += 1
            print(f"✓ {name}")
            
        except Exception as e:
            print(f"✗ {name}: {e}")
            error_count += 1
            conn.rollback()
            continue
    
    # Close connection
    cursor.close()
    conn.close()
    
    print(f"\n{'='*50}")
    print(f"ETL Complete: {success_count} successful, {error_count} errors")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()