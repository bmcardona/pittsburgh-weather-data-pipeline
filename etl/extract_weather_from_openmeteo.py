"""
Weather data extraction from Open-Meteo API
"""
import requests
import json
import psycopg2
from datetime import datetime
import os
from typing import Dict, Optional, List


def get_db_connection():
    """
    Create database connection using environment variables.
    
    Reads connection parameters from environment variables:
    - WEATHER_DB_HOST: Database host address
    - WEATHER_DB_PORT: Database port number
    - WEATHER_DB_NAME: Database name
    - WEATHER_DB_USER: Database username
    - WEATHER_DB_PASSWORD: Database password
    
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
    
    Makes a request to the Open-Meteo forecast endpoint for a specific location,
    requesting current conditions including temperature, humidity, wind, precipitation,
    and atmospheric pressure data.
    
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


def load_coordinates(coordinates_path: str = '/opt/airflow/etl/coordinates.json') -> Dict:
    """
    Load neighborhood coordinates from JSON file.
    
    Reads a JSON file containing NYC neighborhood data organized by community boards,
    including neighborhood names, latitude/longitude coordinates, and borough information.
    
    Args:
        coordinates_path: Path to the JSON file containing coordinate data
    
    Returns:
        Dict: Parsed JSON data containing community boards and neighborhoods
    
    Raises:
        FileNotFoundError: If the coordinates file doesn't exist
        json.JSONDecodeError: If the file contains invalid JSON
    """
    with open(coordinates_path, 'r') as f:
        return json.load(f)


def get_or_create_date(cursor, observation_time: datetime) -> int:
    """
    Get or create date dimension record and return date_id.
    
    Implements a lookup-or-insert pattern for the date dimension table. First attempts
    to find an existing date record, and if not found, creates a new one with derived
    attributes like day of week, quarter, and weekend flag.
    
    Args:
        cursor: Active database cursor
        observation_time: DateTime object for the weather observation
    
    Returns:
        int: The date_id (primary key) for the date dimension record
    """
    date_only = observation_time.date()
    
    # Try to get existing date_id
    cursor.execute("""
        SELECT date_id FROM weather.dim_date WHERE date = %s
    """, (date_only,))
    
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Create new date dimension record
    cursor.execute("""
        INSERT INTO weather.dim_date (
            date, 
            year, 
            month, 
            day, 
            day_of_week, 
            day_name, 
            week_of_year, 
            quarter, 
            is_weekend
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
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
                              longitude: float, community_board: Optional[str] = None) -> int:
    """
    Insert or update location dimension record and return location_id.
    
    Uses PostgreSQL's ON CONFLICT (upsert) to either insert a new location or update
    an existing one if the lat/lon coordinates already exist. Updates the neighborhood
    name, community board, and updated_at timestamp on conflict.
    
    Args:
        cursor: Active database cursor
        neighborhood_name: Full name of the neighborhood (e.g., "Manhattan - Upper East Side")
        latitude: Latitude coordinate in decimal degrees
        longitude: Longitude coordinate in decimal degrees
        community_board: Optional community board identifier (e.g., "Manhattan 8")
    
    Returns:
        int: The location_id (primary key) for the location dimension record
    """
    cursor.execute("""
        INSERT INTO weather.dim_location (neighborhood_name, latitude, longitude, community_board)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (latitude, longitude) 
        DO UPDATE SET 
            neighborhood_name = EXCLUDED.neighborhood_name,
            community_board = EXCLUDED.community_board,
            updated_at = CURRENT_TIMESTAMP
        RETURNING location_id
    """, (neighborhood_name, latitude, longitude, community_board))
    return cursor.fetchone()[0]


def insert_weather_observation(cursor, location_id: int, weather_data: Dict) -> None:
    """
    Insert or update current weather observation in the fact table.
    
    Parses weather data from the Open-Meteo API response and inserts it into the
    fact_current_weather table. Uses ON CONFLICT to update existing records if
    an observation already exists for the same location and time.
    
    Args:
        cursor: Active database cursor
        location_id: Foreign key reference to dim_location
        weather_data: Raw JSON response from Open-Meteo API containing current weather
    
    Raises:
        ValueError: If the weather data doesn't contain a time field
        Exception: If timestamp parsing fails
    """
    current = weather_data.get('current', {})
    time_str = current.get('time')
    
    if not time_str:
        raise ValueError("No time field in weather data")
    
    # Parse the ISO format timestamp - handle both with and without timezone
    try:
        if 'T' in time_str:
            observation_time = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        else:
            observation_time = datetime.fromisoformat(time_str)
    except Exception as e:
        print(f"Error parsing time '{time_str}': {e}")
        raise
    
    # Get or create date_id
    date_id = get_or_create_date(cursor, observation_time)
    
    cursor.execute("""
        INSERT INTO weather.fact_current_weather (
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
    
    Orchestrates the complete ETL pipeline:
    1. Loads neighborhood coordinates from JSON file
    2. Establishes database connection
    3. Iterates through all neighborhoods by community board
    4. Fetches current weather data for each location
    5. Upserts location and weather observation records
    6. Commits after each successful location to avoid data loss
    7. Rolls back and continues on errors
    
    Prints summary statistics upon completion.
    """
    # Load coordinates
    try:
        data = load_coordinates()
        community_boards = data.get('community_boards', [])
    except FileNotFoundError:
        print("Error: coordinates.json file not found")
        return
    except json.JSONDecodeError:
        print("Error: Invalid JSON in coordinates file")
        return
    
    # Connect to database
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
    except Exception as e:
        print(f"Database connection error: {e}")
        return
    
    # Process each location
    success_count = 0
    error_count = 0
    
    for cb in community_boards:
        borough = cb.get('borough')
        board = cb.get('board')
        community_board = f"{borough} {board}"
        
        for neighborhood_data in cb.get('neighborhoods', []):
            try:
                neighborhood_name = neighborhood_data['name']
                latitude = neighborhood_data['latitude']
                longitude = neighborhood_data['longitude']
                
                full_name = f"{borough} - {neighborhood_name}"
                
                # Fetch weather data
                weather_data = get_weather_data(latitude, longitude)
                if not weather_data:
                    error_count += 1
                    continue
                
                # Insert/update location
                location_id = insert_or_update_location(
                    cursor, full_name, latitude, longitude, community_board
                )
                
                # Insert weather observation
                insert_weather_observation(cursor, location_id, weather_data)
                
                # Commit after each successful insert to avoid losing all data on error
                conn.commit()
                
                success_count += 1
                
            except Exception as e:
                print(f"Error processing {neighborhood_name}: {e}")
                error_count += 1
                conn.rollback()
                continue
    
    # Close connection
    cursor.close()
    conn.close()
    
    print(f"ETL Complete: {success_count} successful, {error_count} errors")


if __name__ == "__main__":
    main()