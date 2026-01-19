import requests
import json
import psycopg2
from datetime import datetime
import os
from typing import Dict, Optional, List


def get_db_connection():
    """Create database connection using environment variables"""
    return psycopg2.connect(
        host=os.getenv('WEATHER_DB_HOST'),
        port=os.getenv('WEATHER_DB_PORT'),
        database=os.getenv('WEATHER_DB_NAME'),
        user=os.getenv('WEATHER_DB_USER'),
        password=os.getenv('WEATHER_DB_PASSWORD')
    )


def get_weather_data(latitude: float, longitude: float) -> Optional[Dict]:
    """Fetch current weather data from Open-Meteo API"""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": [
            "temperature_2m", "relative_humidity_2m", "apparent_temperature",
            "is_day", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
            "precipitation", "rain", "showers", "snowfall", "weather_code",
            "cloud_cover", "pressure_msl", "surface_pressure"
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
    """Load neighborhood coordinates from JSON file"""
    with open(coordinates_path, 'r') as f:
        return json.load(f)


def insert_or_update_location(cursor, neighborhood_name: str, latitude: float, 
                              longitude: float, community_board: Optional[str] = None) -> int:
    """Insert or update location and return location_id"""
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
    """Insert current weather observation"""
    current = weather_data.get('current', {})
    observation_time = datetime.fromisoformat(current.get('time'))
    
    cursor.execute("""
        INSERT INTO weather.fact_current_weather (
            location_id, observation_time,
            temperature_2m, apparent_temperature, relative_humidity_2m,
            wind_speed_10m, wind_direction_10m, wind_gusts_10m,
            precipitation, rain, showers, snowfall,
            weather_code, cloud_cover, is_day,
            pressure_msl, surface_pressure
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (location_id, observation_time) 
        DO UPDATE SET
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
        location_id, observation_time,
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
    """Main ETL process"""
    print("Starting weather data extraction...")
    
    # Load coordinates
    try:
        coordinates = load_coordinates()
        print(f"Loaded {len(coordinates)} locations")
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
        print("Database connection established")
    except Exception as e:
        print(f"Database connection error: {e}")
        return
    
    # Process each location
    success_count = 0
    error_count = 0
    
    for neighborhood, coords in coordinates.items():
        try:
            latitude = coords['latitude']
            longitude = coords['longitude']
            community_board = coords.get('community_board')
            
            # Fetch weather data
            weather_data = get_weather_data(latitude, longitude)
            if not weather_data:
                print(f"Failed to fetch weather data for {neighborhood}")
                error_count += 1
                continue
            
            # Insert/update location
            location_id = insert_or_update_location(
                cursor, neighborhood, latitude, longitude, community_board
            )
            
            # Insert weather observation
            insert_weather_observation(cursor, location_id, weather_data)
            
            success_count += 1
            print(f"✓ Processed {neighborhood}")
            
        except Exception as e:
            print(f"✗ Error processing {neighborhood}: {e}")
            error_count += 1
            conn.rollback()
            continue
    
    # Commit and close
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"\nETL Complete: {success_count} successful, {error_count} errors")


if __name__ == "__main__":
    main()