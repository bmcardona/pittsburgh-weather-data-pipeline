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


def get_or_create_date(cursor, observation_time: datetime) -> int:
    """Get or create date dimension record and return date_id"""
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
            date, year, month, day, day_of_week, day_name, 
            week_of_year, quarter, is_weekend
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
    time_str = current.get('time')
    
    if not time_str:
        raise ValueError("No time field in weather data")
    
    # Parse the ISO format timestamp - handle both with and without timezone
    try:
        # Try parsing with timezone info first
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
    """Main ETL process"""
    print("Starting weather data extraction...")
    
    # Load coordinates
    try:
        data = load_coordinates()
        community_boards = data.get('community_boards', [])
        print(f"Loaded {len(community_boards)} community boards")
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
                    print(f"Failed to fetch weather data for {full_name}")
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
                print(f"✓ Processed {full_name} ({community_board})")
                
            except Exception as e:
                print(f"✗ Error processing {neighborhood_name}: {e}")
                error_count += 1
                conn.rollback()
                continue
    
    # Close connection
    cursor.close()
    conn.close()
    
    print(f"\nETL Complete: {success_count} successful, {error_count} errors")


if __name__ == "__main__":
    main()