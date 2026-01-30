"""
Weather forecast data extraction from Open-Meteo API for Pittsburgh neighborhoods
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


def get_forecast_data(latitude: float, longitude: float) -> Optional[Dict]:
    """
    Fetch hourly forecast weather data from Open-Meteo API.
    
    Args:
        latitude: Latitude coordinate in decimal degrees
        longitude: Longitude coordinate in decimal degrees
    
    Returns:
        Optional[Dict]: JSON response containing hourly forecast data, or None if request fails
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": [
            "temperature_2m", 
            "relative_humidity_2m", 
            "dew_point_2m",
            "apparent_temperature",
            "precipitation_probability",
            "precipitation", 
            "rain", 
            "showers", 
            "snowfall",
            "snow_depth",
            "weather_code",
            "pressure_msl", 
            "surface_pressure",
            "cloud_cover",
            "cloud_cover_low",
            "cloud_cover_mid",
            "cloud_cover_high",
            "visibility",
            "evapotranspiration",
            "et0_fao_evapotranspiration",
            "vapour_pressure_deficit",
            "wind_speed_10m",
            "wind_speed_80m",
            "wind_speed_120m",
            "wind_speed_180m",
            "wind_direction_10m",
            "wind_direction_80m",
            "wind_direction_120m",
            "wind_direction_180m",
            "wind_gusts_10m",
            "temperature_80m",
            "temperature_120m",
            "temperature_180m"
        ],
        "timezone": "America/New_York",
        "forecast_hours": 168
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


def insert_forecast_observations(cursor, location_id: int, forecast_data: Dict, schema: str) -> int:
    """
    Insert hourly forecast observations into the fact table.
    
    Args:
        cursor: Active database cursor
        location_id: Foreign key reference to dim_location
        forecast_data: Raw JSON response from Open-Meteo API containing hourly forecasts
        schema: Database schema name
    
    Returns:
        int: Number of forecast records inserted
    
    Raises:
        ValueError: If the forecast data doesn't contain hourly data
    """
    hourly = forecast_data.get('hourly', {})
    
    if not hourly:
        raise ValueError("No hourly forecast data available")
    
    # Get the time array
    times = hourly.get('time', [])
    if not times:
        raise ValueError("No time data in hourly forecast")
    
    inserted_count = 0
    
    # Iterate through each hour in the forecast
    for i in range(len(times)):
        time_str = times[i]
        
        # Parse the ISO format timestamp
        if 'T' in time_str:
            forecast_time = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        else:
            forecast_time = datetime.fromisoformat(time_str)
        
        # Get or create date_id
        date_id = get_or_create_date(cursor, forecast_time, schema)
        
        # Helper function to safely get value at index
        def get_value(key, index):
            values = hourly.get(key, [])
            return values[index] if index < len(values) else None
        
        cursor.execute(f"""
            INSERT INTO {schema}.fact_hourly_forecast (
                location_id, date_id, forecast_time,
                temperature_2m, relative_humidity_2m, dew_point_2m,
                apparent_temperature, precipitation_probability,
                precipitation, rain, showers, snowfall, snow_depth,
                weather_code, pressure_msl, surface_pressure,
                cloud_cover, cloud_cover_low, cloud_cover_mid, cloud_cover_high,
                visibility, evapotranspiration, et0_fao_evapotranspiration,
                vapour_pressure_deficit,
                wind_speed_10m, wind_speed_80m, wind_speed_120m, wind_speed_180m,
                wind_direction_10m, wind_direction_80m, wind_direction_120m, wind_direction_180m,
                wind_gusts_10m,
                temperature_80m, temperature_120m, temperature_180m
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (location_id, forecast_time) 
            DO UPDATE SET
                date_id = EXCLUDED.date_id,
                temperature_2m = EXCLUDED.temperature_2m,
                relative_humidity_2m = EXCLUDED.relative_humidity_2m,
                dew_point_2m = EXCLUDED.dew_point_2m,
                apparent_temperature = EXCLUDED.apparent_temperature,
                precipitation_probability = EXCLUDED.precipitation_probability,
                precipitation = EXCLUDED.precipitation,
                rain = EXCLUDED.rain,
                showers = EXCLUDED.showers,
                snowfall = EXCLUDED.snowfall,
                snow_depth = EXCLUDED.snow_depth,
                weather_code = EXCLUDED.weather_code,
                pressure_msl = EXCLUDED.pressure_msl,
                surface_pressure = EXCLUDED.surface_pressure,
                cloud_cover = EXCLUDED.cloud_cover,
                cloud_cover_low = EXCLUDED.cloud_cover_low,
                cloud_cover_mid = EXCLUDED.cloud_cover_mid,
                cloud_cover_high = EXCLUDED.cloud_cover_high,
                visibility = EXCLUDED.visibility,
                evapotranspiration = EXCLUDED.evapotranspiration,
                et0_fao_evapotranspiration = EXCLUDED.et0_fao_evapotranspiration,
                vapour_pressure_deficit = EXCLUDED.vapour_pressure_deficit,
                wind_speed_10m = EXCLUDED.wind_speed_10m,
                wind_speed_80m = EXCLUDED.wind_speed_80m,
                wind_speed_120m = EXCLUDED.wind_speed_120m,
                wind_speed_180m = EXCLUDED.wind_speed_180m,
                wind_direction_10m = EXCLUDED.wind_direction_10m,
                wind_direction_80m = EXCLUDED.wind_direction_80m,
                wind_direction_120m = EXCLUDED.wind_direction_120m,
                wind_direction_180m = EXCLUDED.wind_direction_180m,
                wind_gusts_10m = EXCLUDED.wind_gusts_10m,
                temperature_80m = EXCLUDED.temperature_80m,
                temperature_120m = EXCLUDED.temperature_120m,
                temperature_180m = EXCLUDED.temperature_180m
        """, (
            location_id, date_id, forecast_time,
            get_value('temperature_2m', i),
            get_value('relative_humidity_2m', i),
            get_value('dew_point_2m', i),
            get_value('apparent_temperature', i),
            get_value('precipitation_probability', i),
            get_value('precipitation', i),
            get_value('rain', i),
            get_value('showers', i),
            get_value('snowfall', i),
            get_value('snow_depth', i),
            get_value('weather_code', i),
            get_value('pressure_msl', i),
            get_value('surface_pressure', i),
            get_value('cloud_cover', i),
            get_value('cloud_cover_low', i),
            get_value('cloud_cover_mid', i),
            get_value('cloud_cover_high', i),
            get_value('visibility', i),
            get_value('evapotranspiration', i),
            get_value('et0_fao_evapotranspiration', i),
            get_value('vapour_pressure_deficit', i),
            get_value('wind_speed_10m', i),
            get_value('wind_speed_80m', i),
            get_value('wind_speed_120m', i),
            get_value('wind_speed_180m', i),
            get_value('wind_direction_10m', i),
            get_value('wind_direction_80m', i),
            get_value('wind_direction_120m', i),
            get_value('wind_direction_180m', i),
            get_value('wind_gusts_10m', i),
            get_value('temperature_80m', i),
            get_value('temperature_120m', i),
            get_value('temperature_180m', i)
        ))
        
        inserted_count += 1
    
    return inserted_count


def main():
    """
    Main ETL process for extracting forecast data from Open-Meteo API.
    
    Loads Pittsburgh neighborhood coordinates, fetches hourly forecasts for each,
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
    total_forecasts = 0
    
    for neighborhood in neighborhoods:
        try:
            name = neighborhood['name']
            lat = neighborhood['latitude']
            lon = neighborhood['longitude']
            
            # Fetch forecast data
            forecast_data = get_forecast_data(lat, lon)
            if not forecast_data:
                error_count += 1
                continue
            
            # Insert/update location
            location_id = insert_or_update_location(cursor, name, lat, lon, schema)
            
            # Insert forecast observations
            forecast_count = insert_forecast_observations(cursor, location_id, forecast_data, schema)
            
            # Commit after each successful insert
            conn.commit()
            success_count += 1
            total_forecasts += forecast_count
            print(f"✓ {name}: {forecast_count} hourly forecasts inserted")
            
        except Exception as e:
            print(f"✗ {name}: {e}")
            error_count += 1
            conn.rollback()
            continue
    
    # Close connection
    cursor.close()
    conn.close()
    
    print(f"\n{'='*50}")
    print(f"ETL Complete: {success_count} neighborhoods successful, {error_count} errors")
    print(f"Total hourly forecasts inserted: {total_forecasts}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()