"""
Pittsburgh Weather Data Pipeline - TaskFlow API
Extracts current weather data from Open-Meteo API and loads into PostgreSQL
"""
import sys
import os
import pendulum
from typing import Dict, List
from airflow.decorators import dag, task


@dag(
    schedule="0 * * * *",  # Run every hour at minute 0
    start_date=pendulum.datetime(2026, 1, 28, tz="America/New_York"),
    catchup=False,
    tags=["weather", "pittsburgh", "etl"],
    doc_md=__doc__,
)
def pittsburgh_weather_pipeline():
    """
    ### Pittsburgh Weather Data Pipeline
    
    This pipeline extracts current weather data for Pittsburgh neighborhoods from the 
    Open-Meteo API and loads it into a PostgreSQL database.
    
    **Steps:**
    1. **Extract** - Fetch coordinates and weather data from API
    2. **Transform** - Structure data for database insertion
    3. **Load** - Insert weather observations into PostgreSQL
    4. **Transform (dbt)** - Create analytics models
    
    **Schedule:** Runs every hour
    **Data Source:** Open-Meteo API
    **Database:** PostgreSQL (weather_db.pittsburgh schema)
    """
    
    @task()
    def extract_coordinates() -> List[Dict]:
        """
        Extract neighborhood coordinates from JSON file.
        
        Loads Pittsburgh neighborhood location data including latitude and longitude
        from the coordinates.json file. This data is used to make targeted API calls 
        for weather information.
        
        Returns:
            List[Dict]: List of neighborhoods with name, latitude, and longitude
        """
        sys.path.insert(0, '/opt/airflow/etl')
        from extract_weather_from_openmeteo import load_coordinates
        
        return load_coordinates()
    
    @task()
    def extract_weather(neighborhoods: List[Dict]) -> List[Dict]:
        """
        Fetch current weather data from Open-Meteo API for all neighborhoods.
        
        Iterates through all neighborhoods and makes individual API calls to fetch 
        current weather conditions. Combines weather data with location metadata 
        for downstream processing.
        
        Args:
            neighborhoods: List of dictionaries with neighborhood name and coordinates
        
        Returns:
            List[Dict]: List of weather observations, each containing neighborhood name,
                       coordinates, and raw weather data from API
        """
        sys.path.insert(0, '/opt/airflow/etl')
        from extract_weather_from_openmeteo import get_weather_data
        
        weather_observations = []
        
        for neighborhood in neighborhoods:
            name = neighborhood['name']
            lat = neighborhood['latitude']
            lon = neighborhood['longitude']
            
            weather_data = get_weather_data(lat, lon)
            
            if weather_data:
                weather_observations.append({
                    'neighborhood_name': name,
                    'latitude': lat,
                    'longitude': lon,
                    'weather_data': weather_data
                })
        
        return weather_observations
    
    @task()
    def load_weather(weather_observations: List[Dict]) -> Dict:
        """
        Load weather observations into PostgreSQL database.
        
        Inserts or updates location records in dim_location and weather observations
        in fact_current_weather. Uses upsert logic to handle duplicate locations and
        observation times. Commits after all successful inserts and tracks both
        successful and failed loads.
        
        Args:
            weather_observations: List of dictionaries containing neighborhood metadata
                                 and weather data
        
        Returns:
            Dict: Summary statistics including total observations, successful loads,
                  failed loads, and timestamp
        """
        sys.path.insert(0, '/opt/airflow/etl')
        from extract_weather_from_openmeteo import (
            get_db_connection,
            insert_or_update_location,
            insert_weather_observation
        )
        
        # Get schema from environment variable
        schema = os.getenv('WEATHER_DB_SCHEMA', 'pittsburgh')
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        success_count = 0
        error_count = 0
        
        try:
            for obs in weather_observations:
                try:
                    # Insert/update location
                    location_id = insert_or_update_location(
                        cursor,
                        obs['neighborhood_name'],
                        obs['latitude'],
                        obs['longitude'],
                        schema
                    )
                    
                    # Insert weather observation
                    insert_weather_observation(
                        cursor,
                        location_id,
                        obs['weather_data'],
                        schema
                    )
                    
                    success_count += 1
                    
                except Exception as e:
                    error_count += 1
                    print(f"Error loading {obs['neighborhood_name']}: {e}")
                    conn.rollback()
            
            # Commit all successful inserts
            conn.commit()
            
            summary = {
                'total_observations': len(weather_observations),
                'successful_loads': success_count,
                'failed_loads': error_count,
                'timestamp': pendulum.now('America/New_York').isoformat()
            }
            
            return summary
            
        finally:
            cursor.close()
            conn.close()

    @task()
    def run_dbt_models() -> Dict:
        """
        Execute dbt transformations to create analytics-ready models.
        
        Runs dbt models to transform raw weather data from staging tables through
        intermediate models to final fact and mart tables. Also executes dbt tests
        to validate data quality. Tests failures are logged but don't fail the pipeline.
        
        Returns:
            Dict: Summary containing success status of dbt run and tests, plus timestamp
        
        Raises:
            Exception: If dbt run command fails with non-zero exit code
        """
        import subprocess
        
        # Run dbt models
        result = subprocess.run(
            [
                'dbt', 'run',
                '--profiles-dir', '/opt/airflow/dbt',
                '--project-dir', '/opt/airflow/dbt'
            ],
            capture_output=True,
            text=True,
            cwd='/opt/airflow/dbt'
        )
        
        print(result.stdout)
        
        if result.returncode != 0:
            print(f"dbt run failed:")
            print(result.stderr)
            raise Exception(f"dbt run failed with return code {result.returncode}")
        
        # Run dbt tests
        test_result = subprocess.run(
            [
                'dbt', 'test',
                '--profiles-dir', '/opt/airflow/dbt',
                '--project-dir', '/opt/airflow/dbt'
            ],
            capture_output=True,
            text=True,
            cwd='/opt/airflow/dbt'
        )
        
        print(test_result.stdout)
        
        # Don't fail pipeline if tests fail, just warn
        if test_result.returncode != 0:
            print("Warning: Some dbt tests failed")
        
        dbt_summary = {
            'run_success': result.returncode == 0,
            'tests_passed': test_result.returncode == 0,
            'timestamp': pendulum.now('America/New_York').isoformat()
        }
        
        return dbt_summary
    
    @task()
    def report_summary(load_summary: Dict, dbt_summary: Dict) -> None:
        """
        Print final pipeline execution summary to logs.
        
        Aggregates and displays statistics from both the ETL load process and
        dbt transformations, including success rates and execution timestamps.
        
        Args:
            load_summary: Summary statistics from the load_weather task
            dbt_summary: Summary statistics from the run_dbt_models task
        """
        print("\n" + "="*60)
        print("Pittsburgh Weather Pipeline Complete")
        print("="*60)
        print(f"Execution Time: {load_summary['timestamp']}")
        print(f"\nETL Summary:")
        print(f"   Total Neighborhoods: {load_summary['total_observations']}")
        print(f"   Successfully Loaded: {load_summary['successful_loads']}")
        print(f"   Failed: {load_summary['failed_loads']}")
        print(f"   Success Rate: {load_summary['successful_loads']/load_summary['total_observations']*100:.1f}%")
        print(f"\ndbt Summary:")
        print(f"   Models Run: {'Success' if dbt_summary['run_success'] else 'Failed'}")
        print(f"   Tests Passed: {'Yes' if dbt_summary['tests_passed'] else 'No'}")
        print("="*60 + "\n")
    
    coords = extract_coordinates()
    weather = extract_weather(coords)
    load_summary = load_weather(weather)
    dbt_summary = run_dbt_models()

    load_summary >> dbt_summary >> report_summary(load_summary, dbt_summary)


pittsburgh_weather_pipeline()