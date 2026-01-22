"""
NYC Weather Data Pipeline - TaskFlow API
Extracts current weather data from Open-Meteo API and loads into PostgreSQL
"""
import sys
import pendulum
from typing import Dict, List
from airflow.decorators import dag, task


@dag(
    schedule="0 * * * *",  # Run every hour at minute 0
    start_date=pendulum.datetime(2026, 1, 19, tz="America/New_York"),
    catchup=False,
    tags=["weather", "nyc", "etl"],
    doc_md=__doc__,
)
def nyc_weather_pipeline():
    """
    ### NYC Weather Data Pipeline
    
    This pipeline extracts current weather data for NYC neighborhoods from the 
    Open-Meteo API and loads it into a PostgreSQL database.
    
    **Steps:**
    1. **Extract** - Fetch coordinates and weather data from API
    2. **Transform** - Structure data for database insertion
    3. **Load** - Insert weather observations into PostgreSQL
    4. **Transform (dbt)** - Create analytics models
    
    **Schedule:** Runs every hour
    **Data Source:** Open-Meteo API
    **Database:** PostgreSQL (weather_db)
    """
    
    @task()
    def extract_coordinates() -> Dict:
        """
        #### Extract Coordinates
        Load NYC neighborhood coordinates from the coordinates.json file.
        Returns a dictionary containing community boards and neighborhoods.
        """
        # Import inside task to ensure path is available
        sys.path.insert(0, '/opt/airflow/etl')
        from extract_weather_from_openmeteo import load_coordinates
        
        print("📍 Loading neighborhood coordinates...")
        coordinates = load_coordinates()
        
        total_neighborhoods = sum(
            len(board['neighborhoods']) 
            for board in coordinates['community_boards']
        )
        
        print(f"✓ Loaded {total_neighborhoods} neighborhoods from "
              f"{len(coordinates['community_boards'])} community boards")
        
        return coordinates
    
    @task()
    def extract_weather(coordinates: Dict) -> List[Dict]:
        """
        #### Extract Weather Data
        Fetch current weather data from Open-Meteo API for each neighborhood.
        Returns a list of weather observations with location metadata.
        """
        # Import inside task to ensure path is available
        sys.path.insert(0, '/opt/airflow/etl')
        from extract_weather_from_openmeteo import get_weather_data
        
        print("🌤️ Fetching weather data from Open-Meteo API...")
        weather_observations = []
        
        for board in coordinates['community_boards']:
            # FIX: Construct board_name from 'borough' and 'board' fields
            borough = board.get('borough', 'Unknown')
            board_code = board.get('board', 'Unknown')
            board_name = f"{borough} {board_code}"
            
            for neighborhood in board['neighborhoods']:
                name = neighborhood['name']
                lat = neighborhood['latitude']
                lon = neighborhood['longitude']
                
                weather_data = get_weather_data(lat, lon)
                
                if weather_data:
                    weather_observations.append({
                        'neighborhood_name': name,
                        'community_board': board_name,
                        'latitude': lat,
                        'longitude': lon,
                        'weather_data': weather_data
                    })
                    print(f"  ✓ {name} ({board_name}): {weather_data['current'].get('temperature_2m')}°C")
                else:
                    print(f"  ✗ {name}: Failed to fetch weather data")
        
        print(f"\n✓ Successfully fetched weather for {len(weather_observations)} neighborhoods")
        return weather_observations
    
    @task()
    def load_weather(weather_observations: List[Dict]) -> Dict:
        """
        #### Load Weather Data
        Insert weather observations into PostgreSQL database.
        Returns a summary of the load operation.
        """
        # Import inside task to ensure path is available
        sys.path.insert(0, '/opt/airflow/etl')
        from extract_weather_from_openmeteo import (
            get_db_connection,
            insert_or_update_location,
            insert_weather_observation
        )
        
        print("💾 Loading weather data into PostgreSQL...")
        
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
                        obs['community_board']
                    )
                    
                    # Insert weather observation
                    insert_weather_observation(
                        cursor,
                        location_id,
                        obs['weather_data']
                    )
                    
                    success_count += 1
                    
                except Exception as e:
                    error_count += 1
                    print(f"  ✗ Error loading {obs['neighborhood_name']}: {e}")
                    conn.rollback()
            
            # Commit all successful inserts
            conn.commit()
            
            summary = {
                'total_observations': len(weather_observations),
                'successful_loads': success_count,
                'failed_loads': error_count,
                'timestamp': pendulum.now('America/New_York').isoformat()
            }
            
            print(f"\n{'='*60}")
            print(f"📊 Load Summary:")
            print(f"   Total: {summary['total_observations']}")
            print(f"   Success: {summary['successful_loads']} ✓")
            print(f"   Failed: {summary['failed_loads']} ✗")
            print(f"{'='*60}")
            
            return summary
            
        finally:
            cursor.close()
            conn.close()

    @task()
    def run_dbt_models() -> Dict:
        """
        #### Run dbt Transformations
        Transform raw weather data into analytics-ready models using dbt.
        Runs staging → intermediate models.
        """
        import subprocess
        
        print("🔄 Running dbt transformations...")
        print("="*60)
        
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
            print(f"❌ dbt run failed:")
            print(result.stderr)
            raise Exception(f"dbt run failed with return code {result.returncode}")
        
        # Run dbt tests
        print("\n🧪 Running dbt tests...")
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
            print("⚠️  Some dbt tests failed (see above)")
        
        dbt_summary = {
            'run_success': result.returncode == 0,
            'tests_passed': test_result.returncode == 0,
            'timestamp': pendulum.now('America/New_York').isoformat()
        }
        
        print("✅ dbt transformations complete!")
        print("="*60)
        
        return dbt_summary
    
    @task()
    def report_summary(load_summary: Dict, dbt_summary: Dict) -> None:
        """
        #### Report Summary
        Print final pipeline execution summary.
        """
        print("\n" + "="*60)
        print("🎉 NYC Weather Pipeline Complete!")
        print("="*60)
        print(f"Execution Time: {load_summary['timestamp']}")
        print(f"\n📥 ETL Summary:")
        print(f"   Total Neighborhoods: {load_summary['total_observations']}")
        print(f"   Successfully Loaded: {load_summary['successful_loads']}")
        print(f"   Failed: {load_summary['failed_loads']}")
        print(f"   Success Rate: {load_summary['successful_loads']/load_summary['total_observations']*100:.1f}%")
        print(f"\n🔄 dbt Summary:")
        print(f"   Models Run: {'✅' if dbt_summary['run_success'] else '❌'}")
        print(f"   Tests Passed: {'✅' if dbt_summary['tests_passed'] else '⚠️'}")
        print("="*60 + "\n")
    
    coords = extract_coordinates()
    weather = extract_weather(coords)
    load_summary = load_weather(weather)
    dbt_summary = run_dbt_models()

    load_summary >> dbt_summary >> report_summary(load_summary, dbt_summary)


nyc_weather_pipeline()