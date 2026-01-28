# dbt Command Reference

## Verify dbt Installation in Airflow
# Check if dbt is accessible inside the Airflow webserver container
docker-compose exec airflow-webserver dbt --version

## Access Airflow UI
# Default port for Airflow web interface
http://localhost:8082/

## Generate and View dbt Documentation Lineage Graph
# Instead of serving docs from inside the container, serve them locally using Python's built-in HTTP server
# This works because we have a volume mount that syncs the dbt/target folder to our local machine

# Step 1: Generate the documentation files
docker exec -it weather_airflow_scheduler bash
cd /opt/airflow/dbt
dbt docs generate
dbt docs serve

# Step 2: Serve the generated docs locally
cd ~/github-projects/nyc-weather-app/dbt/target
python3 -m http.server 8082
http://localhost:8082/

## Run Specific dbt Model in Airflow Container
# Execute a single dbt model (mart_hourly_weather) from within the Airflow scheduler container
docker exec -it weather_airflow_scheduler dbt run --select mart_hourly_weather --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt


### Generate *__sources.yml
# Creates _sources.yml with all tables and columns from the 'weather' schema
dbt run-operation generate_source --args '{"schema_name": "weather", "generate_columns": true}' | pbcopy

### Generate stg_*.sql files
# Creates staging SQL files with all columns pre-populated from source tables
dbt run-operation generate_base_model --args '{"source_name": "weather", "table_name": "fact_current_weather"}' | pbcopy
dbt run-operation generate_base_model --args '{"source_name": "weather", "table_name": "dim_location"}' | pbcopy
dbt run-operation generate_base_model --args '{"source_name": "weather", "table_name": "dim_date"}' | pbcopy

### Generate *__docs.yml
# Run staging models, then generate documentation YAML
dbt run --select staging
dbt run-operation generate_model_yaml --args '{"model_names": ["stg_postgres__current_weather", "stg_postgres__dates", "stg_postgres__locations"]}' | pbcopy
