# Pittsburgh Weather Analytics Platform

## Overview

This project demonstrates end-to-end data engineering practices by building a complete analytics platform for Pittsburgh weather data. The pipeline extracts weather data from the OpenMeteo API, loads it into a PostgreSQL, and transforms it using dbt dimensional modeling. An interactive web application is also provided using Streamlit. This project is orchestrated by Apache Airflow, and containerized with Docker.

**Key Features:**
- Automated hourly data extraction from OpenMeteo API for 90+ Pittsburgh neighborhoods
- Transformations performed with dbt using different models (source, intermediate, and marts)
- Interactive Streamlit dashboard for weather trend analysis and forecasting 
- Fully containerized architecture for easy deployment
- Simple task orchestration with Apache Airflow

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Transformation** | dbt (data build tool) | Data modeling and transformations |
| **Database** | PostgreSQL | Data warehouse (staging + analytics) |
| **Visualization** | Streamlit | Interactive analytics dashboard |
| **Language** | Python3 | ETL scripts and application logic |
| **Data Source** | OpenMeteo API | Weather forecast data |
| **Orchestration** | Apache Airflow | DAG scheduling and pipeline monitoring |
| **Containerization** | Docker & Docker Compose | Environment isolation and deployment |

## Project Structure

```
.
├── dags/
│   └── weather_etl_dag.py           # Airflow DAG definition
├── etl/
│   ├── extract_forecast_from_openmeteo.py  # API extraction logic
│   ├── coordinates.json              # Pittsburgh neighborhood coordinates
│   └── pittsburgh_neighborhoods.geojson    # Geospatial data
├── dbt/
│   ├── models/
│   │   ├── sources/                  # Source definitions
│   │   ├── intermediate/             # Intermediate transformations
│   │   └── marts/                    # Analytics-ready transformations
│   ├── dbt_project.yml
│   └── profiles.yml
├── database/
│   ├── init.sql                      # Current Weather Database initialization
│   └── init_forecast.sql             # Forecast Weather Database initialization
├── streamlit_app/
│   ├── Home.py                       # Streamlit main page
│   ├── pages/                        # Additional dashboard pages
│   └── Dockerfile
├── docker-compose.yml                # Multi-container orchestration
├── Dockerfile.airflow                # Airflow container configuration
├── .env.example                      # Environment variable template
├── .gitignore                        # Git ignore rules (includes .env)
└── requirements.txt                  # Python dependencies
```

## Quick Start

### Prerequisites
- Docker Desktop installed
- Docker Compose installed
- At least 4GB of available RAM

### Setup Instructions

1. **Clone the repository**
   ```bash
   git clone https://github.com/BMCARDONA/pittsburgh-weather-analytics.git
   cd pittsburgh-weather-analytics
   ```

2. **Configure environment variables**
   
   Copy the example environment file:
   ```bash
   cp .env.example .env
   ```
   
   Edit `.env` and fill in the required values:
   ```env
   # PostgreSQL Configuration
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=your_secure_password
   POSTGRES_HOST=postgres
   POSTGRES_PORT=5432
   
   # Weather Database
   WEATHER_DB_USER=weather_user
   WEATHER_DB_PASSWORD=weather_password
   WEATHER_DB_HOST=postgres
   WEATHER_DB_PORT=5432
   WEATHER_DB_NAME=weather_db
   WEATHER_DB_SCHEMA=public
   
   # Airflow settings (can use defaults provided in .env.example)
   # AIRFLOW_ADMIN_USERNAME, AIRFLOW_ADMIN_PASSWORD, etc.
   ```
   
   > ⚠️ **Important**: Never commit your `.env` file to version control. The `.gitignore` file is configured to exclude it.

3. **Build and start all services**
```bash
   docker-compose build
   docker-compose up -d
```
   
   > **Note**: On first run, the `airflow-init` service will initialize the database and create the admin user. This must complete successfully before Airflow starts.

4. **Wait for services to initialize** (about 2-3 minutes)
```bash
   docker-compose logs -f
```

5. **Access the applications**
   - **Airflow UI**: http://localhost:8080
     - Username: Value from `AIRFLOW_ADMIN_USERNAME` (default: `admin`)
     - Password: Value from `AIRFLOW_ADMIN_PASSWORD` (default: `admin`)
   - **Streamlit Dashboard**: http://localhost:8501
   - **PostgreSQL**: `localhost:5432`
     - Database: Value from `WEATHER_DB_NAME`
     - Username: Value from `WEATHER_DB_USER`
     - Password: Value from `WEATHER_DB_PASSWORD`

6. **Trigger the pipeline**
   - Navigate to Airflow UI
   - Find the `pittsburgh_weather_pipeline` DAG
   - Click the play button to trigger a manual run

### Stopping the Services

```bash
docker-compose down
```

To remove all data and start fresh:
```bash
docker-compose down -v
```

## Data Pipeline Details

### Extraction (E)
- The extract_forecast_from_openmeteo.py script fetches current weather from OpenMeteo API 
- The extract_forecast_from_openmeteo.py script fetches 7-day hourly weather forecasts from OpenMeteo API. 
- Covers 90+ Pittsburgh neighborhoods
- Collects 60+ weather features including:
  - Temperature (current, min, max, feels-like)
  - Precipitation (probability, amount)
  - Wind (speed, direction, gusts)
  - Atmospheric conditions (pressure, humidity, cloud cover)

### Loading (L)
- Raw data loaded into Postgres RDBMS
- Uses an incremental loading strategy to avoid duplicates

### Transformation (T) - dbt Models

**Sources Layer** (`models/sources/`)
- Defines source data 
- Still needs: testing and docs

**Intermediate Layer** (`models/intermediate/`)
- Data type conversions
- Column renaming and standardization
- Business logic implementation
- Deduplication and data cleaning

**Marts Layer** (`models/marts/`)
- Analytics-ready tables suited for querying

## 📈 Analytics Dashboard

The Streamlit dashboard provides:
- **Neighborhood Selection**: View detailed forecasts for any of 90+ Pittsburgh neighborhoods
- **Multi-Metric Visualization**: Interactive chart for temperature, precipitation probability, rain, snowfall, cloud cover, visibility, and surface pressure
- **Flexible Forecast Range**: Adjust forecast window from 1 to 7 days
- **Hourly Granularity**: Hour-by-hour weather trends with hover details
- **Real-Time Updates**: Data refreshed hourly using Airflow

## 🔧 Configuration

### Environment Variables (.env file)

The project requires an `.env` file for configuration. Copy `.env.example` to `.env` and customize the values:

```env
# ============================================================================
# POSTGRESQL - Container Configuration
# ============================================================================
POSTGRES_USER=postgres              # PostgreSQL superuser
POSTGRES_PASSWORD=your_password     # Superuser password (CHANGE THIS!)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# ============================================================================
# WEATHER DATA - Database Configuration
# ============================================================================
WEATHER_DB_USER=weather_user
WEATHER_DB_PASSWORD=weather_password    # CHANGE THIS!
WEATHER_DB_HOST=postgres
WEATHER_DB_PORT=5432
WEATHER_DB_NAME=weather_db
WEATHER_DB_SCHEMA=public

# ============================================================================
# AIRFLOW - Database & Configuration
# ============================================================================
AIRFLOW_DB_NAME=airflow_db
AIRFLOW_DB_USER=airflow_user
AIRFLOW_DB_PASSWORD=airflow_pass        # CHANGE THIS!

# Airflow Core Settings
AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__FERNET_KEY=sxRsOaT7DQZ0yG6EOyGVQhJt8e5cFSFfeL5mIWPOL5Q=
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth

# Airflow Database Connection
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_user:airflow_pass@postgres:5432/airflow_db

# Airflow Admin User
AIRFLOW_ADMIN_USERNAME=admin            # CHANGE THIS!
AIRFLOW_ADMIN_PASSWORD=admin            # CHANGE THIS!
AIRFLOW__WEBSERVER__SECRET_KEY=ea66b610217b9986df7826270ab1bc619e3a15ac963ff44bb5443233568adde6
```

**Security Notes:**
- Change all default passwords before deploying
- Never commit `.env` to version control (add to `.gitignore`)
- Generate a new Fernet key for production: 
  ```bash
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```

### Airflow DAG Schedule

**Current Schedule:** Hourly at minute 0 (every hour)

To modify the schedule, update the `schedule` parameter in `dags/weather_etl_dag.py`:

```python
# Hourly schedule
schedule="0 * * * *",

# Change to daily at 6:00 AM UTC:
schedule="0 6 * * *",

### dbt Profiles

Database connections configured in `dbt/profiles.yml`:
```yaml
    weather:
    target: dev
    outputs:
        dev:
        type: postgres
        host: postgres
        port: 5432
        user: "{{ env_var('WEATHER_DB_USER') }}"
        password: "{{ env_var('WEATHER_DB_PASSWORD') }}"
        dbname: "{{ env_var('WEATHER_DB_NAME') }}"
        schema: pittsburgh_analytics
        threads: 4
        keepalives_idle: 0 
        connect_timeout: 10 
        retries: 1 
```

## Running dbt Locally

```bash
# Enter the Airflow container
docker exec -it weather_airflow_scheduler bash

# Navigate to dbt project
cd /opt/airflow/dbt

# Run dbt models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Development Workflow

1. **Add new weather features**
   - Update `etl/extract_weather_from_openmeteo.py` (current weather data) or `etl/extract_forecast_from_openmeteo.py` (forecast weather data)
   - Add corresponding tables and columns to database schema in the `\database` directory

2. **Modify transformations**
   - Edit dbt models in `dbt/models/`
   - Run `dbt run` to test changes
   - Add data quality tests in model YAML files

3. **Update dashboard**
   - Modify Streamlit home page in `streamlit_app/Home.py`
   - Modify Streamlit pages in `streamlit_app/pages/`
   - Refresh browser to see changes (auto-reloads)

4. **Adjust orchestration**
   - Edit `dags/weather_etl_dag.py`
   - Airflow will automatically pick up changes

## Troubleshooting

**Airflow UI won't load:**
```bash
docker-compose logs airflow-webserver
# Wait 2-3 minutes for initialization
```

**dbt models failing:**
```bash
docker exec -it weather_airflow_scheduler bash
cd /opt/airflow/dbt
dbt debug  # Check connections
dbt run --full-refresh  # Rebuild from scratch
```

**PostgreSQL connection issues:**
```bash
docker exec -it postgres_container psql -U postgres -d weather_db
# Verify tables exist
\dt
```

**Streamlit dashboard shows no data:**
- Ensure Airflow DAG has run successfully
- Check dbt models completed: `dbt run`
- Verify data in analytics schema:
  ```sql
  SELECT * FROM pittsburgh_analytics.forecasts LIMIT 10;
  ```

## Future Enhancements

- [ ] Add historical weather data for improved trend analysis
- [ ] Implement alerting for severe weather conditions
- [ ] Integrate machine learning for forecast accuracy prediction

## Resources

- [OpenMeteo API Documentation](https://open-meteo.com/en/docs)
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## License

ThisMIT License - feel free to use this project for learning and portfolio purposes.

## Contributing

This is only a portfolio project, but feedback and suggestions are welcome! Feel free to open an issue or submit a pull request.

## Author

**Bradley M. Cardona**
- GitHub: [@bmcardona](https://github.com/bmcardona)
- LinkedIn: [bmcardona](https://www.linkedin.com/in/bmcardona/)
- Email: bradleymcardona@gmail.com
