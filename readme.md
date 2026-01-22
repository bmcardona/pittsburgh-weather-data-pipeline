# NYC Weather Data Pipeline

## Overview
Automated hourly pipeline that collects weather data for 320 NYC neighborhoods

## Architecture
[Include a simple diagram showing: API → Airflow → PostgreSQL → dbt → Dashboard]

## Tech Stack
- Python 3.12
- Apache Airflow (TaskFlow API)
- PostgreSQL
- dbt
- Docker & Docker Compose

## Project Structure
[Your current tree is perfect, include it]

## Setup Instructions
1. Clone the repo
2. `docker-compose up -d`
3. Access Airflow at localhost:8080

## Key Features
- Hourly automated extraction from Open-Meteo API
- Data quality validation checks
- dbt transformations for analytics
- Containerized for easy deployment

## Sample Insights
[Add 2-3 interesting findings, like "Manhattan averages 2°F warmer than outer boroughs"]

## Future Enhancements
- Add weather alerts
- Historical trend analysis
- ML forecasting model