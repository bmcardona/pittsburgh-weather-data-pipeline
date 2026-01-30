
-- ============================================================================
-- FACT TABLE - HOURLY FORECAST
-- ============================================================================

-- Hourly forecast data from Open-Meteo API
CREATE TABLE IF NOT EXISTS pittsburgh.fact_hourly_forecast (
    forecast_id BIGSERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    forecast_time TIMESTAMP NOT NULL,
    
    -- Temperature metrics (Celsius)
    temperature_2m DECIMAL(5, 2),
    relative_humidity_2m DECIMAL(5, 2),
    dew_point_2m DECIMAL(5, 2),
    apparent_temperature DECIMAL(5, 2),
    
    -- Temperature at different altitudes
    temperature_80m DECIMAL(5, 2),
    temperature_120m DECIMAL(5, 2),
    temperature_180m DECIMAL(5, 2),
    
    -- Precipitation metrics
    precipitation_probability DECIMAL(5, 2),
    precipitation DECIMAL(6, 2),
    rain DECIMAL(6, 2),
    showers DECIMAL(6, 2),
    snowfall DECIMAL(6, 2),
    snow_depth DECIMAL(6, 2),
    
    -- Weather conditions
    weather_code INTEGER,
    
    -- Pressure metrics (hPa)
    pressure_msl DECIMAL(7, 2),
    surface_pressure DECIMAL(7, 2),
    
    -- Cloud cover metrics (%)
    cloud_cover DECIMAL(5, 2),
    cloud_cover_low DECIMAL(5, 2),
    cloud_cover_mid DECIMAL(5, 2),
    cloud_cover_high DECIMAL(5, 2),
    
    -- Visibility and evapotranspiration
    visibility DECIMAL(8, 2),
    evapotranspiration DECIMAL(6, 2),
    et0_fao_evapotranspiration DECIMAL(6, 2),
    vapour_pressure_deficit DECIMAL(6, 2),
    
    -- Wind metrics at 10m
    wind_speed_10m DECIMAL(6, 2),
    wind_direction_10m DECIMAL(5, 2),
    wind_gusts_10m DECIMAL(6, 2),
    
    -- Wind metrics at different altitudes
    wind_speed_80m DECIMAL(6, 2),
    wind_direction_80m DECIMAL(5, 2),
    wind_speed_120m DECIMAL(6, 2),
    wind_direction_120m DECIMAL(5, 2),
    wind_speed_180m DECIMAL(6, 2),
    wind_direction_180m DECIMAL(5, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'),
    
    -- Foreign key constraints
    CONSTRAINT fk_hourly_forecast_location 
        FOREIGN KEY (location_id) REFERENCES pittsburgh.dim_location(location_id) ON DELETE CASCADE,
    CONSTRAINT fk_hourly_forecast_date 
        FOREIGN KEY (date_id) REFERENCES pittsburgh.dim_date(date_id),
    
    -- Prevent duplicate forecasts
    UNIQUE(location_id, forecast_time)
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- fact_hourly_forecast indexes
CREATE INDEX IF NOT EXISTS idx_fact_hourly_forecast_location ON pittsburgh.fact_hourly_forecast(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_hourly_forecast_date ON pittsburgh.fact_hourly_forecast(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_hourly_forecast_time ON pittsburgh.fact_hourly_forecast(forecast_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_hourly_forecast_location_date ON pittsburgh.fact_hourly_forecast(location_id, date_id);
CREATE INDEX IF NOT EXISTS idx_fact_hourly_forecast_location_time ON pittsburgh.fact_hourly_forecast(location_id, forecast_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_hourly_forecast_weather_code ON pittsburgh.fact_hourly_forecast(weather_code);
CREATE INDEX IF NOT EXISTS idx_fact_hourly_forecast_created ON pittsburgh.fact_hourly_forecast(created_at DESC);

-- ============================================================================
-- PERMISSIONS
-- ============================================================================

GRANT USAGE ON SCHEMA pittsburgh TO weather_user;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA pittsburgh TO weather_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA pittsburgh TO weather_user;

-- Permissions for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA pittsburgh 
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO weather_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA pittsburgh 
    GRANT USAGE, SELECT ON SEQUENCES TO weather_user;