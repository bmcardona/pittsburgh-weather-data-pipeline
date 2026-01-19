-- Connect to the weather database
\c weather_db

-- Create schema for weather data
CREATE SCHEMA IF NOT EXISTS weather;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Dimension table for NYC neighborhoods
CREATE TABLE IF NOT EXISTS weather.dim_location (
    location_id SERIAL PRIMARY KEY,
    neighborhood_name VARCHAR(100) NOT NULL,
    community_board VARCHAR(50),
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    timezone VARCHAR(50) DEFAULT 'America/New_York',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(latitude, longitude)
);

-- Dimension table for date/time
CREATE TABLE IF NOT EXISTS weather.dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    week_of_year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE
);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Fact table for current weather observations
CREATE TABLE IF NOT EXISTS weather.fact_current_weather (
    weather_id BIGSERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES weather.dim_location(location_id) ON DELETE CASCADE,
    observation_time TIMESTAMP NOT NULL,
    
    -- Temperature metrics (Celsius)
    temperature_2m DECIMAL(5, 2),
    apparent_temperature DECIMAL(5, 2),
    
    -- Humidity
    relative_humidity_2m DECIMAL(5, 2),
    
    -- Wind metrics
    wind_speed_10m DECIMAL(6, 2),
    wind_direction_10m DECIMAL(5, 2),
    wind_gusts_10m DECIMAL(6, 2),
    
    -- Precipitation metrics (mm)
    precipitation DECIMAL(6, 2),
    rain DECIMAL(6, 2),
    showers DECIMAL(6, 2),
    snowfall DECIMAL(6, 2),
    
    -- Weather conditions
    weather_code INTEGER,
    cloud_cover DECIMAL(5, 2),
    is_day INTEGER,
    
    -- Pressure metrics (hPa)
    pressure_msl DECIMAL(7, 2),
    surface_pressure DECIMAL(7, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure we don't duplicate observations for same location/time
    UNIQUE(location_id, observation_time)
);

-- Optional: Historical weather table for tracking changes over time
CREATE TABLE IF NOT EXISTS weather.fact_weather_history (
    history_id BIGSERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES weather.dim_location(location_id) ON DELETE CASCADE,
    observation_time TIMESTAMP NOT NULL,
    temperature_2m DECIMAL(5, 2),
    apparent_temperature DECIMAL(5, 2),
    relative_humidity_2m DECIMAL(5, 2),
    wind_speed_10m DECIMAL(6, 2),
    wind_direction_10m DECIMAL(5, 2),
    wind_gusts_10m DECIMAL(6, 2),
    precipitation DECIMAL(6, 2),
    rain DECIMAL(6, 2),
    showers DECIMAL(6, 2),
    snowfall DECIMAL(6, 2),
    weather_code INTEGER,
    cloud_cover DECIMAL(5, 2),
    is_day INTEGER,
    pressure_msl DECIMAL(7, 2),
    surface_pressure DECIMAL(7, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Indexes on dim_location
CREATE INDEX IF NOT EXISTS idx_dim_location_neighborhood ON weather.dim_location(neighborhood_name);
CREATE INDEX IF NOT EXISTS idx_dim_location_coords ON weather.dim_location(latitude, longitude);

-- Indexes on dim_date
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON weather.dim_date(year, month);
CREATE INDEX IF NOT EXISTS idx_dim_date_quarter ON weather.dim_date(quarter);

-- Indexes on fact_current_weather
CREATE INDEX IF NOT EXISTS idx_fact_current_weather_location ON weather.fact_current_weather(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_current_weather_time ON weather.fact_current_weather(observation_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_current_weather_location_time ON weather.fact_current_weather(location_id, observation_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_current_weather_weather_code ON weather.fact_current_weather(weather_code);
CREATE INDEX IF NOT EXISTS idx_fact_current_weather_created ON weather.fact_current_weather(created_at DESC);

-- Indexes on fact_weather_history
CREATE INDEX IF NOT EXISTS idx_fact_weather_history_location ON weather.fact_weather_history(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_weather_history_time ON weather.fact_weather_history(observation_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_weather_history_location_time ON weather.fact_weather_history(location_id, observation_time DESC);

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Latest weather for all locations
CREATE OR REPLACE VIEW weather.v_latest_weather AS
SELECT 
    l.neighborhood_name,
    l.community_board,
    l.latitude,
    l.longitude,
    w.observation_time,
    w.temperature_2m,
    w.apparent_temperature,
    w.relative_humidity_2m,
    w.wind_speed_10m,
    w.wind_direction_10m,
    w.precipitation,
    w.weather_code,
    w.is_day
FROM weather.fact_current_weather w
JOIN weather.dim_location l ON w.location_id = l.location_id
WHERE w.observation_time = (
    SELECT MAX(observation_time) 
    FROM weather.fact_current_weather 
    WHERE location_id = w.location_id
);

-- View: Weather summary by neighborhood
CREATE OR REPLACE VIEW weather.v_weather_summary AS
SELECT 
    l.neighborhood_name,
    l.community_board,
    COUNT(*) as observation_count,
    AVG(w.temperature_2m) as avg_temperature,
    MIN(w.temperature_2m) as min_temperature,
    MAX(w.temperature_2m) as max_temperature,
    AVG(w.relative_humidity_2m) as avg_humidity,
    AVG(w.wind_speed_10m) as avg_wind_speed,
    SUM(w.precipitation) as total_precipitation
FROM weather.fact_current_weather w
JOIN weather.dim_location l ON w.location_id = l.location_id
WHERE w.observation_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY l.location_id, l.neighborhood_name, l.community_board;

-- ============================================================================
-- PERMISSIONS
-- ============================================================================

-- Grant schema usage
GRANT USAGE ON SCHEMA weather TO weather_user;

-- Grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA weather TO weather_user;

-- Grant sequence permissions (for SERIAL columns)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA weather TO weather_user;

-- Grant permissions on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA weather 
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO weather_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA weather 
GRANT USAGE, SELECT ON SEQUENCES TO weather_user;

-- Grant view permissions
GRANT SELECT ON weather.v_latest_weather TO weather_user;
GRANT SELECT ON weather.v_weather_summary TO weather_user;

-- ============================================================================
-- HELPFUL COMMENTS
-- ============================================================================

COMMENT ON TABLE weather.dim_location IS 'NYC neighborhood locations with coordinates';
COMMENT ON TABLE weather.dim_date IS 'Date dimension for time-based analysis';
COMMENT ON TABLE weather.fact_current_weather IS 'Current weather observations from Open-Meteo API';
COMMENT ON TABLE weather.fact_weather_history IS 'Historical weather data for trend analysis';
COMMENT ON VIEW weather.v_latest_weather IS 'Most recent weather observation for each location';
COMMENT ON VIEW weather.v_weather_summary IS 'Weather statistics by neighborhood for last 7 days';