
-- Create schema for weather data
CREATE SCHEMA IF NOT EXISTS pittsburgh;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Pittsburgh neighborhoods with geographic coordinates
CREATE TABLE IF NOT EXISTS pittsburgh.dim_location (
    location_id SERIAL PRIMARY KEY,
    neighborhood_name VARCHAR(100) NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    timezone VARCHAR(50) DEFAULT 'America/New_York',
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'),
    updated_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'),
    UNIQUE(latitude, longitude)
);

-- Date dimension for time-based analysis
CREATE TABLE IF NOT EXISTS pittsburgh.dim_date (
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

-- Current weather observations from Open-Meteo API
CREATE TABLE IF NOT EXISTS pittsburgh.fact_current_weather (
    weather_id BIGSERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
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
    created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'),
    
    -- Foreign key constraints
    CONSTRAINT fk_current_weather_location 
        FOREIGN KEY (location_id) REFERENCES pittsburgh.dim_location(location_id) ON DELETE CASCADE,
    CONSTRAINT fk_current_weather_date 
        FOREIGN KEY (date_id) REFERENCES pittsburgh.dim_date(date_id),
    
    -- Prevent duplicate observations
    UNIQUE(location_id, observation_time)
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- dim_location indexes
CREATE INDEX IF NOT EXISTS idx_dim_location_neighborhood ON pittsburgh.dim_location(neighborhood_name);
CREATE INDEX IF NOT EXISTS idx_dim_location_coords ON pittsburgh.dim_location(latitude, longitude);

-- dim_date indexes
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON pittsburgh.dim_date(year, month);
CREATE INDEX IF NOT EXISTS idx_dim_date_quarter ON pittsburgh.dim_date(quarter);

-- fact_current_weather indexes
CREATE INDEX IF NOT EXISTS idx_fact_current_weather_location ON pittsburgh.fact_current_weather(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_current_weather_date ON pittsburgh.fact_current_weather(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_current_weather_time ON pittsburgh.fact_current_weather(observation_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_current_weather_location_date ON pittsburgh.fact_current_weather(location_id, date_id);
CREATE INDEX IF NOT EXISTS idx_fact_current_weather_location_time ON pittsburgh.fact_current_weather(location_id, observation_time DESC);
CREATE INDEX IF NOT EXISTS idx_fact_current_weather_weather_code ON pittsburgh.fact_current_weather(weather_code);
CREATE INDEX IF NOT EXISTS idx_fact_current_weather_created ON pittsburgh.fact_current_weather(created_at DESC);


-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- Most recent weather observation for each location
CREATE OR REPLACE VIEW pittsburgh.v_latest_weather AS
SELECT 
    l.neighborhood_name,
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
FROM pittsburgh.fact_current_weather w
JOIN pittsburgh.dim_location l ON w.location_id = l.location_id
WHERE w.observation_time = (
    SELECT MAX(observation_time) 
    FROM pittsburgh.fact_current_weather 
    WHERE location_id = w.location_id
);

-- Weather statistics by neighborhood for last 7 days
CREATE OR REPLACE VIEW pittsburgh.v_weather_summary AS
SELECT 
    l.neighborhood_name,
    COUNT(*) as observation_count,
    AVG(w.temperature_2m) as avg_temperature,
    MIN(w.temperature_2m) as min_temperature,
    MAX(w.temperature_2m) as max_temperature,
    AVG(w.relative_humidity_2m) as avg_humidity,
    AVG(w.wind_speed_10m) as avg_wind_speed,
    SUM(w.precipitation) as total_precipitation
FROM pittsburgh.fact_current_weather w
JOIN pittsburgh.dim_location l ON w.location_id = l.location_id
WHERE w.observation_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY l.location_id, l.neighborhood_name;

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Auto-update updated_at timestamp on dim_location
CREATE OR REPLACE FUNCTION pittsburgh.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York';
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

DROP TRIGGER IF EXISTS update_dim_location_updated_at ON pittsburgh.dim_location;
CREATE TRIGGER update_dim_location_updated_at
    BEFORE UPDATE ON pittsburgh.dim_location
    FOR EACH ROW
    EXECUTE FUNCTION pittsburgh.update_updated_at_column();

-- ============================================================================
-- PERMISSIONS
-- ============================================================================

GRANT USAGE ON SCHEMA pittsburgh TO weather_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pittsburgh TO weather_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA pittsburgh TO weather_user;

-- Permissions for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA pittsburgh 
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO weather_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA pittsburgh 
    GRANT USAGE, SELECT ON SEQUENCES TO weather_user;

-- View permissions
GRANT SELECT ON pittsburgh.v_latest_weather TO weather_user;
GRANT SELECT ON pittsburgh.v_weather_summary TO weather_user;