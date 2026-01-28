with 

categorized as (

    select * from {{ ref('int_weather_categorized') }}
    
),

readings as (
    select
        -- IDs
        c.weather_id,
        c.location_id,
        c.date_id,

        -- Location Details
        c.neighborhood_name,
        c.community_board,
        c.latitude,
        c.longitude,
        c.timezone,
        
        -- Date Details
        c.date,
        c.year,
        c.month,
        c.day,
        c.day_of_week,
        c.day_name,
        c.week_of_year,
        c.quarter,
        c.is_weekend,
        c.is_holiday,
        
        -- Timestamp
        c.observation_time_est,
        c.loaded_at_est,
        
        -- Temperature
        c.temp_celsius,
        c.temp_fahrenheit,
        c.feels_like_celsius,
        c.feels_like_fahrenheit,
        c.temp_category,
        
        -- Weather Conditions
        c.weather_code,
        c.weather_condition,
        c.cloud_cover_percent,
        c.day_night,
        
        -- Wind
        c.wind_speed_mps,
        c.wind_speed_mph,
        c.wind_direction_degrees,
        c.wind_gusts_mps,
        c.wind_category,
        c.is_windy,
        
        -- Precipitation
        c.total_precip_mm,
        c.rain_mm,
        c.showers_mm,
        c.snowfall_mm,
        c.precip_type,
        c.is_precipitating,
        c.is_snowing,
        
        -- Pressure & Humidity
        c.humidity_percent,
        c.pressure_sea_level_hpa,
        c.pressure_surface_hpa,
        
        -- Comfort & Flags
        c.comfort_level,
        c.is_freezing,
        
        -- Metadata
        c.created_at,
        c.updated_at

    from categorized c

)

select * from readings