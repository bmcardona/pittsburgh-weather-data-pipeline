
  create view "weather_db"."weather_staging"."stg_current_weather__dbt_tmp"
    
    
  as (
    

with source as (
    -- Reference the raw table using source() function
    select * from "weather_db"."weather"."fact_current_weather"
),

cleaned as (
    select
        -- IDs
        weather_id,
        location_id,
        date_id,
        observation_time,
        
        -- Temperature metrics
        temperature_2m as temp_celsius,
        round((temperature_2m * 9.0/5.0) + 32, 1) as temp_fahrenheit,
        apparent_temperature as feels_like_celsius,
        round((apparent_temperature * 9.0/5.0) + 32, 1) as feels_like_fahrenheit,
        
        -- Humidity
        relative_humidity_2m as humidity_percent,
        
        -- Wind metrics
        wind_speed_10m as wind_speed_mps,
        round(wind_speed_10m * 2.237, 1) as wind_speed_mph,  -- Convert m/s to mph
        wind_direction_10m as wind_direction_degrees,
        wind_gusts_10m as wind_gusts_mps,
        
        -- Precipitation
        precipitation as total_precip_mm,
        rain as rain_mm,
        showers as showers_mm,
        snowfall as snowfall_mm,
        
        -- Weather conditions
        weather_code,
        cloud_cover as cloud_cover_percent,
        case 
            when is_day = 1 then 'Day'
            when is_day = 0 then 'Night'
            else 'Unknown'
        end as day_night,
        
        -- Pressure
        pressure_msl as pressure_sea_level_hpa,
        surface_pressure as pressure_surface_hpa,
        
        -- Metadata
        created_at as loaded_at_utc
        
    from source
)

select * from cleaned
  );