with 

add_categories as (

    select * from {{ ref('int_forecast_add_categories') }}
    
),

forecast as (
    select
    -- 1. Primary/Foreign Keys (IDs)
    ac.forecast_id,
    ac.location_id,
    ac.date_id,

    -- 2. Core business/descriptive attributes
    ac.forecast_time,
    ac.neighborhood_name,
    ac.latitude,
    ac.longitude,
    ac.timezone,

    -- 3. Date/time dimensions
    ac.date,
    ac.year,
    ac.month,
    ac.day,
    ac.day_of_week,
    ac.day_name,
    ac.week_of_year,
    ac.quarter,
    ac.is_weekend,
    ac.is_holiday,

    -- 4. Temperature metrics (grouped by altitude)
    ac.temp_celsius_2m,
    ac.temp_fahrenheit_2m,
    ac.feels_like_celsius,
    ac.feels_like_fahrenheit,
    ac.relative_humidity_2m,
    ac.dew_point_2m,
    ac.temp_celcius_80m,
    ac.temp_fahrenheit_80m,
    ac.temp_celcius_120m,
    ac.temp_fahrenheit_120m,
    ac.temp_celcius_180m,
    ac.temp_fahrenheit_180m,

    -- 5. Precipitation metrics
    ac.precipitation_probability,
    ac.precipitation,
    ac.rain,
    ac.showers,
    ac.snowfall,
    ac.snow_depth,

    -- 6. Atmospheric pressure
    ac.pressure_msl,
    ac.surface_pressure,

    -- 7. Cloud cover metrics
    ac.cloud_cover,
    ac.cloud_cover_low,
    ac.cloud_cover_mid,
    ac.cloud_cover_high,

    -- 8. Wind metrics (grouped by altitude)
    ac.wind_speed_10m,
    ac.wind_direction_10m,
    ac.wind_gusts_10m,
    ac.wind_speed_80m,
    ac.wind_direction_80m,
    ac.wind_speed_120m,
    ac.wind_direction_120m,
    ac.wind_speed_180m,
    ac.wind_direction_180m,

    -- 9. Other atmospheric conditions
    ac.visibility,
    ac.weather_code,
    ac.evapotranspiration,
    ac.et0_fao_evapotranspiration,
    ac.vapour_pressure_deficit,

    -- 10. Derived/categorized fields
    ac.temperature_category,
    ac.wind_category,
    ac.wind_direction_cardinal,
    ac.visibility_category,

    -- 11. Metadata 
    ac.created_at,
    ac.updated_at
        
    from add_categories as ac
)

select * from forecast