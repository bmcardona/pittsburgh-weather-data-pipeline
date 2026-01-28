{{ config(
    materialized='view'
) }}

with weather as (
    select * from {{ ref('stg_postgres__current_weather') }}
),

enriched as (
    select
        -- Pass through all base fields
        weather_id,
        location_id,
        date_id,
        observation_time_est,
        temp_celsius,
        temp_fahrenheit,
        feels_like_celsius,
        feels_like_fahrenheit,
        humidity_percent,
        wind_speed_mps,
        wind_speed_mph,
        wind_direction_degrees,
        wind_gusts_mps,
        total_precip_mm,
        rain_mm,
        showers_mm,
        snowfall_mm,
        weather_code,
        cloud_cover_percent,
        day_night,
        pressure_sea_level_hpa,
        pressure_surface_hpa,
        loaded_at_est,
        
        -- CATEGORIZATIONS (this is the business logic!)
        
        -- Temperature category
        case 
            when temp_celsius < 0 then 'Freezing'
            when temp_celsius < 10 then 'Cold'
            when temp_celsius < 20 then 'Mild'
            when temp_celsius < 30 then 'Warm'
            else 'Hot'
        end as temp_category,
        
        -- Weather condition (decode weather_code)
        case 
            when weather_code = 0 then 'Clear sky'
            when weather_code in (1, 2, 3) then 'Partly cloudy'
            when weather_code in (45, 48) then 'Foggy'
            when weather_code in (51, 53, 55, 56, 57) then 'Drizzle'
            when weather_code in (61, 63, 65, 66, 67) then 'Rain'
            when weather_code in (71, 73, 75, 77) then 'Snow'
            when weather_code in (80, 81, 82) then 'Rain showers'
            when weather_code in (85, 86) then 'Snow showers'
            when weather_code in (95, 96, 99) then 'Thunderstorm'
            else 'Unknown'
        end as weather_condition,
        
        -- Wind category (simplified Beaufort scale, using mph)
        case 
            when wind_speed_mph < 1 then 'Calm'
            when wind_speed_mph < 8 then 'Light air'
            when wind_speed_mph < 13 then 'Light breeze'
            when wind_speed_mph < 25 then 'Moderate breeze'
            when wind_speed_mph < 39 then 'Strong breeze'
            else 'High wind'
        end as wind_category,
        
        -- Precipitation type
        case 
            when snowfall_mm > 0 then 'Snow'
            when rain_mm > 0 then 'Rain'
            when showers_mm > 0 then 'Showers'
            when total_precip_mm > 0 then 'Other precipitation'
            else 'No precipitation'
        end as precip_type,
        
        -- Boolean flags
        case when total_precip_mm > 0 then true else false end as is_precipitating,
        case when snowfall_mm > 0 then true else false end as is_snowing,
        case when temp_celsius <= 0 then true else false end as is_freezing,
        case when wind_speed_mph >= 25 then true else false end as is_windy,
        
        -- Comfort index (simplified feels-like interpretation)
        case 
            when feels_like_fahrenheit < 32 then 'Very uncomfortable (freezing)'
            when feels_like_fahrenheit < 50 then 'Uncomfortable (cold)'
            when feels_like_fahrenheit < 65 then 'Cool'
            when feels_like_fahrenheit < 75 then 'Comfortable'
            when feels_like_fahrenheit < 85 then 'Warm'
            else 'Uncomfortable (hot)'
        end as comfort_level
        
    from weather
)

select * from enriched