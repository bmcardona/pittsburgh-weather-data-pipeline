{{ config(
    materialized='view'
) }}

with weather as (
    select * from {{ ref('int_weather_enriched') }}
),

precip_events as (
    select
        weather_id,
        location_id,
        date_id,
        observation_time_est,
        
        -- Precipitation amounts
        total_precip_mm,
        rain_mm,
        showers_mm,
        snowfall_mm,
        
        -- From enriched model
        precip_type,
        is_precipitating,
        is_snowing,
        temp_celsius,
        
        -- Precipitation intensity (mm/hour assumed)
        case 
            when total_precip_mm = 0 then 'None'
            when total_precip_mm < 2.5 then 'Light'
            when total_precip_mm < 7.6 then 'Moderate'
            when total_precip_mm < 50 then 'Heavy'
            else 'Violent'
        end as precip_intensity,
        
        -- Snow intensity (different scale than rain)
        case 
            when snowfall_mm = 0 then 'None'
            when snowfall_mm < 2.5 then 'Light snow'
            when snowfall_mm < 5 then 'Moderate snow'
            else 'Heavy snow'
        end as snow_intensity,
        
        -- Mixed precipitation flag
        case 
            when snowfall_mm > 0 and rain_mm > 0 then true 
            else false 
        end as is_mixed_precip,
        
        -- Weather alert flags
        case 
            when snowfall_mm > 5 then true 
            else false 
        end as is_heavy_snow_event,
        
        case 
            when total_precip_mm > 7.6 then true 
            else false 
        end as is_heavy_rain_event,
        
        -- Freezing rain potential
        case 
            when rain_mm > 0 and temp_celsius <= 0 then true 
            else false 
        end as is_freezing_rain_potential
        
    from weather
    -- Only include observations with precipitation
    where is_precipitating = true
)

select * from precip_events