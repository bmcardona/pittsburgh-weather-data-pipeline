{{ config(
    materialized='table'
) }}

with latest_observations as (
    -- Get the most recent observation time across all locations
    select max(observation_time_est) as latest_time
    from {{ ref('stg_postgres__current_weather') }}
),

current_weather as (
    select
        e.*,
        l.neighborhood_name,
        l.community_board,
        l.latitude,
        l.longitude
    from {{ ref('int_weather_enriched') }} e
    inner join {{ ref('stg_postgres__locations') }} l 
        on e.location_id = l.location_id
    inner join latest_observations lo
        on e.observation_time_est = lo.latest_time
)

select
    -- Location info
    neighborhood_name,
    community_board,
    latitude,
    longitude,
    
    -- Time
    observation_time_est as last_updated,
    day_night,
    
    -- Temperature
    temp_fahrenheit,
    feels_like_fahrenheit,
    temp_category,
    
    -- Conditions
    weather_condition,
    humidity_percent,
    cloud_cover_percent,
    
    -- Wind
    wind_speed_mph,
    wind_category,
    wind_direction_degrees,
    
    -- Precipitation
    is_precipitating,
    is_snowing,
    total_precip_mm,
    
    -- Flags
    is_freezing,
    is_windy,
    comfort_level

from current_weather
order by neighborhood_name