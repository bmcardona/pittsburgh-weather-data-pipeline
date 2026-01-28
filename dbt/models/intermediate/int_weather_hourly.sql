{{ config(
    materialized='view'
) }}

with enriched as (
    select * from {{ ref('int_weather_enriched') }}
),

locations as (
    select * from {{ ref('stg_postgres__locations') }}
),

-- First, find the latest observation for each hour/location
latest_obs_per_hour as (
    select
        date_trunc('hour', observation_time_est) as hour_start_est,
        location_id,
        max(observation_time_est) as latest_observation_time
    from enriched
    group by 1, 2
),

-- Get the full row for that latest observation
latest_weather as (
    select
        e.*,
        date_trunc('hour', e.observation_time_est) as hour_start_est,
        l.latest_observation_time  -- Add this line!
    from enriched e
    inner join latest_obs_per_hour l
        on e.location_id = l.location_id
        and e.observation_time_est = l.latest_observation_time
        and date_trunc('hour', e.observation_time_est) = l.hour_start_est
),

-- Now aggregate other metrics
hourly_aggregated as (
    select
        lw.hour_start_est,
        lw.date_id,
        lw.location_id,
        loc.neighborhood_name,
        loc.community_board,
        
        -- Latest observation values (from the most recent reading)
        lw.latest_observation_time,
        lw.temp_celsius,
        lw.temp_fahrenheit,
        lw.feels_like_celsius,
        lw.feels_like_fahrenheit,
        lw.temp_category,
        lw.weather_condition,
        lw.wind_category,
        lw.day_night,
        
        -- Aggregated metrics across all observations in the hour
        avg(e.humidity_percent) as avg_humidity_percent,
        avg(e.wind_speed_mph) as avg_wind_speed_mph,
        max(e.wind_gusts_mps) as max_wind_gust_mps,
        avg(e.wind_direction_degrees) as avg_wind_direction_degrees,
        
        -- Sum precipitation across the hour
        sum(e.total_precip_mm) as total_precip_mm,
        sum(e.rain_mm) as total_rain_mm,
        sum(e.snowfall_mm) as total_snowfall_mm,
        
        -- Cloud cover and pressure (average)
        avg(e.cloud_cover_percent) as avg_cloud_cover_percent,
        avg(e.pressure_sea_level_hpa) as avg_pressure_hpa,
        
        -- Flags (any occurrence in the hour)
        bool_or(e.is_precipitating) as had_precipitation,
        bool_or(e.is_snowing) as had_snow,
        bool_or(e.is_freezing) as was_freezing,
        bool_or(e.is_windy) as was_windy,
        
        -- Count observations
        count(*) as observation_count
        
    from latest_weather lw
    left join locations loc on lw.location_id = loc.location_id
    left join enriched e 
        on e.location_id = lw.location_id 
        and date_trunc('hour', e.observation_time_est) = lw.hour_start_est
    group by
        lw.hour_start_est,
        lw.date_id,
        lw.location_id,
        loc.neighborhood_name,
        loc.community_board,
        lw.latest_observation_time,
        lw.temp_celsius,
        lw.temp_fahrenheit,
        lw.feels_like_celsius,
        lw.feels_like_fahrenheit,
        lw.temp_category,
        lw.weather_condition,
        lw.wind_category,
        lw.day_night
)

select * from hourly_aggregated