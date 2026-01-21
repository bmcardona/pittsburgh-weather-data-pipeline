{{ config(
    materialized='view'
) }}

with hourly as (
    select * from {{ ref('int_weather_hourly') }}
),

daily_aggregated as (
    select
        -- Time dimension
        date_id,
        cast(hour_start_est as date) as observation_date,
        
        -- Location
        location_id,
        neighborhood_name,
        community_board,
        
        -- Temperature stats
        avg(temp_fahrenheit) as avg_temp_fahrenheit,
        min(temp_fahrenheit) as min_temp_fahrenheit,
        max(temp_fahrenheit) as max_temp_fahrenheit,
        max(temp_fahrenheit) - min(temp_fahrenheit) as temp_range_fahrenheit,
        
        avg(feels_like_fahrenheit) as avg_feels_like_fahrenheit,
        min(feels_like_fahrenheit) as min_feels_like_fahrenheit,
        max(feels_like_fahrenheit) as max_feels_like_fahrenheit,
        
        -- Humidity
        avg(avg_humidity_percent) as avg_humidity_percent,
        
        -- Wind
        avg(avg_wind_speed_mph) as avg_wind_speed_mph,
        max(avg_wind_speed_mph) as max_wind_speed_mph,
        max(max_wind_gust_mps) as max_wind_gust_mps,
        
        -- Precipitation (total for the day)
        sum(total_precip_mm) as total_precip_mm,
        sum(total_rain_mm) as total_rain_mm,
        sum(total_snowfall_mm) as total_snowfall_mm,
        
        -- Cloud cover
        avg(avg_cloud_cover_percent) as avg_cloud_cover_percent,
        
        -- Pressure
        avg(avg_pressure_hpa) as avg_pressure_hpa,
        min(avg_pressure_hpa) as min_pressure_hpa,
        max(avg_pressure_hpa) as max_pressure_hpa,
        
        -- Weather conditions (most common of the day)
        mode() within group (order by weather_condition) as predominant_weather_condition,
        
        -- Event flags
        bool_or(had_precipitation) as had_precipitation,
        bool_or(had_snow) as had_snow,
        bool_or(was_freezing) as was_freezing,
        bool_or(was_windy) as was_windy,
        
        -- Hours with precipitation
        sum(case when had_precipitation then 1 else 0 end) as hours_with_precip,
        sum(case when had_snow then 1 else 0 end) as hours_with_snow,
        
        -- Data quality
        count(*) as hours_observed,
        min(latest_observation_time) as first_observation,
        max(latest_observation_time) as last_observation
        
    from hourly
    group by
        date_id,
        cast(hour_start_est as date),
        location_id,
        neighborhood_name,
        community_board
)

select * from daily_aggregated