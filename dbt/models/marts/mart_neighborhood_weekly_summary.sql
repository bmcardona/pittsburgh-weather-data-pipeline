{{ config(
    materialized='table'
) }}

with daily_data as (
    select * from {{ ref('int_weather_daily') }}
),

weekly_aggregated as (
    select
        neighborhood_name,
        community_board,
        
        -- Week identification
        date_trunc('week', observation_date) as week_start_date,
        min(observation_date) as first_day_of_week,
        max(observation_date) as last_day_of_week,
        count(distinct observation_date) as days_with_data,
        
        -- Temperature stats
        avg(avg_temp_fahrenheit) as avg_temp_fahrenheit,
        min(min_temp_fahrenheit) as coldest_temp_fahrenheit,
        max(max_temp_fahrenheit) as warmest_temp_fahrenheit,
        max(max_temp_fahrenheit) - min(min_temp_fahrenheit) as temp_range_fahrenheit,
        
        -- Precipitation
        sum(total_precip_mm) as total_precip_mm,
        sum(total_rain_mm) as total_rain_mm,
        sum(total_snowfall_mm) as total_snowfall_mm,
        
        -- Days with events
        sum(case when had_precipitation then 1 else 0 end) as days_with_precipitation,
        sum(case when had_snow then 1 else 0 end) as days_with_snow,
        sum(case when was_freezing then 1 else 0 end) as days_below_freezing,
        
        -- Total hours
        sum(hours_with_precip) as total_hours_with_precip,
        sum(hours_with_snow) as total_hours_with_snow,
        
        -- Wind
        avg(avg_wind_speed_mph) as avg_wind_speed_mph,
        max(max_wind_speed_mph) as max_wind_speed_mph,
        
        -- Most common weather
        mode() within group (order by predominant_weather_condition) as most_common_weather
        
    from daily_data
    group by
        neighborhood_name,
        community_board,
        date_trunc('week', observation_date)
)

select * from weekly_aggregated
order by week_start_date desc, neighborhood_name