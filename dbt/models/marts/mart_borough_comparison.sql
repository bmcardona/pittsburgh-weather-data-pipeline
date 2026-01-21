{{ config(
    materialized='table'
) }}

with locations as (
    select
        location_id,
        neighborhood_name,
        community_board,
        -- Extract borough from community_board field (e.g., "Manhattan 1" -> "Manhattan")
        split_part(community_board, ' ', 1) as borough
    from {{ ref('stg_locations') }}
),

daily_with_borough as (
    select
        d.*,
        l.borough
    from {{ ref('int_weather_daily') }} d
    inner join locations l on d.location_id = l.location_id
),

borough_daily_stats as (
    select
        borough,
        observation_date,
        
        -- Temperature (averages across neighborhoods in borough)
        avg(avg_temp_fahrenheit) as avg_temp_fahrenheit,
        min(min_temp_fahrenheit) as min_temp_fahrenheit,
        max(max_temp_fahrenheit) as max_temp_fahrenheit,
        
        -- Precipitation (average across neighborhoods)
        avg(total_precip_mm) as avg_precip_mm,
        avg(total_rain_mm) as avg_rain_mm,
        avg(total_snowfall_mm) as avg_snowfall_mm,
        
        -- Wind
        avg(avg_wind_speed_mph) as avg_wind_speed_mph,
        
        -- Counts
        count(*) as neighborhood_count,
        sum(case when had_precipitation then 1 else 0 end) as neighborhoods_with_precip,
        sum(case when had_snow then 1 else 0 end) as neighborhoods_with_snow
        
    from daily_with_borough
    group by borough, observation_date
)

select
    *,
    -- Calculate percentages
    round(100.0 * neighborhoods_with_precip / neighborhood_count, 1) as pct_neighborhoods_with_precip,
    round(100.0 * neighborhoods_with_snow / neighborhood_count, 1) as pct_neighborhoods_with_snow
    
from borough_daily_stats
order by observation_date desc, borough