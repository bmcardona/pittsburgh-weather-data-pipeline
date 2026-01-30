with 

forecast_joined_locations as (

    select * from {{ ref('int_forecast_joined_locations') }}
    
),

dates as (

    select * from {{ ref('stg_postgres__dates') }}

),

forecast_joined_locations_and_dates as (
    select
        fjl.forecast_id,
        fjl.location_id,
        fjl.date_id,
        fjl.forecast_time,
        fjl.temperature_2m,
        fjl.relative_humidity_2m,
        fjl.dew_point_2m,
        fjl.apparent_temperature,
        fjl.temperature_80m,
        fjl.temperature_120m,
        fjl.temperature_180m,
        fjl.precipitation_probability,
        fjl.precipitation,
        fjl.rain,
        fjl.showers,
        fjl.snowfall,
        fjl.snow_depth,
        fjl.weather_code,
        fjl.pressure_msl,
        fjl.surface_pressure,
        fjl.cloud_cover,
        fjl.cloud_cover_low,
        fjl.cloud_cover_mid,
        fjl.cloud_cover_high,
        fjl.visibility,
        fjl.evapotranspiration,
        fjl.et0_fao_evapotranspiration,
        fjl.vapour_pressure_deficit,
        fjl.wind_speed_10m,
        fjl.wind_direction_10m,
        fjl.wind_gusts_10m,
        fjl.wind_speed_80m,
        fjl.wind_direction_80m,
        fjl.wind_speed_120m,
        fjl.wind_direction_120m,
        fjl.wind_speed_180m,
        fjl.wind_direction_180m,
        fjl.neighborhood_name,
        fjl.latitude,
        fjl.longitude,
        fjl.timezone,
        fjl.created_at,
        fjl.updated_at,
        d.date,
        d.year,
        d.month,
        d.day,
        d.day_of_week,
        d.day_name,
        d.week_of_year,
        d.quarter,
        d.is_weekend,
        d.is_holiday

    from forecast_joined_locations as fjl

    join dates as d
    
    on fjl.date_id = d.date_id

)

select * from forecast_joined_locations_and_dates