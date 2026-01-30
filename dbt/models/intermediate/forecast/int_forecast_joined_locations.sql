with 

forecast as (

    select * from {{ ref('stg_postgres__hourly_forecast') }}
    
),

locations as (

    select * from {{ ref('stg_postgres__locations') }}

),

forecast_and_locations as (
    select
        f.forecast_id,
        f.location_id,
        f.date_id,
        f.forecast_time,
        f.temperature_2m,
        f.relative_humidity_2m,
        f.dew_point_2m,
        f.apparent_temperature,
        f.temperature_80m,
        f.temperature_120m,
        f.temperature_180m,
        f.precipitation_probability,
        f.precipitation,
        f.rain,
        f.showers,
        f.snowfall,
        f.snow_depth,
        f.weather_code,
        f.pressure_msl,
        f.surface_pressure,
        f.cloud_cover,
        f.cloud_cover_low,
        f.cloud_cover_mid,
        f.cloud_cover_high,
        f.visibility,
        f.evapotranspiration,
        f.et0_fao_evapotranspiration,
        f.vapour_pressure_deficit,
        f.wind_speed_10m,
        f.wind_direction_10m,
        f.wind_gusts_10m,
        f.wind_speed_80m,
        f.wind_direction_80m,
        f.wind_speed_120m,
        f.wind_direction_120m,
        f.wind_speed_180m,
        f.wind_direction_180m,
        l.neighborhood_name,
        l.latitude,
        l.longitude,
        l.timezone,
        l.created_at,
        l.updated_at

    from forecast as f

    join locations as l
    
    on f.location_id = l.location_id

)

select * from forecast_and_locations