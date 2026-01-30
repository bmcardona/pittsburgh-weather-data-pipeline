with source as (

    select * from {{ source('pittsburgh', 'fact_hourly_forecast') }}

),

renamed as (

    select
        forecast_id,
        location_id,
        date_id,
        forecast_time,
        temperature_2m,
        relative_humidity_2m,
        dew_point_2m,
        apparent_temperature,
        temperature_80m,
        temperature_120m,
        temperature_180m,
        precipitation_probability,
        precipitation,
        rain,
        showers,
        snowfall,
        snow_depth,
        weather_code,
        pressure_msl,
        surface_pressure,
        cloud_cover,
        cloud_cover_low,
        cloud_cover_mid,
        cloud_cover_high,
        visibility,
        evapotranspiration,
        et0_fao_evapotranspiration,
        vapour_pressure_deficit,
        wind_speed_10m,
        wind_direction_10m,
        wind_gusts_10m,
        wind_speed_80m,
        wind_direction_80m,
        wind_speed_120m,
        wind_direction_120m,
        wind_speed_180m,
        wind_direction_180m,
        created_at

    from source

)

select * from renamed