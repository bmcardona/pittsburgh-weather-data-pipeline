
with source as (

    select * from {{ source('pittsburgh', 'fact_weather_history') }}

),

renamed as (

    select
        history_id,
        location_id,
        date_id,
        observation_time,
        temperature_2m,
        apparent_temperature,
        relative_humidity_2m,
        wind_speed_10m,
        wind_direction_10m,
        wind_gusts_10m,
        precipitation,
        rain,
        showers,
        snowfall,
        weather_code,
        cloud_cover,
        is_day,
        pressure_msl,
        surface_pressure,
        created_at

    from source

)

select * from renamed