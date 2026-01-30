with 

joined_locations_and_dates as (

    select * from {{ ref('int_forecast_joined_locations_and_dates') }}
    
),

add_categories as (
    select
        i.forecast_id,
        i.location_id,
        i.date_id,
        i.forecast_time,
        i.temperature_2m as temp_celsius_2m,
        round((i.temperature_2m * 9.0/5.0) + 32, 1) as temp_fahrenheit_2m,
        i.relative_humidity_2m,
        i.dew_point_2m,
        i.apparent_temperature as feels_like_celsius,
        round((i.apparent_temperature * 9.0/5.0) + 32, 1) as feels_like_fahrenheit,
        i.temperature_80m as temp_celcius_80m,
        round((i.temperature_80m * 9.0/5.0) + 32, 1) as temp_fahrenheit_80m,
        i.temperature_120m as temp_celcius_120m,
        round((i.temperature_120m * 9.0/5.0) + 32, 1) as temp_fahrenheit_120m,
        i.temperature_180m as temp_celcius_180m,
        round((i.temperature_180m * 9.0/5.0) + 32, 1) as temp_fahrenheit_180m,
        i.precipitation_probability,
        i.precipitation,
        i.rain,
        i.showers,
        i.snowfall,
        i.snow_depth,
        i.weather_code,
        i.pressure_msl,
        i.surface_pressure,
        i.cloud_cover,
        i.cloud_cover_low,
        i.cloud_cover_mid,
        i.cloud_cover_high,
        i.visibility,
        i.evapotranspiration,
        i.et0_fao_evapotranspiration,
        i.vapour_pressure_deficit,
        i.wind_speed_10m,
        i.wind_direction_10m,
        i.wind_gusts_10m,
        i.wind_speed_80m,
        i.wind_direction_80m,
        i.wind_speed_120m,
        i.wind_direction_120m,
        i.wind_speed_180m,
        i.wind_direction_180m,
        i.neighborhood_name,
        i.latitude,
        i.longitude,
        i.timezone,
        i.date,
        i.year,
        i.month,
        i.day,
        i.day_of_week,
        i.day_name,
        i.week_of_year,
        i.quarter,
        i.is_weekend,
        i.is_holiday,
        i.created_at,
        i.updated_at,

        -- Temperature comfort
        case 
            when apparent_temperature < 32 then 'Freezing'
            when apparent_temperature < 50 then 'Cold'
            when apparent_temperature < 65 then 'Cool'
            when apparent_temperature < 75 then 'Comfortable'
            when apparent_temperature < 85 then 'Warm'
            else 'Hot'
        end as temperature_category,

        -- Wind comfort
        case 
            when wind_speed_10m < 5 then 'Calm'
            when wind_speed_10m < 15 then 'Breezy'
            when wind_speed_10m < 25 then 'Windy'
            else 'Very Windy'
        end as wind_category,

        -- Cardinal directions (more readable than degrees)
        case 
            when wind_direction_10m between 337.5 and 360 or wind_direction_10m between 0 and 22.5 then 'N'
            when wind_direction_10m between 22.5 and 67.5 then 'NE'
            when wind_direction_10m between 67.5 and 112.5 then 'E'
            when wind_direction_10m between 112.5 and 157.5 then 'SE'
            when wind_direction_10m between 157.5 and 202.5 then 'S'
            when wind_direction_10m between 202.5 and 247.5 then 'SW'
            when wind_direction_10m between 247.5 and 292.5 then 'W'
            when wind_direction_10m between 292.5 and 337.5 then 'NW'
        end as wind_direction_cardinal,

        -- Visibility quality
        case 
            when visibility >= 10000 then 'Excellent'
            when visibility >= 5000 then 'Good'
            when visibility >= 2000 then 'Moderate'
            else 'Poor'
        end as visibility_category

    from joined_locations_and_dates as i

)

select * from add_categories