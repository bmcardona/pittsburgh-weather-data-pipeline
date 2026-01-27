{{ config(
    materialized='table'
) }}

-- Hourly temperature trends by neighborhood for today
SELECT 
    iwe.observation_time_est,
    iwe.temp_fahrenheit,
    iwe.feels_like_fahrenheit,
    iwe.humidity_percent,
    iwe.wind_speed_mph,
    iwe.weather_condition,
    dl.location_id,
    dl.neighborhood_name,
    dl.community_board,
    dl.latitude,
    dl.longitude
FROM {{ ref('int_weather_enriched') }} AS iwe
JOIN {{ ref('stg_locations') }} AS dl 
    ON iwe.location_id = dl.location_id
WHERE DATE(iwe.observation_time_est) = CURRENT_DATE
ORDER BY iwe.observation_time_est, dl.neighborhood_name