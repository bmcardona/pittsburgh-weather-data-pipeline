with 

current_weather as (

    select * from {{ ref('stg_postgres__current_weather') }}
    
),

locations as (

    select * from {{ ref('stg_postgres__locations') }}

),

current_weather_and_locations as (
    select
        cw.*,
        l.neighborhood_name,
        l.latitude,
        l.longitude,
        l.timezone,
        l.created_at,
        l.updated_at

    from current_weather as cw

    join locations as l
    
    on cw.location_id = l.location_id

)

select * from current_weather_and_locations