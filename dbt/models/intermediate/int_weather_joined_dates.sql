with 

weather as (

    select * from {{ ref('int_weather_joined_locations') }}
    
),

dates as (

    select * from {{ ref('stg_postgres__dates') }}

),

weather_and_dates as (
    select
        w.*,
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

    from weather as w

    join dates as d
    
    on w.date_id = d.date_id

)

select * from weather_and_dates