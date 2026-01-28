with source as (

    select * from {{ source('weather', 'dim_date') }}

),

renamed as (

    select
        date_id,
        date,
        year,
        month,
        day,
        day_of_week,
        day_name,
        week_of_year,
        quarter,
        is_weekend,
        is_holiday

    from source

)

select * from renamed