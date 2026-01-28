with source as (

    select * from {{ source('pittsburgh', 'dim_location') }}

),

renamed as (

    select
        location_id,
        neighborhood_name,
        latitude,
        longitude,
        timezone,
        created_at,
        updated_at

    from source

)

select * from renamed