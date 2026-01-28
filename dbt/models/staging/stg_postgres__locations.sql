with source as (

    select * from {{ source('weather', 'dim_location') }}

),

renamed as (

    select
        location_id,
        neighborhood_name,
        community_board,
        latitude,
        longitude,
        timezone,
        created_at,
        updated_at

    from source

)

select * from renamed