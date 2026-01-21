
  create view "weather_db"."weather_staging"."stg_locations__dbt_tmp"
    
    
  as (
    

with source as (
    select * from "weather_db"."weather"."dim_location"
),

cleaned as (
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

select * from cleaned
  );