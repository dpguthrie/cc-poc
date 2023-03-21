with

source as (
    select * from {{ source('app', 'movies') }}
),

unpacked as (
    select
        load_id,

        -- object values
        raw:ID::int as movie_id,
        raw:NAME::varchar as movie_name,
        raw:RATING::float as movie_rating,
        
        -- timestamps
        current_timestamp as insertion_time,
        current_timestamp as update_time
    from source
)

select * from unpacked
