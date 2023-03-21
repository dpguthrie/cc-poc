with

source as (
    select * from {{ source('app', 'users') }}
),

unpacked as (
    select
        load_id,

        -- object values
        raw:ID::int as user_id,
        raw:EMAIL::varchar as email,
        raw:FIRST_NAME::varchar as first_name,
        raw:LAST_NAME::varchar as last_name,
        raw:USER_TYPE::varchar as user_type,
        raw:IP_ADDRESS::varchar as ip_address,
        raw:MOVIES::array as movies,
        raw as raw_data,
        
        -- calculated fields
        FALSE as is_premium,
        -- put in get_ip_number here
        
        -- timestamps
        current_timestamp as insertion_time,
        current_timestamp as update_time
    from source
)

select * from unpacked
