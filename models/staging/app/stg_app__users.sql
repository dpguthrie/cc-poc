{{ config(materialized='incremental', unique_key='user_id') }}

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
        raw:INSERTION_TIME::timestamp_ntz as insertion_time,
        raw:UPDATE_TIME::timestamp_ntz as update_time

    from source
    {% if is_incremental() %}

    where update_time > (select max(update_time) from {{ this }})

    {% endif %}
)

select * from unpacked
