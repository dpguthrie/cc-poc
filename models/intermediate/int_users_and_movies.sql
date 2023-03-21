with

source as (
    select * from {{ ref('stg_app__users') }}
),

flatten as (
    select
        u.load_id,
        u.user_id,
        f.value as movie_id,
        u.insertion_time,
        u.update_time
    from dbt_dguthrie.stg_app__users u,
    lateral flatten(input => u.movies) f
)

select * from flatten
