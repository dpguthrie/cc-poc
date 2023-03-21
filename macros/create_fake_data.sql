{% macro create_fake_data(database=None, schema=None) %}

    {% if execute %}

        {% set database = database or target.database %}
        {% set schema = schema or target.schema %}

        {% set sql %}

        -- Create the raw users table from faker data
        create or replace table {{ database }}.{{ schema }}.dbt_poc_raw_users as
            with faker_data as (
                select
                    row_number() over (order by null) as id,
                    fake('en_US','email', {})::varchar as email,
                    fake('en_US','first_name',{})::varchar as first_name,
                    fake('en_US','last_name', {})::varchar as last_name,
                    fake('en_US', 'random_element', {'elements': ['New', 'Normal']})::varchar as user_type,
                    fake('en_US', 'ipv4_public', {})::varchar as ip_address,
                    fake('en_US', 'random_elements', {'elements': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50], 'unique': True}) as movies
                from table(generator(rowcount => 500))
            )
            select
                1 as load_id,
                object_construct(*) as raw
            from faker_data;

        -- Create the raw movies table from faker data
        create or replace table {{ database }}.{{ schema }}.dbt_poc_raw_movies as
            with faker_data as (
                select
                    row_number() over (order by null) as id,
                    fake('en_US', 'catch_phrase', {})::varchar as name,
                    round(fake('en_US', 'random_int', {'min': 200, 'max': 500}) / 100, 1) as rating
                from table(generator(rowcount => 50))
            )
            select
                1 as load_id,
                object_construct(*) as raw
            from faker_data;

        {% endset %}

        {% do run_query(sql) %}

    {% endif %}

{% endmacro %}