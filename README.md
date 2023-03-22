Welcome to the PoC!

Follow the steps below to begin to check off the identified criteria

## Steps

1. Create the fake data to be used for our scenario

```bash
dbt run-operation create_inital_data
```

This will create two different tables `dbt_poc_raw_movies` and `dbt_poc_raw_users` in the target database and schema.  To alter that behavior, just specify database and/or schema arguments

```bash
dbt run-operation create_inital_data  --args '{database: foo, schema: bar}'
```

Once those are created, let's build some of our dbt models!

2. Batch Insert

```bash
dbt build
```

This step will do the following:

- Create 3 different tables - **Note:  tables follow dbt naming convention not what is laid out in the document.  Please see mapping below.**
  - `stg_app__users` - Unpacks `raw` json column to individual columns, adds calculated columns
  - `stg_app__movies` - Unpacks `raw` json column to individual columns
  - `int_users_and_movies` - Unpacks the `movies` column in the `stg_app__users` table from an array to individual rows.  Identifies all of the movies that an individual user has "favorited".
- Create validation checks:
  - Ensure IDs are not missing and are unique (when appropriate)
  - Ensure email is a valid email address
  - Movie rating between 0 and 5
  - User type is one of `New` or `Normal`

3. Simple Update

Run the following macro to create an "updated" set of rows in the users table.

```bash
dbt run-operation create_update_data
```

Again, modify the database and schema with arguments:

```bash
dbt run-operation create_update_data  --args '{database: foo, schema: bar}'
```

Since only our raw users data has changed with this update, let's just run the following:

```bash
dbt build -s stg_app__users+
```

This will ensure that we update rows in our existing table with what now exists in the source and then we rebuild the downstream table after that.

Now, let's do some validation.

First, our `stg_app__users` model should contain 475 rows with one `insertion_time` timestamp and 25 more with a more recent timestamp.  Run the following to validate:

```sql
select insertion_time, count(*) as total
from {{ ref('stg_app__users') }}
group by 1
order by 2 desc
```

Let's validate now that our incremental model contains all of the information that the source has:

```sql
{% set from_source %}
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

    from {{ source('app', 'users') }}
{% endset %}

{% set from_model %}
    select *
    from {{ ref('stg_app__users') }}
{% endset %}

{{ audit_helper.compare_queries(
    a_query=from_source,
    b_query=from_model,
    primary_key='user_id'
) }}
```
