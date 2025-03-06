with source as (
    select * from {{ source('jaffle_shop', 'customers') }}
),

transformed as (
    select 
        id as customer_id,
        last_name as customer_last_name,
        first_name as customer_first_name,
        --string concatenation - || and a quote of a space, as opposed to Tableau logic with +
        first_name || ' ' || last_name as full_name
    from source
)

select * from transformed