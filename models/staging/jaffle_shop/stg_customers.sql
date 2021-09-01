with customers as (

    select
        id as customer_id,
        first_name,
        last_name

    from {{ source('base_tables', 'customers') }}

)

select * from customers