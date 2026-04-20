with source as (
    select * from {{ source('staging', 'raw_sales') }}
),

renamed as (
    select
        transaction_id,
        transaction_date::date       as transaction_date,
        product_id,
        store_id,
        customer_id,
        quantity::integer            as quantity,
        unit_price::numeric(10,2)    as unit_price,
        discount_pct::numeric(5,2)   as discount_pct,
        total_amount::numeric(10,2)  as total_amount,
        created_at::timestamp        as created_at,
        loaded_at
    from source
)

select * from renamed