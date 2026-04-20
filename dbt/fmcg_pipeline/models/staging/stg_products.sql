with source as (
    select * from {{ source('staging', 'raw_products') }}
),

renamed as (
    select
        product_id,
        product_name,
        category,
        brand,
        supplier,
        unit_cost::numeric(10,2)  as unit_cost,
        unit_price::numeric(10,2) as unit_price,
        loaded_at
    from source
)

select * from renamed