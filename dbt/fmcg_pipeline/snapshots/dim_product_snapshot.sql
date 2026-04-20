{% snapshot dim_product_snapshot %}

{{
    config(
        target_schema = 'gold',
        unique_key    = 'product_id',
        strategy      = 'check',
        check_cols    = ['unit_price', 'unit_cost', 'category']
    )
}}

select
    product_id,
    product_name,
    category,
    brand,
    supplier,
    unit_cost,
    unit_price
from {{ ref('stg_products') }}

{% endsnapshot %}