{% snapshot dim_store_snapshot %}

{{
    config(
        target_schema = 'gold',
        unique_key    = 'store_id',
        strategy      = 'check',
        check_cols    = ['region', 'store_type', 'state']
    )
}}

select
    store_id,
    store_name,
    state,
    region,
    store_type,
    city
from {{ source('staging', 'raw_stores') }}

{% endsnapshot %}