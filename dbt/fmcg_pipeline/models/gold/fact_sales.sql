{{
    config(
        materialized     = 'incremental',
        unique_key       = 'transaction_id',
        on_schema_change = 'sync_all_columns'
    )
}}

with staged as (
    select
        transaction_id,
        transaction_date,
        product_id,
        store_id,
        customer_id,
        quantity,
        unit_price,
        discount_pct,
        total_amount,
        created_at
    from {{ ref('stg_sales') }}

    {% if is_incremental() %}
        where created_at > (
            select coalesce(max(created_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}
)

select * from staged