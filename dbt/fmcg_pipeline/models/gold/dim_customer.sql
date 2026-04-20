{{
    config(
        materialized = 'table',
        unique_key   = 'customer_id'
    )
}}

with staged as (
    select
        customer_id,
        age_group,
        loyalty_tier,
        state,
        loaded_at
    from {{ ref('stg_customers') }}
),

deduped as (
    select distinct on (customer_id)
        customer_id,
        age_group,
        loyalty_tier,
        state,
        loaded_at
    from staged
    order by customer_id, loaded_at desc
)

select * from deduped