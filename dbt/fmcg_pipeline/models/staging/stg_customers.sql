with source as (
    select * from {{ source('staging', 'raw_customers') }}
),

renamed as (
    select
        customer_id,
        age_group,
        loyalty_tier,
        state,
        loaded_at
    from source
)

select * from renamed