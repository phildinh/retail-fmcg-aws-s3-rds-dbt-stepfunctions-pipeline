with source as (
    select * from {{ source('staging', 'raw_stores') }}
),

renamed as (
    select
        store_id,
        store_name,
        state,
        region,
        store_type,
        city,
        loaded_at
    from source
)

select * from renamed