{{
    config(
        materialized = 'table'
    )
}}

with date_spine as (
    select
        generate_series(
            '2026-01-01'::date,
            '2027-12-31'::date,
            '1 day'::interval
        )::date as date_day
)

select
    date_day                                as date_id,
    date_day                                as full_date,
    extract(year from date_day)::integer    as year,
    extract(month from date_day)::integer   as month,
    extract(day from date_day)::integer     as day,
    extract(week from date_day)::integer    as week_of_year,
    extract(quarter from date_day)::integer as quarter,
    to_char(date_day, 'Month')              as month_name,
    to_char(date_day, 'Day')               as day_name,
    case
        when extract(dow from date_day) in (0,6)
        then true else false
    end                                     as is_weekend
from date_spine