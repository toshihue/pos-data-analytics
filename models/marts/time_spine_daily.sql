{{ config(materialized='table') }}

with date_spine as (
    {{
        dbt.date_spine(
            'day',
            "dateadd(year, -5, current_date())",
            "dateadd(day, 31, current_date())"
        )
    }}
),

final as (
    select cast(date_day as date) as date_day
    from date_spine
)

select *
from final
