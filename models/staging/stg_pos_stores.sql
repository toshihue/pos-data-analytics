{{ config(materialized='view') }}

select
    cast(store_id as varchar) as store_id,
    cast(store_name as varchar) as store_name,
    cast(prefecture as varchar) as prefecture,
    cast(opened_at as date) as opened_at
from {{ source('pos', 'raw_stores') }}
