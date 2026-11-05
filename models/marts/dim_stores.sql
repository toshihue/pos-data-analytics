{{ config(materialized='table') }}

select
    store_id,
    store_name,
    prefecture
from {{ ref('stg_stores') }}
