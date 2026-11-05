{{ config(materialized='table') }}

select
    transaction_id,
    store_id,
    transaction_date,
    total_amount,
    quantity
from {{ ref('stg_transactions') }}
