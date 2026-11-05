{{ config(materialized='view') }}

select *
from semantic_view(
    {{ ref('semantic_view_daily_store_sales') }}
    metrics total_sales, total_quantity, transaction_count
    dimensions store_id, store_name, prefecture, transaction_date
)
