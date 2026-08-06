{{ config(materialized='view') }}

select
    cast(transaction_id as varchar) as transaction_id,
    cast(store_id as varchar) as store_id,
    cast(product_id as varchar) as product_id,
    cast(quantity as number) as quantity,
    cast(unit_price as number(38, 6)) as unit_price,
    cast(total_amount as number(38, 6)) as source_total_amount,
    cast(transaction_at as timestamp_ntz) as transaction_at,
    cast(transaction_at as date) as transaction_date
from {{ source('pos', 'raw_transactions') }}
