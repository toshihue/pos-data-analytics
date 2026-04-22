with source as (
    select * from {{ source('pos', 'raw_transactions') }}
)

select
    transaction_id,
    store_id,
    product_id,
    quantity,
    unit_price,
    total_amount,
    transaction_at,
    cast(transaction_at as date) as transaction_date
from source
