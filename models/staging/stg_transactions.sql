with transactions as (
    select * from {{ source('pos', 'raw_transactions') }}
),

products as (
    select * from {{ source('pos', 'raw_products') }}
)

select
    t.transaction_id,
    t.store_id,
    t.product_id,
    t.quantity,
    t.unit_price,
    p.category,
    p.tax_rate,
    -- t.quantity * t.unit_price as total_amount,
    t.quantity * t.unit_price * (1 + p.tax_rate) as total_amount,
    t.transaction_at,
    cast(t.transaction_at as date) as transaction_date
from transactions t
left join products p
    on t.product_id = p.product_id
