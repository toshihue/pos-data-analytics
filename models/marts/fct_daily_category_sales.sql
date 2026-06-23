with transactions as (
    select * from {{ ref('stg_transactions') }}
),

products as (
    select * from {{ ref('stg_products') }}
)

select
    p.category,
    t.transaction_date as sales_date,
    sum(t.total_amount) as total_sales,
    sum(t.quantity) as total_quantity,
    count(distinct t.transaction_id) as transaction_count,
    round(sum(t.total_amount) / nullif(count(distinct t.transaction_id), 0), 2) as average_basket_value
from transactions t
left join products p on t.product_id = p.product_id
group by
    p.category,
    t.transaction_date
