with transactions as (
    select * from {{ ref('stg_transactions') }}
),

stores as (
    select * from {{ ref('stg_stores') }}
)

select
    t.store_id,
    s.store_name,
    s.prefecture,
    t.transaction_date as sales_date,
    sum(t.total_amount) as total_sales,
    sum(t.quantity) as total_quantity,
    count(distinct t.transaction_id) as transaction_count,
    round(sum(t.total_amount) / nullif(count(distinct t.transaction_id), 0), 2) as average_basket_value
from transactions t
left join stores s on t.store_id = s.store_id
group by
    t.store_id,
    s.store_name,
    s.prefecture,
    t.transaction_date
