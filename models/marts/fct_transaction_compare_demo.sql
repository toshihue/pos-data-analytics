with base as (
    select * from {{ ref('stg_transactions') }}
),

deduped as (
    select
        transaction_id,
        store_id,
        product_id,
        category,
        transaction_date,
        quantity,
        unit_price,
        tax_rate,
        total_amount,
        transaction_at,
        row_number() over (
            partition by transaction_id
            order by transaction_at desc, product_id desc
        ) as transaction_rank
    from base
    where transaction_id is not null
)

select
    transaction_id,
    store_id,
    product_id,
    category,
    transaction_date,
    quantity,
    unit_price,
    tax_rate,
    total_amount
from deduped
where transaction_rank = 1
