select
    t.transaction_id,
    t.product_id,
    p.product_name,
    p.is_active,
    t.transaction_at
from {{ ref('stg_transactions') }} t
left join {{ ref('stg_products') }} p
    on t.product_id = p.product_id
where p.is_active = false