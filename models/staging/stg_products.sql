with source as (
    select * from {{ source('pos', 'raw_products') }}
)

select
    product_id,
    product_name,
    category,
    unit_price,
    tax_rate,
    is_active
from source
