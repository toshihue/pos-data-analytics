{{ config(materialized='view') }}

select
    cast(product_id as varchar) as product_id,
    cast(product_name as varchar) as product_name,
    cast(category as varchar) as category,
    cast(unit_price as number(38, 6)) as source_unit_price,
    cast(tax_rate as number(38, 6)) as tax_rate,
    cast(is_active as boolean) as is_active
from {{ source('pos', 'raw_products') }}
