select *
from {{ ref('stg_transactions') }}
where product_id = 'P012'