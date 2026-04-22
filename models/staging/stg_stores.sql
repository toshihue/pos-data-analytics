with source as (
    select * from {{ ref('raw_stores') }}
)

select
    store_id,
    store_name,
    prefecture,
    opened_at
from source
