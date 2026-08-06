{{ config(materialized='view') }}

with transactions as (
    select *
    from {{ ref('stg_pos_transactions') }}
),

products as (
    select *
    from {{ ref('stg_pos_products') }}
),

stores as (
    select *
    from {{ ref('stg_pos_stores') }}
),

joined_once as (
    select
        t.transaction_id,
        t.store_id,
        t.product_id,
        t.quantity,
        t.unit_price,
        t.source_total_amount,
        t.transaction_at,
        t.transaction_date,
        p.product_name,
        p.category,
        p.tax_rate,
        p.is_active,
        s.store_name,
        s.prefecture,
        s.opened_at,
        t.quantity * t.unit_price as total_amount,
        t.quantity * t.unit_price as gross_amount,
        t.quantity * t.unit_price * coalesce(p.tax_rate, 0) as tax_amount,
        t.quantity * t.unit_price as net_amount,
        case
            when p.is_active = false then 'inactive_product'
            when t.quantity < 0 then 'negative_quantity'
            when t.unit_price < 0 then 'negative_price'
            else 'valid'
        end as record_status
    from transactions as t
    left join products as p
        on t.product_id = p.product_id
    left join stores as s
        on t.store_id = s.store_id
),

duplicated_flags as (
    select
        j.*,
        count(*) over (partition by j.transaction_id) as transaction_id_dup_count,
        row_number() over (
            partition by j.transaction_id
            order by j.transaction_at desc, j.product_id desc, j.store_id desc
        ) as transaction_id_rn
    from joined_once as j
)

select
    transaction_id,
    store_id,
    product_id,
    quantity,
    unit_price,
    source_total_amount,
    total_amount,
    transaction_at,
    transaction_date,
    product_name,
    category,
    tax_rate,
    is_active,
    store_name,
    prefecture,
    opened_at,
    gross_amount,
    tax_amount,
    net_amount,
    record_status,
    transaction_id_dup_count,
    transaction_id_rn,
    case
        when transaction_id_dup_count > 1 and transaction_id_rn > 1 then true
        else false
    end as is_duplicate_record,
    current_timestamp() as processed_at
from duplicated_flags
