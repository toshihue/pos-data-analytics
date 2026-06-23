{{
    config(
        materialized='incremental',
        unique_key=['store_id', 'sales_date'],
        on_schema_change='sync_all_columns'
    )
}}

with enriched_transactions as (
    select *
    from {{ ref('int_pos_transactions_enriched') }}

    {% if is_incremental() %}
        where transaction_date >= (
            select coalesce(max(sales_date), to_date('1900-01-01'))
            from {{ this }}
        )
    {% endif %}
)

select
    store_id,
    store_name,
    prefecture,
    transaction_date as sales_date,
    sum(total_amount) as total_sales,
    sum(quantity) as total_quantity,
    count(distinct transaction_id) as transaction_count,
    sum(case when record_status <> 'valid' then 1 else 0 end) as invalid_record_count,
    sum(case when is_duplicate_record then 1 else 0 end) as duplicate_record_count,
    min(transaction_at) as first_transaction_at,
    max(transaction_at) as last_transaction_at,
    current_timestamp() as loaded_at
from enriched_transactions
group by
    store_id,
    store_name,
    prefecture,
    transaction_date
