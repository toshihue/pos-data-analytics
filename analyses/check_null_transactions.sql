-- transaction_idがNULLの全行を確認
select *
from {{ ref('stg_transactions') }}
where transaction_id is null