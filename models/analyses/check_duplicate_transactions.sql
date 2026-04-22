-- 重複しているtransaction_idの全行を確認
select *
from {{ ref('stg_transactions') }}
where transaction_id in (
    select transaction_id
    from {{ ref('stg_transactions') }}
    group by transaction_id
    having count(*) > 1
)
order by transaction_id, transaction_at