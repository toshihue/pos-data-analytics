select *
from {{ ref('fct_daily_store_sales') }}
where average_basket_value <= 0
