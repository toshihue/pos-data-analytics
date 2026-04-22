with by_store as (
    select sales_date, sum(total_sales) as store_total
    from {{ ref('fct_daily_store_sales') }}
    group by sales_date
),

by_category as (
    select sales_date, sum(total_sales) as category_total
    from {{ ref('fct_daily_category_sales') }}
    group by sales_date
)

select
    coalesce(s.sales_date, c.sales_date) as sales_date,
    s.store_total,
    c.category_total
from by_store s
full outer join by_category c on s.sales_date = c.sales_date
where s.store_total != c.category_total
   or s.store_total is null
   or c.category_total is null