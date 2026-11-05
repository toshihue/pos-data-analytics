{{ config(materialized='semantic_view') }}

TABLES(
    transactions AS {{ ref('fct_transactions') }}
        PRIMARY KEY (transaction_id),
    stores AS {{ ref('dim_stores') }}
        PRIMARY KEY (store_id)
)

RELATIONSHIPS(
    transaction_to_store AS transactions(store_id) REFERENCES stores(store_id)
)

FACTS(
    transactions.total_amount AS total_amount,
    transactions.quantity AS quantity
)

DIMENSIONS(
    stores.store_id AS store_id,
    stores.store_name AS store_name,
    stores.prefecture AS prefecture,
    transactions.transaction_date AS transaction_date
)

METRICS(
    transactions.total_sales AS SUM(transactions.total_amount),
    transactions.total_quantity AS SUM(transactions.quantity),
    transactions.transaction_count AS COUNT(DISTINCT transactions.transaction_id)
)
