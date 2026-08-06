{% macro deploy_demo_monolithic_pos_pipeline() %}
    {% set sql %}
create or replace procedure demo_monolithic_pos_pipeline(run_date date)
returns variant
language sql
execute as caller
as
$$
declare
    -- 実行対象日。未指定時は current_date() を使用。
    v_run_date date default coalesce(run_date, current_date());

    -- 実行開始・終了時刻
    v_started_at timestamp_ntz default current_timestamp();
    v_finished_at timestamp_ntz;

    -- 処理件数
    v_rows_transactions number default 0;
    v_rows_store_sales number default 0;
    v_rows_category_sales number default 0;

    -- delete 件数および warning 件数
    v_deleted_store_sales number default 0;
    v_deleted_category_sales number default 0;
    v_warning_count number default 0;

    -- 動的 SQL 組み立て用
    v_sql string;

    -- 戻り値
    v_result variant;
begin
    -- -------------------------------------------------------------------------
    -- 初期化 / セッションローカル監査ログ
    -- -------------------------------------------------------------------------
    -- 実行中の監査情報を一時テーブルに保持する。
    create or replace temporary table tmp_pipeline_audit (
        step_name string,
        step_status string,
        row_count number,
        message string,
        logged_at timestamp_ntz
    );

    insert into tmp_pipeline_audit
    values ('bootstrap', 'success', null, 'procedure started', current_timestamp());

    -- -------------------------------------------------------------------------
    -- step 1: transaction の enrich
    -- -------------------------------------------------------------------------
    -- transaction に product / store 情報、品質判定、重複判定を付与する。
    v_sql := 'create or replace temporary table tmp_transactions_enriched as
        with transactions as (
            select
                transaction_id,
                store_id,
                product_id,
                quantity,
                unit_price,
                transaction_at
            from raw_pos_toshi.dbt_toshi_dev_pos_data.raw_transactions
            where cast(transaction_at as date) = ?
        ),
        products as (
            select
                product_id,
                product_name,
                category,
                tax_rate,
                is_active
            from raw_pos_toshi.dbt_toshi_dev_pos_data.raw_products
        ),
        stores as (
            select
                store_id,
                store_name,
                prefecture,
                opened_at
            from raw_pos_toshi.dbt_toshi_dev_pos_data.raw_stores
        ),
        joined_once as (
            select
                t.transaction_id,
                t.store_id,
                t.product_id,
                t.quantity,
                t.unit_price,
                t.transaction_at,
                cast(t.transaction_at as date) as transaction_date,
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
                    when p.is_active = false then ''inactive_product''
                    when t.quantity < 0 then ''negative_quantity''
                    when t.unit_price < 0 then ''negative_price''
                    else ''valid''
                end as record_status
            from transactions t
            left join products p
                on t.product_id = p.product_id
            left join stores s
                on t.store_id = s.store_id
        ),
        duplicated_flags as (
            select
                j.*,
                count(*) over (partition by transaction_id) as transaction_id_dup_count,
                row_number() over (
                    partition by transaction_id
                    order by transaction_at desc, product_id desc, store_id desc
                ) as transaction_id_rn
            from joined_once j
        )
        select
            transaction_id,
            store_id,
            product_id,
            quantity,
            unit_price,
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
        from duplicated_flags';
    execute immediate :v_sql using (v_run_date);

    -- enrich 後件数
    select count(*) into :v_rows_transactions
    from tmp_transactions_enriched;

    insert into tmp_pipeline_audit
    values ('tmp_transactions_enriched', 'success', :v_rows_transactions, 'enriched transaction rows created', current_timestamp());

    -- -------------------------------------------------------------------------
    -- step 2a: 日次店舗売上へ集計
    -- -------------------------------------------------------------------------
    -- enrich 済み transaction を店舗・日付単位で集計する。
    create or replace temporary table tmp_daily_store_sales as
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
    from tmp_transactions_enriched
    group by
        store_id,
        store_name,
        prefecture,
        transaction_date;

    select count(*) into :v_rows_store_sales
    from tmp_daily_store_sales;

    insert into tmp_pipeline_audit
    values ('tmp_daily_store_sales', 'success', :v_rows_store_sales, 'daily store sales aggregated', current_timestamp());

    -- -------------------------------------------------------------------------
    -- step 2b: 日次カテゴリ売上へ集計
    -- -------------------------------------------------------------------------
    -- enrich 済み transaction をカテゴリ・日付単位で集計する。
    create or replace temporary table tmp_daily_category_sales as
    select
        category,
        transaction_date as sales_date,
        sum(total_amount) as total_sales,
        sum(quantity) as total_quantity,
        count(distinct transaction_id) as transaction_count,
        sum(case when record_status <> 'valid' then 1 else 0 end) as invalid_record_count,
        sum(case when is_duplicate_record then 1 else 0 end) as duplicate_record_count,
        min(transaction_at) as first_transaction_at,
        max(transaction_at) as last_transaction_at,
        current_timestamp() as loaded_at
    from tmp_transactions_enriched
    group by
        category,
        transaction_date;

    select count(*) into :v_rows_category_sales
    from tmp_daily_category_sales;

    insert into tmp_pipeline_audit
    values ('tmp_daily_category_sales', 'success', :v_rows_category_sales, 'daily category sales aggregated', current_timestamp());


    execute immediate 'create table if not exists analytics.daily_store_sales_monolithic_demo (

        store_id string,
        store_name string,
        prefecture string,
        sales_date date,
        total_sales number(38, 6),
        total_quantity number,
        transaction_count number,
        invalid_record_count number,
        duplicate_record_count number,
        first_transaction_at timestamp_ntz,
        last_transaction_at timestamp_ntz,
        loaded_at timestamp_ntz
    )';

    execute immediate 'create table if not exists analytics.daily_category_sales_monolithic_demo (
        category string,
        sales_date date,
        total_sales number(38, 6),
        total_quantity number,
        transaction_count number,
        invalid_record_count number,
        duplicate_record_count number,
        first_transaction_at timestamp_ntz,
        last_transaction_at timestamp_ntz,
        loaded_at timestamp_ntz
    )';

    v_sql := 'delete from analytics.daily_store_sales_monolithic_demo where sales_date = ?';
    execute immediate :v_sql using (v_run_date);
    v_deleted_store_sales := SQLROWCOUNT;

    insert into analytics.daily_store_sales_monolithic_demo (
        store_id,
        store_name,
        prefecture,
        sales_date,
        total_sales,
        total_quantity,
        transaction_count,
        invalid_record_count,
        duplicate_record_count,
        first_transaction_at,
        last_transaction_at,
        loaded_at
    )
    select
        store_id,
        store_name,
        prefecture,
        sales_date,
        total_sales,
        total_quantity,
        transaction_count,
        invalid_record_count,
        duplicate_record_count,
        first_transaction_at,
        last_transaction_at,
        loaded_at
    from tmp_daily_store_sales;

    insert into tmp_pipeline_audit
    values ('load_daily_store_sales', 'success', :v_rows_store_sales, 'store sales reloaded for run date', current_timestamp());

    v_sql := 'delete from analytics.daily_category_sales_monolithic_demo where sales_date = ?';
    execute immediate :v_sql using (v_run_date);
    v_deleted_category_sales := SQLROWCOUNT;

    insert into analytics.daily_category_sales_monolithic_demo (
        category,
        sales_date,
        total_sales,
        total_quantity,
        transaction_count,
        invalid_record_count,
        duplicate_record_count,
        first_transaction_at,
        last_transaction_at,
        loaded_at
    )
    select
        category,
        sales_date,
        total_sales,
        total_quantity,
        transaction_count,
        invalid_record_count,
        duplicate_record_count,
        first_transaction_at,
        last_transaction_at,
        loaded_at
    from tmp_daily_category_sales;

    insert into tmp_pipeline_audit
    values ('load_daily_category_sales', 'success', :v_rows_category_sales, 'category sales reloaded for run date', current_timestamp());

    select count(*) into :v_warning_count
    from tmp_transactions_enriched
    where record_status <> 'valid';

    if (v_warning_count > 0) then
        insert into tmp_pipeline_audit
        values (
            'warning_scan',
            'warning',
            :v_warning_count,
            'invalid transaction-like records detected but not rejected',
            current_timestamp()
        );
    else
        insert into tmp_pipeline_audit
        values ('warning_scan', 'success', 0, 'no invalid transaction-like records detected', current_timestamp());
    end if;

    v_finished_at := current_timestamp();

    select object_construct(
        'status', 'SUCCESS',
        'message', 'Monolithic demo pipeline completed',
        'run_date', :v_run_date,
        'started_at', :v_started_at,
        'finished_at', :v_finished_at,
        'transactions_processed', :v_rows_transactions,
        'store_sales_rows', :v_rows_store_sales,
        'category_sales_rows', :v_rows_category_sales,
        'deleted_store_sales_rows', :v_deleted_store_sales,
        'deleted_category_sales_rows', :v_deleted_category_sales,
        'audit', array_agg(
            object_construct(
                'step_name', step_name,
                'step_status', step_status,
                'row_count', row_count,
                'message', message,
                'logged_at', logged_at
            )
        )
    ) into :v_result
    from tmp_pipeline_audit;

    return v_result;

exception
    when other then
        v_finished_at := current_timestamp();
        return object_construct(
            'status', 'FAILED',
            'message', sqlerrm,
            'run_date', :v_run_date,
            'started_at', :v_started_at,
            'finished_at', :v_finished_at,
            'transactions_processed', :v_rows_transactions,
            'store_sales_rows', :v_rows_store_sales,
            'category_sales_rows', :v_rows_category_sales
        );
end;
$$;
    {% endset %}

    {% do run_query(sql) %}
    {{ return('demo_monolithic_pos_pipeline deployed') }}
{% endmacro %}

{% macro call_demo_monolithic_pos_pipeline(run_date=None) %}
    {% if run_date is none %}
        {% set call_sql %}
            call demo_monolithic_pos_pipeline(current_date())
        {% endset %}
    {% else %}
        {% set call_sql %}
            call demo_monolithic_pos_pipeline(to_date('{{ run_date }}'))
        {% endset %}
    {% endif %}

    {% set results = run_query(call_sql) %}

    {% if execute and results is not none and (results.rows | length) > 0 %}
        {% set payload = results.rows[0][0] %}
        {{ log('demo_monolithic_pos_pipeline result: ' ~ payload, info=True) }}
        {{ return(payload) }}
    {% else %}
        {{ log('demo_monolithic_pos_pipeline result: no_result', info=True) }}
        {{ return('no_result') }}
    {% endif %}
{% endmacro %}

-- -----------------------------------------------------------------------------
-- 以下は補足メモ
-- -----------------------------------------------------------------------------
-- この macro は demo_monolithic_pos_pipeline の deploy / call を
-- dbt run-operation で実行できるようにするための補助コード。
--
-- 上側では実行ロジックだけが見えるようにし、背景説明は末尾に集約する。
--
-- 想定ユースケース:
--   - deploy_demo_monolithic_pos_pipeline: procedure の作成 / 更新
--   - call_demo_monolithic_pos_pipeline: 作成済み procedure の呼び出し
--
-- 実行例:
--
--   1. stored procedure を作成 / 更新する
--      dbt run-operation deploy_demo_monolithic_pos_pipeline
--
--   2. 当日分で実行する
--      dbt run-operation call_demo_monolithic_pos_pipeline --log-level info
--
--   3. 日付を指定して実行する
--      dbt run-operation call_demo_monolithic_pos_pipeline --args '{"run_date": "2025-01-01"}' --log-level info
--
--   4. Snowflake 上の procedure を直接呼び出す
--      call ANALYTICS.DBT_TOSHI_DEV_POS_DATA.DEMO_MONOLITHIC_POS_PIPELINE('2025-01-01'::date);

