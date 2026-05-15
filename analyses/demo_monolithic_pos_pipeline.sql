-- =============================================================================
-- demo_monolithic_pos_pipeline
-- =============================================================================
-- 概要:
--   このプロシージャは、ありがちなモノリシックな warehouse ETL を
--   わざと再現したデモ用の実装です。
--   ソースのクレンジング、業務ロジック、集計、DDL、DML、ログ出力、
--   warning 判定、返却 payload の組み立てまでを、1つのデプロイ単位に
--   全部押し込んでいます。
--
-- これを置いている理由:
--   - デモ / 研修用
--   - アンチパターンの説明用
--   - modular な dbt DAG と比較するための題材
--
-- 注意:
--   この SQL は意図的に過剰実装です。
--   コメントの中には有用なものもあれば、古いもの、矛盾しているもの、
--   そもそもそんな判断をすべきではなかったことを正当化しているものも
--   含まれます。そういう「現場で育った感じ」を出すのが目的です。
--
-- 既知の問題 / ありがちな事情:
--   - 出力先 schema は ANALYTICS にベタ書きです。
--   - merge ではなく delete + insert で日次再作成しています。
--     以前たまたま速かった、という理由のまま残っている想定です。
--   - ちゃんとした logging 基盤ではなく temp table に監査っぽい情報を
--     溜めています。
--   - 品質チェックは計算しているものの、ロード自体は止めません。
--   - 複数の下流集計が同じ enriched temp table にぶら下がっています。
--
-- 変更履歴（正確性は保証しない）:
--   2024-01: 旧 warehouse script から最初の移植
--   2024-03: 税額計算を変更したが、古いコメントが別箇所に残っている可能性あり
--   2024-05: ダッシュボード事故を受けて重複判定ロジックを追加
--   2024-07: スケジューラから 1 回呼ぶだけで済むよう SQL procedure 化
--   2024-09: warning 判定を追加したが、warning でも止まらないまま
-- =============================================================================
create or replace procedure demo_monolithic_pos_pipeline(run_date date)
returns variant
language sql
execute as caller
as
$$
declare
    -- もともとは scheduler 側で日付を補完していたが、adhoc 実行でも
    -- 「だいたい同じ動き」にしたいという理由で procedure 側に寄せた想定。
    v_run_date date default coalesce(run_date, current_date());

    -- 雑な運用監視向け timestamp
    v_started_at timestamp_ntz default current_timestamp();
    v_finished_at timestamp_ntz;

    -- 件数カウンタ。最終 JSON に入れたいという要望が増えたので個別保持。
    v_rows_transactions number default 0;
    v_rows_store_sales number default 0;
    v_rows_category_sales number default 0;

    -- 日次再読込っぽく見せるための delete 件数
    v_deleted_store_sales number default 0;
    v_deleted_category_sales number default 0;

    -- DDL を dynamic SQL で実行するための変数。
    -- 今回の固定オブジェクト名では不要だが、こういうのは残りがち。
    v_sql string;

    -- 呼び出し元に返す envelope
    v_status string default 'STARTED';
    v_message string default 'Pipeline initialized';
    v_result variant;
begin
    -- -------------------------------------------------------------------------
    -- 初期化 / セッションローカル監査ログ
    -- -------------------------------------------------------------------------
    -- 開発時にとりあえず便利だったので temp table を使い、そのまま
    -- 設計になってしまった想定。セッションが終わるとログも消える。
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
    -- step 1: procedure 内で staging 相当の enrich を実施
    -- -------------------------------------------------------------------------
    -- この巨大な statement の中で実質的に staging 層を再実装している。
    --   * transaction の抽出
    --   * product 情報の付与
    --   * store 情報の付与
    --   * 業務ルール判定
    --   * 重複検知
    -- dbt なら別ノードに分けるべき処理を、運用上 1 本の方が楽という
    -- 名目でまとめている想定。
    create or replace temporary table tmp_transactions_enriched as
    with transactions as (
        -- run_date のみ対象にして単日 rerun をしやすくしている。
        -- ただし本当の incremental ではなく、気持ちだけ日次再処理。
        select
            transaction_id,
            store_id,
            product_id,
            quantity,
            unit_price,
            transaction_at
        from {{ source('pos', 'raw_transactions') }}
        where cast(transaction_at as date) = v_run_date
    ),
    products as (
        -- 毎回 product 全件を読む。
        -- 遅くなったら temp table をもう 1 つ増やして延命しそうな設計。
        select
            product_id,
            product_name,
            category,
            tax_rate,
            is_active
        from {{ source('pos', 'raw_products') }}
    ),
    stores as (
        -- こちらも毎回フルスキャン想定。
        select
            store_id,
            store_name,
            prefecture,
            opened_at,
            closed_at
        from {{ source('pos', 'raw_stores') }}
    ),
    joined_once as (
        -- raw fact に dimension 情報と品質っぽい判定を付与する区間。
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
            s.closed_at,

            -- 税抜き金額
            t.quantity * t.unit_price as gross_amount,

            -- 税額。tax_rate が null でも落とさず 0 扱い。
            t.quantity * t.unit_price * coalesce(p.tax_rate, 0) as tax_amount,

            -- 別経路では total_amount と呼んでいた時期がある想定。
            -- 会話上は古い名前が残っていてもおかしくない。
            t.quantity * t.unit_price * (1 + coalesce(p.tax_rate, 0)) as net_amount,

            -- 品質 / 業務ルール判定
            case
                when p.is_active = false then 'inactive_product'
                when s.closed_at is not null and cast(t.transaction_at as date) > cast(s.closed_at as date) then 'closed_store_transaction'
                when t.quantity < 0 then 'negative_quantity'
                when t.unit_price < 0 then 'negative_price'
                else 'valid'
            end as record_status
        from transactions t
        left join products p
            on t.product_id = p.product_id
        left join stores s
            on t.store_id = s.store_id
    ),
    duplicated_flags as (
        -- dashboard 側で duplicate transaction_id が見つかった後に
        -- 後付けで追加された想定の重複判定。
        -- upstream で uniqueness を担保する代わりに、ここで検知だけして
        -- downstream に判断を委ねる形になっている。
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
        transaction_at,
        transaction_date,
        product_name,
        category,
        tax_rate,
        is_active,
        store_name,
        prefecture,
        opened_at,
        closed_at,
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
    from duplicated_flags;

    -- enrich 後の件数を telemetry 用に保持。
    -- 監視ダッシュボードに出したい、という要望に応えた体裁。
    select count(*) into :v_rows_transactions
    from tmp_transactions_enriched;

    insert into tmp_pipeline_audit
    values ('tmp_transactions_enriched', 'success', v_rows_transactions, 'enriched transaction rows created', current_timestamp());

    -- -------------------------------------------------------------------------
    -- step 2a: 日次店舗売上へ集計
    -- -------------------------------------------------------------------------
    -- すでに enriched temp table があるので、以降の集計は全部ここを共通入力
    -- にしている。抽象として正しいかはさておき、ありがちな流れ。
    create or replace temporary table tmp_daily_store_sales as
    select
        store_id,
        store_name,
        prefecture,
        transaction_date as sales_date,
        sum(net_amount) as total_sales,
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
    values ('tmp_daily_store_sales', 'success', v_rows_store_sales, 'daily store sales aggregated', current_timestamp());

    -- -------------------------------------------------------------------------
    -- step 2b: 日次カテゴリ売上へ集計
    -- -------------------------------------------------------------------------
    -- 同じ入力だが grain だけ違う。dbt なら別 mart に切る想定。
    create or replace temporary table tmp_daily_category_sales as
    select
        category,
        transaction_date as sales_date,
        sum(net_amount) as total_sales,
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
    values ('tmp_daily_category_sales', 'success', v_rows_category_sales, 'daily category sales aggregated', current_timestamp());

    -- -------------------------------------------------------------------------
    -- step 3: 出力先オブジェクトの存在保証
    -- -------------------------------------------------------------------------
    -- 固定オブジェクト名なのに dynamic SQL で DDL を組み立てている。
    -- もともと別 procedure からのコピペだった、みたいな雰囲気を想定。
    -- schema が ANALYTICS 固定なのも含めて、いかにも感を出している。
    v_sql := 'create table if not exists analytics.daily_store_sales_monolithic_demo (
        store_id number,
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
    execute immediate :v_sql;

    v_sql := 'create table if not exists analytics.daily_category_sales_monolithic_demo (
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
    execute immediate :v_sql;

    -- -------------------------------------------------------------------------
    -- step 4a: 店舗売上の日次再ロード
    -- -------------------------------------------------------------------------
    -- merge ではなく delete + insert。
    -- 最初は妥当だったが、要件が変わってもそのまま残り続けた想定。
    delete from analytics.daily_store_sales_monolithic_demo
    where sales_date = v_run_date;

    get diagnostics v_deleted_store_sales = row_count;

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
    values ('load_daily_store_sales', 'success', v_rows_store_sales, 'store sales reloaded for run date', current_timestamp());

    -- -------------------------------------------------------------------------
    -- step 4b: カテゴリ売上の日次再ロード
    -- -------------------------------------------------------------------------
    delete from analytics.daily_category_sales_monolithic_demo
    where sales_date = v_run_date;

    get diagnostics v_deleted_category_sales = row_count;

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
    values ('load_daily_category_sales', 'success', v_rows_category_sales, 'category sales reloaded for run date', current_timestamp());

    -- -------------------------------------------------------------------------
    -- step 5: warning は出すが処理は止めない
    -- -------------------------------------------------------------------------
    -- 怪しいレコードは検知してログに積むが、朝までに dashboard を更新したい
    -- ので処理は継続、というよくある運用を再現している。
    if exists (
        select 1
        from tmp_transactions_enriched
        where record_status <> 'valid'
    ) then
        insert into tmp_pipeline_audit
        select
            'warning_scan',
            'warning',
            count(*),
            'invalid transaction-like records detected but not rejected',
            current_timestamp()
        from tmp_transactions_enriched
        where record_status <> 'valid';
    else
        insert into tmp_pipeline_audit
        values ('warning_scan', 'success', 0, 'no invalid transaction-like records detected', current_timestamp());
    end if;

    -- -------------------------------------------------------------------------
    -- step 6: 戻り値 payload の組み立て
    -- -------------------------------------------------------------------------
    v_finished_at := current_timestamp();
    v_status := 'SUCCESS';
    v_message := 'Monolithic demo pipeline completed';

    -- temp の audit ログをまとめて 1 つの VARIANT に押し込む。
    -- 便利ではあるが、型安全ではない。
    select object_construct(
        'status', v_status,
        'message', v_message,
        'run_date', v_run_date,
        'started_at', v_started_at,
        'finished_at', v_finished_at,
        'transactions_processed', v_rows_transactions,
        'store_sales_rows', v_rows_store_sales,
        'category_sales_rows', v_rows_category_sales,
        'deleted_store_sales_rows', v_deleted_store_sales,
        'deleted_category_sales_rows', v_deleted_category_sales,
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
        -- 失敗時の返却内容はかなり最低限。
        -- 実運用だとこの辺で structured logging が欲しくなりがち。
        v_finished_at := current_timestamp();
        v_status := 'FAILED';
        v_message := sqlerrm;

        return object_construct(
            'status', v_status,
            'message', v_message,
            'run_date', v_run_date,
            'started_at', v_started_at,
            'finished_at', v_finished_at,
            'transactions_processed', v_rows_transactions,
            'store_sales_rows', v_rows_store_sales,
            'category_sales_rows', v_rows_category_sales
        );
end;
$$;

