-- SOURCE-SWITCH-01：将现役typed事实来源身份切换为BaoStock/TDX通用捕获数字键。
-- 前置：本批RESET_ALL_BUSINESS_FACTS初始化中的来源阶段已完成；来源事实可保留，派生事实必须为空。

DO $$
DECLARE
    table_name text;
    row_count bigint;
    item record;
BEGIN
    IF current_database() <> 'stock_a' OR current_schema() <> 'stock'
       OR session_user <> 'stock_a' OR current_user <> 'stock_a' THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0055_TARGET_IDENTITY_REJECTED';
    END IF;
    IF (SELECT count(*) FROM stock.installation_identity
        WHERE singleton IS TRUE AND application_id='STOCK_A'
          AND migration_lineage='STOCK_A_INITIAL_R1') <> 1 THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0055_INSTALLATION_IDENTITY_REJECTED';
    END IF;
    IF (SELECT count(*) FROM stock.migration_ledger
        WHERE migration_id BETWEEN 1 AND 54
          AND lineage='STOCK_A_INITIAL_R1') <> 54
       OR (SELECT count(DISTINCT migration_id) FROM stock.migration_ledger
           WHERE migration_id BETWEEN 1 AND 54) <> 54 THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0055_MIGRATION_PREFIX_REJECTED';
    END IF;
    IF (SELECT count(*) FROM stock.data_jobs) <> 1
       OR (SELECT count(*) FROM stock.data_jobs
           WHERE status='COMPLETED'
             AND mode='INITIALIZE'
             AND initialization_mode='RESET_ALL_BUSINESS_FACTS'
             AND daily_mode='CUSTOM'
             AND (
                 selected_modules @> ARRAY[
                     'BAOSTOCK_PRICES','TDX_SECTOR_MEMBERS'
                 ]::text[]
                 OR selected_modules @> ARRAY[
                     'BAOSTOCK','TDX_SECTOR_MEMBERS'
                 ]::text[]
             )
             AND cardinality(selected_modules)=2) <> 1
       OR EXISTS (SELECT 1 FROM stock.data_jobs WHERE source_package_id IS NOT NULL)
       OR EXISTS (SELECT 1 FROM stock.data_job_progress WHERE source_package_id IS NOT NULL)
       OR EXISTS (SELECT 1 FROM stock.data_job_progress WHERE baostock_capture_id IS NOT NULL)
       OR EXISTS (
           SELECT 1 FROM stock.data_jobs
           WHERE selected_modules @> ARRAY['ADJUSTED_PRICE']::text[]
       ) THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0055_SOURCE_ONLY_RESET_JOB_NOT_COMPLETED';
    END IF;

    -- Preserve source-layer Raw, captures, requests, names and sector snapshots only.
    FOR table_name IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='stock'
          AND c.relkind IN ('r','p')
          AND c.relname NOT IN (
              'installation_identity','migration_ledger','namespaces','role_bindings',
              'data_jobs','data_job_progress','source_captures',
              'source_capture_requests','source_baostock_daily',
              'source_baostock_unit_contracts','source_raw_daily',
              'source_symbol_display_names','tdx_sector_source_snapshots',
              'tdx_sector_catalog','tdx_sector_members'
          )
        ORDER BY c.relname
    LOOP
        EXECUTE format('SELECT count(*) FROM stock.%I',table_name)
        INTO row_count;
        IF row_count <> 0 THEN
            RAISE EXCEPTION 'SOURCE_SWITCH_0055_DERIVED_FACTS_NOT_EMPTY: stock.% has % rows',
                table_name,row_count;
        END IF;
    END LOOP;
    IF (SELECT count(*) FROM stock.source_baostock_unit_contracts) <> 1
       OR (SELECT count(*) FROM stock.source_baostock_unit_contracts
           WHERE unit_contract_version='BAOSTOCK_API_DOCUMENTED_UNITS_NOT_INDEPENDENTLY_BOUND_R1'
             AND close_input_unit='CNY_PER_SHARE_DOCUMENTED'
             AND close_to_cny=1
             AND amount_input_unit='CNY_DOCUMENTED'
             AND amount_to_cny=1
             AND volume_input_unit='SHARE_DOCUMENTED'
             AND volume_to_shares=1
             AND turn_input_unit='PERCENT') <> 1 THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0055_UNIT_CONTRACT_NOT_EXACT';
    END IF;
    IF (SELECT count(*) FROM stock.source_packages) <> 0
       OR (SELECT count(*) FROM stock.source_baostock_daily) <> 0
       OR (SELECT count(*) FROM stock.source_corporate_action_events) <> 0
       OR (SELECT count(*) FROM stock.source_adjusted_daily_prices) <> 0 THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0055_RETIRED_PACKAGE_OR_AFFINE_FACTS_REMAIN';
    END IF;
    FOR item IN
        SELECT source_columns.table_name,source_columns.column_name
        FROM information_schema.columns AS source_columns
        WHERE source_columns.table_schema='stock'
          AND source_columns.column_name IN (
              'source_package_id','previous_source_package_id',
              'target_source_package_id','t0_source_package_id',
              'endpoint_source_package_id','package_sha256',
              'previous_package_sha256','target_package_sha256','t0_package_sha256'
          )
        ORDER BY source_columns.table_name,source_columns.ordinal_position
    LOOP
        EXECUTE format(
            'SELECT count(*) FROM stock.%I WHERE %I IS NOT NULL',
            item.table_name,
            item.column_name
        ) INTO row_count;
        IF row_count <> 0 THEN
            RAISE EXCEPTION 'SOURCE_SWITCH_0055_PACKAGE_IDENTITY_NOT_NULL: stock.%',
                item.table_name;
        END IF;
    END LOOP;
END $$;

-- migration 54将source-only任务记录为旧BaoStock模块别名；先移除旧枚举约束，
-- 再在本迁移事务中把唯一任务行规范化为现役BAOSTOCK_PRICES模块名。
ALTER TABLE stock.data_jobs DROP CONSTRAINT data_jobs_selected_modules_check;
UPDATE stock.data_jobs
SET selected_modules=ARRAY['BAOSTOCK_PRICES','TDX_SECTOR_MEMBERS']::text[]
WHERE mode='INITIALIZE'
  AND initialization_mode='RESET_ALL_BUSINESS_FACTS'
  AND daily_mode='CUSTOM'
  AND selected_modules @> ARRAY['BAOSTOCK','TDX_SECTOR_MEMBERS']::text[]
  AND cardinality(selected_modules)=2;
DO $$
BEGIN
    IF (SELECT count(*) FROM stock.data_jobs
        WHERE status='COMPLETED'
          AND mode='INITIALIZE'
          AND initialization_mode='RESET_ALL_BUSINESS_FACTS'
          AND daily_mode='CUSTOM'
          AND selected_modules @> ARRAY[
              'BAOSTOCK_PRICES','TDX_SECTOR_MEMBERS'
          ]::text[]
          AND cardinality(selected_modules)=2) <> 1 THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0055_SOURCE_ONLY_MODULE_NORMALIZATION_FAILED';
    END IF;
END $$;

-- PG53目录中旧来源包列为NOT NULL且非主键的事实，转换后继续强制对应捕获键。
CREATE TEMP TABLE source_switch_0055_required_capture_columns (
    table_name text NOT NULL,
    column_name text NOT NULL,
    PRIMARY KEY (table_name,column_name)
) ON COMMIT DROP;
INSERT INTO source_switch_0055_required_capture_columns(table_name,column_name) VALUES
    ('source_d0_formal_admission_day_facts','source_capture_id'),
    ('source_d0_launch_day_facts','source_capture_id'),
    ('source_d0_scenario_day_facts','source_capture_id'),
    ('source_d1_structure_day_facts','source_capture_id'),
    ('source_fep_baseline_facts','source_capture_id'),
    ('source_forward_path_facts','source_capture_id'),
    ('source_market_breadth_days','source_capture_id'),
    ('source_market_breadth_symbols','source_capture_id'),
    ('source_market_participation_days','source_capture_id'),
    ('source_pool_seed_day_facts','source_capture_id'),
    ('source_raw_daily','source_capture_id'),
    ('source_research_events','source_capture_id'),
    ('source_research_observation_segment_days','source_capture_id'),
    ('source_rps_pulse_t0_change_facts','source_capture_id'),
    ('source_rule_evaluations','source_capture_id'),
    ('source_sector_cross_period_pulse_facts','source_capture_id'),
    ('source_sector_pulse_cohort_group_facts','t0_source_capture_id'),
    ('source_sector_pulse_followup_facts','target_source_capture_id'),
    ('source_symbol_price_profiles','source_capture_id');

-- 市场聚合新路径直接绑定真实capture；删除无法由capture证明的旧TDX包字段。
ALTER TABLE stock.source_market_participation_days
    DROP CONSTRAINT source_market_participation_days_source_dataset_id_check;
ALTER TABLE stock.source_market_breadth_days
    DROP COLUMN IF EXISTS source_dataset_id,
    DROP COLUMN IF EXISTS source_byte_count,
    DROP COLUMN IF EXISTS parser_contract;
ALTER TABLE stock.source_market_participation_days
    DROP COLUMN IF EXISTS source_dataset_id;

-- 删除旧列前先登记并映射包含来源包身份的主键和唯一约束。
CREATE TEMP TABLE source_switch_0055_key_constraints ON COMMIT DROP AS
SELECT
    relation.relname::text AS table_name,
    constraint_row.conname::text AS constraint_name,
    constraint_row.contype::text AS constraint_type,
    ARRAY(
        SELECT mapped_column
        FROM (
            SELECT
                CASE attribute.attname
                    WHEN 'source_package_id' THEN 'source_capture_id'
                    WHEN 'previous_source_package_id' THEN 'previous_source_capture_id'
                    WHEN 'target_source_package_id' THEN 'target_source_capture_id'
                    WHEN 't0_source_package_id' THEN 't0_source_capture_id'
                    WHEN 'endpoint_source_package_id' THEN 'endpoint_source_capture_id'
                    WHEN 'package_sha256' THEN 'source_capture_id'
                    WHEN 'previous_package_sha256' THEN 'previous_source_capture_id'
                    WHEN 'target_package_sha256' THEN 'target_source_capture_id'
                    WHEN 't0_package_sha256' THEN 't0_source_capture_id'
                    ELSE attribute.attname
                END AS mapped_column,
                min(key_column.ordinality) AS first_ordinality
            FROM unnest(constraint_row.conkey) WITH ORDINALITY key_column(attnum,ordinality)
            JOIN pg_attribute attribute
              ON attribute.attrelid=constraint_row.conrelid
             AND attribute.attnum=key_column.attnum
            GROUP BY mapped_column
        ) mapped
        ORDER BY first_ordinality
    ) AS mapped_columns
FROM pg_constraint constraint_row
JOIN pg_class relation ON relation.oid=constraint_row.conrelid
JOIN pg_namespace relation_schema ON relation_schema.oid=relation.relnamespace
WHERE relation_schema.nspname='stock'
  AND relation.relname NOT IN (
      'source_packages','source_captures','source_baostock_daily',
      'source_corporate_action_events','source_adjusted_daily_prices'
  )
  AND constraint_row.contype IN ('p','u')
  AND EXISTS (
      SELECT 1
      FROM unnest(constraint_row.conkey) key_column(attnum)
      JOIN pg_attribute attribute
        ON attribute.attrelid=constraint_row.conrelid
       AND attribute.attnum=key_column.attnum
      WHERE attribute.attname IN (
          'source_package_id','previous_source_package_id','target_source_package_id',
          't0_source_package_id','endpoint_source_package_id','package_sha256',
          'previous_package_sha256','target_package_sha256','t0_package_sha256'
      )
  );

CREATE TEMP TABLE source_switch_0055_secondary_indexes ON COMMIT DROP AS
SELECT
    table_relation.relname::text AS table_name,
    index_relation.relname::text AS index_name,
    index_definition.indisunique AS is_unique,
    access_method.amname::text AS access_method,
    ARRAY(
        SELECT mapped_column
        FROM (
            SELECT
                CASE attribute.attname
                    WHEN 'source_package_id' THEN 'source_capture_id'
                    WHEN 'previous_source_package_id' THEN 'previous_source_capture_id'
                    WHEN 'target_source_package_id' THEN 'target_source_capture_id'
                    WHEN 't0_source_package_id' THEN 't0_source_capture_id'
                    WHEN 'endpoint_source_package_id' THEN 'endpoint_source_capture_id'
                    WHEN 'package_sha256' THEN 'source_capture_id'
                    WHEN 'previous_package_sha256' THEN 'previous_source_capture_id'
                    WHEN 'target_package_sha256' THEN 'target_source_capture_id'
                    WHEN 't0_package_sha256' THEN 't0_source_capture_id'
                    ELSE attribute.attname
                END AS mapped_column,
                min(key_column.ordinality) AS first_ordinality
            FROM unnest(index_definition.indkey) WITH ORDINALITY key_column(attnum,ordinality)
            JOIN pg_attribute attribute
              ON attribute.attrelid=table_relation.oid
             AND attribute.attnum=key_column.attnum
            WHERE key_column.ordinality<=index_definition.indnkeyatts
            GROUP BY mapped_column
        ) mapped
        ORDER BY first_ordinality
    ) AS mapped_columns,
    ARRAY(
        SELECT
            (CASE WHEN (index_definition.indoption[mapped.first_ordinality-1] & 1)=1
                  THEN 'DESC' ELSE 'ASC' END)
            || CASE WHEN (index_definition.indoption[mapped.first_ordinality-1] & 2)=2
                    THEN ' NULLS FIRST' ELSE ' NULLS LAST' END
        FROM (
            SELECT
                CASE attribute.attname
                    WHEN 'source_package_id' THEN 'source_capture_id'
                    WHEN 'previous_source_package_id' THEN 'previous_source_capture_id'
                    WHEN 'target_source_package_id' THEN 'target_source_capture_id'
                    WHEN 't0_source_package_id' THEN 't0_source_capture_id'
                    WHEN 'endpoint_source_package_id' THEN 'endpoint_source_capture_id'
                    WHEN 'package_sha256' THEN 'source_capture_id'
                    WHEN 'previous_package_sha256' THEN 'previous_source_capture_id'
                    WHEN 'target_package_sha256' THEN 'target_source_capture_id'
                    WHEN 't0_package_sha256' THEN 't0_source_capture_id'
                    ELSE attribute.attname
                END AS mapped_column,
                min(key_column.ordinality) AS first_ordinality
            FROM unnest(index_definition.indkey) WITH ORDINALITY key_column(attnum,ordinality)
            JOIN pg_attribute attribute
              ON attribute.attrelid=table_relation.oid
             AND attribute.attnum=key_column.attnum
            WHERE key_column.ordinality<=index_definition.indnkeyatts
            GROUP BY mapped_column
        ) mapped
        ORDER BY mapped.first_ordinality
    ) AS mapped_key_options,
    ARRAY(
        SELECT mapped_column
        FROM (
            SELECT
                CASE attribute.attname
                    WHEN 'source_package_id' THEN 'source_capture_id'
                    WHEN 'previous_source_package_id' THEN 'previous_source_capture_id'
                    WHEN 'target_source_package_id' THEN 'target_source_capture_id'
                    WHEN 't0_source_package_id' THEN 't0_source_capture_id'
                    WHEN 'endpoint_source_package_id' THEN 'endpoint_source_capture_id'
                    WHEN 'package_sha256' THEN 'source_capture_id'
                    WHEN 'previous_package_sha256' THEN 'previous_source_capture_id'
                    WHEN 'target_package_sha256' THEN 'target_source_capture_id'
                    WHEN 't0_package_sha256' THEN 't0_source_capture_id'
                    ELSE attribute.attname
                END AS mapped_column,
                min(key_column.ordinality) AS first_ordinality
            FROM unnest(index_definition.indkey) WITH ORDINALITY key_column(attnum,ordinality)
            JOIN pg_attribute attribute
              ON attribute.attrelid=table_relation.oid
             AND attribute.attnum=key_column.attnum
            WHERE key_column.ordinality>index_definition.indnkeyatts
            GROUP BY mapped_column
        ) mapped
        ORDER BY first_ordinality
    ) AS mapped_include_columns,
    pg_get_expr(index_definition.indpred,index_definition.indrelid) AS predicate,
    pg_get_expr(index_definition.indexprs,index_definition.indrelid) AS expressions,
    constraint_link.conname::text AS backing_constraint
FROM pg_index index_definition
JOIN pg_class table_relation ON table_relation.oid=index_definition.indrelid
JOIN pg_namespace table_schema ON table_schema.oid=table_relation.relnamespace
JOIN pg_class index_relation ON index_relation.oid=index_definition.indexrelid
JOIN pg_am access_method ON access_method.oid=index_relation.relam
LEFT JOIN pg_constraint constraint_link
  ON constraint_link.conindid=index_relation.oid
 AND constraint_link.contype IN ('p','u')
WHERE table_schema.nspname='stock'
  AND table_relation.relname NOT IN (
      'source_packages','source_captures','source_baostock_daily',
      'source_corporate_action_events','source_adjusted_daily_prices'
  )
  AND EXISTS (
      SELECT 1
      FROM unnest(index_definition.indkey) key_column(attnum)
      JOIN pg_attribute attribute
        ON attribute.attrelid=table_relation.oid
       AND attribute.attnum=key_column.attnum
      WHERE attribute.attname IN (
          'source_package_id','previous_source_package_id','target_source_package_id',
          't0_source_package_id','endpoint_source_package_id','package_sha256',
          'previous_package_sha256','target_package_sha256','t0_package_sha256'
      )
  );

CREATE TEMP TABLE source_switch_0055_foreign_keys ON COMMIT DROP AS
SELECT
    child.relname::text AS child_table,
    constraint_row.conname::text AS constraint_name,
    parent_namespace.nspname::text AS parent_schema,
    parent.relname::text AS parent_table,
    constraint_row.confdeltype::text AS delete_action,
    constraint_row.confupdtype::text AS update_action,
    constraint_row.confmatchtype::text AS match_type,
    constraint_row.condeferrable AS is_deferrable,
    constraint_row.condeferred AS is_deferred,
    ARRAY(
        SELECT mapped_column
        FROM (
            SELECT
                CASE attribute.attname
                    WHEN 'source_package_id' THEN 'source_capture_id'
                    WHEN 'previous_source_package_id' THEN 'previous_source_capture_id'
                    WHEN 'target_source_package_id' THEN 'target_source_capture_id'
                    WHEN 't0_source_package_id' THEN 't0_source_capture_id'
                    WHEN 'endpoint_source_package_id' THEN 'endpoint_source_capture_id'
                    WHEN 'package_sha256' THEN 'source_capture_id'
                    WHEN 'previous_package_sha256' THEN 'previous_source_capture_id'
                    WHEN 'target_package_sha256' THEN 'target_source_capture_id'
                    WHEN 't0_package_sha256' THEN 't0_source_capture_id'
                    ELSE attribute.attname
                END AS mapped_column,
                min(key_column.ordinality) AS first_ordinality
            FROM unnest(constraint_row.conkey) WITH ORDINALITY key_column(attnum,ordinality)
            JOIN pg_attribute attribute
              ON attribute.attrelid=constraint_row.conrelid
             AND attribute.attnum=key_column.attnum
            GROUP BY mapped_column
        ) mapped
        ORDER BY first_ordinality
    ) AS mapped_child_columns,
    ARRAY(
        SELECT mapped_column
        FROM (
            SELECT
                CASE attribute.attname
                    WHEN 'source_package_id' THEN 'source_capture_id'
                    WHEN 'previous_source_package_id' THEN 'previous_source_capture_id'
                    WHEN 'target_source_package_id' THEN 'target_source_capture_id'
                    WHEN 't0_source_package_id' THEN 't0_source_capture_id'
                    WHEN 'endpoint_source_package_id' THEN 'endpoint_source_capture_id'
                    WHEN 'package_sha256' THEN 'source_capture_id'
                    WHEN 'previous_package_sha256' THEN 'previous_source_capture_id'
                    WHEN 'target_package_sha256' THEN 'target_source_capture_id'
                    WHEN 't0_package_sha256' THEN 't0_source_capture_id'
                    ELSE attribute.attname
                END AS mapped_column,
                min(key_column.ordinality) AS first_ordinality
            FROM unnest(constraint_row.confkey) WITH ORDINALITY key_column(attnum,ordinality)
            JOIN pg_attribute attribute
              ON attribute.attrelid=constraint_row.confrelid
             AND attribute.attnum=key_column.attnum
            GROUP BY mapped_column
        ) mapped
        ORDER BY first_ordinality
    ) AS mapped_parent_columns
FROM pg_constraint constraint_row
JOIN pg_class child ON child.oid=constraint_row.conrelid
JOIN pg_namespace child_namespace ON child_namespace.oid=child.relnamespace
JOIN pg_class parent ON parent.oid=constraint_row.confrelid
JOIN pg_namespace parent_namespace ON parent_namespace.oid=parent.relnamespace
WHERE constraint_row.contype='f'
  AND child_namespace.nspname='stock'
  AND (
      parent_namespace.nspname='stock'
      AND parent.relname='source_packages'
      OR EXISTS (
          SELECT 1
          FROM unnest(constraint_row.conkey) key_column(attnum)
          JOIN pg_attribute attribute
            ON attribute.attrelid=constraint_row.conrelid
           AND attribute.attnum=key_column.attnum
          WHERE attribute.attname IN (
              'source_package_id','previous_source_package_id',
              'target_source_package_id','t0_source_package_id',
              'endpoint_source_package_id','package_sha256',
              'previous_package_sha256','target_package_sha256','t0_package_sha256'
          )
      )
      OR EXISTS (
          SELECT 1
          FROM unnest(constraint_row.confkey) key_column(attnum)
          JOIN pg_attribute attribute
            ON attribute.attrelid=constraint_row.confrelid
           AND attribute.attnum=key_column.attnum
          WHERE attribute.attname IN (
              'source_package_id','previous_source_package_id',
              'target_source_package_id','t0_source_package_id',
              'endpoint_source_package_id','package_sha256',
              'previous_package_sha256','target_package_sha256','t0_package_sha256'
          )
      )
  );

-- 宽度事实不再依赖旧日历摘要或包内容；新键精确匹配现役捕获写入器。
UPDATE source_switch_0055_key_constraints
SET mapped_columns=array_remove(mapped_columns,'calendar_sha256')
WHERE table_name IN ('source_market_breadth_days','source_market_breadth_symbols');
UPDATE source_switch_0055_secondary_indexes
SET mapped_columns=array_remove(mapped_columns,'calendar_sha256'),
    mapped_include_columns=array_remove(mapped_include_columns,'calendar_sha256')
WHERE table_name IN ('source_market_breadth_days','source_market_breadth_symbols');
UPDATE source_switch_0055_foreign_keys
SET mapped_child_columns=array_remove(mapped_child_columns,'calendar_sha256'),
    mapped_parent_columns=array_remove(mapped_parent_columns,'calendar_sha256')
WHERE child_table='source_market_breadth_symbols'
  AND parent_table='source_market_breadth_days';
-- 旧复合捕获/包/目标日外键对多会话paired capture不成立；保留同表单列capture FK，
-- 目标行会话和来源代码由source_raw_daily实际业务外键闭合。
DELETE FROM source_switch_0055_foreign_keys
WHERE parent_table='source_captures'
  AND cardinality(mapped_parent_columns)>1;
DELETE FROM source_switch_0055_foreign_keys
WHERE child_table='source_market_breadth_days'
  AND 'source_content_id'=ANY(mapped_child_columns);

ALTER TABLE stock.source_market_breadth_days
    DROP COLUMN IF EXISTS source_content_id,
    DROP COLUMN IF EXISTS capture_digest,
    DROP COLUMN IF EXISTS calendar_sha256;
ALTER TABLE stock.source_market_breadth_symbols
    DROP COLUMN IF EXISTS calendar_sha256;

ALTER TABLE stock.source_market_breadth_days
    ADD COLUMN price_coordinate text NOT NULL
        CHECK (price_coordinate='FORWARD_ADJUSTED');
COMMENT ON COLUMN stock.source_market_breadth_days.price_coordinate IS
    '市场宽度收益状态使用的BaoStock价格坐标；现役合同固定为FORWARD_ADJUSTED前复权，用于跨会话可比。';
ALTER TABLE stock.source_market_breadth_symbols
    ADD COLUMN price_coordinate text NOT NULL
        CHECK (price_coordinate='FORWARD_ADJUSTED');
COMMENT ON COLUMN stock.source_market_breadth_symbols.price_coordinate IS
    '本证券收益差所使用的BaoStock价格坐标；现役合同固定为FORWARD_ADJUSTED前复权。';

ALTER TABLE stock.source_market_breadth_days
    DROP CONSTRAINT source_market_breadth_days_business_uq,
    ADD CONSTRAINT source_market_breadth_days_baostock_key_uq
        UNIQUE (target_session,contract_version,calendar_revision,
                source_capture_id,price_coordinate);
COMMENT ON INDEX stock.source_market_breadth_days_baostock_key_uq IS
    '按会话、算法、日历、BaoStock捕获和价格坐标唯一标识一条市场宽度日事实。';
ALTER TABLE stock.source_market_breadth_symbols
    DROP CONSTRAINT source_market_breadth_symbols_business_uq,
    ADD CONSTRAINT source_market_breadth_symbols_baostock_key_uq
        UNIQUE (target_session,contract_version,calendar_revision,
                source_capture_id,price_coordinate,source_symbol);
COMMENT ON INDEX stock.source_market_breadth_symbols_baostock_key_uq IS
    '按官方会话、算法、日历、BaoStock捕获/坐标和来源代码唯一标识逐股宽度事实。';
COMMENT ON TABLE stock.source_market_breadth_days IS
    '保存按主板、创业板和科创板BaoStock有效来源分母计算的全市场跨会话宽度日汇总；收益状态采用NON_PIT前复权QFQ并绑定确切来源捕获。';
COMMENT ON TABLE stock.source_market_breadth_symbols IS
    '保存市场宽度每只BaoStock来源证券的跨期涨幅状态和差分typed事实；分母为本次主板、创业板和科创板来源全集，价格坐标为NON_PIT前复权QFQ。';

ALTER TABLE stock.source_market_participation_days
    DROP CONSTRAINT source_market_participation_days_price_coordinate_check,
    DROP CONSTRAINT source_market_participation_days_amount_unit_status_check,
    ADD CONSTRAINT source_market_participation_qfq_coordinate_ck
        CHECK (price_coordinate='FORWARD_ADJUSTED'),
    ADD CONSTRAINT source_market_participation_cny_amount_ck
        CHECK (amount_unit_status='CNY_VERIFIED');
COMMENT ON COLUMN stock.source_market_participation_days.price_coordinate IS
    '参与画像的唯一Core/Profile价格坐标：BaoStock FORWARD_ADJUSTED前复权；成交金额事实仍使用未复权量额。';
COMMENT ON COLUMN stock.source_market_participation_days.amount_unit_status IS
    '画像原始成交金额单位合同状态；BaoStock amount规范化为人民币元时为CNY_VERIFIED。';

DO $$
DECLARE
    item record;
BEGIN
    IF EXISTS (
        SELECT 1 FROM source_switch_0055_secondary_indexes
        WHERE backing_constraint IS NULL
          AND (access_method<>'btree' OR expressions IS NOT NULL
               OR cardinality(mapped_columns)=0)
    ) THEN
        RAISE EXCEPTION 'SOURCE_SWITCH_0055_INDEX_REQUIRES_EXPLICIT_MAPPING';
    END IF;
    FOR item IN
        SELECT relation.relname AS table_name,c.conname
        FROM pg_constraint c
        JOIN pg_class relation ON relation.oid=c.conrelid
        JOIN pg_namespace relation_schema ON relation_schema.oid=relation.relnamespace
        WHERE relation_schema.nspname='stock'
          AND c.contype='f'
          AND (
              c.confrelid='stock.source_packages'::regclass
              OR pg_get_constraintdef(c.oid) ILIKE '%source_package_id%'
              OR pg_get_constraintdef(c.oid) ILIKE '%previous_source_package_id%'
              OR pg_get_constraintdef(c.oid) ILIKE '%target_source_package_id%'
              OR pg_get_constraintdef(c.oid) ILIKE '%t0_source_package_id%'
              OR pg_get_constraintdef(c.oid) ILIKE '%endpoint_source_package_id%'
              OR pg_get_constraintdef(c.oid) ILIKE '%package_sha256%'
              OR pg_get_constraintdef(c.oid) ILIKE '%previous_package_sha256%'
              OR pg_get_constraintdef(c.oid) ILIKE '%target_package_sha256%'
              OR pg_get_constraintdef(c.oid) ILIKE '%t0_package_sha256%'
              OR pg_get_constraintdef(c.oid) ILIKE '%source_entry_bytes%'
              OR pg_get_constraintdef(c.oid) ILIKE '%source_record_count%'
              OR pg_get_constraintdef(c.oid) ILIKE '%first_source_date%'
              OR pg_get_constraintdef(c.oid) ILIKE '%last_source_date%'
              OR pg_get_constraintdef(c.oid) ILIKE '%future_source_rows_excluded%'
              OR pg_get_constraintdef(c.oid) ILIKE '%source_content_id%'
              OR pg_get_constraintdef(c.oid) ILIKE '%source_entry_sha256%'
          )
        ORDER BY relation.relname,c.conname
    LOOP
        EXECUTE format('ALTER TABLE stock.%I DROP CONSTRAINT %I',item.table_name,item.conname);
    END LOOP;

-- 必须先删除外键，再删除其引用的来源包唯一键或主键；包括
-- source_captures_identity_package_session_uq。
    FOR item IN
        SELECT relation.relname AS table_name,c.conname
        FROM pg_constraint c
        JOIN pg_class relation ON relation.oid=c.conrelid
        JOIN pg_namespace relation_schema ON relation_schema.oid=relation.relnamespace
        WHERE relation_schema.nspname='stock'
          AND c.contype IN ('p','u','c')
          AND (
              pg_get_constraintdef(c.oid) ILIKE '%source_package_id%'
              OR pg_get_constraintdef(c.oid) ILIKE '%previous_source_package_id%'
              OR pg_get_constraintdef(c.oid) ILIKE '%target_source_package_id%'
              OR pg_get_constraintdef(c.oid) ILIKE '%t0_source_package_id%'
              OR pg_get_constraintdef(c.oid) ILIKE '%endpoint_source_package_id%'
              OR pg_get_constraintdef(c.oid) ILIKE '%package_sha256%'
              OR pg_get_constraintdef(c.oid) ILIKE '%previous_package_sha256%'
              OR pg_get_constraintdef(c.oid) ILIKE '%target_package_sha256%'
              OR pg_get_constraintdef(c.oid) ILIKE '%t0_package_sha256%'
              OR pg_get_constraintdef(c.oid) ILIKE '%source_entry_bytes%'
              OR pg_get_constraintdef(c.oid) ILIKE '%source_record_count%'
              OR pg_get_constraintdef(c.oid) ILIKE '%first_source_date%'
              OR pg_get_constraintdef(c.oid) ILIKE '%last_source_date%'
              OR pg_get_constraintdef(c.oid) ILIKE '%future_source_rows_excluded%'
              OR pg_get_constraintdef(c.oid) ILIKE '%source_content_id%'
              OR pg_get_constraintdef(c.oid) ILIKE '%source_entry_sha256%'
          )
        ORDER BY relation.relname,c.conname
    LOOP
        EXECUTE format('ALTER TABLE stock.%I DROP CONSTRAINT %I',item.table_name,item.conname);
    END LOOP;

    FOR item IN
        SELECT table_name,index_name
        FROM source_switch_0055_secondary_indexes
        WHERE backing_constraint IS NULL
        ORDER BY table_name,index_name
    LOOP
        EXECUTE format('DROP INDEX stock.%I',item.index_name);
    END LOOP;

    FOR item IN
        SELECT index_relation.relname AS index_name
        FROM pg_index index_definition
        JOIN pg_class table_relation ON table_relation.oid=index_definition.indrelid
        JOIN pg_namespace table_schema ON table_schema.oid=table_relation.relnamespace
        JOIN pg_class index_relation ON index_relation.oid=index_definition.indexrelid
        LEFT JOIN pg_constraint constraint_link
          ON constraint_link.conindid=index_relation.oid
         AND constraint_link.contype IN ('p','u')
        WHERE table_schema.nspname='stock'
          AND table_relation.relname='source_raw_daily'
          AND constraint_link.oid IS NULL
          AND EXISTS (
              SELECT 1 FROM unnest(index_definition.indkey) key_column(attnum)
              JOIN pg_attribute attribute
                ON attribute.attrelid=table_relation.oid
               AND attribute.attnum=key_column.attnum
              WHERE attribute.attname IN (
                  'source_entry_bytes','source_record_count','first_source_date',
                  'last_source_date','future_source_rows_excluded',
                  'source_content_id','source_entry_sha256'
              )
          )
    LOOP
        EXECUTE format('DROP INDEX stock.%I',item.index_name);
    END LOOP;

    FOR item IN
        SELECT relation.relname AS table_name,c.conname
        FROM pg_constraint c
        JOIN pg_class relation ON relation.oid=c.conrelid
        JOIN pg_namespace relation_schema ON relation_schema.oid=relation.relnamespace
        WHERE relation_schema.nspname='stock'
          AND c.contype='f'
          AND c.confrelid IN (
              'stock.source_baostock_daily'::regclass,
              'stock.source_corporate_action_events'::regclass,
              'stock.source_adjusted_daily_prices'::regclass
          )
    LOOP
        EXECUTE format('ALTER TABLE stock.%I DROP CONSTRAINT %I',item.table_name,item.conname);
    END LOOP;
END $$;

DROP TABLE stock.source_adjusted_daily_prices;
DROP TABLE stock.source_corporate_action_events;
DROP TABLE stock.source_baostock_daily;

ALTER TABLE stock.source_raw_daily
    DROP COLUMN IF EXISTS source_entry_bytes,
    DROP COLUMN IF EXISTS source_record_count,
    DROP COLUMN IF EXISTS first_source_date,
    DROP COLUMN IF EXISTS last_source_date,
    DROP COLUMN IF EXISTS future_source_rows_excluded,
    DROP COLUMN IF EXISTS source_content_id,
    DROP COLUMN IF EXISTS source_entry_sha256;

DO $$
DECLARE
    item record;
    key_columns_sql text;
    column_name text;
BEGIN
    FOR item IN
        SELECT table_name,constraint_name,constraint_type,mapped_columns
        FROM source_switch_0055_key_constraints
        ORDER BY table_name,constraint_name
    LOOP
        IF cardinality(item.mapped_columns)=0 THEN
            RAISE EXCEPTION 'SOURCE_SWITCH_0055_EMPTY_CAPTURE_KEY: stock.%',item.table_name;
        END IF;
        SELECT string_agg(format('%I',mapped.column_name),', ' ORDER BY mapped.ordinality)
        INTO key_columns_sql
        FROM unnest(item.mapped_columns) WITH ORDINALITY mapped(column_name,ordinality);
        IF item.constraint_type='p' THEN
            FOREACH column_name IN ARRAY item.mapped_columns LOOP
                EXECUTE format(
                    'ALTER TABLE stock.%I ALTER COLUMN %I SET NOT NULL',
                    item.table_name,column_name
                );
            END LOOP;
            EXECUTE format(
                'ALTER TABLE stock.%I ADD CONSTRAINT %I PRIMARY KEY (%s)',
                item.table_name,item.constraint_name,key_columns_sql
            );
        ELSE
            EXECUTE format(
                'ALTER TABLE stock.%I ADD CONSTRAINT %I UNIQUE (%s)',
                item.table_name,item.constraint_name,key_columns_sql
            );
        END IF;
        EXECUTE format(
            'COMMENT ON INDEX stock.%I IS %L',
            item.constraint_name,
            '按真实source_capture_id及其余业务维度保持唯一业务行，不绑定TDX来源包或SHA。'
        );
    END LOOP;
END $$;

-- 该独立部分索引没有新的业务键替代；先删除后才能移除source_captures的旧来源包列。
DROP INDEX stock.source_captures_package_parser_null_session_uq;

DO $$
DECLARE
    item record;
    child_columns_sql text;
    parent_columns_sql text;
    delete_action_sql text;
    update_action_sql text;
    match_type_sql text;
    deferrable_sql text;
BEGIN
    FOR item IN
        SELECT * FROM source_switch_0055_foreign_keys
        WHERE parent_schema='stock'
          AND parent_table NOT IN (
              'source_packages','source_baostock_daily',
              'source_corporate_action_events','source_adjusted_daily_prices'
          )
          AND child_table NOT IN (
              'source_packages','source_baostock_daily',
              'source_corporate_action_events','source_adjusted_daily_prices'
          )
        ORDER BY child_table,constraint_name
    LOOP
        IF cardinality(item.mapped_child_columns)=0
           OR cardinality(item.mapped_child_columns)<>cardinality(item.mapped_parent_columns) THEN
            RAISE EXCEPTION 'SOURCE_SWITCH_0055_FOREIGN_KEY_MAPPING_INVALID: stock.%',
                item.constraint_name;
        END IF;
        SELECT string_agg(format('%I',mapped.column_name),', ' ORDER BY mapped.ordinality)
        INTO child_columns_sql
        FROM unnest(item.mapped_child_columns) WITH ORDINALITY mapped(column_name,ordinality);
        SELECT string_agg(format('%I',mapped.column_name),', ' ORDER BY mapped.ordinality)
        INTO parent_columns_sql
        FROM unnest(item.mapped_parent_columns) WITH ORDINALITY mapped(column_name,ordinality);
        delete_action_sql := CASE item.delete_action
            WHEN 'a' THEN 'NO ACTION'
            WHEN 'r' THEN 'RESTRICT'
            WHEN 'c' THEN 'CASCADE'
            WHEN 'n' THEN 'SET NULL'
            WHEN 'd' THEN 'SET DEFAULT'
            ELSE NULL
        END;
        update_action_sql := CASE item.update_action
            WHEN 'a' THEN 'NO ACTION'
            WHEN 'r' THEN 'RESTRICT'
            WHEN 'c' THEN 'CASCADE'
            WHEN 'n' THEN 'SET NULL'
            WHEN 'd' THEN 'SET DEFAULT'
            ELSE NULL
        END;
        match_type_sql := CASE item.match_type
            WHEN 's' THEN 'SIMPLE'
            WHEN 'f' THEN 'FULL'
            WHEN 'p' THEN 'PARTIAL'
            ELSE NULL
        END;
        IF delete_action_sql IS NULL OR update_action_sql IS NULL OR match_type_sql IS NULL THEN
            RAISE EXCEPTION 'SOURCE_SWITCH_0055_FOREIGN_KEY_ACTION_UNSUPPORTED: stock.%',
                item.constraint_name;
        END IF;
        deferrable_sql := CASE
            WHEN item.is_deferrable AND item.is_deferred THEN ' DEFERRABLE INITIALLY DEFERRED'
            WHEN item.is_deferrable THEN ' DEFERRABLE INITIALLY IMMEDIATE'
            ELSE ' NOT DEFERRABLE'
        END;
        EXECUTE format(
            'ALTER TABLE stock.%I ADD CONSTRAINT %I FOREIGN KEY (%s) REFERENCES stock.%I (%s) MATCH %s ON UPDATE %s ON DELETE %s%s',
            item.child_table,
            item.constraint_name,
            child_columns_sql,
            item.parent_table,
            parent_columns_sql,
            match_type_sql,
            update_action_sql,
            delete_action_sql,
            deferrable_sql
        );
        EXECUTE format(
            'COMMENT ON CONSTRAINT %I ON stock.%I IS %L',
            item.constraint_name,
            item.child_table,
            '按source_capture_id替换原来源包复合外键，继续保证本表来源捕获引用存在。'
        );
    END LOOP;
END $$;

-- 来源捕获目录的旧来源包列对应本表自身capture_id，不对应source_capture_id列。
-- 其来源包外键、唯一约束和部分索引已在上文删除；保留capture_id主键。
ALTER TABLE stock.source_captures DROP COLUMN source_package_id;

DO $$
DECLARE
    item record;
    key_columns_sql text;
    key_options_sql text;
    include_columns_sql text;
    predicate_sql text;
    unique_sql text;
BEGIN
    FOR item IN
        SELECT * FROM source_switch_0055_secondary_indexes
        WHERE backing_constraint IS NULL
        ORDER BY table_name,index_name
    LOOP
        SELECT string_agg(
                   format('%I %s',mapped.column_name,mapped.sort_options),
                   ', ' ORDER BY mapped.ordinality
               )
        INTO key_options_sql
        FROM unnest(item.mapped_columns,item.mapped_key_options)
             WITH ORDINALITY mapped(column_name,sort_options,ordinality);
        key_columns_sql := key_options_sql;
        SELECT string_agg(format('%I',mapped.column_name),', ' ORDER BY mapped.ordinality)
        INTO include_columns_sql
        FROM unnest(item.mapped_include_columns) WITH ORDINALITY mapped(column_name,ordinality)
        WHERE NOT mapped.column_name=ANY(item.mapped_columns);
        predicate_sql := item.predicate;
        IF predicate_sql IS NOT NULL THEN
            predicate_sql := replace(predicate_sql,'source_package_id','source_capture_id');
            predicate_sql := replace(predicate_sql,'previous_source_package_id','previous_source_capture_id');
            predicate_sql := replace(predicate_sql,'target_source_package_id','target_source_capture_id');
            predicate_sql := replace(predicate_sql,'t0_source_package_id','t0_source_capture_id');
            predicate_sql := replace(predicate_sql,'endpoint_source_package_id','endpoint_source_capture_id');
            predicate_sql := replace(predicate_sql,'package_sha256','source_capture_id');
            predicate_sql := replace(predicate_sql,'previous_package_sha256','previous_source_capture_id');
            predicate_sql := replace(predicate_sql,'target_package_sha256','target_source_capture_id');
            predicate_sql := replace(predicate_sql,'t0_package_sha256','t0_source_capture_id');
        END IF;
        unique_sql := CASE WHEN item.is_unique THEN 'UNIQUE ' ELSE '' END;
        EXECUTE format(
            'CREATE %sINDEX %I ON stock.%I USING btree (%s)%s%s',
            unique_sql,item.index_name,item.table_name,key_columns_sql,
            CASE WHEN include_columns_sql IS NULL THEN '' ELSE format(' INCLUDE (%s)',include_columns_sql) END,
            CASE WHEN predicate_sql IS NULL THEN '' ELSE format(' WHERE %s',predicate_sql) END
        );
        EXECUTE format(
            'COMMENT ON INDEX stock.%I IS %L',
            item.index_name,
            '按BaoStock/TDX来源捕获及原业务维度加速读回；不绑定来源包或SHA。'
        );
    END LOOP;
END $$;

DO $$
DECLARE
    item record;
    capture_column text;
BEGIN
    FOR item IN
        SELECT table_name,column_name
        FROM information_schema.columns
        WHERE table_schema='stock'
          AND column_name IN (
              'source_package_id','previous_source_package_id',
              'target_source_package_id','t0_source_package_id',
              'endpoint_source_package_id','package_sha256',
              'previous_package_sha256','target_package_sha256','t0_package_sha256'
          )
          AND table_name<>'source_packages'
        ORDER BY table_name,ordinal_position
    LOOP
        capture_column := CASE item.column_name
            WHEN 'source_package_id' THEN 'source_capture_id'
            WHEN 'previous_source_package_id' THEN 'previous_source_capture_id'
            WHEN 'target_source_package_id' THEN 'target_source_capture_id'
            WHEN 't0_source_package_id' THEN 't0_source_capture_id'
            WHEN 'endpoint_source_package_id' THEN 'endpoint_source_capture_id'
            WHEN 'package_sha256' THEN 'source_capture_id'
            WHEN 'previous_package_sha256' THEN 'previous_source_capture_id'
            WHEN 'target_package_sha256' THEN 'target_source_capture_id'
            WHEN 't0_package_sha256' THEN 't0_source_capture_id'
        END;
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='stock' AND table_name=item.table_name
              AND column_name=capture_column
        ) THEN
            RAISE EXCEPTION 'SOURCE_SWITCH_0055_CAPTURE_COLUMN_MISSING: stock.%.%',
                item.table_name,capture_column;
        END IF;
        EXECUTE format('ALTER TABLE stock.%I DROP COLUMN %I',item.table_name,item.column_name);
    END LOOP;
END $$;

DO $$
DECLARE
    item record;
BEGIN
    FOR item IN
        SELECT table_name,column_name
        FROM source_switch_0055_required_capture_columns
        ORDER BY table_name,column_name
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='stock' AND table_name=item.table_name
              AND column_name=item.column_name
        ) THEN
            RAISE EXCEPTION 'SOURCE_SWITCH_0055_REQUIRED_CAPTURE_COLUMN_MISSING: stock.%.%',
                item.table_name,item.column_name;
        END IF;
        EXECUTE format(
            'ALTER TABLE stock.%I ALTER COLUMN %I SET NOT NULL',
            item.table_name,item.column_name
        );
    END LOOP;
END $$;

ALTER TABLE stock.data_jobs DROP COLUMN IF EXISTS baostock_capture_id;
ALTER TABLE stock.data_job_progress DROP COLUMN baostock_capture_id;
-- source_captures不进入通用索引映射：其旧来源包索引没有可替代的新捕获业务键。
DROP TABLE stock.source_packages;

ALTER TABLE stock.source_sector_pulse_cohort_group_facts
    ADD CONSTRAINT source_sector_pulse_cohort_group_identity_ck CHECK (
        t0_session<=research_cutoff
        AND t0_source_capture_id>0
        AND t0_profile_revision<>''
        AND snapshot_id>0
        AND pulse_algorithm_contract='SOURCE_CROSS_PERIOD_PULSE_R1'
        AND cohort_contract='SOURCE_SECTOR_PULSE_COHORT_OUTCOMES_R1'
        AND followup_contract='SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1'
        AND board_kind IN ('INDUSTRY','CONCEPT')
        AND cohort_axis IN ('PULSE_STATE','ADJACENT_TRANSITION')
        AND (
            (cohort_axis='PULSE_STATE' AND cohort_state IN ('TRUE','FALSE','UNKNOWN'))
            OR (
                cohort_axis='ADJACENT_TRANSITION'
                AND cohort_state IN (
                    'PULSE_APPEARED','PULSE_PERSISTED','PULSE_CEASED',
                    'PULSE_NOT_PRESENT_BOTH','PULSE_TRANSITION_UNKNOWN',
                    'NO_STORED_TRANSITION'
                )
            )
        )
        AND horizon_sessions IN (1,3,5)
        AND (
            (target_session IS NULL AND target_source_capture_id IS NULL)
            OR (
                target_session>t0_session
                AND target_session<=research_cutoff
                AND target_source_capture_id>0
            )
        )
        AND calendar_id<>''
        AND calendar_revision<>''
        AND group_status IN (
            'AVAILABLE','UNKNOWN','NOT_COMPUTED','NOT_DUE','INCOMPLETE','NO_GROUP_MEMBERS'
        )
        AND research_mode='NON_PIT_RETROSPECTIVE'
        AND membership_mode='CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT'
        AND price_coordinate='FORWARD_ADJUSTED'
        AND source_time_quality<>''
        AND source_clock_role<>''
        AND formal_rotation_state='NOT_EVALUATED'
    );
COMMENT ON CONSTRAINT source_sector_pulse_cohort_group_identity_ck
    ON stock.source_sector_pulse_cohort_group_facts IS
    '校验T0和后继BaoStock捕获、QFQ坐标、非PIT成员快照及板块统计业务身份。';

DO $$
DECLARE
    item record;
    revised_definition text;
BEGIN
    FOR item IN
        SELECT relation.relname AS table_name,constraint_row.conname AS constraint_name,
               pg_get_constraintdef(constraint_row.oid) AS definition
        FROM pg_constraint constraint_row
        JOIN pg_class relation ON relation.oid=constraint_row.conrelid
        JOIN pg_namespace relation_schema ON relation_schema.oid=relation.relnamespace
        WHERE relation_schema.nspname='stock'
          AND constraint_row.contype='c'
          AND pg_get_constraintdef(constraint_row.oid) ILIKE '%price_coordinate%'
        ORDER BY relation.relname,constraint_row.conname
    LOOP
        revised_definition := replace(
            item.definition,'RAW_UNADJUSTED','FORWARD_ADJUSTED'
        );
        IF revised_definition<>item.definition THEN
            EXECUTE format(
                'ALTER TABLE stock.%I DROP CONSTRAINT %I',
                item.table_name,item.constraint_name
            );
            EXECUTE format(
                'ALTER TABLE stock.%I ADD CONSTRAINT %I %s',
                item.table_name,item.constraint_name,revised_definition
            );
        END IF;
    END LOOP;
END $$;

DO $$
DECLARE
    item record;
BEGIN
    FOR item IN
        SELECT table_name
        FROM information_schema.columns
        WHERE table_schema='stock' AND column_name='price_coordinate'
        ORDER BY table_name
    LOOP
        EXECUTE format(
            'COMMENT ON COLUMN stock.%I.price_coordinate IS %L',
            item.table_name,
            '价格研究坐标；本批跨日技术与派生研究使用BaoStock FORWARD_ADJUSTED前复权并标记NON_PIT；当日成交、量额及制度事实仍使用独立RAW未复权列。'
        );
    END LOOP;
END $$;

ALTER TABLE stock.data_jobs
    ADD CONSTRAINT data_jobs_selected_modules_check CHECK (
        selected_modules <@ ARRAY[
            'BAOSTOCK_PRICES','TDX_SECTOR_MEMBERS',
            'CORE_PROFILE','MARKET_BREADTH','MARKET_PARTICIPATION','D0','D1',
            'FOCUS_FORMAL','SECTOR_PULSE','SECTOR_TRANSITION','SECTOR_FOLLOWUP'
        ]::text[]
    );

COMMENT ON COLUMN stock.data_jobs.selected_modules IS
    '本次用户明确选择的来源与研究模块；股票价格唯一来源为BaoStock，旧TDX股票Raw和仿射复权模块已退役。';
COMMENT ON TABLE stock.source_raw_daily IS
    '按来源捕获、证券代码和官方交易会话保存BaoStock原始未复权RAW及前复权QFQ日线事实；每个价格坐标独立记录质量、单位和观察时钟。';
COMMENT ON TABLE stock.source_symbol_price_profiles IS
    '保存由BaoStock前复权QFQ OHLC及原始成交输入生成的有界Core/Profile typed画像；业务来源版本只用source_capture_id绑定。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.source_capture_id IS
    'BaoStock价格输入对应的numeric来源捕获键；与同日source_raw_daily双坐标行及其唯一证券分母一致。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.price_coordinate IS
    'Core/Profile价格坐标；现役值固定FORWARD_ADJUSTED，即BaoStock adjustflag=2前复权。';
COMMENT ON TABLE stock.source_captures IS
    '记录BaoStock双坐标价格或TDX远端行业/GN成员读取的真实来源捕获、观察时钟、覆盖范围和质量；不保存来源原件或响应payload。';
COMMENT ON COLUMN stock.source_captures.capture_id IS
    '数据库内numeric来源捕获主键，业务事实和运行进度用其绑定精确来源捕获。';

DO $$
DECLARE
    item record;
BEGIN
    FOR item IN
        SELECT c.conrelid::regclass AS relation_name,c.conname
        FROM pg_constraint c
        WHERE c.contype='f'
          AND c.confrelid='stock.source_captures'::regclass
          AND NOT c.convalidated
    LOOP
        EXECUTE format(
            'ALTER TABLE %s VALIDATE CONSTRAINT %I',item.relation_name,item.conname
        );
    END LOOP;
END $$;
