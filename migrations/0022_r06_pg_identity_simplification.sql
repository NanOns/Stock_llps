-- R06 前向结构收敛：保留事实行与版本语义，将来源/父子身份改为数字引用。
LOCK TABLE
    stock.installation_identity,
    stock.migration_ledger,
    stock.namespaces,
    stock.role_bindings,
    stock.source_contents,
    stock.source_captures,
    stock.entities,
    stock.identity_observations,
    stock.calendar_sessions,
    stock.canonical_daily,
    stock.source_raw_daily,
    stock.source_symbol_price_profiles,
    stock.tdx_sector_source_snapshots,
    stock.tdx_sector_catalog,
    stock.tdx_sector_members,
    stock.source_rule_evaluations,
    stock.source_forward_path_facts,
    stock.source_research_events,
    stock.source_d0_launch_day_facts,
    stock.source_d1_structure_day_facts,
    stock.source_d1_material_anchor_day_facts,
    stock.source_market_breadth_days,
    stock.source_market_breadth_symbols,
    stock.source_d0_scenario_day_facts,
    stock.source_pool_seed_day_facts,
    stock.source_sector_cross_period_pulse_facts,
    stock.source_sector_pulse_adjacent_transitions,
    stock.source_sector_pulse_followup_facts
IN ACCESS EXCLUSIVE MODE;

CREATE TEMP TABLE r06_migration_ledger_before ON COMMIT DROP AS
SELECT migration_id, applied_at, lineage
FROM stock.migration_ledger;

DO $$
DECLARE
    item record;
BEGIN
    -- 先解除旧宽键、跨环境及摘要字段上的约束，后续用较短业务键重建。
    FOR item IN
        SELECT c.conrelid::regclass AS relation_name, c.conname
        FROM pg_constraint c
        JOIN pg_namespace n ON n.oid = c.connamespace
        WHERE n.nspname = 'stock'
          AND c.contype IN ('p', 'u', 'f')
          AND c.conrelid NOT IN (
              'stock.installation_identity'::regclass, 'stock.migration_ledger'::regclass
          )
        ORDER BY CASE WHEN c.contype = 'f' THEN 0 ELSE 1 END,
                 c.conrelid::regclass::text, c.conname
    LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', item.relation_name, item.conname);
    END LOOP;

    -- 只移除依赖即将删除身份/摘要字段的CHECK；数值、质量及状态域检查保留。
    FOR item IN
        SELECT c.conrelid::regclass AS relation_name, c.conname
        FROM pg_constraint c
        JOIN pg_namespace n ON n.oid = c.connamespace
        WHERE n.nspname = 'stock'
          AND c.contype = 'c'
          AND (
              pg_get_constraintdef(c.oid) ~* '(namespace_id|package_sha256|source_sha|sha256|digest|content_id|profile_revision|capture_id|snapshot_id|anchor_id)'
              OR (
                  c.conrelid = 'stock.source_d1_material_anchor_day_facts'::regclass
                  AND pg_get_constraintdef(c.oid) ~* '(before_session|target_session|source_symbol|structure_type|d1_history_session_count)'
              )
              OR (
                  c.conrelid = 'stock.source_sector_pulse_adjacent_transitions'::regclass
                  AND pg_get_constraintdef(c.oid) ~* '(pulse_algorithm_contract)'
              )
              OR (
                  c.conrelid = 'stock.source_sector_pulse_followup_facts'::regclass
                  AND pg_get_constraintdef(c.oid) ~* '(pulse_algorithm_contract|t0_source_time_quality|t0_source_clock_role)'
              )
          )
          AND c.conrelid NOT IN (
              'stock.installation_identity'::regclass, 'stock.migration_ledger'::regclass
          )
    LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', item.relation_name, item.conname);
    END LOOP;
END $$;

-- 保留已有TDX原件登记，通过旧内容外键迁移来源包数字ID；后续事实不再持久化SHA。
ALTER TABLE stock.source_contents RENAME TO source_packages;
ALTER TABLE stock.source_packages RENAME COLUMN byte_sha TO package_sha256;
ALTER TABLE stock.source_packages ALTER COLUMN byte_count DROP NOT NULL;
ALTER TABLE stock.source_packages
    ADD COLUMN source_package_id bigint GENERATED ALWAYS AS IDENTITY;
CREATE TEMP TABLE r06_content_package_map ON COMMIT DROP AS
SELECT namespace_id, content_id, source_package_id
FROM stock.source_packages;
DO $$
BEGIN
    IF EXISTS (
        SELECT dataset_id, package_sha256
        FROM stock.source_packages
        GROUP BY dataset_id, package_sha256
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION '同一TDX包字节SHA存在多个父登记行，拒绝任意挑选数字ID';
    END IF;
END $$;

-- 捕获时钟和解析计数仍是有效来源事实，移除DEV/PROD/TEST字段前保留精确原内容引用。
ALTER TABLE stock.source_packages DROP COLUMN namespace_id;
ALTER TABLE stock.source_packages DROP COLUMN content_id;

ALTER TABLE stock.source_captures
    ADD COLUMN numeric_capture_id bigint GENERATED ALWAYS AS IDENTITY,
    ADD COLUMN source_package_id bigint,
    ADD COLUMN target_session date;
UPDATE stock.source_captures c
SET source_package_id = p.source_package_id
FROM r06_content_package_map p
WHERE p.namespace_id = c.namespace_id AND p.content_id = c.content_id;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stock.source_captures WHERE source_package_id IS NULL) THEN
        RAISE EXCEPTION '来源捕获无法通过原有内容外键映射到来源包数字ID';
    END IF;
END $$;
ALTER TABLE stock.source_captures ALTER COLUMN source_package_id SET NOT NULL;
UPDATE stock.source_captures c
SET target_session = r.session_date
FROM stock.source_raw_daily r
WHERE r.namespace_id = c.namespace_id
  AND r.source_capture_id = c.capture_id;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM stock.source_captures c
        JOIN stock.source_raw_daily r
          ON r.namespace_id = c.namespace_id
         AND r.source_capture_id = c.capture_id
        GROUP BY c.namespace_id, c.capture_id
        HAVING count(DISTINCT r.session_date) > 1
    ) THEN
        RAISE EXCEPTION '同一来源捕获实际覆盖多个目标交易日，不能压成单日捕获键';
    END IF;
END $$;

ALTER TABLE stock.tdx_sector_source_snapshots
    ADD COLUMN numeric_snapshot_id bigint GENERATED ALWAYS AS IDENTITY;
CREATE TEMP TABLE r06_snapshot_map ON COMMIT DROP AS
SELECT snapshot_id AS old_snapshot_id, numeric_snapshot_id AS snapshot_id
FROM stock.tdx_sector_source_snapshots;
UPDATE stock.tdx_sector_source_snapshots
SET source_path_role = COALESCE(
    NULLIF(
        trim(
            BOTH '|; ' FROM regexp_replace(
                regexp_replace(
                    source_path_role,
                    '(?i)(^|[^A-Z0-9_])[A-Z0-9_]*(SHA|DIGEST)[A-Z0-9_]*=[0-9a-f]{64}',
                    '',
                    'g'
                ),
                '[|;]{2,}',
                '|',
                'g'
            )
        ),
        ''
    ),
    'DIRECT_SOURCE_FILE'
);

-- 先为被引用事实分配数字ID，随后以旧键将子行映射到这些ID。
ALTER TABLE stock.source_symbol_price_profiles ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_raw_daily ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_d0_launch_day_facts ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_d0_scenario_day_facts ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_d1_structure_day_facts ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_d1_material_anchor_day_facts ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_forward_path_facts ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_market_breadth_days ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_market_breadth_symbols ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_pool_seed_day_facts ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_research_events ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_rule_evaluations ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_sector_cross_period_pulse_facts ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_sector_pulse_adjacent_transitions ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.source_sector_pulse_followup_facts ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.tdx_sector_catalog ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE stock.tdx_sector_members ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY;

DO $$
DECLARE
    item record;
    mapped_name text;
    capture_name text;
    missing_mapping boolean;
BEGIN
    -- 只在迁移瞬间用原件SHA校验捕获父包对应关系；下游持久身份改为BIGINT。
    FOR item IN
        SELECT table_name, column_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name IN (
              'package_sha256', 'previous_package_sha256',
              'target_package_sha256', 't0_package_sha256'
          )
          AND table_name <> 'source_packages'
    LOOP
        mapped_name := CASE item.column_name
            WHEN 'package_sha256' THEN 'source_package_id'
            WHEN 'previous_package_sha256' THEN 'previous_source_package_id'
            WHEN 'target_package_sha256' THEN 'target_source_package_id'
            ELSE 't0_source_package_id'
        END;
        capture_name := CASE item.column_name
            WHEN 'package_sha256' THEN
                CASE WHEN EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='stock' AND table_name=item.table_name
                      AND column_name='source_capture_id'
                ) THEN 'source_capture_id' ELSE 'target_capture_id' END
            WHEN 'previous_package_sha256' THEN 'previous_source_capture_id'
            WHEN 'target_package_sha256' THEN 'target_source_capture_id'
            ELSE 't0_source_capture_id'
        END;
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='stock' AND table_name=item.table_name
              AND column_name=capture_name
        ) THEN
            RAISE EXCEPTION '表 %.% 缺少用于包身份映射的真实捕获外键', item.table_name, item.column_name;
        END IF;
        EXECUTE format('ALTER TABLE stock.%I ADD COLUMN %I bigint', item.table_name, 'r06_' || mapped_name);
        EXECUTE format(
            'UPDATE stock.%I child SET %I = c.source_package_id FROM stock.source_captures c WHERE child.%I = c.capture_id',
            item.table_name, 'r06_' || mapped_name, capture_name
        );
        EXECUTE format(
            'SELECT EXISTS (
                SELECT 1 FROM stock.%I child
                LEFT JOIN stock.source_captures c ON child.%I=c.capture_id
                LEFT JOIN stock.source_packages p ON p.source_package_id=c.source_package_id
                WHERE child.%I IS NOT NULL
                  AND (child.%I IS NULL OR c.capture_id IS NULL
                       OR p.package_sha256 IS DISTINCT FROM child.%I)
            )',
            item.table_name, capture_name, item.column_name, 'r06_' || mapped_name,
            item.column_name
        ) INTO missing_mapping;
        IF missing_mapping THEN
            RAISE EXCEPTION '表 %.% 的包摘要与其真实捕获外键不一致，拒绝转换为数字来源包ID', item.table_name, item.column_name;
        END IF;
        IF item.is_nullable = 'NO' THEN
            EXECUTE format('ALTER TABLE stock.%I ALTER COLUMN %I SET NOT NULL', item.table_name, 'r06_' || mapped_name);
        END IF;
    END LOOP;

    -- 逐个保存UUID捕获引用对应的BIGINT ID。
    FOR item IN
        SELECT table_name, column_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND data_type = 'uuid'
          AND column_name LIKE '%capture_id'
          AND NOT (table_name = 'source_captures' AND column_name = 'capture_id')
    LOOP
        EXECUTE format('ALTER TABLE stock.%I ADD COLUMN %I bigint', item.table_name, 'r06_' || item.column_name);
        EXECUTE format(
            'UPDATE stock.%I child SET %I = c.numeric_capture_id FROM stock.source_captures c WHERE child.%I = c.capture_id',
            item.table_name, 'r06_' || item.column_name, item.column_name
        );
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM stock.%I child WHERE child.%I IS NOT NULL AND child.%I IS NULL)',
            item.table_name, item.column_name, 'r06_' || item.column_name
        ) INTO missing_mapping;
        IF missing_mapping THEN
            RAISE EXCEPTION '表 %.% 的非空捕获外键无法映射到数字捕获ID', item.table_name, item.column_name;
        END IF;
        IF item.is_nullable = 'NO' THEN
            EXECUTE format('ALTER TABLE stock.%I ALTER COLUMN %I SET NOT NULL', item.table_name, 'r06_' || item.column_name);
        END IF;
    END LOOP;

    -- 成员快照和板块父行统一映射到数字快照/目录ID。
    FOR item IN
        SELECT table_name, column_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND table_name <> 'tdx_sector_source_snapshots'
          AND column_name = 'snapshot_id'
          AND data_type = 'uuid'
    LOOP
        EXECUTE format('ALTER TABLE stock.%I ADD COLUMN r06_snapshot_id bigint', item.table_name);
        EXECUTE format(
            'UPDATE stock.%I child SET r06_snapshot_id = s.snapshot_id FROM r06_snapshot_map s WHERE child.snapshot_id = s.old_snapshot_id',
            item.table_name
        );
        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM stock.%I child WHERE child.snapshot_id IS NOT NULL AND child.r06_snapshot_id IS NULL)',
            item.table_name
        ) INTO missing_mapping;
        IF missing_mapping THEN
            RAISE EXCEPTION '表 %.snapshot_id 的非空快照外键无法映射到数字快照ID', item.table_name;
        END IF;
        IF item.is_nullable = 'NO' THEN
            EXECUTE format('ALTER TABLE stock.%I ALTER COLUMN r06_snapshot_id SET NOT NULL', item.table_name);
        END IF;
    END LOOP;
END $$;

-- 短父ID替代重复宽外键；映射发生在旧业务键仍完整时。
ALTER TABLE stock.source_d1_material_anchor_day_facts ADD COLUMN r06_structure_id bigint;
UPDATE stock.source_d1_material_anchor_day_facts a
SET r06_structure_id = d.id
FROM stock.source_d1_structure_day_facts d
WHERE a.namespace_id = d.namespace_id
  AND a.target_session = d.target_session
  AND a.source_symbol = d.source_symbol
  AND a.structure_type = d.structure_type
  AND a.package_sha256 = d.package_sha256
  AND a.target_capture_id = d.target_capture_id
  AND a.d1_history_session_count = d.d1_history_session_count
  AND a.calendar_revision = d.calendar_revision
  AND a.calendar_sha256 = d.calendar_sha256
  AND a.producer_contract = d.producer_contract
  AND a.producer_version = d.producer_version
  AND a.material_change_contract = d.material_change_contract;

ALTER TABLE stock.source_market_breadth_symbols ADD COLUMN r06_breadth_day_id bigint;
UPDATE stock.source_market_breadth_symbols s
SET r06_breadth_day_id = d.id
FROM stock.source_market_breadth_days d
WHERE s.namespace_id = d.namespace_id
  AND s.target_session = d.target_session
  AND s.package_sha256 = d.package_sha256
  AND s.source_capture_id = d.source_capture_id
  AND s.calendar_revision = d.calendar_revision
  AND s.calendar_sha256 = d.calendar_sha256
  AND s.contract_version = d.contract_version;

ALTER TABLE stock.tdx_sector_members ADD COLUMN r06_catalog_id bigint;
UPDATE stock.tdx_sector_members m
SET r06_catalog_id = c.id
FROM stock.tdx_sector_catalog c
WHERE m.snapshot_id = c.snapshot_id
  AND m.board_kind = c.board_kind
  AND m.board_native_id = c.board_native_id;

ALTER TABLE stock.source_sector_cross_period_pulse_facts ADD COLUMN r06_catalog_id bigint;
UPDATE stock.source_sector_cross_period_pulse_facts p
SET r06_catalog_id = c.id
FROM stock.tdx_sector_catalog c
WHERE p.snapshot_id = c.snapshot_id
  AND p.board_kind = c.board_kind
  AND p.board_native_id = c.board_native_id;

ALTER TABLE stock.source_symbol_price_profiles ADD COLUMN r06_raw_id bigint;
UPDATE stock.source_symbol_price_profiles p
SET r06_raw_id = r.id
FROM stock.source_raw_daily r
WHERE p.namespace_id = r.namespace_id
  AND p.source_symbol = r.source_symbol
  AND p.target_session = r.session_date
  AND p.package_sha256 = r.package_sha256;

ALTER TABLE stock.source_sector_pulse_adjacent_transitions
    ADD COLUMN r06_previous_pulse_id bigint,
    ADD COLUMN r06_target_pulse_id bigint;
UPDATE stock.source_sector_pulse_adjacent_transitions t
SET r06_previous_pulse_id = p.id
FROM stock.source_sector_cross_period_pulse_facts p
WHERE t.namespace_id = p.namespace_id
  AND t.previous_session = p.target_session
  AND t.previous_package_sha256 = p.package_sha256
  AND t.previous_source_capture_id = p.source_capture_id
  AND t.previous_source_content_id = p.source_content_id
  AND t.previous_source_raw_digest = p.source_raw_digest
  AND t.previous_profile_revision = p.profile_revision
  AND t.snapshot_id = p.snapshot_id
  AND t.board_kind = p.board_kind
  AND t.board_native_id = p.board_native_id
  AND t.pulse_algorithm_contract = p.algorithm_contract;
UPDATE stock.source_sector_pulse_adjacent_transitions t
SET r06_target_pulse_id = p.id
FROM stock.source_sector_cross_period_pulse_facts p
WHERE t.namespace_id = p.namespace_id
  AND t.target_session = p.target_session
  AND t.target_package_sha256 = p.package_sha256
  AND t.target_source_capture_id = p.source_capture_id
  AND t.target_source_content_id = p.source_content_id
  AND t.target_source_raw_digest = p.source_raw_digest
  AND t.target_profile_revision = p.profile_revision
  AND t.snapshot_id = p.snapshot_id
  AND t.board_kind = p.board_kind
  AND t.board_native_id = p.board_native_id
  AND t.pulse_algorithm_contract = p.algorithm_contract;

ALTER TABLE stock.source_sector_pulse_followup_facts ADD COLUMN r06_pulse_id bigint;
UPDATE stock.source_sector_pulse_followup_facts f
SET r06_pulse_id = p.id
FROM stock.source_sector_cross_period_pulse_facts p
WHERE f.namespace_id = p.namespace_id
  AND f.t0_session = p.target_session
  AND f.t0_package_sha256 = p.package_sha256
  AND f.t0_source_capture_id = p.source_capture_id
  AND f.t0_source_content_id = p.source_content_id
  AND f.t0_source_raw_digest = p.source_raw_digest
  AND f.t0_profile_revision = p.profile_revision
  AND f.snapshot_id = p.snapshot_id
  AND f.board_kind = p.board_kind
  AND f.board_native_id = p.board_native_id
  AND f.pulse_algorithm_contract = p.algorithm_contract;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stock.source_d1_material_anchor_day_facts WHERE r06_structure_id IS NULL)
       OR EXISTS (SELECT 1 FROM stock.source_market_breadth_symbols WHERE r06_breadth_day_id IS NULL)
       OR EXISTS (SELECT 1 FROM stock.tdx_sector_members WHERE r06_catalog_id IS NULL)
       OR EXISTS (SELECT 1 FROM stock.source_sector_cross_period_pulse_facts WHERE r06_catalog_id IS NULL)
       OR EXISTS (SELECT 1 FROM stock.source_symbol_price_profiles WHERE r06_raw_id IS NULL)
       OR EXISTS (SELECT 1 FROM stock.source_sector_pulse_adjacent_transitions WHERE r06_previous_pulse_id IS NULL OR r06_target_pulse_id IS NULL)
       OR EXISTS (SELECT 1 FROM stock.source_sector_pulse_followup_facts WHERE r06_pulse_id IS NULL) THEN
        RAISE EXCEPTION '现存事实无法通过旧业务键映射到唯一父行';
    END IF;
END $$;

-- profile_revision仍保留真实版本差异，但将旧摘要值一次性换成短的显式修订标签。
CREATE TEMP TABLE r06_profile_revision_map (
    old_revision character(64) PRIMARY KEY,
    first_seen timestamp with time zone,
    new_revision text
) ON COMMIT DROP;
DO $$
DECLARE item record;
BEGIN
    INSERT INTO r06_profile_revision_map(old_revision, first_seen)
    SELECT profile_revision, min(created_at)
    FROM stock.source_symbol_price_profiles
    GROUP BY profile_revision;

    FOR item IN
        SELECT table_name, column_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name IN ('profile_revision', 'previous_profile_revision', 'target_profile_revision', 't0_profile_revision')
          AND data_type IN ('character', 'text')
    LOOP
        EXECUTE format(
            'INSERT INTO r06_profile_revision_map(old_revision) SELECT DISTINCT %I FROM stock.%I WHERE %I IS NOT NULL ON CONFLICT DO NOTHING',
            item.column_name, item.table_name, item.column_name
        );
    END LOOP;
    IF EXISTS (SELECT 1 FROM r06_profile_revision_map WHERE first_seen IS NULL) THEN
        RAISE EXCEPTION '研究事实包含画像表之外的未知历史修订，拒绝丢弃其版本差异';
    END IF;
    WITH ranked AS (
        SELECT old_revision, row_number() OVER (ORDER BY first_seen, old_revision)::integer AS revision_number
        FROM r06_profile_revision_map
    )
    UPDATE r06_profile_revision_map m
    SET new_revision = 'PROFILE_R' || lpad(r.revision_number::text, 2, '0')
    FROM ranked r
    WHERE r.old_revision = m.old_revision;

    FOR item IN
        SELECT table_name, column_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name IN ('profile_revision', 'previous_profile_revision', 'target_profile_revision', 't0_profile_revision')
          AND data_type IN ('character', 'text')
    LOOP
        EXECUTE format('ALTER TABLE stock.%I ADD COLUMN %I text', item.table_name, 'r06_' || item.column_name);
        EXECUTE format(
            'UPDATE stock.%I t SET %I = m.new_revision FROM r06_profile_revision_map m WHERE btrim(t.%I::text) = btrim(m.old_revision::text)',
            item.table_name, 'r06_' || item.column_name, item.column_name
        );
        IF item.is_nullable = 'NO' THEN
            EXECUTE format('ALTER TABLE stock.%I ALTER COLUMN %I SET NOT NULL', item.table_name, 'r06_' || item.column_name);
        END IF;
        EXECUTE format('ALTER TABLE stock.%I DROP COLUMN %I', item.table_name, item.column_name);
        EXECUTE format('ALTER TABLE stock.%I RENAME COLUMN %I TO %I', item.table_name, 'r06_' || item.column_name, item.column_name);
    END LOOP;
END $$;

DO $$
DECLARE
    item record;
    constraint_number integer := 0;
BEGIN
    -- 画像修订摘要改成显式标签后，继续在数据库中约束其合法域。
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name IN ('profile_revision', 'previous_profile_revision', 'target_profile_revision', 't0_profile_revision')
          AND data_type = 'text'
        ORDER BY table_name, column_name
    LOOP
        constraint_number := constraint_number + 1;
        EXECUTE format(
            'ALTER TABLE stock.%I ADD CONSTRAINT %I CHECK (%I ~ ''^PROFILE_R[0-9]{2}$'')',
            item.table_name,
            'r06_profile_revision_ck_' || constraint_number,
            item.column_name
        );
    END LOOP;
END $$;

-- 捕获、快照UUID被替换为生成的BIGINT，并保留所有子行的实际来源关系。
DO $$
DECLARE item record;
BEGIN
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND data_type = 'uuid'
          AND column_name LIKE '%capture_id'
          AND NOT (table_name = 'source_captures' AND column_name = 'capture_id')
    LOOP
        EXECUTE format('ALTER TABLE stock.%I DROP COLUMN %I', item.table_name, item.column_name);
        EXECUTE format('ALTER TABLE stock.%I RENAME COLUMN %I TO %I', item.table_name, 'r06_' || item.column_name, item.column_name);
    END LOOP;
    FOR item IN
        SELECT table_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name = 'snapshot_id'
          AND data_type = 'uuid'
          AND table_name <> 'tdx_sector_source_snapshots'
    LOOP
        EXECUTE format('ALTER TABLE stock.%I DROP COLUMN snapshot_id', item.table_name);
        EXECUTE format('ALTER TABLE stock.%I RENAME COLUMN r06_snapshot_id TO snapshot_id', item.table_name);
    END LOOP;
END $$;

ALTER TABLE stock.source_captures RENAME COLUMN capture_id TO legacy_capture_uuid;
ALTER TABLE stock.source_captures RENAME COLUMN numeric_capture_id TO capture_id;
ALTER TABLE stock.tdx_sector_source_snapshots RENAME COLUMN snapshot_id TO legacy_snapshot_uuid;
ALTER TABLE stock.tdx_sector_source_snapshots RENAME COLUMN numeric_snapshot_id TO snapshot_id;

-- SHA列仅留TDX原始数据包登记字段；下游改为source_package_id，内容/捕获摘要全部删除。
DO $$
DECLARE item record;
BEGIN
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name IN ('package_sha256', 'previous_package_sha256', 'target_package_sha256', 't0_package_sha256')
          AND table_name <> 'source_packages'
    LOOP
        CASE item.column_name
            WHEN 'package_sha256' THEN
                EXECUTE format('ALTER TABLE stock.%I DROP COLUMN package_sha256', item.table_name);
                EXECUTE format('ALTER TABLE stock.%I RENAME COLUMN r06_source_package_id TO source_package_id', item.table_name);
            WHEN 'previous_package_sha256' THEN
                EXECUTE format('ALTER TABLE stock.%I DROP COLUMN previous_package_sha256', item.table_name);
                EXECUTE format('ALTER TABLE stock.%I RENAME COLUMN r06_previous_source_package_id TO previous_source_package_id', item.table_name);
            WHEN 'target_package_sha256' THEN
                EXECUTE format('ALTER TABLE stock.%I DROP COLUMN target_package_sha256', item.table_name);
                EXECUTE format('ALTER TABLE stock.%I RENAME COLUMN r06_target_source_package_id TO target_source_package_id', item.table_name);
            ELSE
                EXECUTE format('ALTER TABLE stock.%I DROP COLUMN t0_package_sha256', item.table_name);
                EXECUTE format('ALTER TABLE stock.%I RENAME COLUMN r06_t0_source_package_id TO t0_source_package_id', item.table_name);
        END CASE;
    END LOOP;
END $$;

ALTER TABLE stock.source_captures DROP COLUMN legacy_capture_uuid;
ALTER TABLE stock.tdx_sector_source_snapshots DROP COLUMN legacy_snapshot_uuid;

ALTER TABLE stock.source_d1_material_anchor_day_facts RENAME COLUMN r06_structure_id TO d1_structure_id;
ALTER TABLE stock.source_market_breadth_symbols RENAME COLUMN r06_breadth_day_id TO breadth_day_id;
ALTER TABLE stock.tdx_sector_members RENAME COLUMN r06_catalog_id TO catalog_id;
ALTER TABLE stock.source_sector_cross_period_pulse_facts RENAME COLUMN r06_catalog_id TO catalog_id;
ALTER TABLE stock.source_symbol_price_profiles RENAME COLUMN r06_raw_id TO source_raw_id;
ALTER TABLE stock.source_sector_pulse_adjacent_transitions RENAME COLUMN r06_previous_pulse_id TO previous_pulse_id;
ALTER TABLE stock.source_sector_pulse_adjacent_transitions RENAME COLUMN r06_target_pulse_id TO target_pulse_id;
ALTER TABLE stock.source_sector_pulse_followup_facts RENAME COLUMN r06_pulse_id TO pulse_fact_id;

-- PULSE后继和成员行通过单一父ID即可定位其快照，不重复存快照UUID/数字ID。
ALTER TABLE stock.source_sector_cross_period_pulse_facts DROP COLUMN snapshot_id;
ALTER TABLE stock.source_sector_pulse_adjacent_transitions DROP COLUMN snapshot_id;
ALTER TABLE stock.source_sector_pulse_followup_facts DROP COLUMN snapshot_id;
ALTER TABLE stock.tdx_sector_members DROP COLUMN snapshot_id;

-- 变化行以两端PULSE父行ID表达身份；后继行以T0 PULSE父行ID表达身份。
ALTER TABLE stock.source_sector_pulse_adjacent_transitions
    DROP COLUMN previous_session,
    DROP COLUMN target_session,
    DROP COLUMN previous_source_package_id,
    DROP COLUMN previous_source_capture_id,
    DROP COLUMN previous_profile_revision,
    DROP COLUMN target_source_package_id,
    DROP COLUMN target_source_capture_id,
    DROP COLUMN target_profile_revision,
    DROP COLUMN pulse_algorithm_contract;
ALTER TABLE stock.source_sector_pulse_followup_facts
    DROP COLUMN t0_session,
    DROP COLUMN t0_source_package_id,
    DROP COLUMN t0_source_capture_id,
    DROP COLUMN t0_profile_revision,
    DROP COLUMN t0_source_time_quality,
    DROP COLUMN t0_source_clock_role,
    DROP COLUMN t0_source_available_at,
    DROP COLUMN pulse_algorithm_contract;

ALTER TABLE stock.source_symbol_price_profiles ALTER COLUMN source_raw_id SET NOT NULL;
ALTER TABLE stock.source_d1_material_anchor_day_facts ALTER COLUMN d1_structure_id SET NOT NULL;
ALTER TABLE stock.source_market_breadth_symbols ALTER COLUMN breadth_day_id SET NOT NULL;
ALTER TABLE stock.tdx_sector_members ALTER COLUMN catalog_id SET NOT NULL;
ALTER TABLE stock.source_sector_cross_period_pulse_facts ALTER COLUMN catalog_id SET NOT NULL;
ALTER TABLE stock.source_sector_pulse_adjacent_transitions ALTER COLUMN previous_pulse_id SET NOT NULL;
ALTER TABLE stock.source_sector_pulse_adjacent_transitions ALTER COLUMN target_pulse_id SET NOT NULL;
ALTER TABLE stock.source_sector_pulse_followup_facts ALTER COLUMN pulse_fact_id SET NOT NULL;

-- D1锚点的内容摘要身份改用父D1事实ID和锚点创建会话表达；保留坐标、关系与数值边界。
ALTER TABLE stock.source_d1_structure_day_facts DROP COLUMN anchor_id;
ALTER TABLE stock.source_d1_structure_day_facts DROP COLUMN source_event_id;
ALTER TABLE stock.source_d1_material_anchor_day_facts DROP COLUMN anchor_id;
ALTER TABLE stock.source_d1_material_anchor_day_facts
    DROP COLUMN before_session,
    DROP COLUMN target_session,
    DROP COLUMN source_symbol,
    DROP COLUMN structure_type,
    DROP COLUMN source_package_id,
    DROP COLUMN target_capture_id,
    DROP COLUMN d1_history_session_count,
    DROP COLUMN calendar_revision,
    DROP COLUMN producer_contract,
    DROP COLUMN producer_version,
    DROP COLUMN material_change_contract;
ALTER TABLE stock.source_d1_structure_day_facts
    ADD CONSTRAINT source_d1_structure_anchor_state_ck CHECK (
        (generation_state = 'GENERATED' AND anchor_created_session IS NOT NULL AND relation IS NOT NULL)
        OR
        (generation_state <> 'GENERATED' AND anchor_created_session IS NULL AND relation IS NULL
            AND lower_numerator IS NULL AND lower_denominator IS NULL
            AND upper_numerator IS NULL AND upper_denominator IS NULL)
    );

DO $$
DECLARE item record;
BEGIN
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name = 'namespace_id'
    LOOP
        EXECUTE format('ALTER TABLE stock.%I DROP COLUMN namespace_id', item.table_name);
    END LOOP;

    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND (column_name ILIKE '%sha%' OR column_name ILIKE '%digest%' OR column_name IN ('content_id', 'source_content_id'))
          AND NOT (table_name = 'source_packages' AND column_name = 'package_sha256')
          AND table_name <> 'migration_ledger'
    LOOP
        EXECUTE format('ALTER TABLE stock.%I DROP COLUMN %I', item.table_name, item.column_name);
    END LOOP;
END $$;

-- 仅未实现且无业务消费者的历史身份表清除；有效来源和研究事实行均保留。
DROP TABLE stock.canonical_daily;
DROP TABLE stock.calendar_sessions;
DROP TABLE stock.identity_observations;
DROP TABLE stock.entities;
DROP TABLE stock.role_bindings;
DROP TABLE stock.namespaces;

-- 迁移账本保留顺序、应用时间与lineage，不再保存迁移源码SHA。
DROP TABLE stock.migration_ledger;
CREATE TABLE stock.migration_ledger (
    migration_id integer NOT NULL PRIMARY KEY CHECK (migration_id > 0),
    applied_at timestamp with time zone NOT NULL,
    lineage text NOT NULL CHECK (lineage = 'STOCK_A_INITIAL_R1')
);
INSERT INTO stock.migration_ledger(migration_id, applied_at, lineage)
SELECT migration_id, applied_at, lineage
FROM r06_migration_ledger_before
ORDER BY migration_id;
COMMENT ON TABLE stock.migration_ledger IS '记录Stock A集中迁移的连续编号、应用时点与迁移谱系。';
COMMENT ON COLUMN stock.migration_ledger.migration_id IS '严格连续的集中迁移编号。';
COMMENT ON COLUMN stock.migration_ledger.applied_at IS '迁移事务成功应用时的数据库时钟。';
COMMENT ON COLUMN stock.migration_ledger.lineage IS '迁移所属Stock A业务谱系。';

-- 每个事实表使用紧凑数字主键；短业务唯一键承担同输入幂等约束。
ALTER TABLE stock.source_packages ADD PRIMARY KEY (source_package_id);
ALTER TABLE stock.source_packages ADD CONSTRAINT source_packages_dataset_package_sha256_uq UNIQUE (dataset_id, package_sha256);
CREATE INDEX source_packages_dataset_idx ON stock.source_packages (dataset_id, source_package_id);

ALTER TABLE stock.source_captures ADD PRIMARY KEY (capture_id);
ALTER TABLE stock.source_captures ADD CONSTRAINT source_captures_package_parser_session_uq UNIQUE (source_package_id, parser_contract, target_session);
CREATE UNIQUE INDEX source_captures_package_parser_null_session_uq
    ON stock.source_captures (source_package_id, parser_contract)
    WHERE target_session IS NULL;
ALTER TABLE stock.source_raw_daily ADD PRIMARY KEY (id);
ALTER TABLE stock.source_raw_daily ADD CONSTRAINT source_raw_daily_business_uq UNIQUE (source_capture_id, source_symbol, session_date);
ALTER TABLE stock.source_symbol_price_profiles ADD PRIMARY KEY (id);
ALTER TABLE stock.source_symbol_price_profiles ADD CONSTRAINT source_symbol_price_profiles_business_uq UNIQUE (source_raw_id, profile_revision);
ALTER TABLE stock.source_d0_launch_day_facts ADD PRIMARY KEY (id);
ALTER TABLE stock.source_d0_launch_day_facts ADD CONSTRAINT source_d0_launch_business_uq UNIQUE NULLS NOT DISTINCT (target_capture_id, source_symbol, before_capture_id, producer_version, algorithm_contract, session_contract, calendar_revision);
ALTER TABLE stock.source_d0_scenario_day_facts ADD PRIMARY KEY (id);
ALTER TABLE stock.source_d0_scenario_day_facts ADD CONSTRAINT source_d0_scenario_business_uq UNIQUE (source_capture_id, source_symbol, scenario, producer_version, calendar_revision);
ALTER TABLE stock.source_d1_structure_day_facts ADD PRIMARY KEY (id);
ALTER TABLE stock.source_d1_structure_day_facts ADD CONSTRAINT source_d1_structure_business_uq UNIQUE (target_capture_id, source_symbol, structure_type, d1_history_session_count, calendar_revision, producer_contract, producer_version, material_change_contract);
ALTER TABLE stock.source_d1_material_anchor_day_facts ADD PRIMARY KEY (id);
ALTER TABLE stock.source_d1_material_anchor_day_facts ADD CONSTRAINT source_d1_anchor_business_uq UNIQUE (d1_structure_id, created_session);
ALTER TABLE stock.source_forward_path_facts ADD PRIMARY KEY (id);
ALTER TABLE stock.source_forward_path_facts ADD CONSTRAINT source_forward_business_uq UNIQUE (source_capture_id, source_symbol, horizon, path_rule_version, calendar_revision);
ALTER TABLE stock.source_market_breadth_days ADD PRIMARY KEY (id);
ALTER TABLE stock.source_market_breadth_days ADD CONSTRAINT source_market_breadth_days_business_uq UNIQUE (source_capture_id, contract_version, calendar_revision);
ALTER TABLE stock.source_market_breadth_symbols ADD PRIMARY KEY (id);
ALTER TABLE stock.source_market_breadth_symbols ADD CONSTRAINT source_market_breadth_symbols_business_uq UNIQUE (breadth_day_id, source_symbol);
ALTER TABLE stock.source_pool_seed_day_facts ADD PRIMARY KEY (id);
ALTER TABLE stock.source_pool_seed_day_facts ADD CONSTRAINT source_pool_seed_business_uq UNIQUE (source_capture_id, source_symbol, profile_revision, producer_version);
ALTER TABLE stock.source_research_events ADD PRIMARY KEY (id);
ALTER TABLE stock.source_research_events ADD CONSTRAINT source_research_events_business_uq UNIQUE NULLS NOT DISTINCT (target_capture_id, source_symbol, event_type, anchor_capture_id, event_version, d1_history_session_count, calendar_revision);
ALTER TABLE stock.source_rule_evaluations ADD PRIMARY KEY (id);
ALTER TABLE stock.source_rule_evaluations ADD CONSTRAINT source_rule_evaluations_business_uq UNIQUE NULLS NOT DISTINCT (source_capture_id, source_symbol, rule_id, rule_version, profile_revision, history_session_count, d1_history_session_count, event_window_start, calendar_revision);
ALTER TABLE stock.source_sector_cross_period_pulse_facts ADD PRIMARY KEY (id);
ALTER TABLE stock.source_sector_cross_period_pulse_facts ADD CONSTRAINT source_sector_cross_period_pulse_business_uq UNIQUE (source_capture_id, catalog_id, profile_revision, algorithm_contract);
ALTER TABLE stock.source_sector_pulse_adjacent_transitions ADD PRIMARY KEY (id);
ALTER TABLE stock.source_sector_pulse_adjacent_transitions ADD CONSTRAINT source_sector_pulse_adjacent_business_uq UNIQUE (previous_pulse_id, target_pulse_id, transition_contract, calendar_id, calendar_revision);
ALTER TABLE stock.source_sector_pulse_followup_facts ADD PRIMARY KEY (id);
ALTER TABLE stock.source_sector_pulse_followup_facts ADD CONSTRAINT source_sector_pulse_followup_business_uq UNIQUE (pulse_fact_id, target_source_capture_id, horizon_sessions, followup_contract, calendar_id, calendar_revision);
ALTER TABLE stock.tdx_sector_source_snapshots ADD PRIMARY KEY (snapshot_id);
ALTER TABLE stock.tdx_sector_catalog ADD PRIMARY KEY (id);
ALTER TABLE stock.tdx_sector_catalog ADD CONSTRAINT tdx_sector_catalog_business_uq UNIQUE (snapshot_id, board_kind, board_native_id);
ALTER TABLE stock.tdx_sector_members ADD PRIMARY KEY (id);
ALTER TABLE stock.tdx_sector_members ADD CONSTRAINT tdx_sector_members_business_uq UNIQUE (catalog_id, source_symbol);

-- 以日期、包ID、证券或父ID支持现有范围查询，避免因删去旧宽键而退化为全表扫描。
CREATE INDEX source_raw_daily_session_package_symbol_idx ON stock.source_raw_daily (session_date, source_package_id, source_symbol);
CREATE INDEX source_symbol_price_profiles_session_package_symbol_idx ON stock.source_symbol_price_profiles (target_session, source_package_id, source_symbol, profile_revision);
CREATE INDEX source_d0_launch_session_package_symbol_idx ON stock.source_d0_launch_day_facts (target_session, source_package_id, source_symbol);
CREATE INDEX source_d0_scenario_session_package_scenario_idx ON stock.source_d0_scenario_day_facts (target_session, source_package_id, scenario, source_symbol);
CREATE INDEX source_d1_structure_session_package_symbol_idx ON stock.source_d1_structure_day_facts (target_session, source_package_id, source_symbol, structure_type);
CREATE INDEX source_d1_anchor_structure_created_idx ON stock.source_d1_material_anchor_day_facts (d1_structure_id, created_session);
CREATE INDEX source_forward_session_package_horizon_idx ON stock.source_forward_path_facts (target_session, source_package_id, horizon);
CREATE INDEX source_market_breadth_session_package_idx ON stock.source_market_breadth_days (target_session, source_package_id, contract_version);
CREATE INDEX source_pool_seed_session_package_profile_idx ON stock.source_pool_seed_day_facts (target_session, source_package_id, profile_revision, source_symbol);
CREATE INDEX source_research_events_session_symbol_idx ON stock.source_research_events (target_session, source_symbol, event_type);
CREATE INDEX source_rule_evaluations_session_package_rule_idx ON stock.source_rule_evaluations (target_session, source_package_id, rule_id, state);
CREATE INDEX source_sector_cross_period_pulse_session_catalog_idx ON stock.source_sector_cross_period_pulse_facts (target_session, catalog_id, algorithm_contract);
CREATE INDEX source_sector_pulse_adjacent_target_idx ON stock.source_sector_pulse_adjacent_transitions (target_pulse_id, previous_pulse_id);
CREATE INDEX source_sector_pulse_followup_parent_target_idx ON stock.source_sector_pulse_followup_facts (pulse_fact_id, target_session, horizon_sessions);

-- 重建简短父子外键，不保留重复的复合引用列。
ALTER TABLE stock.source_captures ADD FOREIGN KEY (source_package_id) REFERENCES stock.source_packages(source_package_id);
ALTER TABLE stock.source_raw_daily ADD FOREIGN KEY (source_package_id) REFERENCES stock.source_packages(source_package_id);
ALTER TABLE stock.source_symbol_price_profiles ADD FOREIGN KEY (source_package_id) REFERENCES stock.source_packages(source_package_id);
ALTER TABLE stock.source_symbol_price_profiles ADD FOREIGN KEY (source_raw_id) REFERENCES stock.source_raw_daily(id);
ALTER TABLE stock.tdx_sector_catalog ADD FOREIGN KEY (snapshot_id) REFERENCES stock.tdx_sector_source_snapshots(snapshot_id);
ALTER TABLE stock.tdx_sector_members ADD FOREIGN KEY (catalog_id) REFERENCES stock.tdx_sector_catalog(id);
ALTER TABLE stock.source_sector_cross_period_pulse_facts ADD FOREIGN KEY (source_package_id) REFERENCES stock.source_packages(source_package_id);
ALTER TABLE stock.source_sector_cross_period_pulse_facts ADD FOREIGN KEY (catalog_id) REFERENCES stock.tdx_sector_catalog(id);
ALTER TABLE stock.source_d1_material_anchor_day_facts ADD FOREIGN KEY (d1_structure_id) REFERENCES stock.source_d1_structure_day_facts(id);
ALTER TABLE stock.source_market_breadth_symbols ADD FOREIGN KEY (breadth_day_id) REFERENCES stock.source_market_breadth_days(id);
ALTER TABLE stock.source_sector_pulse_adjacent_transitions ADD FOREIGN KEY (previous_pulse_id) REFERENCES stock.source_sector_cross_period_pulse_facts(id);
ALTER TABLE stock.source_sector_pulse_adjacent_transitions ADD FOREIGN KEY (target_pulse_id) REFERENCES stock.source_sector_cross_period_pulse_facts(id);
ALTER TABLE stock.source_sector_pulse_followup_facts ADD FOREIGN KEY (pulse_fact_id) REFERENCES stock.source_sector_cross_period_pulse_facts(id);

DO $$
DECLARE
    item record;
    constraint_number integer := 0;
BEGIN
    -- 其余数值来源引用均收敛为单列BIGINT外键。
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND data_type = 'bigint'
          AND column_name LIKE '%capture_id'
          AND table_name <> 'source_captures'
        ORDER BY table_name, column_name
    LOOP
        constraint_number := constraint_number + 1;
        EXECUTE format(
            'ALTER TABLE stock.%I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES stock.source_captures(capture_id)',
            item.table_name, 'r06_capture_fk_' || constraint_number, item.column_name
        );
    END LOOP;
    constraint_number := 0;
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND data_type = 'bigint'
          AND column_name IN ('source_package_id', 'previous_source_package_id', 'target_source_package_id', 't0_source_package_id')
          AND table_name <> 'source_packages'
          AND NOT (table_name = 'source_raw_daily' AND column_name = 'source_package_id')
          AND NOT (table_name = 'source_captures' AND column_name = 'source_package_id')
          AND NOT (table_name = 'source_symbol_price_profiles' AND column_name = 'source_package_id')
          AND NOT (table_name = 'source_sector_cross_period_pulse_facts' AND column_name = 'source_package_id')
        ORDER BY table_name, column_name
    LOOP
        constraint_number := constraint_number + 1;
        EXECUTE format(
            'ALTER TABLE stock.%I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES stock.source_packages(source_package_id)',
            item.table_name, 'r06_package_fk_' || constraint_number, item.column_name
        );
    END LOOP;
END $$;

-- 为新增身份列及保留的包摘要补充中文数据库原生注释。
COMMENT ON TABLE stock.source_packages IS 'TDX原始下载/接收包登记；SHA-256只用于原件入口校验，下游仅引用数字包ID。';
COMMENT ON COLUMN stock.source_packages.source_package_id IS '原始数据包的PostgreSQL数字身份。';
COMMENT ON COLUMN stock.source_packages.package_sha256 IS 'TDX原始下载包接收完成后的字节SHA-256，仅用于导入前原件校验。';
COMMENT ON TABLE stock.source_captures IS '记录来源包的解析、可用时间、质量和捕获时钟。';
COMMENT ON COLUMN stock.source_captures.capture_id IS '来源捕获记录的BIGINT IDENTITY数字主键。';
COMMENT ON COLUMN stock.source_captures.source_package_id IS '对应的原始来源包数字ID。';
COMMENT ON COLUMN stock.source_captures.target_session IS '本次来源捕获覆盖的目标交易日；非逐日包捕获可为空。';
COMMENT ON COLUMN stock.source_packages.dataset_id IS '来源数据集稳定业务名称，用于区分不同TDX原始包类别。';
COMMENT ON COLUMN stock.source_packages.byte_count IS '原始包接收字节数；历史值无法从派生事实反推时为空，新包由原件接收入口记录。';
COMMENT ON TABLE stock.source_raw_daily IS '保存TDX代码级原始日线事实及其数值质量。';
COMMENT ON COLUMN stock.source_raw_daily.id IS 'Raw日线事实BIGINT IDENTITY数字主键。';
COMMENT ON COLUMN stock.source_raw_daily.source_package_id IS '产生该Raw事实的原始数据包数字ID。';
COMMENT ON COLUMN stock.source_raw_daily.source_capture_id IS '产生该Raw事实的来源捕获数字ID。';
COMMENT ON TABLE stock.source_symbol_price_profiles IS '保存Raw日线派生的逐股价格与流动性画像。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.id IS '价格画像事实BIGINT IDENTITY数字主键。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.source_package_id IS '画像输入来源包数字ID。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.source_capture_id IS '画像输入捕获数字ID。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.source_raw_id IS '画像对应的Raw日线事实数字ID。';
COMMENT ON COLUMN stock.source_symbol_price_profiles.profile_revision IS '显式价格画像算法修订标签；例如PROFILE_R08。';
COMMENT ON TABLE stock.tdx_sector_source_snapshots IS '记录TDX行业与GN概念成员来源快照的采集时间及质量。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.snapshot_id IS 'TDX成员来源快照BIGINT IDENTITY数字主键。';
COMMENT ON TABLE stock.tdx_sector_catalog IS '按成员快照保存的TDX板块目录事实。';
COMMENT ON COLUMN stock.tdx_sector_catalog.id IS 'TDX板块目录行BIGINT IDENTITY数字主键。';
COMMENT ON COLUMN stock.tdx_sector_catalog.snapshot_id IS '该TDX板块目录对应的来源快照BIGINT数字ID。';
COMMENT ON TABLE stock.tdx_sector_members IS '保存板块目录对应的证券成员事实。';
COMMENT ON COLUMN stock.tdx_sector_members.id IS 'TDX板块成员行BIGINT IDENTITY数字主键。';
COMMENT ON COLUMN stock.tdx_sector_members.catalog_id IS '所属TDX板块目录数字ID。';
COMMENT ON COLUMN stock.source_d1_material_anchor_day_facts.d1_structure_id IS '所属D1结构事实数字ID。';
COMMENT ON COLUMN stock.source_market_breadth_symbols.breadth_day_id IS '所属市场宽度日期事实数字ID。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.catalog_id IS 'PULSE对应TDX板块目录数字ID。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.previous_pulse_id IS '变化前PULSE事实数字ID。';
COMMENT ON COLUMN stock.source_sector_pulse_adjacent_transitions.target_pulse_id IS '变化后PULSE事实数字ID。';
COMMENT ON COLUMN stock.source_sector_pulse_followup_facts.pulse_fact_id IS '后继事实对应T0 PULSE父记录数字ID。';

DO $$
DECLARE item record;
BEGIN
    FOR item IN
        SELECT table_name
        FROM information_schema.columns
        WHERE table_schema = 'stock' AND column_name = 'id'
    LOOP
        EXECUTE format('COMMENT ON COLUMN stock.%I.id IS %L', item.table_name, '该业务事实行的BIGINT IDENTITY数字主键。');
    END LOOP;
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name IN ('source_package_id', 'previous_source_package_id', 'target_source_package_id', 't0_source_package_id')
          AND table_name <> 'source_packages'
    LOOP
        EXECUTE format('COMMENT ON COLUMN stock.%I.%I IS %L', item.table_name, item.column_name, '对应TDX原始来源包的BIGINT数字ID；不携带包SHA。');
    END LOOP;
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name LIKE '%capture_id'
          AND data_type = 'bigint'
          AND table_name <> 'source_captures'
    LOOP
        EXECUTE format('COMMENT ON COLUMN stock.%I.%I IS %L', item.table_name, item.column_name, '对应来源捕获记录的BIGINT数字ID。');
    END LOOP;
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'stock'
          AND column_name IN ('profile_revision', 'previous_profile_revision', 'target_profile_revision', 't0_profile_revision')
    LOOP
        EXECUTE format('COMMENT ON COLUMN stock.%I.%I IS %L', item.table_name, item.column_name, '显式价格画像算法修订标签，保留不同合法修订版本。');
    END LOOP;
END $$;

COMMENT ON TABLE stock.installation_identity IS '记录当前Stock A项目、部署实例及PostgreSQL簇身份。';
