-- R06：保存现役来源池计算器生成的板块跨日PULSE与精确横截面证据。
-- 只在既有stock_a.stock中前向迁移；使用当前NON_PIT成员快照，不生成正式Rotation阶段。
CREATE TABLE stock.source_sector_cross_period_pulse_facts (
    namespace_id uuid NOT NULL REFERENCES stock.namespaces(namespace_id),
    target_session date NOT NULL,
    package_sha256 char(64) NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    source_capture_id uuid NOT NULL,
    source_content_id char(64) NOT NULL CHECK (source_content_id ~ '^[0-9a-f]{64}$'),
    source_raw_digest char(64) NOT NULL CHECK (source_raw_digest ~ '^[0-9a-f]{64}$'),
    profile_revision char(64) NOT NULL CHECK (profile_revision ~ '^[0-9a-f]{64}$'),
    snapshot_id uuid NOT NULL REFERENCES stock.tdx_sector_source_snapshots(snapshot_id),
    board_kind text NOT NULL CHECK (board_kind IN ('INDUSTRY', 'CONCEPT')),
    board_native_id text NOT NULL CHECK (btrim(board_native_id) <> ''),
    board_name text NOT NULL CHECK (btrim(board_name) <> ''),
    algorithm_contract text NOT NULL CHECK (algorithm_contract = 'SOURCE_CROSS_PERIOD_PULSE_R1'),
    prior_ret5_session date NOT NULL,
    prior_breadth_session date NOT NULL,
    source_pool_count integer NOT NULL CHECK (source_pool_count > 0),
    snapshot_board_count integer NOT NULL CHECK (snapshot_board_count > 0),
    snapshot_source_quality text NOT NULL CHECK (btrim(snapshot_source_quality) <> ''),
    member_count integer NOT NULL CHECK (member_count >= 0),
    mapped_count integer NOT NULL CHECK (mapped_count >= 0),
    unmapped_count integer NOT NULL CHECK (unmapped_count >= 0),
    target_quote_count integer NOT NULL CHECK (target_quote_count >= 0),
    prior_ret5_quote_count integer NOT NULL CHECK (prior_ret5_quote_count >= 0),
    prior_breadth_quote_count integer NOT NULL CHECK (prior_breadth_quote_count >= 0),
    ret5_target_numerator numeric,
    ret5_target_denominator numeric,
    ret5_prior_numerator numeric,
    ret5_prior_denominator numeric,
    ret5_target_percentile_numerator numeric,
    ret5_target_percentile_denominator numeric,
    ret5_prior_percentile_numerator numeric,
    ret5_prior_percentile_denominator numeric,
    dq5_pp_numerator numeric,
    dq5_pp_denominator numeric,
    ret5_common_count integer NOT NULL CHECK (ret5_common_count >= 0),
    ret5_denominator integer NOT NULL CHECK (ret5_denominator >= 0),
    ret5_target_known_count integer NOT NULL CHECK (ret5_target_known_count >= 0),
    ret5_prior_known_count integer NOT NULL CHECK (ret5_prior_known_count >= 0),
    ret5_target_peer_count integer NOT NULL CHECK (ret5_target_peer_count >= 0),
    ret5_prior_peer_count integer NOT NULL CHECK (ret5_prior_peer_count >= 0),
    ret5_reason_codes text[] NOT NULL,
    breadth_target_numerator numeric,
    breadth_target_denominator numeric,
    breadth_prior_numerator numeric,
    breadth_prior_denominator numeric,
    breadth_delta1_numerator numeric,
    breadth_delta1_denominator numeric,
    breadth_common_count integer NOT NULL CHECK (breadth_common_count >= 0),
    breadth_denominator integer NOT NULL CHECK (breadth_denominator >= 0),
    breadth_target_known_count integer NOT NULL CHECK (breadth_target_known_count >= 0),
    breadth_prior_known_count integer NOT NULL CHECK (breadth_prior_known_count >= 0),
    breadth_reason_codes text[] NOT NULL,
    pulse_state text NOT NULL CHECK (pulse_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    pulse_reason_codes text[] NOT NULL,
    calculation_status text NOT NULL CHECK (calculation_status = 'COMPLETE'),
    research_mode text NOT NULL CHECK (research_mode = 'NON_PIT_RETROSPECTIVE'),
    membership_mode text NOT NULL CHECK (membership_mode = 'CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT'),
    price_coordinate text NOT NULL CHECK (price_coordinate = 'RAW_UNADJUSTED'),
    source_time_quality text NOT NULL CHECK (btrim(source_time_quality) <> ''),
    source_clock_role text NOT NULL CHECK (btrim(source_clock_role) <> ''),
    source_available_at timestamptz,
    formal_rotation_state text NOT NULL CHECK (formal_rotation_state = 'NOT_EVALUATED'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT source_sector_cross_period_pulse_pkey PRIMARY KEY (
        namespace_id,
        target_session,
        package_sha256,
        source_capture_id,
        source_content_id,
        source_raw_digest,
        profile_revision,
        snapshot_id,
        board_kind,
        board_native_id,
        algorithm_contract
    ),
    CONSTRAINT source_sector_cross_period_pulse_capture_fk
        FOREIGN KEY (namespace_id, source_capture_id)
        REFERENCES stock.source_captures(namespace_id, capture_id),
    CONSTRAINT source_sector_cross_period_pulse_content_fk
        FOREIGN KEY (namespace_id, source_content_id)
        REFERENCES stock.source_contents(namespace_id, content_id),
    CONSTRAINT source_sector_cross_period_pulse_board_fk
        FOREIGN KEY (snapshot_id, board_kind, board_native_id)
        REFERENCES stock.tdx_sector_catalog(snapshot_id, board_kind, board_native_id),
    CONSTRAINT source_sector_cross_period_pulse_member_counts_ck CHECK (
        mapped_count + unmapped_count = member_count
        AND target_quote_count <= member_count
        AND prior_ret5_quote_count <= member_count
        AND prior_breadth_quote_count <= member_count
        AND ret5_common_count <= ret5_denominator
        AND breadth_common_count <= breadth_denominator
        AND ret5_target_known_count <= ret5_denominator
        AND ret5_prior_known_count <= ret5_denominator
        AND breadth_target_known_count <= breadth_denominator
        AND breadth_prior_known_count <= breadth_denominator
    ),
    CONSTRAINT source_sector_cross_period_pulse_fraction_ck CHECK (
        (ret5_target_numerator IS NULL) = (ret5_target_denominator IS NULL)
        AND (ret5_target_denominator IS NULL OR ret5_target_denominator > 0)
        AND (ret5_prior_numerator IS NULL) = (ret5_prior_denominator IS NULL)
        AND (ret5_prior_denominator IS NULL OR ret5_prior_denominator > 0)
        AND (ret5_target_percentile_numerator IS NULL) = (ret5_target_percentile_denominator IS NULL)
        AND (ret5_target_percentile_denominator IS NULL OR ret5_target_percentile_denominator > 0)
        AND (ret5_prior_percentile_numerator IS NULL) = (ret5_prior_percentile_denominator IS NULL)
        AND (ret5_prior_percentile_denominator IS NULL OR ret5_prior_percentile_denominator > 0)
        AND (dq5_pp_numerator IS NULL) = (dq5_pp_denominator IS NULL)
        AND (dq5_pp_denominator IS NULL OR dq5_pp_denominator > 0)
        AND (breadth_target_numerator IS NULL) = (breadth_target_denominator IS NULL)
        AND (breadth_target_denominator IS NULL OR breadth_target_denominator > 0)
        AND (breadth_prior_numerator IS NULL) = (breadth_prior_denominator IS NULL)
        AND (breadth_prior_denominator IS NULL OR breadth_prior_denominator > 0)
        AND (breadth_delta1_numerator IS NULL) = (breadth_delta1_denominator IS NULL)
        AND (breadth_delta1_denominator IS NULL OR breadth_delta1_denominator > 0)
    )
);

CREATE INDEX source_sector_cross_period_pulse_lookup_idx
    ON stock.source_sector_cross_period_pulse_facts
    (namespace_id, target_session, package_sha256, snapshot_id, board_kind, board_native_id, algorithm_contract);

COMMENT ON TABLE stock.source_sector_cross_period_pulse_facts IS '保存现役来源池计算器按同类型完整板块快照生成的跨日PULSE及精确数值、分母和质量原因；仅NON_PIT来源级研究，不代表正式Rotation阶段。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.namespace_id IS '业务PG来源命名空间UUID；所有行绑定同一实际stock_a.stock项目身份。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.target_session IS '跨日PULSE对应的目标官方交易会话；计算窗口不晚于该日。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.package_sha256 IS '目标Raw来源包SHA-256；不同来源修订使用不同业务键。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.source_capture_id IS '目标会话对应的唯一Raw来源捕获UUID。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.source_content_id IS '目标会话引用的规范来源内容摘要SHA-256。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.source_raw_digest IS '目标Raw捕获规范行摘要SHA-256；用于识别同包内容变更。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.profile_revision IS '本次单日研究链实际绑定的完整来源画像修订SHA-256；仅记录来源版本，不改变PULSE输入公式。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.snapshot_id IS 'TDX行业或GN概念成员快照UUID；历史成员解释固定为后验NON_PIT。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.board_kind IS '同类板块类型枚举：INDUSTRY行业或CONCEPT对应的GN概念。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.board_native_id IS 'TDX来源目录中的原生板块编号；与精确快照及类型组成外键。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.board_name IS 'TDX来源快照中的板块名称；用于现有板块概要显示。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.algorithm_contract IS '冻结的来源级跨期横截面算法合同标识；不代表正式Rotation生命周期。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.prior_ret5_session IS 'dq5比较的前三个官方交易会话前端点；基于目标日同一Raw包。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.prior_breadth_session IS 'breadth_delta1比较的前一个官方交易会话端点；基于目标日同一Raw包。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.source_pool_count IS '目标Raw闭合来源池总行数，单位为来源代码行；不等于官方上市Universe。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.snapshot_board_count IS '该快照中本类型完整目录板块数；用于验证同行分母闭合。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.snapshot_source_quality IS 'TDX成员快照来源质量及未映射/排除计数；不伪称来源原件完整。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.member_count IS '该板块后验快照成员分母，单位为来源代码个数。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.mapped_count IS '成员中有本次来源历史输入的来源代码数；不表示稳定证券身份。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.unmapped_count IS '成员中没有本次来源历史输入的来源代码数；对应指标按现役三态规则处理。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.target_quote_count IS '目标会话具有合法RAW未复权OHLC的成员数。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.prior_ret5_quote_count IS 'ret5百分位前三会话前端点具有合法RAW未复权OHLC的成员数。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.prior_breadth_quote_count IS 'breadth_delta1前一会话端点具有合法RAW未复权OHLC的成员数。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_target_numerator IS '目标端同一共同成员集ret5中位数的精确分子；未知时为空。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_target_denominator IS '目标端ret5中位数的精确正分母；未知时为空。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_prior_numerator IS '前三会话前端同一共同成员集ret5中位数的精确分子；未知时为空。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_prior_denominator IS '前三会话前ret5中位数的精确正分母；未知时为空。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_target_percentile_numerator IS '目标端同类型同行中秩百分位的精确分子，单位为百分点。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_target_percentile_denominator IS '目标端同行中秩百分位的精确正分母。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_prior_percentile_numerator IS '前三会话前端同类型同行中秩百分位的精确分子，单位为百分点。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_prior_percentile_denominator IS '前三会话前同行中秩百分位的精确正分母。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.dq5_pp_numerator IS 'dq5百分位变化的精确整数分子，单位为百分点；UNKNOWN时为空。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.dq5_pp_denominator IS 'dq5百分位变化的精确正分母；UNKNOWN时为空。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_common_count IS '目标端与前三会话前端均有有效ret5的共同成员数。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_denominator IS 'ret5共同成员覆盖率使用的完整板块成员分母。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_target_known_count IS '目标端可计算ret5的板块成员数。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_prior_known_count IS '前三会话前端可计算ret5的板块成员数。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_target_peer_count IS '目标端有有效ret5并参与同类型中秩的板块数。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_prior_peer_count IS '前三会话前端有有效ret5并参与同类型中秩的板块数。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.ret5_reason_codes IS 'ret5/dq5指标未知的具名质量原因数组；正常时为空数组。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.breadth_target_numerator IS '目标端共同成员正ret1比例的精确分子；未知时为空。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.breadth_target_denominator IS '目标端共同成员正ret1比例的精确分母；未知时为空。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.breadth_prior_numerator IS '前一官方会话端共同成员正ret1比例的精确分子；未知时为空。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.breadth_prior_denominator IS '前一官方会话端共同成员正ret1比例的精确分母；未知时为空。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.breadth_delta1_numerator IS '目标端与前一会话breadth变化的精确分子；单位为比例点。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.breadth_delta1_denominator IS 'breadth_delta1精确正分母；未知时为空。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.breadth_common_count IS '目标端与前一会话均有有效ret1的共同成员数。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.breadth_denominator IS 'breadth_delta1覆盖率使用的完整板块成员分母。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.breadth_target_known_count IS '目标端可计算ret1的板块成员数。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.breadth_prior_known_count IS '前一官方会话端可计算ret1的板块成员数。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.breadth_reason_codes IS 'breadth_delta1未知的具名质量原因数组；正常时为空数组。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.pulse_state IS '现役来源级PULSE三态：TRUE、FALSE或UNKNOWN；采用dq5>=10且breadth_delta1>=0.05。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.pulse_reason_codes IS 'PULSE三态归约及输入缺失原因码；不表示正式Rotation阶段。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.calculation_status IS '本行完整离线计算状态；只有整类快照原子写入后才记录COMPLETE。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.research_mode IS '研究时间语义固定为NON_PIT_RETROSPECTIVE；不作历史时点预测。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.membership_mode IS '成员关系模式固定为当前TDX快照后验回看；不作历史PIT成分。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.price_coordinate IS '计算价格坐标固定为RAW_UNADJUSTED原始未复权。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.source_time_quality IS '目标Raw捕获来源时钟质量；保留原始质量，不倒签available_at。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.source_clock_role IS '目标Raw捕获时钟角色；区分本地观察与真实来源可用时刻。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.source_available_at IS '仅记录来源实际已验证可用时刻；未知时为空。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.formal_rotation_state IS '正式Rotation生命周期固定为NOT_EVALUATED；本事实不冒充阶段状态。';
COMMENT ON COLUMN stock.source_sector_cross_period_pulse_facts.recorded_at IS '该离线typed事实首次写入PG的带时区记录时刻，不是行情可用时刻。';
COMMENT ON INDEX stock.source_sector_cross_period_pulse_lookup_idx IS '按目标会话、来源修订、板块快照和原生板块编号批量读取跨日脉冲。';

ALTER TABLE stock.source_sector_cross_period_pulse_facts OWNER TO stock_a;
ALTER INDEX stock.source_sector_cross_period_pulse_lookup_idx OWNER TO stock_a;
