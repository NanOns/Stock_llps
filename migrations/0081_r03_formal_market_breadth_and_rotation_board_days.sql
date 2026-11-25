-- R03 正式市场宽度日级事实与 Rotation 板块日级状态表（支持无 Episode 日级状态与正式市场宽度持久化）

-- 1. 正式市场宽度日级事实表（与固定来源代码池 source_market_breadth 独立）
CREATE TABLE stock.r03_formal_market_breadth_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trade_date date NOT NULL,
    scope_key text NOT NULL,
    universe_version_t text NOT NULL,
    universe_version_t3 text NOT NULL,
    contract_version text NOT NULL,
    status text NOT NULL CHECK (status IN ('OBSERVED', 'UNKNOWN')),
    reason_code text,
    n_t integer NOT NULL CHECK (n_t >= 0),
    n_t3 integer NOT NULL CHECK (n_t3 >= 0),
    n_common integer NOT NULL CHECK (n_common >= 0),
    n_eligible integer NOT NULL CHECK (n_eligible >= 0),
    unknown_count integer NOT NULL CHECK (unknown_count >= 0),
    exact_numerator bigint,
    exact_denominator bigint CHECK (exact_denominator IS NULL OR exact_denominator > 0),
    breadth_value numeric(18, 10),
    recorded_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (trade_date, scope_key, universe_version_t, universe_version_t3, contract_version)
);

COMMENT ON TABLE stock.r03_formal_market_breadth_facts IS
    'R03 正式市场宽度日级事实表：基于 t 与 t-3 各自日期有效 Research Universe 共同集合 E 计算 mean(I(ret1_t>0)-I(ret1_t-3>0))，与固定来源池诊断完全分离。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.id IS '单列自增主键。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.trade_date IS '目标官方交易日 t。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.scope_key IS '上游捕获、Profile 修订与 PIT 口径绑定键。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.universe_version_t IS 't 日日期有效 Research Universe 版本标识。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.universe_version_t3 IS 't-3 日日期有效 Research Universe 版本标识。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.contract_version IS 'R03 正式合同版本。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.status IS '宽度计算质量状态：OBSERVED（可观测）或 UNKNOWN（共同集合为空或无可评估样本）。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.reason_code IS '不可观测或降级原因代码（可空）。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.n_t IS 't 日有效研究总体成员数（单位：只）。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.n_t3 IS 't-3 日有效研究总体成员数（单位：只）。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.n_common IS 't 与 t-3 共同实体集合 E 大小（单位：只）。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.n_eligible IS 'E 中双端 ret1 均可观测的可评估实体数（单位：只）。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.unknown_count IS 'E 中任一端 ret1 不可观测的实体数（单位：只）。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.exact_numerator IS '正式宽度精确分数分子（I(ret1_t>0)-I(ret1_t-3>0) 之和）。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.exact_denominator IS '正式宽度精确分数分母（等于 n_eligible，正整数）。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.breadth_value IS '正式市场宽度数值，区间 [-1, 1]（单位：比例）。';
COMMENT ON COLUMN stock.r03_formal_market_breadth_facts.recorded_at IS '事实落库或修订时间戳（UTC/带时区）。';

-- 2. R03 Rotation 板块日级状态表（覆盖全部板块，包含 NONE、出生失败 UNKNOWN 与缺前日 UNKNOWN）
CREATE TABLE stock.r03_sector_rotation_board_day_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trade_date date NOT NULL,
    board_kind text NOT NULL CHECK (board_kind IN ('INDUSTRY', 'CONCEPT')),
    board_id text NOT NULL,
    scope_key text NOT NULL,
    member_version text NOT NULL,
    contract_version text NOT NULL,
    episode_id bigint REFERENCES stock.r03_sector_rotation_episodes(id) ON DELETE SET NULL,
    rotation_state text NOT NULL,
    pulse_raw_state text NOT NULL CHECK (pulse_raw_state IN ('TRUE', 'FALSE', 'UNKNOWN')),
    last_known_state text,
    reason_code text,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (trade_date, board_kind, board_id, scope_key, member_version, contract_version)
);

COMMENT ON TABLE stock.r03_sector_rotation_board_day_facts IS
    'R03 Rotation 板块日级完整状态事实表：记录当日每个合法板块的轮动状态，包括无 Episode 的 NONE、出生失败 UNKNOWN、缺前日 UNKNOWN 以及关联到具体 Episode 的活跃/终态。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.id IS '单列自增主键。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.trade_date IS '目标官方交易日。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.board_kind IS '板块类型：INDUSTRY 或 CONCEPT。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.board_id IS '板块原生编码。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.scope_key IS '上游捕获、Profile 修订与 PIT 口径绑定键。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.member_version IS '板块成员快照版本键。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.contract_version IS 'R03 正式合同版本。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.episode_id IS '关联的 Rotation Episode 主键；NONE 或出生失败时为 NULL。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.rotation_state IS '当日板块轮动状态（NONE/PULSE/IN/ACCEPTED/EXPANDING/REACCELERATING/FAILED/OUT/UNKNOWN）。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.pulse_raw_state IS '当日原始脉冲信号三态：TRUE、FALSE 或 UNKNOWN。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.last_known_state IS '最近已知轮动状态（用于 UNKNOWN 期间保留展示前态，可空）。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.reason_code IS '状态转换、出生拒绝或 UNKNOWN 原因代码（可空）。';
COMMENT ON COLUMN stock.r03_sector_rotation_board_day_facts.recorded_at IS '事实落库或修订时间戳（UTC/带时区）。';
