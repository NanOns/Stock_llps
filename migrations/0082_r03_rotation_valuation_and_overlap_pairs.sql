-- R03 Rotation 停牌估值质量与 quote_age 字段及 Overlap 板块对 Jaccard 持久化表

ALTER TABLE stock.r03_sector_rotation_day_states
    ADD COLUMN valuation_quality text NOT NULL DEFAULT 'UNKNOWN'
        CHECK (valuation_quality IN ('OBSERVED', 'MARKED_ESTIMATE', 'UNKNOWN')),
    ADD COLUMN max_quote_age integer
        CHECK (max_quote_age IS NULL OR max_quote_age >= 0);

COMMENT ON COLUMN stock.r03_sector_rotation_day_states.valuation_quality IS
    '篮子估值质量：OBSERVED（全员实际行情）、MARKED_ESTIMATE（含<=20会话已证停牌末次实际价估值）或 UNKNOWN（缺终值/超20会话/未知状态）。';
COMMENT ON COLUMN stock.r03_sector_rotation_day_states.max_quote_age IS
    '篮子成员最大行情滞后市场会话数（全员当日 ACTUAL 时为 0，含停牌估值时为最大停牌会话 age，UNKNOWN 时可空）。';

CREATE TABLE stock.r03_sector_overlap_pair_facts (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    trade_date date NOT NULL,
    board_kind text NOT NULL CHECK (board_kind IN ('INDUSTRY', 'CONCEPT')),
    board_id_a text NOT NULL,
    board_id_b text NOT NULL,
    scope_key text NOT NULL,
    member_version text NOT NULL,
    contract_version text NOT NULL,
    jaccard_status text NOT NULL CHECK (jaccard_status IN ('OBSERVED', 'UNKNOWN')),
    jaccard_value numeric,
    reason_code text,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT r03_sector_overlap_pair_facts_order_ck CHECK (board_id_a < board_id_b),
    CONSTRAINT r03_sector_overlap_pair_facts_uq UNIQUE (trade_date, board_kind, board_id_a, board_id_b, scope_key, member_version, contract_version)
);

ALTER TABLE stock.r03_sector_overlap_pair_facts OWNER TO stock_a;
COMMENT ON TABLE stock.r03_sector_overlap_pair_facts IS
    '保存 ALG-25 同类型合格板块对两两 Jaccard 重叠度及三态质量，供普通消费者与详情接口 O(1) 只读，避免 GET 全池重算。';
COMMENT ON COLUMN stock.r03_sector_overlap_pair_facts.id IS '单列自增主键。';
COMMENT ON COLUMN stock.r03_sector_overlap_pair_facts.trade_date IS '目标官方交易日。';
COMMENT ON COLUMN stock.r03_sector_overlap_pair_facts.board_kind IS '板块类型（INDUSTRY 或 CONCEPT）。';
COMMENT ON COLUMN stock.r03_sector_overlap_pair_facts.board_id_a IS '规范字典序较小的板块原生编码。';
COMMENT ON COLUMN stock.r03_sector_overlap_pair_facts.board_id_b IS '规范字典序较大的板块原生编码。';
COMMENT ON COLUMN stock.r03_sector_overlap_pair_facts.scope_key IS '上游捕获、Profile 修订与 PIT 口径绑定键。';
COMMENT ON COLUMN stock.r03_sector_overlap_pair_facts.member_version IS '板块成员快照版本键。';
COMMENT ON COLUMN stock.r03_sector_overlap_pair_facts.contract_version IS 'R03 正式合同版本。';
COMMENT ON COLUMN stock.r03_sector_overlap_pair_facts.jaccard_status IS 'Jaccard 边质量状态：OBSERVED 或 UNKNOWN。';
COMMENT ON COLUMN stock.r03_sector_overlap_pair_facts.jaccard_value IS 'Jaccard 精确数值 |M_A∩M_B|/|M_A∪M_B|，UNKNOWN 时为 NULL。';
COMMENT ON COLUMN stock.r03_sector_overlap_pair_facts.reason_code IS 'UNKNOWN 原因码（可空）。';
COMMENT ON COLUMN stock.r03_sector_overlap_pair_facts.recorded_at IS '事实落库或修订时间戳（UTC/带时区）。';
