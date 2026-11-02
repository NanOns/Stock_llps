-- R03：保存本机TDX行业和GN概念当前成员快照；所有时态均保持NON_PIT。
-- 仅使用stock_a登录账号在既有stock schema中前向迁移，不碰其它业务表。
CREATE TABLE stock.tdx_sector_source_snapshots (
    snapshot_id uuid PRIMARY KEY,
    source_kind text NOT NULL CHECK (source_kind IN ('TDX_TDXHY_CFG', 'TDX_INFOHARBOR_BLOCK_DAT')),
    source_file_sha256 char(64) NOT NULL CHECK (source_file_sha256 ~ '^[0-9a-f]{64}$'),
    source_path_role text NOT NULL,
    source_file_size bigint NOT NULL CHECK (source_file_size >= 0),
    observed_at timestamptz NOT NULL,
    source_valid_on date,
    source_known_at timestamptz,
    source_available_at timestamptz,
    clock_quality text NOT NULL CHECK (clock_quality IN ('UNKNOWN', 'NON_PIT_OBSERVED', 'VERIFIED_SOURCE_TIME')),
    research_mode text NOT NULL CHECK (research_mode = 'NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT'),
    parse_contract_version text NOT NULL,
    rows_declared bigint NOT NULL CHECK (rows_declared >= 0),
    source_quality text NOT NULL,
    UNIQUE (source_kind, source_file_sha256, parse_contract_version),
    CHECK (source_available_at IS NULL OR clock_quality = 'VERIFIED_SOURCE_TIME'),
    CHECK (
        clock_quality <> 'UNKNOWN'
        OR (source_valid_on IS NULL AND source_known_at IS NULL AND source_available_at IS NULL)
    )
);

COMMENT ON TABLE stock.tdx_sector_source_snapshots IS '保存TDX行业或概念成员来源修订、文件摘要、观察时钟与非PIT质量；不证明历史有效期或稳定证券身份。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.snapshot_id IS '本来源成员快照的稳定UUID；由来源类型、原件SHA-256和解析合同确定。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.source_kind IS '来源文件类型枚举：TDX_TDXHY_CFG行业分配或TDX_INFOHARBOR_BLOCK_DAT中的GN概念成员。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.source_file_sha256 IS '直接成员原件字节的SHA-256小写64位摘要；行业名称辅助目录摘要记录在source_path_role中。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.source_path_role IS '来源文件相对TDX安装根的角色说明及必要辅助目录摘要；不是业务主键或运行时回读依赖。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.source_file_size IS '直接成员原件读取时的字节数，不包含辅助目录文件大小。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.observed_at IS '本机完整读取与验证该成员来源的带时区观察时间；不代表供应方发布时间。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.source_valid_on IS '来源成员关系已证明生效的日期；缺少来源有效期事实时为空。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.source_known_at IS '来源方首次已知该成员关系的时间；文件时间戳不能替代，缺证据时为空。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.source_available_at IS '来源方真实可获取该成员关系的时间；本机读取或文件修改时间不能替代。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.clock_quality IS '来源时钟质量：UNKNOWN未知、NON_PIT_OBSERVED后验观察或VERIFIED_SOURCE_TIME已验证来源时间。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.research_mode IS '成员关系研究模式；当前只允许NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT，不作为历史PIT成员。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.parse_contract_version IS '解析行业分配或GN概念成员时实际使用的稳定合同版本。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.rows_declared IS '原件中该来源类别声明并验证的记录数；行业按分配行，概念按GN成员token。';
COMMENT ON COLUMN stock.tdx_sector_source_snapshots.source_quality IS '来源质量枚举及互斥未映射/排除计数说明；不以已知子集伪称完整。';

CREATE TABLE stock.tdx_sector_catalog (
    snapshot_id uuid NOT NULL REFERENCES stock.tdx_sector_source_snapshots(snapshot_id),
    board_kind text NOT NULL CHECK (board_kind IN ('INDUSTRY', 'CONCEPT')),
    board_native_id text NOT NULL CHECK (btrim(board_native_id) <> ''),
    board_name text NOT NULL CHECK (btrim(board_name) <> ''),
    PRIMARY KEY (snapshot_id, board_kind, board_native_id)
);

COMMENT ON TABLE stock.tdx_sector_catalog IS '保存经来源头类型或TDX行业目录核实的行业/概念板块名称与原生板块编号；排除风格和指数组。';
COMMENT ON COLUMN stock.tdx_sector_catalog.snapshot_id IS '引用stock.tdx_sector_source_snapshots的精确来源快照UUID。';
COMMENT ON COLUMN stock.tdx_sector_catalog.board_kind IS '板块类型，仅INDUSTRY行业或CONCEPT概念；不包含STYLE风格、指数组或未知类型。';
COMMENT ON COLUMN stock.tdx_sector_catalog.board_native_id IS '来源原生板块编号；行业来自tdxzs.cfg映射，概念来自GN头记录。';
COMMENT ON COLUMN stock.tdx_sector_catalog.board_name IS '来源文件记录的行业或概念名称；不推导行业优先级或主题语义。';

CREATE TABLE stock.tdx_sector_members (
    snapshot_id uuid NOT NULL,
    board_kind text NOT NULL CHECK (board_kind IN ('INDUSTRY', 'CONCEPT')),
    board_native_id text NOT NULL,
    source_symbol text NOT NULL CHECK (source_symbol ~ '^(SH|SZ|BJ)\.[0-9]{6}$'),
    membership_quality text NOT NULL CHECK (membership_quality = 'OBSERVED_SOURCE_RELATION'),
    source_record_ordinal bigint NOT NULL CHECK (source_record_ordinal > 0),
    PRIMARY KEY (snapshot_id, board_kind, board_native_id, source_symbol),
    FOREIGN KEY (snapshot_id, board_kind, board_native_id)
        REFERENCES stock.tdx_sector_catalog(snapshot_id, board_kind, board_native_id)
);

COMMENT ON TABLE stock.tdx_sector_members IS '保存原件可逐行复现的当前行业/概念来源代码关系；不表示稳定证券实体、上市Universe或历史有效成员。';
COMMENT ON COLUMN stock.tdx_sector_members.snapshot_id IS '引用成员所属的精确TDX来源快照UUID。';
COMMENT ON COLUMN stock.tdx_sector_members.board_kind IS '成员关系对应的板块类型：INDUSTRY或CONCEPT。';
COMMENT ON COLUMN stock.tdx_sector_members.board_native_id IS '成员关系对应的来源原生板块编号，须匹配同快照目录键。';
COMMENT ON COLUMN stock.tdx_sector_members.source_symbol IS '显式来源交易所编号映射后的来源代码，格式SH/SZ/BJ加六位代码；不等于稳定security_id。';
COMMENT ON COLUMN stock.tdx_sector_members.membership_quality IS '成员关系质量固定为OBSERVED_SOURCE_RELATION，表示原件直接记录，不代表有效时态或PIT。';
COMMENT ON COLUMN stock.tdx_sector_members.source_record_ordinal IS '成员来源记录序号：行业为tdxhy.cfg行号，概念为文件中的成员token顺序。';

ALTER TABLE stock.tdx_sector_source_snapshots OWNER TO stock_a;
ALTER TABLE stock.tdx_sector_catalog OWNER TO stock_a;
ALTER TABLE stock.tdx_sector_members OWNER TO stock_a;
