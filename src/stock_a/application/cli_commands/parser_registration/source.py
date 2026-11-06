"""Stock A CLI命令按实际子域注册。"""

from __future__ import annotations

from stock_a.storage.postgres.source_raw import (
    RAW_DAILY_APPEND_TASK_SCOPES,
    RawSourcePgError,
    import_verified_vipdata_target,
    import_verified_vipdata_history,
    read_raw_daily_catalog,
    read_raw_daily_page,
    read_raw_daily_history_page,
    read_raw_daily_history_stock,
    read_raw_daily_stock,
)

from stock_a.application.cli_commands.parser import ChineseParser


def register_commands(commands):
    raw_import = commands.add_parser(
        "source-vipdata-persist",
        help="按稳定Raw每日APPEND操作将已验证VIPDATA单日观察写入固定PG typed表",
        description="只接受已登记的稳定Raw每日APPEND操作标识、显式G盘ZIP和目标交易会话；最多5000候选，分块写stock_a.stock.source_raw_daily。",
    )
    raw_import.add_argument(
        "--task-scope",
        required=True,
        choices=RAW_DAILY_APPEND_TASK_SCOPES,
        help="仅登记Raw每日APPEND操作及过渡兼容标识",
    )
    raw_import.add_argument(
        "--vipdata-package",
        required=True,
        help="G:/codex_tmp内已存在且只读的VIPDATA ZIP绝对路径",
    )
    raw_import.add_argument(
        "--vipdata-package-sha256", required=True, help="完整ZIP的64位小写SHA-256"
    )
    raw_import.add_argument(
        "--max-vipdata-validation-seconds",
        type=float,
        default=300.0,
        help="本地ZIP SHA与CRC完整复验秒数上限，默认300",
    )
    raw_import.add_argument(
        "--target-session", required=True, help="必填目标交易会话，格式YYYY-MM-DD"
    )
    raw_import.add_argument(
        "--history-session-count",
        type=int,
        default=1,
        help="显式官方日历历史窗口会话数，1保持单日行为，最多75；每个VIPDATA条目只扫描一次",
    )
    raw_import.add_argument(
        "--max-source-quality-bytes",
        type=int,
        default=4294967296,
        help="全来源候选条目解压字节上限，默认4GiB",
    )
    raw_import.add_argument(
        "--max-source-quality-records",
        type=int,
        default=125000000,
        help="全来源候选记录上限，默认125000000",
    )
    raw_import.add_argument(
        "--max-source-quality-seconds",
        type=float,
        default=1800.0,
        help="单遍全来源目标日扫描秒数上限，默认1800",
    )
    raw_import.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="每个可恢复PG事务最多写入行数，固定上限100",
    )
    raw_catalog = commands.add_parser(
        "source-raw-catalog",
        help="只读查询PG中Stock A Raw来源日期版本目录",
    )
    raw_catalog.add_argument(
        "--research-cutoff",
        required=True,
        help="必填研究截止会话，格式YYYY-MM-DD",
    )
    raw_catalog.add_argument("--year", type=int, help="可选年份筛选Raw目录")
    raw_catalog.add_argument("--offset", type=int, default=0, help="版本页偏移，默认0")
    raw_catalog.add_argument(
        "--limit", type=int, default=100, help="每页版本数，默认100，最多200"
    )
    sector_members = commands.add_parser(
        "source-tdx-sector-members",
        help="接入或只读查询本机与远端TDX行业、概念成员及精确PG研究上下文",
    )
    sector_actions = sector_members.add_subparsers(
        dest="sector_action", parser_class=ChineseParser, required=True
    )
    sector_ingest = sector_actions.add_parser(
        "ingest", help="只读解析唯一配置TDX根并幂等写入三个成员表"
    )
    sector_ingest.add_argument(
        "--confirm-task-scope",
        required=True,
        choices=("R03_TDX_REAL_SECTOR_MEMBERS_TO_PG_R1",),
        help="显式绑定本批已批准任务范围",
    )
    sector_refresh = sector_actions.add_parser(
        "refresh-remote",
        help="动态发现并校验远端TDX行业与概念成员来源",
    )
    sector_refresh.add_argument(
        "--confirm-task-scope",
        required=True,
        choices=("R03_TDX_HEADLESS_SECTOR_SOURCE_REFRESH_R1",),
        help="显式绑定本批已批准远端成员刷新范围",
    )
    sector_refresh.add_argument(
        "--validate-only",
        action="store_true",
        help="只下载并校验远端成员原件、差异与PG身份，不执行INSERT",
    )
    sector_refresh.add_argument(
        "--validated-source-sha256",
        action="append",
        metavar="文件=SHA256",
        help="绑定TRF01核验的远端文件原件SHA；同一文件名只可出现一次",
    )
    sector_refresh.add_argument(
        "--oracle-set-sha256",
        action="append",
        metavar="INDUSTRY|CONCEPT=SHA256",
        help="绑定TRF02独立目录与成员集合SHA；新增PG版本时必填",
    )
    sector_catalog = sector_actions.add_parser(
        "catalog", help="分页查询精确行业或概念来源快照目录"
    )
    sector_catalog.add_argument("--snapshot-id", required=True)
    sector_catalog.add_argument("--board-kind", choices=("INDUSTRY", "CONCEPT"))
    sector_catalog.add_argument("--search", help="名称子串或原生编号")
    sector_catalog.add_argument("--offset", type=int, default=0)
    sector_catalog.add_argument("--limit", type=int, default=100)
    sector_query = sector_actions.add_parser(
        "members", help="读取精确板块成员与Core/Profile状态"
    )
    sector_query.add_argument("--snapshot-id", required=True)
    sector_query.add_argument(
        "--board-kind", required=True, choices=("INDUSTRY", "CONCEPT")
    )
    sector_query.add_argument("--board-native-id", required=True)
    sector_query.add_argument("--target-session", required=True)
    sector_query.add_argument("--research-cutoff", required=True)
    sector_query.add_argument("--package-sha256", required=True)
    sector_query.add_argument("--profile-revision")
    sector_query.add_argument("--offset", type=int, default=0)
    sector_query.add_argument("--limit", type=int, default=100)
    sector_breadth = sector_actions.add_parser(
        "breadth", help="读取真实行业或概念完整成员的两日MA20广度及关系变化"
    )
    sector_breadth.add_argument("--snapshot-id", required=True)
    sector_breadth.add_argument(
        "--board-kind", required=True, choices=("INDUSTRY", "CONCEPT")
    )
    sector_breadth.add_argument(
        "--board-native-id", help="可选精确原生板块编号；省略时按板块分页"
    )
    sector_breadth.add_argument("--before-session", required=True)
    sector_breadth.add_argument("--after-session", required=True)
    sector_breadth.add_argument("--research-cutoff", required=True)
    sector_breadth.add_argument("--before-package-sha256", required=True)
    sector_breadth.add_argument("--before-profile-revision", required=True)
    sector_breadth.add_argument("--after-package-sha256", required=True)
    sector_breadth.add_argument("--after-profile-revision", required=True)
    sector_breadth.add_argument("--offset", type=int, default=0)
    sector_breadth.add_argument("--limit", type=int, default=30)
    sector_breadth.add_argument("--member-offset", type=int, default=0)
    sector_breadth.add_argument("--member-limit", type=int, default=30)
    sector_breadth.add_argument(
        "--include-core-transitions",
        action="store_true",
        help="附带每板块的Core跨日变化分母、覆盖原因及四类变化计数",
    )
    sector_breadth.add_argument(
        "--core-transition-kind",
        choices=(
            "ALL",
            "DAMAGE_NEW",
            "DAMAGE_RECOVERED",
            "SLOPE_NEG_TO_POS",
            "SLOPE_POS_TO_NEG",
        ),
        default="ALL",
        help="选择指定板块的Core变化成员分页类型；非ALL须指定板块并启用Core变化",
    )
    sector_strength = sector_actions.add_parser(
        "strength",
        help="按精确当前TDX成员快照比较5/20会话来源级Core收益与三会话前变化",
    )
    sector_strength.add_argument("--snapshot-id", required=True)
    sector_strength.add_argument(
        "--board-kind", required=True, choices=("INDUSTRY", "CONCEPT")
    )
    sector_strength.add_argument(
        "--board-native-id", help="精确原生板块编号；省略时查询所选类型全部板块"
    )
    sector_strength.add_argument("--target-session", required=True)
    sector_strength.add_argument("--research-cutoff", required=True)
    sector_strength.add_argument("--package-sha256", required=True)
    sector_strength.add_argument("--profile-revision")
    sector_strength.add_argument(
        "--sort-by",
        choices=("delta20", "delta5", "ret20", "ret5"),
        default="delta20",
        help="排序字段：同集合20/5会话变化或目标日20/5会话收益中位数",
    )
    sector_strength.add_argument(
        "--direction",
        choices=("STRONGER", "WEAKER"),
        default="STRONGER",
        help="按数值从高到低展示变强方向，或从低到高展示变弱方向",
    )
    sector_strength.add_argument(
        "--changed-only",
        action="store_true",
        help="只显示共同可评估集合发生非零变化的板块",
    )
    sector_strength.add_argument("--offset", type=int, default=0)
    sector_strength.add_argument("--limit", type=int, default=30)
    sector_strength.add_argument("--member-offset", type=int, default=0)
    sector_strength.add_argument(
        "--member-limit",
        type=int,
        default=30,
        help="每板块成员页大小，0表示只显示板块汇总，最多100",
    )
    sector_change_stocks = sector_actions.add_parser(
        "change-stocks",
        help="全量联接行业概念强弱变化与同一来源包D1支撑变化股票",
    )
    sector_change_stocks.add_argument("--industry-snapshot-id", required=True)
    sector_change_stocks.add_argument("--concept-snapshot-id", required=True)
    sector_change_stocks.add_argument(
        "--target-session", required=True, help="目标官方交易会话YYYY-MM-DD"
    )
    sector_change_stocks.add_argument(
        "--research-cutoff", required=True, help="显式研究截止会话YYYY-MM-DD"
    )
    sector_change_stocks.add_argument("--package-sha256", required=True)
    sector_change_stocks.add_argument("--profile-revision")
    sector_change_stocks.add_argument(
        "--event-window-start", required=True, help="D1历史事件窗口起始官方会话"
    )
    sector_change_stocks.add_argument(
        "--pg-history-session-count",
        type=int,
        default=75,
        help="读取包含目标日的官方历史窗口，1至75，默认75",
    )
    sector_change_stocks.add_argument(
        "--board-kind",
        choices=("INDUSTRY", "CONCEPT"),
        help="可选限制板块榜单类型；省略时同时查询全部行业与GN概念",
    )
    sector_change_stocks.add_argument(
        "--detail-board-kind",
        choices=("INDUSTRY", "CONCEPT"),
        help="指定板块明细类型，须与--detail-board-native-id同时使用",
    )
    sector_change_stocks.add_argument(
        "--detail-board-native-id", help="指定一个精确原生板块查看板内股票状态"
    )
    sector_change_stocks.add_argument(
        "--sort-by",
        choices=(
            "changed_stocks",
            "delta20",
            "delta5",
            "ret20",
            "ret5",
            "d0_launch_new",
        ),
        default="changed_stocks",
        help="排序字段；d0_launch_new为板内D0 FALSE_TO_TRUE新增来源成员数",
    )
    sector_change_stocks.add_argument(
        "--direction",
        choices=("STRONGER", "WEAKER"),
        default="STRONGER",
        help="STRONGER按数值从多到少，WEAKER按数值从少到多",
    )
    sector_change_stocks.add_argument(
        "--changed-only",
        action="store_true",
        help="仅显示至少有一只板内成员确认支撑变化的板块",
    )
    sector_change_stocks.add_argument("--offset", type=int, default=0)
    sector_change_stocks.add_argument("--limit", type=int, default=30)
    sector_change_stocks.add_argument(
        "--member-state",
        choices=(
            "ALL",
            "CHANGED",
            "UNCHANGED",
            "UNKNOWN",
            "NOT_COMPARABLE",
            "NOT_EVALUABLE",
        ),
        default="CHANGED",
        help="板内股票状态过滤；默认显示已确认CHANGED股票",
    )
    sector_change_stocks.add_argument("--member-offset", type=int, default=0)
    sector_change_stocks.add_argument("--member-limit", type=int, default=30)
    sector_change_stocks.add_argument(
        "--include-changed-stock-page",
        action="store_true",
        help="附加按来源代码全局去重的CHANGED个股分页，不影响板块榜单",
    )
    sector_change_stocks.add_argument(
        "--include-d0-launch-new",
        action="store_true",
        help="可选附加D0相邻会话FALSE_TO_TRUE新增启动股票及当前板块成员联动",
    )
    sector_change_stocks.add_argument(
        "--d0-new-only",
        action="store_true",
        help="配合--include-d0-launch-new，仅显示有D0新增启动成员的板块",
    )
    sector_change_stocks.add_argument(
        "--d0-before-session",
        help="启用D0新增启动联动时的前一官方会话YYYY-MM-DD",
    )
    sector_change_stocks.add_argument(
        "--stock-offset",
        type=int,
        default=0,
        help="全局D1/D0变化个股分页偏移，默认0",
    )
    sector_change_stocks.add_argument(
        "--stock-limit",
        type=int,
        default=30,
        help="全局D1/D0变化个股页大小，1至30，默认30",
    )
    sector_change_stocks.add_argument(
        "--include-source-market-context",
        action="store_true",
        help="可选附加同一来源池的市场宽度摘要，不改变板块或D1结果",
    )
    sector_change_stocks.add_argument(
        "--include-price-safety-facts",
        action="store_true",
        help="仅为实际显示的板块成员附加Core与D0首日RAW价格损伤事实",
    )
