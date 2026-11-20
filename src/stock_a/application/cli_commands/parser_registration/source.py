"""Stock A CLI命令按实际子域注册。"""

from __future__ import annotations

from stock_a.application.cli_commands.parser import ChineseParser


def register_commands(commands):
    sector_members = commands.add_parser(
        "source-tdx-sector-members",
        help="接入远端TDX行业、GN概念成员并按BaoStock捕获读取精确PG研究上下文",
    )
    sector_actions = sector_members.add_subparsers(
        dest="sector_action", parser_class=ChineseParser, required=True
    )
    sector_catalog = sector_actions.add_parser(
        "catalog", help="分页查询精确行业或概念来源快照目录"
    )
    sector_catalog.add_argument("--snapshot-id", type=int, required=True)
    sector_catalog.add_argument("--board-kind", choices=("INDUSTRY", "CONCEPT"))
    sector_catalog.add_argument("--search", help="名称子串或原生编号")
    sector_catalog.add_argument("--offset", type=int, default=0)
    sector_catalog.add_argument("--limit", type=int, default=100)
    sector_query = sector_actions.add_parser(
        "members", help="读取精确板块成员与BaoStock QFQ Core/Profile状态"
    )
    sector_query.add_argument("--snapshot-id", type=int, required=True)
    sector_query.add_argument(
        "--board-kind", required=True, choices=("INDUSTRY", "CONCEPT")
    )
    sector_query.add_argument("--board-native-id", required=True)
    sector_query.add_argument("--target-session", required=True)
    sector_query.add_argument("--research-cutoff", required=True)
    sector_query.add_argument("--source-capture-id", type=int, required=True)
    sector_query.add_argument("--profile-revision")
    sector_query.add_argument("--offset", type=int, default=0)
    sector_query.add_argument("--limit", type=int, default=100)
    sector_breadth = sector_actions.add_parser(
        "breadth", help="读取真实行业或概念完整成员的两日QFQ MA20广度及关系变化"
    )
    sector_breadth.add_argument("--snapshot-id", type=int, required=True)
    sector_breadth.add_argument(
        "--board-kind", required=True, choices=("INDUSTRY", "CONCEPT")
    )
    sector_breadth.add_argument(
        "--board-native-id", help="可选精确原生板块编号；省略时按板块分页"
    )
    sector_breadth.add_argument("--before-session", required=True)
    sector_breadth.add_argument("--after-session", required=True)
    sector_breadth.add_argument("--research-cutoff", required=True)
    sector_breadth.add_argument("--before-source-capture-id", type=int, required=True)
    sector_breadth.add_argument("--before-profile-revision", required=True)
    sector_breadth.add_argument("--after-source-capture-id", type=int, required=True)
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
        help="按精确当前TDX成员快照比较BaoStock QFQ Core收益与三会话前变化",
    )
    sector_strength.add_argument("--snapshot-id", type=int, required=True)
    sector_strength.add_argument(
        "--board-kind", required=True, choices=("INDUSTRY", "CONCEPT")
    )
    sector_strength.add_argument(
        "--board-native-id", help="精确原生板块编号；省略时查询所选类型全部板块"
    )
    sector_strength.add_argument("--target-session", required=True)
    sector_strength.add_argument("--research-cutoff", required=True)
    sector_strength.add_argument("--source-capture-id", type=int, required=True)
    sector_strength.add_argument("--profile-revision")
    sector_strength.add_argument(
        "--rotation-start",
        help="按后验固定成员研究从该官方会话至目标日的Rotation候选；需指定板块编号",
    )
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
        help="全量联接行业概念强弱变化与同一BaoStock capture的QFQ D1支撑变化",
    )
    sector_change_stocks.add_argument("--industry-snapshot-id", type=int, required=True)
    sector_change_stocks.add_argument("--concept-snapshot-id", type=int, required=True)
    sector_change_stocks.add_argument(
        "--target-session", required=True, help="目标官方交易会话YYYY-MM-DD"
    )
    sector_change_stocks.add_argument(
        "--research-cutoff", required=True, help="显式研究截止会话YYYY-MM-DD"
    )
    sector_change_stocks.add_argument("--source-capture-id", type=int, required=True)
    sector_change_stocks.add_argument("--profile-revision")
    sector_change_stocks.add_argument(
        "--include-source-seed-board",
        action="store_true",
        help="附加指定板块的后验跨期Core/Profile强度输入",
    )
    sector_change_stocks.add_argument(
        "--event-window-start", required=True, help="D1历史事件窗口起始官方会话"
    )
    sector_change_stocks.add_argument(
        "--pg-history-session-count",
        type=int,
        help="可选的目标日历史会话数；省略时按BaoStock配置预算选择并受capture覆盖范围限制",
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
