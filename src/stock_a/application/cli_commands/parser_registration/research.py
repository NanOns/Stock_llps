"""Stock A CLI命令按实际子域注册。"""

from __future__ import annotations

from stock_a.application.cli_commands.parser import _add_research_arguments


def register_commands(commands):
    stock = commands.add_parser(
        "research-stock", help="只读查看单只股票的原始行情、Core字段和基础观察"
    )
    stock.add_argument("symbol", help="来源代码，格式SH.六位代码或SZ.六位代码")
    _add_research_arguments(stock, include_vipdata=True)
    stock.add_argument(
        "--pg-source-raw",
        action="store_true",
        help="按显式目标日和来源包数字ID从stock_a.stock查询Raw事实；不打开ZIP、不重算、不执行DDL",
    )
    stock.add_argument(
        "--pg-source-history",
        action="store_true",
        help="显式从PG读取指定来源包的官方会话历史，计算现役Core43/Profile11研究预览；只读、NON_PIT",
    )
    stock.add_argument(
        "--pg-source-package-id",
        type=int,
        help="PG Raw查询必填：正数来源包ID，不回退latest或跨日",
    )
    stock.add_argument(
        "--pg-history-session-count",
        type=int,
        default=75,
        help="PG历史画像官方会话窗口，1至75，默认75",
    )
    stock.add_argument(
        "--local-tdx-diagnostic",
        action="store_true",
        help="显式启用本地TDX单股诊断；不读取PG，且受本地记录上限约束",
    )
    stock.add_argument(
        "--local-tdx-max-sessions",
        type=int,
        default=60,
        help="本地TDX诊断最多解析的单日记录数，1至75，默认60；超限时拒绝继续扫描",
    )
    stock.add_argument(
        "--format",
        dest="output_format",
        choices=("summary", "json"),
        default="json",
        help="输出可读事实摘要或完整JSON，默认json",
    )
    stock.add_argument(
        "--include-profile-preview",
        action="store_true",
        help="在同一次Raw/Core只读结果上附加11项基础画像组件预览；非PIT研究结果",
    )
    stock.add_argument(
        "--include-baostock-evidence",
        action="store_true",
        help="显式查询目标日单条BaoStock只读补充字段；可能联网，单位与身份仍需独立核验",
    )
    stock.add_argument(
        "--include-baostock-identity",
        action="store_true",
        help="显式一次登录查询单证券Bao基本资料和目标日名录；先返回TDX/Core，再在有限预算内补充身份，NON_PIT",
    )
    stock.add_argument(
        "--baostock-timeout-seconds",
        type=float,
        default=8.0,
        help="BaoStock单个socket等待上限，默认8秒",
    )
    stock.add_argument(
        "--baostock-max-seconds",
        type=float,
        default=25.0,
        help="BaoStock身份会话总预算上限，默认25秒；只在显式启用身份查询时生效",
    )
    compare = commands.add_parser(
        "research-compare", help="盘后只读输出有界多标的Core基础观察摘要"
    )
    compare.add_argument(
        "symbols", nargs="+", help="一个或多个来源代码，格式SH.六位代码或SZ.六位代码"
    )
    _add_research_arguments(compare)
    compare.add_argument(
        "--max-total-bytes",
        type=int,
        default=335544320,
        help="本次多股票扫描总字节上限，默认335544320",
    )
    compare.add_argument(
        "--max-total-records",
        type=int,
        default=4000000,
        help="本次多股票解析总记录上限，默认4000000",
    )
    compare.add_argument(
        "--max-total-seconds",
        type=float,
        default=120.0,
        help="本次多股票扫描总耗时秒数上限，默认120",
    )
    universe = commands.add_parser(
        "research-universe-preview",
        help="发现沪深主板/创业板TDX来源候选并分页查看盘后基础观察",
    )
    _add_research_arguments(universe, include_vipdata=True)
    universe.add_argument(
        "--pg-source-raw",
        action="store_true",
        help="从stock_a.stock按显式目标日和来源包数字ID真实SQL分页读取Raw候选",
    )
    universe.add_argument(
        "--pg-source-history",
        action="store_true",
        help="显式从PG批量读取来源候选历史并分页展示Core/Profile研究摘要；只读、NON_PIT",
    )
    universe.add_argument(
        "--pg-source-package-id", type=int, help="PG Raw分页查询必填：正数来源包ID"
    )
    universe.add_argument(
        "--pg-history-session-count",
        type=int,
        default=75,
        help="PG历史画像官方会话窗口，1至75，默认75",
    )
    universe.add_argument(
        "--offset", type=int, default=0, help="来源候选分页起点，默认0"
    )
    universe.add_argument(
        "--limit", type=int, default=20, help="本页证券数，默认20，最多100"
    )
    universe.add_argument(
        "--market", choices=("ALL", "SH", "SZ"), default="ALL", help="市场过滤，默认ALL"
    )
    universe.add_argument(
        "--board",
        choices=("ALL", "MAIN_BOARD", "GEM"),
        default="ALL",
        help="板别过滤，默认ALL",
    )
    universe.add_argument(
        "--full-target-quality",
        action="store_true",
        help="显式扫描全部来源候选目标日质量；默认只评价当前页",
    )
    universe.add_argument(
        "--full-source-core-profile",
        action="store_true",
        help="显式单遍扫描ZIP全来源候选，并逐只调用现有Core与11项Profile预览；只读、内存、NON_PIT",
    )
    universe.add_argument(
        "--max-source-quality-bytes",
        type=int,
        default=4294967296,
        help="显式目标日质量扫描总字节上限，默认4GiB",
    )
    universe.add_argument(
        "--max-source-quality-records",
        type=int,
        default=125000000,
        help="显式目标日质量扫描总记录上限，默认125000000",
    )
    universe.add_argument(
        "--max-source-quality-seconds",
        type=float,
        default=1800.0,
        help="显式目标日质量扫描总耗时上限，默认1800秒",
    )
    universe.add_argument(
        "--format",
        dest="output_format",
        choices=("summary", "json"),
        default="summary",
        help="输出短摘要或完整JSON，默认summary",
    )
    universe.add_argument(
        "--include-profile-preview",
        action="store_true",
        help="在本页已评价Raw/Core结果上附加11项基础画像组件预览；非PIT研究结果",
    )
    universe.add_argument(
        "--max-directory-entries",
        type=int,
        default=50000,
        help="来源目录累计枚举上限，默认50000",
    )
    universe.add_argument(
        "--max-discovery-seconds",
        type=float,
        default=30.0,
        help="来源目录发现时间上限秒，默认30",
    )
    universe.add_argument(
        "--max-total-bytes",
        type=int,
        default=335544320,
        help="当前页累计TDX读取字节上限，默认335544320",
    )
    universe.add_argument(
        "--max-total-records",
        type=int,
        default=4000000,
        help="当前页累计TDX记录上限，默认4000000",
    )
    universe.add_argument(
        "--max-total-seconds",
        type=float,
        default=120.0,
        help="当前页累计扫描秒数上限，默认120",
    )
