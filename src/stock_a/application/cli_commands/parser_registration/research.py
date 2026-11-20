"""Stock A CLI命令按实际子域注册。"""

from __future__ import annotations

from stock_a.application.cli_commands.parser import _add_research_arguments


def register_commands(commands):
    stock = commands.add_parser(
        "research-stock",
        help="只读查看单只股票的本地TDX离线诊断；不读取PG或作为生产价格源",
    )
    stock.add_argument("symbol", help="来源代码，格式SH.六位代码或SZ.六位代码")
    _add_research_arguments(stock)
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
    compare = commands.add_parser(
        "research-compare", help="显式本地TDX离线只读比较多个来源代码"
    )
    compare.add_argument(
        "symbols", nargs="+", help="一个或多个来源代码，格式SH.六位代码或SZ.六位代码"
    )
    _add_research_arguments(compare)
    compare.add_argument(
        "--local-tdx-diagnostic",
        action="store_true",
        required=True,
        help="仅对本地TDX文件执行离线只读比较，不访问PG或作为生产价格源",
    )
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
