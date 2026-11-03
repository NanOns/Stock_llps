"""Stock A CLI命令按实际子域注册。"""

from __future__ import annotations


def register_commands(commands):
    inspect = commands.add_parser("source-inspect", help="只读检查单个TDX日线文件")
    inspect.add_argument("path", help="待检查日线文件的绝对路径")
    inspect.add_argument("--unit-version", required=True, help="显式日线单位合同版本")
    inspect.add_argument(
        "--amount-to-cny", required=True, help="金额到元的精确十进制倍率"
    )
    inspect.add_argument(
        "--volume-to-shares", required=True, type=int, help="数量到股的正整数倍率"
    )
    inspect.add_argument(
        "--max-bytes", required=True, type=int, help="本次最大读取字节数"
    )
    inspect.add_argument(
        "--max-records", required=True, type=int, help="本次最大记录数"
    )
    inspect.add_argument(
        "--chunk-records", default=256, type=int, help="单次读取块记录数，默认256"
    )
    inspect.add_argument("--max-seconds", default="30", help="本次最大读取秒数，默认30")
