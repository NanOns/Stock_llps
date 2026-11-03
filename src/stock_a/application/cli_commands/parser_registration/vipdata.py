"""Stock A CLI命令按实际子域注册。"""

from __future__ import annotations


def register_commands(commands):
    target_quality = commands.add_parser(
        "source-vipdata-target-quality",
        help="逐页评价已校验VIPDATA ZIP的沪深主板/创业板目标日来源候选及逐字段质量",
    )
    target_quality.add_argument(
        "--vipdata-package", required=True, help="G盘已下载且不修改的官网ZIP绝对路径"
    )
    target_quality.add_argument(
        "--vipdata-package-sha256", required=True, help="已知的64位ZIP SHA-256"
    )
    target_quality.add_argument(
        "--max-vipdata-validation-seconds",
        type=float,
        default=300.0,
        help="一次ZIP SHA/CRC重验的耗时上限秒数，默认300",
    )
    target_quality.add_argument(
        "--target-session",
        required=True,
        help="已核验官方市场日历中的目标交易会话，格式YYYY-MM-DD",
    )
    target_quality.add_argument(
        "--unit-version",
        required=True,
        help="显式研究单位合同版本；金额/数量仍按调用方假设",
    )
    target_quality.add_argument(
        "--amount-to-cny", required=True, help="金额到人民币元的调用方倍率假设"
    )
    target_quality.add_argument(
        "--volume-to-shares", required=True, type=int, help="数量到股的调用方倍率假设"
    )
    target_quality.add_argument(
        "--page-size", type=int, default=100, help="每页候选数，默认100，最大100"
    )
    target_quality.add_argument(
        "--max-total-bytes",
        type=int,
        default=4294967296,
        help="总条目读取字节上限，默认4GiB",
    )
    target_quality.add_argument(
        "--max-total-records",
        type=int,
        default=125000000,
        help="总记录读取上限，默认125000000",
    )
    target_quality.add_argument(
        "--max-total-seconds",
        type=float,
        default=1800.0,
        help="总评价耗时上限秒数，默认1800",
    )
    target_quality.add_argument(
        "--max-candidate-bytes",
        type=int,
        default=33554432,
        help="单条目最多读取字节，默认32MiB",
    )
    target_quality.add_argument(
        "--max-candidate-records",
        type=int,
        default=1000000,
        help="单条目最多解析记录数，默认1000000",
    )
    target_quality.add_argument(
        "--max-candidate-seconds",
        type=float,
        default=30.0,
        help="单条目读取秒数上限，默认30",
    )
    target_quality.add_argument(
        "--chunk-records", type=int, default=1024, help="单次解码块记录数，默认1024"
    )
    revision_compare = commands.add_parser(
        "source-vipdata-revision-compare",
        help="只读对照两份已校验VIPDATA ZIP的同会话来源条目修订",
    )
    revision_compare.add_argument(
        "--old-package", required=True, help="G盘现存旧版官方VIPDATA ZIP绝对路径"
    )
    revision_compare.add_argument(
        "--old-package-sha256", required=True, help="旧包已知SHA-256"
    )
    revision_compare.add_argument(
        "--new-package", required=True, help="G盘现存新版官方VIPDATA ZIP绝对路径"
    )
    revision_compare.add_argument(
        "--new-package-sha256", required=True, help="新版已知SHA-256"
    )
    revision_compare.add_argument(
        "--target-session", required=True, help="两包共同对照的目标交易会话，YYYY-MM-DD"
    )
    revision_compare.add_argument(
        "--max-validation-seconds",
        type=float,
        default=300.0,
        help="每包SHA/CRC复验秒数上限，默认300",
    )
    revision_compare.add_argument(
        "--max-total-bytes",
        type=int,
        default=4294967296,
        help="候选条目解压字节总上限，默认4GiB",
    )
    revision_compare.add_argument(
        "--max-total-records",
        type=int,
        default=125000000,
        help="候选条目记录总上限，默认125000000",
    )
    revision_compare.add_argument(
        "--max-total-seconds",
        type=float,
        default=1800.0,
        help="逐条目对照秒数上限，默认1800",
    )
    revision_compare.add_argument(
        "--sample-limit", type=int, default=20, help="最多保留多少条变化样本，默认20"
    )
    vipdata = commands.add_parser(
        "source-vipdata",
        help="显式检查通达信官网盘后ZIP，或在G盘临时目录下载并校验",
        description="仅此显式命令联网。默认只检查官网；download-verify会在满足日期门后下载一次完整ZIP并校验。",
    )
    vipdata.add_argument(
        "--mode",
        choices=("inspect", "download-verify"),
        default="inspect",
        help="只检查官网元数据，或下载并验证完整ZIP，默认inspect",
    )
    vipdata.add_argument(
        "--target-session", required=True, help="必填目标正式交易会话，格式YYYY-MM-DD"
    )
    vipdata.add_argument(
        "--max-page-bytes",
        type=int,
        default=2097152,
        help="官网页面最大读取字节数，默认2097152",
    )
    vipdata.add_argument(
        "--max-package-bytes",
        type=int,
        default=2147483648,
        help="完整ZIP最大下载字节数，默认2147483648",
    )
    vipdata.add_argument(
        "--max-zip-entries",
        type=int,
        default=50000,
        help="ZIP中央目录最大条目数，默认50000",
    )
    vipdata.add_argument(
        "--max-uncompressed-bytes",
        type=int,
        default=4294967296,
        help="ZIP最大总解压字节数，默认4294967296",
    )
    vipdata.add_argument(
        "--max-entry-bytes",
        type=int,
        default=33554432,
        help="ZIP单条目最大解压字节数，默认33554432",
    )
    vipdata.add_argument(
        "--max-compression-ratio",
        type=int,
        default=200,
        help="ZIP条目最大压缩比，默认200",
    )
    vipdata.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="单次连接或读取超时秒数，默认20",
    )
    vipdata.add_argument(
        "--max-total-seconds",
        type=float,
        default=300.0,
        help="页面和下载的总读取秒数上限，默认300",
    )
    vipdata.add_argument(
        "--max-redirects", type=int, default=4, help="官网HTTPS重定向最大次数，默认4"
    )
    vipdata.add_argument(
        "--reserve-free-bytes",
        type=int,
        default=536870912,
        help="下载期间必须保留的G盘可用空间，默认536870912",
    )
    vipdata.add_argument(
        "--consume-symbol",
        help="可选：将已验证ZIP中的一个SH./SZ.日线样本送入现有Raw/Canonical/Core只读消费者",
    )
    vipdata.add_argument("--unit-version", help="使用Core消费者时必填的显式TDX单位版本")
    vipdata.add_argument(
        "--amount-to-cny", help="使用Core消费者时必填的精确金额到元倍率"
    )
    vipdata.add_argument(
        "--volume-to-shares", type=int, help="使用Core消费者时必填的数量到股正整数倍率"
    )
    vipdata.add_argument(
        "--max-symbol-bytes",
        type=int,
        default=16777216,
        help="Core消费者单个日线文件最大读取字节数，默认16777216",
    )
    vipdata.add_argument(
        "--max-symbol-records",
        type=int,
        default=200000,
        help="Core消费者单个日线文件最大记录数，默认200000",
    )
    vipdata.add_argument(
        "--max-symbol-seconds",
        default="30",
        help="Core消费者单个日线文件最大扫描秒数，默认30",
    )
