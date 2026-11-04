"""Stock A CLI命令按实际子域注册。"""

from __future__ import annotations

from decimal import Decimal

from stock_a.application.pg_price_profiles import (
    PgPriceProfileError,
    SOURCE_SYMBOL_PROFILE_SCOPE,
    query_source_symbol_price_profile_changes,
    query_source_symbol_price_profiles,
    run_source_symbol_price_profile,
    source_symbol_price_profile_snapshots,
)


def register_commands(commands):
    d0_raw = commands.add_parser(
        "source-d0-raw",
        help="只读计算精确SEALED来源池的D0 LAUNCH_CONFIRM原始谓词与相邻变化",
    )
    d0_raw.add_argument(
        "--action",
        choices=("current", "changes"),
        default="current",
        help="current读取单日三态；changes读取相邻官方会话变化",
    )
    d0_raw.add_argument(
        "--scenario",
        choices=("LAUNCH_CONFIRM",),
        required=True,
        help="本批仅实现LAUNCH_CONFIRM诊断原始场景",
    )
    d0_raw.add_argument(
        "--target-session", required=True, help="目标官方交易会话YYYY-MM-DD"
    )
    d0_raw.add_argument(
        "--before-session", help="changes动作的前一官方交易会话YYYY-MM-DD"
    )
    d0_raw.add_argument(
        "--research-cutoff", required=True, help="研究数据截止官方会话YYYY-MM-DD"
    )
    d0_raw.add_argument(
        "--source-set-id", required=True, help="目标日SEALED P03 source_set UUID"
    )
    d0_raw.add_argument("--before-source-set-id", help="前日SEALED P03 source_set UUID")
    d0_raw.add_argument(
        "--package-sha256", required=True, help="目标日完整TDX来源包SHA-256"
    )
    d0_raw.add_argument("--before-package-sha256", help="前日完整TDX来源包SHA-256")
    d0_raw.add_argument(
        "--state",
        choices=("TRUE", "FALSE", "UNKNOWN", "ALL"),
        default=None,
        help="在完整来源池计算后按三态筛选，默认ALL",
    )
    d0_raw.add_argument(
        "--transition",
        choices=(
            "FALSE_TO_TRUE",
            "FALSE_TO_FALSE",
            "FALSE_TO_UNKNOWN",
            "TRUE_TO_TRUE",
            "TRUE_TO_FALSE",
            "TRUE_TO_UNKNOWN",
            "UNKNOWN_TO_TRUE",
            "UNKNOWN_TO_FALSE",
            "UNKNOWN_TO_UNKNOWN",
            "BEFORE_ONLY",
            "AFTER_ONLY",
            "ALL",
        ),
        default=None,
        help="changes动作的三态转移筛选；缺端类别也可单独筛选",
    )
    d0_raw.add_argument(
        "--symbol", help="可选精确来源代码SH./SZ.加六位数字；输出21会话原值详情"
    )
    d0_raw.add_argument("--offset", type=int, default=0, help="稳定分页偏移，默认0")
    d0_raw.add_argument(
        "--limit", type=int, default=30, help="稳定分页条数，默认30，最多5000"
    )

    source_market_breadth = commands.add_parser(
        "source-market-breadth",
        help="只读计算固定P03来源代码集合的RAW正收益状态差宽度",
    )
    source_market_breadth.add_argument(
        "--target-session", required=True, help="目标官方交易会话YYYY-MM-DD"
    )
    source_market_breadth.add_argument(
        "--research-cutoff", required=True, help="研究数据截止交易会话YYYY-MM-DD"
    )
    source_market_breadth.add_argument(
        "--source-set-id", required=True, help="目标日SEALED P03 source_set UUID"
    )
    source_market_breadth.add_argument(
        "--package-sha256", required=True, help="目标日完整TDX来源包SHA-256"
    )
    source_d1_breakout = commands.add_parser(
        "source-d1-breakout-preview",
        help="只读预览精确SEALED来源池的ALG-29原始突破谓词",
    )
    source_d1_breakout.add_argument(
        "--target-session", required=True, help="目标官方交易会话YYYY-MM-DD"
    )
    source_d1_breakout.add_argument(
        "--research-cutoff", required=True, help="研究数据截止交易会话YYYY-MM-DD"
    )
    source_d1_breakout.add_argument(
        "--source-set-id", required=True, help="目标日SEALED P03 source_set UUID"
    )
    source_d1_breakout.add_argument(
        "--package-sha256", required=True, help="目标日完整TDX来源包SHA-256"
    )
    selection = source_d1_breakout.add_mutually_exclusive_group()
    selection.add_argument(
        "--symbol", help="可选单个精确来源代码SH./六位数字或SZ./六位数字"
    )
    selection.add_argument("--offset", type=int, help="分页偏移，需与--limit同时使用")
    source_d1_breakout.add_argument(
        "--only-true",
        action="store_true",
        help="先计算完整来源池，再分页返回原始突破TRUE",
    )
    source_d1_breakout.add_argument(
        "--previous-state",
        choices=("ALL", "TRUE", "FALSE", "UNKNOWN"),
        help="仅配合--only-true：按紧邻前一官方会话的原始D1三态分类后过滤",
    )
    source_d1_breakout.add_argument(
        "--limit", type=int, help="来源池分页数量，1至100；配合--offset使用"
    )
    source_d1_breakout.add_argument(
        "--pg-history-session-count",
        type=int,
        default=75,
        help="按现役官方日历读取的历史会话数，1至75，默认75",
    )
    source_d1_breakout.add_argument("--history-start", help="历史观察起始官方会话")
    source_d1_breakout.add_argument("--history-end", help="历史观察结束官方会话")
    source_d1_breakout.add_argument(
        "--history-last", type=int, help="观察最近N个官方会话，1至75"
    )
    source_d1_breakout.add_argument(
        "--support-anchor-session",
        help="显式指定一个历史D1突破平台出生会话；仅限单代码历史模式",
    )
    source_d1_breakout.add_argument(
        "--support-cohort-anchor-session",
        help="按该历史会话D1 TRUE批次，查询目标会话同锚支撑变化",
    )
    source_d1_breakout.add_argument(
        "--discover-support-changes",
        action="store_true",
        help="遍历显式历史窗口内全部D1 TRUE事件并发现目标日支撑变化",
    )
    source_d1_breakout.add_argument(
        "--event-window-start",
        help="多历史事件观察起始官方会话；包含起始日、不含目标日",
    )
    source_d1_breakout.add_argument(
        "--change-only",
        action="store_true",
        help="仅返回完整来源批次中已确认CHANGED的支撑观察",
    )
    price_profile = commands.add_parser(
        "source-symbol-price-profile",
        help="按精确P03封口版本计算、保存或分页查询来源代码级NON_PIT价格画像",
    )
    price_profile.add_argument(
        "--action",
        choices=("compute", "query", "snapshots", "changes"),
        required=True,
        help="compute计算保存；query按精确键分页；snapshots列出现存版本；changes比较两日画像",
    )
    price_profile.add_argument(
        "--research-cutoff",
        required=True,
        help="必填研究截止交易日；此日之后的查询和计算一律拒绝",
    )
    price_profile.add_argument("--before-session", help="变化查询的前一画像日期")
    price_profile.add_argument("--after-session", help="变化查询的后一画像日期")
    price_profile.add_argument(
        "--before-source-set-id", help="changes前态画像的精确SEALED source_set UUID"
    )
    price_profile.add_argument(
        "--before-package-sha256", help="changes前态画像的精确来源包SHA-256"
    )
    price_profile.add_argument(
        "--before-profile-revision",
        help="changes前态画像的精确profile_revision SHA-256",
    )
    price_profile.add_argument(
        "--after-source-set-id", help="changes后态画像的精确SEALED source_set UUID"
    )
    price_profile.add_argument(
        "--after-package-sha256", help="changes后态画像的精确来源包SHA-256"
    )
    price_profile.add_argument(
        "--after-profile-revision", help="changes后态画像的精确profile_revision SHA-256"
    )
    price_profile.add_argument(
        "--change-type",
        choices=(
            "ALL",
            "BEFORE_ONLY",
            "AFTER_ONLY",
            "TARGET_BAR_STATUS_CHANGED",
            "PRICE_WINDOW_STATUS_CHANGED",
            "RAW_CLOSE_VS_MA20_CHANGED",
            "PROFILE_STATE_CHANGED",
            "CORE_QUALITY_CHANGED",
        ),
        default="ALL",
        help="changes动作的变化类型过滤",
    )
    price_profile.add_argument(
        "--price-damage-transition",
        choices=("ALL", "FALSE_TO_TRUE", "TRUE_TO_FALSE"),
        default="ALL",
        help="按Core价格损伤FALSE→TRUE或TRUE→FALSE变化筛选完整来源集合",
    )
    price_profile.add_argument(
        "--ma-slope20-transition",
        choices=(
            "ALL",
            "NEGATIVE_TO_POSITIVE",
            "POSITIVE_TO_NEGATIVE",
            "ANY_SIGN_CHANGED",
        ),
        default="ALL",
        help="按Core MA20/ATR斜率精确符号变化筛选完整来源集合",
    )
    price_profile.add_argument("--target-session", help="精确目标交易会话YYYY-MM-DD")
    price_profile.add_argument("--source-set-id", help="正式SEALED P03 source_set UUID")
    price_profile.add_argument("--package-sha256", help="精确TDX来源包SHA-256")
    price_profile.add_argument(
        "--scope",
        choices=(SOURCE_SYMBOL_PROFILE_SCOPE,),
        help="显式计算范围SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE",
    )
    price_profile.add_argument(
        "--session-count",
        type=int,
        default=75,
        help="官方历史窗口会话数，1至75，默认75",
    )
    price_profile.add_argument(
        "--page-size", type=int, default=100, help="PG候选分页批大小，1至100，默认100"
    )
    price_profile.add_argument("--profile-revision", help="查询时必填的画像修订SHA-256")
    price_profile.add_argument("--symbol", help="可选精确来源代码，如SH.六位数字")
    price_profile.add_argument(
        "--market", choices=("ALL", "SH", "SZ"), default="ALL", help="查询市场过滤"
    )
    price_profile.add_argument(
        "--board",
        choices=("ALL", "MAIN_BOARD", "GEM"),
        default="ALL",
        help="查询板别过滤",
    )
    price_profile.add_argument(
        "--target-bar-status",
        choices=(
            "ALL",
            "TDX_TARGET_BAR_PRESENT",
            "TDX_TARGET_BAR_MISSING",
            "TDX_READ_FAILED",
        ),
        default="ALL",
        help="目标日Bar状态筛选",
    )
    price_profile.add_argument(
        "--price-window-status",
        choices=(
            "ALL",
            "USABLE",
            "TARGET_MISSING",
            "READ_FAILED",
            "HISTORY_ROW_NOT_STORED",
            "INSUFFICIENT_VALID_BARS",
            "PRICE_FIELDS_UNKNOWN",
        ),
        default="ALL",
        help="价格窗口状态筛选",
    )
    price_profile.add_argument(
        "--core-field",
        choices=(
            "core.ma5",
            "core.ma10",
            "core.ma20",
            "core.ma60",
            "core.atr20",
            "core.clv",
            "core.prior_high20",
            "core.ret20",
            "core.pos60",
            "core.bias20_atr",
            "core.ma_slope20_atr",
            "core.price_damage",
            "core.amount_ratio5",
            "core.amount_ratio20",
            "core.volume_ratio5",
            "core.volume_ratio20",
        ),
        help="可选Core价格字段质量筛选",
    )
    price_profile.add_argument(
        "--core-quality",
        choices=("OBSERVED", "UNKNOWN", "NOT_APPLICABLE"),
        help="配合Core字段的质量值",
    )
    price_profile.add_argument(
        "--price-damage-state",
        choices=("ALL", "TRUE", "FALSE", "UNKNOWN"),
        default="ALL",
        help="按完整来源池中的Core价格损伤事实筛选",
    )
    price_profile.add_argument(
        "--ma-slope20-sign",
        choices=("ALL", "POSITIVE", "ZERO", "NEGATIVE", "UNKNOWN"),
        default="ALL",
        help="按Core MA20/ATR斜率的精确符号筛选",
    )
    for argument, help_text in (
        ("--amount-ratio20-min", "原生金额比值下界，含边界，非负有限Decimal"),
        ("--amount-ratio20-max", "原生金额比值上界，含边界，非负有限Decimal"),
        ("--volume-ratio20-min", "原生数量比值下界，含边界，非负有限Decimal"),
        ("--volume-ratio20-max", "原生数量比值上界，含边界，非负有限Decimal"),
    ):
        price_profile.add_argument(argument, type=Decimal, help=help_text)
    price_profile.add_argument("--offset", type=int, default=0, help="查询偏移，默认0")
    price_profile.add_argument(
        "--limit", type=int, default=30, help="查询页大小，默认30；changes最多30"
    )
    price_profile.add_argument(
        "--format",
        choices=("summary", "json"),
        default="json",
        help="输出摘要或完整JSON",
    )
    source_p04 = commands.add_parser(
        "source-p04-read",
        help="按精确日期与包SHA只读正式P03来源集合",
    )
    source_p04.add_argument(
        "--target-session", required=True, help="目标交易会话YYYY-MM-DD"
    )
    source_p04.add_argument(
        "--package-sha256", required=True, help="完整TDX来源包SHA-256"
    )
    source_p04.add_argument(
        "--audit-mode",
        action="store_true",
        help="显式逐行读取Raw并按该来源版本的实际行数重算封口摘要",
    )
    coverage = commands.add_parser(
        "source-p04-identity-coverage", help="只读封口来源代码与身份隔离覆盖"
    )
    coverage.add_argument("--target-session", required=True, help="目标交易会话")
    coverage.add_argument("--research-cutoff", required=True, help="研究截止日")
    coverage.add_argument("--source-set-id", required=True, help="精确来源集合ID")
    coverage.add_argument("--package-sha256", required=True, help="完整来源包SHA-256")
    coverage.add_argument("--symbol", help="可选精确来源代码")
    coverage.add_argument("--offset", type=int, default=0, help="分页偏移")
    coverage.add_argument("--limit", type=int, default=30, help="页大小1至100")
    preview_serve = commands.add_parser(
        "preview-serve",
        help="显式启动本机PG来源级研究预览页面（仅127.0.0.1:28766）",
        description="仅用于开发研究预览；只读现有PG Raw，非正式发布、非PIT。",
    )
    preview_serve.add_argument(
        "--host", default="127.0.0.1", help="固定127.0.0.1，不允许远程监听"
    )
    preview_serve.add_argument(
        "--port", type=int, default=28766, help="固定开发端口28766"
    )
    preview_serve.add_argument(
        "--research-cutoff",
        required=True,
        help="显式冻结研究截止日；之后的页面查询在访问PG前拒绝",
    )
