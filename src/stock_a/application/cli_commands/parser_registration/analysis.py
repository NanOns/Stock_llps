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
from stock_a.application.source_d1_structures import (
    MATERIAL_CHANGE_REASONS,
    STRUCTURE_TYPES,
)


def register_commands(commands):
    observation = commands.add_parser(
        "source-market-observation", help="研究来源池变化、板块归属及后续价格"
    )
    observation.add_argument("--start-session", required=True, help="起始官方交易会话")
    observation.add_argument("--end-session", required=True, help="结束官方交易会话")
    observation.add_argument("--research-cutoff", required=True, help="研究数据截止日")
    observation.add_argument(
        "--source-package-id", type=int, required=True, help="明确绑定的数字来源包ID"
    )
    observation.add_argument(
        "--profile-revision", help="精确画像修订；D1材料视图不需要画像"
    )
    observation.add_argument(
        "--industry-snapshot-id", type=int, required=True, help="行业成员数字快照ID"
    )
    observation.add_argument(
        "--concept-snapshot-id", type=int, required=True, help="GN成员数字快照ID"
    )
    observation.add_argument("--event-window-start", help="D1历史事件窗起始会话")
    observation.add_argument(
        "--history-session-count", type=int, default=75, help="画像历史会话数"
    )
    observation.add_argument(
        "--d1-history-session-count", type=int, default=40, help="D1历史会话数"
    )
    observation.add_argument(
        "--horizon",
        type=int,
        action="append",
        help="后继官方会话数，可重复指定",
    )
    observation.add_argument(
        "--view",
        choices=(
            "full",
            "summary",
            "cohort",
            "persisted",
            "persisted-changes",
            "fep",
            "d1-material",
        ),
        default="full",
        help="完整研究、盘后变化、独立研究组、持久读取、FEP统计或D1材料变化",
    )
    observation.add_argument(
        "--structure-type",
        choices=STRUCTURE_TYPES,
        help="D1材料变化结构类型；完整全池分母仍先行统计",
    )
    observation.add_argument(
        "--material-change-reason",
        choices=MATERIAL_CHANGE_REASONS,
        help="按现有具名D1材料变化原因筛选；原因计数可重叠",
    )
    observation.add_argument(
        "--rule-id",
        help="持久读取或FEP的精确规则标识；FEP默认无板块为启动与恢复，有板块为全部规则",
    )
    observation.add_argument(
        "--fep-condition",
        choices=("ALL", "B_FIXED_POSITIVE", "B_FIXED_NON_POSITIVE", "B_FIXED_UNKNOWN"),
        help="FEP日期条件；不筛减逐日来源分母",
    )
    observation.add_argument("--before-session", help="持久规则对照的前一日期")
    observation.add_argument(
        "--before-event-window-start", help="前一日期的D1事件窗口起点"
    )
    observation.add_argument(
        "--risk-readiness",
        action="store_true",
        help="计算所选来源代码的风险分量和资格边界",
    )
    observation.add_argument(
        "--persist-research-facts",
        action="store_true",
        help="将本次完整来源池研究事实写入PG",
    )
    observation.add_argument(
        "--persist-research-events",
        action="store_true",
        help="同时保存来源D1独立锚事件",
    )
    observation.add_argument("--symbol", help="仅筛选该来源代码的观察明细")
    observation.add_argument(
        "--signal",
        choices=(
            "LAUNCH_CONFIRM",
            "RECOVERY_TURN",
            "s1",
            "s2",
            "seed",
            "D1_SUPPORT_CHANGE",
        ),
        help="完整计算后按事件类别筛选观察明细",
    )
    observation.add_argument(
        "--board-kind", choices=("INDUSTRY", "CONCEPT"), help="精确板块类型"
    )
    observation.add_argument("--board-id", help="精确板块原生编号")
    observation.add_argument("--rotation-start", help="板块Rotation窗口起始会话")
    observation.add_argument(
        "--relative-position",
        action="store_true",
        help="输出所选板块的跨日相对位置研究",
    )
    observation.add_argument("--offset", type=int, default=0, help="观察明细分页偏移")
    observation.add_argument("--limit", type=int, default=30, help="观察明细分页条数")
    research_day = commands.add_parser(
        "source-research-day", help="按现役D0/D1计算单日研究并可选保存typed事实"
    )
    research_day.add_argument(
        "--target-session", required=True, help="目标官方交易会话YYYY-MM-DD"
    )
    research_day.add_argument(
        "--start-session", help="可选：从PG已存来源的此日期起逐日初始化至目标会话"
    )
    research_day.add_argument(
        "--research-cutoff", required=True, help="研究数据截止官方会话YYYY-MM-DD"
    )
    research_day.add_argument("--source-package-id", type=int, required=True)
    research_day.add_argument(
        "--d1-history-session-count",
        type=int,
        required=True,
        help="D1窗口官方会话数，须与精确研究键一致",
    )
    research_day.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Seed计算工作进程数；默认串行，最多8个且不超过本机逻辑CPU数",
    )
    research_day.add_argument(
        "--history-session-count",
        type=int,
        help="静态画像历史官方会话数；省略时要求现行语义的完整画像窗口唯一",
    )
    research_day.add_argument(
        "--persist",
        action="store_true",
        help="以单一串行化事务幂等写入三类typed事实",
    )
    research_day.add_argument(
        "--static-only",
        action="store_true",
        help="按既存Raw会话运行单日D0/Seed静态生产链，必须显式指定--persist",
    )
    research_day.add_argument(
        "--enrollment-from-saved",
        action="store_true",
        help="单日从完整已存D0和规则事实登记研究匹配，不重新计算历史",
    )
    research_day.add_argument(
        "--cohort-stats-from-saved",
        action="store_true",
        help="从已存规则和路径事实离线汇总冻结Cohort，不读取价格历史",
    )
    research_day.add_argument(
        "--d0-launch-reference-only",
        action="store_true",
        help="仅生产相邻会话消费者明确需要的LAUNCH_CONFIRM前日D0参考事实",
    )
    research_day.add_argument(
        "--include-sector-pulse",
        action="store_true",
        help="在现役单日静态生产链中计算并保存行业与GN概念完整同类板块跨日PULSE",
    )
    research_day.add_argument(
        "--industry-snapshot-id",
        type=int,
        help="板块PULSE使用的精确TDX行业成员数字快照ID",
    )
    research_day.add_argument(
        "--concept-snapshot-id",
        type=int,
        help="板块PULSE使用的精确TDX GN概念成员数字快照ID",
    )
    pulse_transition = commands.add_parser(
        "source-sector-pulse-transition-day",
        help="仅补算必要的缺失PULSE并保存两日完整行业与概念相邻变化",
    )
    pulse_transition.add_argument(
        "--previous-session", required=True, help="前一官方交易会话YYYY-MM-DD"
    )
    pulse_transition.add_argument(
        "--target-session", required=True, help="目标官方交易会话YYYY-MM-DD"
    )
    pulse_transition.add_argument(
        "--research-cutoff", required=True, help="显式研究数据截止会话YYYY-MM-DD"
    )
    pulse_transition.add_argument(
        "--previous-source-package-id",
        type=int,
        required=True,
        help="前一会话Raw来源包数字ID",
    )
    pulse_transition.add_argument(
        "--target-source-package-id",
        type=int,
        required=True,
        help="目标会话Raw来源包数字ID",
    )
    pulse_transition.add_argument(
        "--previous-profile-revision",
        required=True,
        help="前一会话画像修订标签，须使用快照返回的完整修订",
    )
    pulse_transition.add_argument(
        "--target-profile-revision",
        required=True,
        help="目标会话画像修订标签，须使用快照返回的完整修订",
    )
    pulse_transition.add_argument(
        "--industry-snapshot-id",
        type=int,
        required=True,
        help="精确TDX行业成员数字快照ID",
    )
    pulse_transition.add_argument(
        "--concept-snapshot-id",
        type=int,
        required=True,
        help="精确TDX GN概念成员数字快照ID",
    )
    pulse_followup = commands.add_parser(
        "source-sector-pulse-followup-day",
        help="读取已存PULSE与实际后继Raw并原子保存固定成员T+1/T+3/T+5结果",
    )
    pulse_followup.add_argument(
        "--target-session", required=True, help="已保存PULSE的T0官方会话YYYY-MM-DD"
    )
    pulse_followup.add_argument(
        "--research-cutoff", required=True, help="明确的Raw研究截止官方会话YYYY-MM-DD"
    )
    pulse_followup.add_argument("--source-package-id", type=int, required=True)
    pulse_followup.add_argument(
        "--target-package",
        action="append",
        default=[],
        metavar="期限=包ID",
        help="指定终点来源包，可重复传入，例如 1=数字ID；多修订时必须指定",
    )
    pulse_followup.add_argument(
        "--persist",
        action="store_true",
        required=True,
        help="在唯一PG以单一SERIALIZABLE事务幂等保存完整板块与到期窗口",
    )
    d0_raw = commands.add_parser(
        "source-d0-raw",
        help="只读计算Raw来源池D0启动、恢复诊断及启动相邻变化",
    )
    d0_raw.add_argument(
        "--action",
        choices=("current", "changes"),
        default="current",
        help="current读取单日三态；changes读取相邻官方会话变化",
    )
    d0_raw.add_argument(
        "--scenario",
        choices=("LAUNCH_CONFIRM", "RECOVERY_TURN"),
        required=True,
        help="选择来源级启动或恢复诊断场景",
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
        "--source-package-id",
        type=int,
        required=True,
        help="目标会话精确TDX来源包BIGINT数字ID",
    )
    d0_raw.add_argument(
        "--before-source-package-id", type=int, help="前日完整TDX来源包数字ID"
    )
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
    d0_raw.add_argument("--symbol", help="可选精确来源代码SH./SZ.加六位数字")
    d0_raw.add_argument(
        "--risk-readiness", action="store_true", help="单股读取同日风险与资格研究事实"
    )
    d0_raw.add_argument(
        "--followup-session",
        help="启动或恢复诊断候选线后继观察终点；须指定单只来源代码",
    )
    d0_raw.add_argument(
        "--shape-control-symbol",
        action="append",
        help="目标日前指定同日同源探索性比较代码，可重复；须同时指定后继会话",
    )
    d0_raw.add_argument(
        "--shape-followup-states",
        action="store_true",
        help="逐会话追踪T0诊断父对应的形态状态，须指定后继会话",
    )
    d0_raw.add_argument("--offset", type=int, default=0, help="稳定分页偏移，默认0")
    d0_raw.add_argument(
        "--limit", type=int, default=30, help="稳定分页条数，默认30，最多5000"
    )

    source_market_breadth = commands.add_parser(
        "source-market-breadth",
        help="读取或显式计算保存固定来源篮的RAW正收益状态差宽度",
    )
    source_market_breadth.add_argument(
        "--target-session", required=True, help="目标官方交易会话YYYY-MM-DD"
    )
    source_market_breadth.add_argument(
        "--research-cutoff", required=True, help="研究数据截止交易会话YYYY-MM-DD"
    )
    source_market_breadth.add_argument("--source-package-id", type=int, required=True)
    source_market_breadth.add_argument(
        "--persist", action="store_true", help="显式计算并原子保存市场宽度事实"
    )
    source_market_breadth.add_argument(
        "--start-session", help="配合--persist从已存日期起逐日计算至目标日"
    )
    source_d1_breakout = commands.add_parser(
        "source-d1-breakout-preview",
        help="只读预览精确来源版本的ALG-29原始突破谓词",
    )
    source_d1_breakout.add_argument(
        "--target-session", required=True, help="目标官方交易会话YYYY-MM-DD"
    )
    source_d1_breakout.add_argument(
        "--research-cutoff", required=True, help="研究数据截止交易会话YYYY-MM-DD"
    )
    source_d1_breakout.add_argument(
        "--source-package-id",
        type=int,
        required=True,
        help="必填：目标日精确TDX来源包数字ID",
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
        "--discover-structures",
        action="store_true",
        help="单遍评价完整目标日Raw来源池并分页返回五类新增D1结构",
    )
    source_d1_breakout.add_argument(
        "--structure-changes",
        action="store_true",
        help="配合--discover-structures：全池比较同一旧结构锚的相邻会话变化",
    )
    source_d1_breakout.add_argument(
        "--material-changes-only",
        action="store_true",
        help="只返回状态转折、首次有效触及、首次分离或真实Gap填补阶段变化",
    )
    source_d1_breakout.add_argument(
        "--structure-type",
        choices=STRUCTURE_TYPES,
        help="结构变化分页按一种D1来源结构类型过滤",
    )
    source_d1_breakout.add_argument(
        "--structure-change-status",
        choices=(
            "CHANGED",
            "UNCHANGED",
            "UNKNOWN",
            "NOT_COMPARABLE",
            "NOT_EVALUABLE",
            "NOT_APPLICABLE",
        ),
        help="按同锚字段变化状态过滤；要看实质结构转折使用--material-changes-only",
    )
    source_d1_breakout.add_argument(
        "--structure-symbol",
        help="全池结构发现时附带返回一个来源代码的目标日结构明细",
    )
    source_d1_breakout.add_argument(
        "--include-structures",
        action="store_true",
        help="单股历史模式同时输出五类D1锚出生及后继观察",
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
        help="按精确日期和来源版本计算、保存或分页查询来源代码级NON_PIT价格画像",
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
        "--before-source-package-id", type=int, help="changes前态画像的精确来源包数字ID"
    )
    price_profile.add_argument(
        "--before-profile-revision",
        help="changes前态画像修订标签，须使用快照返回的完整修订",
    )
    price_profile.add_argument(
        "--after-source-package-id", type=int, help="changes后态画像的精确来源包数字ID"
    )
    price_profile.add_argument(
        "--after-profile-revision",
        help="changes后态画像修订标签，须使用快照返回的完整修订",
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
    price_profile.add_argument(
        "--start-session", help="compute可选：从PG已存日期起逐日计算至目标日"
    )
    price_profile.add_argument(
        "--source-package-id", type=int, help="精确TDX来源包数字ID"
    )
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
    price_profile.add_argument(
        "--profile-revision", help="查询时必填的画像修订标签，须使用快照返回的完整修订"
    )
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
    preview_serve = commands.add_parser(
        "product-serve",
        aliases=["preview-serve"],
        help="启动本机正式产品研究页面（开发环境仅127.0.0.1:28766）",
        description="只读现有PG，研究截止日由调用者指定；不代表生产发布。",
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
