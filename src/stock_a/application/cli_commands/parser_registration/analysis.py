"""Stock A CLI命令按实际子域注册。"""

from __future__ import annotations


def register_commands(commands):
    d0_raw = commands.add_parser(
        "source-d0-raw",
        help="只读计算BaoStock paired capture的D0启动、恢复诊断及相邻变化",
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
        "--source-capture-id",
        type=int,
        required=True,
        help="覆盖目标会话及前态会话的BaoStock paired capture数字ID",
    )
    d0_raw.add_argument(
        "--before-source-capture-id",
        type=int,
        help="可选前态捕获；单paired捕获窗口内通常与--source-capture-id相同",
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

    fep_baseline = commands.add_parser(
        "source-fep-baseline",
        help="从已存来源T0与BaoStock Forward typed事实离线生成FEP描述统计",
    )
    fep_baseline.add_argument(
        "--research-cutoff", required=True, help="显式最晚可读官方会话"
    )
    fep_baseline.add_argument(
        "--source-capture-id",
        type=int,
        required=True,
        help="BaoStock股票价格来源捕获数字ID",
    )
    fep_baseline.add_argument(
        "--rule-id",
        action="append",
        required=True,
        help="精确来源规则，可重复指定；按每条规则版本分区保存",
    )
    fep_baseline.add_argument(
        "--horizon",
        type=int,
        choices=(1, 3, 5),
        action="append",
        required=True,
        help="T0后的官方会话数，可重复指定",
    )
    fep_baseline.add_argument(
        "--lane",
        choices=("ENTRY", "DAILY_LANDMARK", "REENTRY"),
        action="append",
        required=True,
        help="独立FEP观察lane，可重复指定；REENTRY和左截断不并入ENTRY",
    )
    fep_baseline.add_argument(
        "--persist",
        action="store_true",
        required=True,
        help="显式授权本次离线typed结果写入唯一PG",
    )

    product_serve = commands.add_parser(
        "product-serve",
        aliases=["preview-serve"],
        help="启动本机正式产品研究页面（开发环境仅127.0.0.1:28766）",
        description="只读现有PG，研究截止日由调用者指定；不代表生产发布。",
    )
    product_serve.add_argument(
        "--host", default="127.0.0.1", help="固定127.0.0.1，不允许远程监听"
    )
    product_serve.add_argument(
        "--port", type=int, default=28766, help="固定开发端口28766"
    )
    product_serve.add_argument(
        "--research-cutoff",
        required=True,
        help="显式冻结研究截止日；之后的页面查询在访问PG前拒绝",
    )
    product_serve.add_argument(
        "--authorize-data-jobs",
        action="store_true",
        help="仅在用户另行解除数据冻结后启用数据任务；默认关闭",
    )
