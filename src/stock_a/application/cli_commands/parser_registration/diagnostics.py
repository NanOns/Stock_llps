"""Stock A CLI命令按实际子域注册。"""

from __future__ import annotations

from stock_a.storage.postgres.binding import (
    BindingCommittedConfigError,
    BindingRefused,
    bind_stock_namespace,
    registered_migration_ids,
    registered_task_scopes,
)


def register_commands(commands):
    doctor = commands.add_parser("doctor", help="诊断本地配置、包身份和声明能力")
    doctor.add_argument(
        "--config", help="仅接受唯一登记配置runtime.defaults.json的绝对路径"
    )
    pg = commands.add_parser(
        "pg-identity", help="以只读事务核验stock_a登录、固定数据库/schema及项目迁移身份"
    )
    pg.add_argument(
        "--timeout-seconds",
        type=float,
        default=5.0,
        help="连接与每条查询的秒数上限，默认5秒，最大30秒",
    )
    bind = commands.add_parser(
        "pg-bind", help="在明确任务范围内以stock_a账号应用指定集中迁移"
    )
    bind.add_argument(
        "--task-scope",
        required=True,
        choices=registered_task_scopes(),
        help="必填：已批准且受限的PG任务范围",
    )
    bind.add_argument(
        "--migration-id",
        required=True,
        type=int,
        choices=registered_migration_ids(),
        help="必填：任务范围对应的已登记迁移ID；范围与ID配对由数据库绑定层再次核验",
    )
    bind.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="身份绑定总耗时上限，默认20秒，最大30秒",
    )
    phase0 = commands.add_parser(
        "source-phase0",
        help="有界核验已绑定的本地TDX来源能力",
        description="按明示预算有界核验已绑定的本地TDX来源能力，只读不写",
    )
    phase0.add_argument(
        "--max-entries",
        type=int,
        default=50000,
        help="每个行情目录最多枚举的目录项数，默认50000",
    )
    phase0.add_argument(
        "--max-file-bytes",
        type=int,
        default=16777216,
        help="单个来源文件最大读取字节数，默认16777216",
    )
    phase0.add_argument(
        "--max-total-bytes",
        type=int,
        default=33554432,
        help="本次来源文件总读取字节数上限，默认33554432",
    )
    phase0.add_argument(
        "--max-records",
        type=int,
        default=200000,
        help="每个日线文件及GBBQ最多解析记录数，默认200000",
    )
    phase0.add_argument(
        "--max-seconds", default="30", help="本次总核验耗时上限，默认30秒"
    )
