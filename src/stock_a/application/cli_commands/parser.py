"""Stock A CLI正式命令树；各领域命令参数由相邻注册模块唯一维护。"""

from __future__ import annotations

import argparse
from stock_a.application.runtime_config import (
    DiagnosticError,
    _CONFIG_ROOT,
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_OK,
    _EXIT_PERMISSION,
    _EXIT_UNAVAILABLE,
    _WORKSPACE,
    load_runtime_config as _load_runtime_config,
)

from stock_a.application.cli_commands.common import _emit


class ChineseFormatter(argparse.HelpFormatter):
    def _format_usage(self, usage, actions, groups, prefix):
        return super()._format_usage(usage, actions, groups, "用法：")

    def start_section(self, heading):
        translated = {"positional arguments": "位置参数", "options": "选项"}.get(
            heading, heading
        )
        super().start_section(translated)


class ChineseParser(argparse.ArgumentParser):
    formatter_class = ChineseFormatter

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("add_help", False)
        super().__init__(*args, **kwargs)
        self.add_argument(
            "-h",
            "--help",
            action="help",
            default=argparse.SUPPRESS,
            help="显示此帮助信息并退出",
        )

    def format_usage(self) -> str:
        return super().format_usage().replace("usage:", "用法：", 1)

    def format_help(self) -> str:
        rendered = super().format_help().replace("usage:", "用法：", 1)
        for english, chinese in (
            ("positional arguments:", "位置参数："),
            ("optional arguments:", "选项："),
            ("options:", "选项："),
        ):
            rendered = rendered.replace(english, chinese)
        return rendered

    def error(self, message: str) -> None:
        _emit(
            {
                "ok": False,
                "code": "ARGUMENT_ERROR",
                "message": "参数错误：请核对命令格式及必需参数",
            },
            error=True,
        )
        raise SystemExit(_EXIT_ARGUMENT)


def _add_research_arguments(
    command: argparse.ArgumentParser, *, include_vipdata: bool = False
) -> None:
    command.add_argument(
        "--target-session", required=True, help="必填目标交易会话，格式YYYY-MM-DD"
    )
    command.add_argument(
        "--unit-version", required=True, help="本次研究使用的显式TDX单位合同版本"
    )
    command.add_argument(
        "--amount-to-cny", help="可选：金额到人民币元的研究倍率；省略则金额单位保持未知"
    )
    command.add_argument(
        "--volume-to-shares",
        type=int,
        help="可选：数量到股的研究倍率；省略则数量单位保持未知",
    )
    command.add_argument(
        "--max-bytes",
        type=int,
        default=16777216,
        help="单文件最多读取字节数，默认16777216",
    )
    command.add_argument(
        "--max-records",
        type=int,
        default=200000,
        help="单文件最多读取记录数，默认200000",
    )
    command.add_argument(
        "--chunk-records", type=int, default=256, help="单次读取块记录数，默认256"
    )
    command.add_argument(
        "--max-seconds", default="30", help="单文件最大读取秒数，默认30"
    )
    if include_vipdata:
        command.add_argument(
            "--vipdata-package",
            help="显式选择G:/codex_tmp内已下载的VIPDATA ZIP；只读并逐次核验SHA/CRC",
        )
        command.add_argument(
            "--vipdata-package-sha256",
            help="显式VIPDATA包的预期SHA-256；与--vipdata-package配套必填",
        )
        command.add_argument(
            "--max-vipdata-validation-seconds",
            type=float,
            default=300.0,
            help="本次ZIP SHA/CRC完整校验总秒数上限，默认300",
        )
        command.add_argument(
            "--max-vipdata-extract-bytes",
            type=int,
            default=536870912,
            help="当前单页临时解压字节上限，默认536870912",
        )


def _parser() -> argparse.ArgumentParser:
    parser = ChineseParser(
        prog="stock-a",
        description="Stock A中文诊断与TDX只读来源检查",
    )
    commands = parser.add_subparsers(dest="command", parser_class=ChineseParser)
    from stock_a.application.cli_commands.parser_registration.diagnostics import (
        register_commands as register_diagnostics,
    )
    from stock_a.application.cli_commands.parser_registration.inspection import (
        register_commands as register_inspection,
    )
    from stock_a.application.cli_commands.parser_registration.research import (
        register_commands as register_research,
    )
    from stock_a.application.cli_commands.parser_registration.vipdata import (
        register_commands as register_vipdata,
    )
    from stock_a.application.cli_commands.parser_registration.source import (
        register_commands as register_source,
    )
    from stock_a.application.cli_commands.parser_registration.analysis import (
        register_commands as register_analysis,
    )

    register_diagnostics(commands)
    register_inspection(commands)
    register_research(commands)
    register_vipdata(commands)
    register_source(commands)
    register_analysis(commands)
    return parser
