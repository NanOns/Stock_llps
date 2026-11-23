"""Stock A CLI正式命令树；各领域命令参数由相邻注册模块唯一维护。"""

from __future__ import annotations

import argparse
from stock_a.application.runtime_config import (
    _EXIT_ARGUMENT,
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


def _parser() -> argparse.ArgumentParser:
    parser = ChineseParser(
        prog="stock-a",
        description="Stock A中文诊断与只读来源检查",
    )
    commands = parser.add_subparsers(dest="command", parser_class=ChineseParser)
    from stock_a.application.cli_commands.parser_registration.diagnostics import (
        register_commands as register_diagnostics,
    )
    from stock_a.application.cli_commands.parser_registration.source import (
        register_commands as register_source,
    )
    from stock_a.application.cli_commands.parser_registration.analysis import (
        register_commands as register_analysis,
    )

    register_diagnostics(commands)
    register_source(commands)
    register_analysis(commands)
    return parser
