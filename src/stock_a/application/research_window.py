"""按现役官方日历解析显式请求的历史会话窗口。"""

from datetime import date

from stock_a.application.runtime_config import (
    DiagnosticError,
    _EXIT_ARGUMENT,
    _EXIT_UNAVAILABLE,
)
from stock_a.research.entry import ResearchEntryError, _load_calendar, _parse_target


def resolve_pg_history_window(
    target_text: str, count: int
) -> tuple[date, tuple[date, ...], str]:
    if type(count) is not int or not 1 <= count <= 75:
        raise DiagnosticError(
            "PG_HISTORY_WINDOW_INVALID", "PG历史画像会话数须为1至75", _EXIT_ARGUMENT
        )
    try:
        target = _parse_target(target_text)
    except ResearchEntryError as error:
        raise DiagnosticError(error.code, str(error), _EXIT_ARGUMENT) from error
    _calendar_payload, _metadata, revision, all_sessions = _load_calendar(target)
    if target not in all_sessions:
        raise DiagnosticError(
            "TARGET_DATE_INVALID", "目标日不在现役版本化官方交易日历中", _EXIT_ARGUMENT
        )
    eligible = tuple(session for session in all_sessions if session <= target)
    if len(eligible) < count:
        raise DiagnosticError(
            "UNKNOWN_CALENDAR",
            "官方日历覆盖不足以提供请求的历史会话窗口",
            _EXIT_UNAVAILABLE,
        )
    return target, eligible[-count:], revision
