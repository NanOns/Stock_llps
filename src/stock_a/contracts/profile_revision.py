"""画像算法修订与显式窗口、日历身份的业务键。"""

import re
from urllib.parse import quote

MAX_PROFILE_HISTORY_SESSIONS = 250
PROFILE_REVISION_PATTERN = r"PROFILE_R[0-9]{2}(?:\|(?:[1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|250)\|(?:[A-Za-z0-9_.~\-]|%[0-9A-F]{2})+\|(?:[A-Za-z0-9_.~\-]|%[0-9A-F]{2})+)?"


def profile_revision(
    base: str, count: int, calendar_id: str, calendar_revision: str
) -> str:
    """使用原始日历业务身份与窗口，不生成摘要或代用身份。"""
    if type(count) is not int or not 1 <= count <= MAX_PROFILE_HISTORY_SESSIONS:
        raise ValueError("画像历史会话数必须为1至250之间的整数")
    return f"{base}|{count}|{quote(calendar_id, safe='')}|{quote(calendar_revision, safe='')}"


def profile_revision_history_session_count(value: str) -> int | None:
    """从完整画像修订键读取其显式历史会话数。"""
    if (
        not isinstance(value, str)
        or re.fullmatch(PROFILE_REVISION_PATTERN, value) is None
    ):
        return None
    parts = value.split("|")
    if len(parts) != 4 or not parts[1].isdecimal():
        return None
    count = int(parts[1])
    return count if 0 < count <= MAX_PROFILE_HISTORY_SESSIONS else None
