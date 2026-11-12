"""画像算法修订与显式窗口、日历身份的业务键。"""

from urllib.parse import quote

PROFILE_REVISION_PATTERN = (
    r"PROFILE_R[0-9]{2}(?:\|[1-9][0-9]?\|[A-Za-z0-9_.~%\-]+\|[A-Za-z0-9_.~%\-]+)?"
)


def profile_revision(
    base: str, count: int, calendar_id: str, calendar_revision: str
) -> str:
    """使用原始日历业务身份与窗口，不生成摘要或代用身份。"""
    return f"{base}|{count}|{quote(calendar_id, safe='')}|{quote(calendar_revision, safe='')}"
