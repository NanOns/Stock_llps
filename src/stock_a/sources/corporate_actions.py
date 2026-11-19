"""受限解析证券交易所公开实施公告中的公司行为事实。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from urllib.parse import urlparse
from zoneinfo import ZoneInfo


_SHANGHAI = ZoneInfo("Asia/Shanghai")
_DATE_PATTERN = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日")


class CorporateActionSourceError(ValueError):
    """公司行为来源文本不满足已采用的解析合同。"""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class CorporateActionRecord:
    """从一份官方实施公告解析的来源代码级权益分派事件。"""

    event_ref: str
    semantic_revision: str
    source_ref: str
    source_notice_id: str
    source_market: str
    source_symbol: str
    effective_session: date
    published_date: date
    available_at: datetime
    available_at_quality: str
    captured_at: datetime
    action_kind: str
    cash_per_10: Decimal
    rights_price: Decimal
    bonus_per_10: Decimal
    rights_per_10: Decimal
    declared_bonus_per_10: Decimal
    share_change_ratio: Decimal
    source_quality: str


def _date_from_match(match: re.Match[str], field: str) -> date:
    try:
        return date(*(int(value) for value in match.groups()))
    except ValueError as error:
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_DATE_INVALID", f"公告中的{field}日期非法"
        ) from error


def parse_szse_capital_transfer_notice(
    text: str,
    *,
    source_ref: str,
    captured_at: datetime,
) -> CorporateActionRecord:
    """解析深交所仅含资本公积转增、无现金及配股的实施公告。

    公告只提供发布日期时，available_at使用次日零时作为保守的日期边界；
    该边界不冒充精确发布时间，且结果仍须标注后验研究时钟。
    """

    if not isinstance(text, str) or not text.strip() or len(text) > 2_000_000:
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_TEXT_INVALID", "公告文本为空或超过解析上限"
        )
    if (
        not isinstance(captured_at, datetime)
        or captured_at.tzinfo is None
        or captured_at.utcoffset() is None
    ):
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_CAPTURE_CLOCK_INVALID", "公告捕获时刻必须带时区"
        )
    parsed_url = urlparse(source_ref) if isinstance(source_ref, str) else None
    if (
        parsed_url is None
        or parsed_url.scheme != "https"
        or parsed_url.hostname != "disc.static.szse.cn"
        or not parsed_url.path.lower().endswith(".pdf")
    ):
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_SOURCE_REJECTED", "只接受深交所静态公告PDF来源"
        )

    code_match = re.search(r"证券代码\s*[：:]\s*(\d{6})", text)
    notice_match = re.search(r"公告编号\s*[：:]\s*([A-Za-z0-9-]+)", text)
    effective_match = re.search(
        r"除权除息日\s*(?:为)?\s*[：:]\s*" + _DATE_PATTERN.pattern,
        text,
    )
    if code_match is None or notice_match is None or effective_match is None:
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_REQUIRED_FIELD_MISSING",
            "公告缺证券代码、公告编号或除权除息日",
        )

    dates = list(_DATE_PATTERN.finditer(text))
    if not dates:
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_PUBLISHED_DATE_UNKNOWN", "公告未提供可解析的发布日期"
        )
    effective_session = _date_from_match(effective_match, "除权除息")
    published_date = _date_from_match(dates[-1], "公告发布日期")
    if published_date > effective_session:
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_PUBLICATION_AFTER_EFFECTIVE",
            "公告发布日期晚于除权除息日，拒绝倒签事件可得时间",
        )

    transfer_match = re.search(r"每\s*10\s*股转增\s*(\d+(?:\.\d+)?)\s*股", text)
    if transfer_match is None:
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_UNSUPPORTED_TERMS", "未识别每十股转增比例"
        )
    try:
        declared_bonus = Decimal(transfer_match.group(1))
    except InvalidOperation as error:
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_NUMERIC_INVALID", "转增比例不是有限十进制数"
        ) from error

    scope_start = text.find("权益分派方案")
    scope_end = text.find("股权登记日", scope_start + 1)
    scope = text[scope_start : scope_end if scope_end > scope_start else None]
    if (
        "不送红股" not in text
        or not re.search(r"不派发?现金红利", text)
        or "配股" in scope
        or "除权除息参考价格" not in text
    ):
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_UNSUPPORTED_TERMS",
            "公告含未支持的现金、送股、配股或参考价格语义",
        )

    ratio_start = text.find("股份变动比例")
    ratio_scope = text[ratio_start : ratio_start + 320] if ratio_start >= 0 else ""
    ratio_sentence = re.split(r"[。；]", ratio_scope, maxsplit=1)[0]
    ratio_values = re.findall(r"=\s*(\d+(?:\.\d+)?)", ratio_sentence)
    if not ratio_values:
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_SHARE_RATIO_UNKNOWN", "公告未提供可解析的股份变动比例"
        )
    try:
        share_change_ratio = Decimal(ratio_values[-1])
    except InvalidOperation as error:
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_NUMERIC_INVALID", "股份变动比例不是有限十进制数"
        ) from error
    if (
        not share_change_ratio.is_finite()
        or share_change_ratio <= 0
        or declared_bonus <= 0
        or "除权除息日前一交易日收盘价" not in text
        or not re.search(r"/\s*[（(]?\s*1\s*\+\s*股份变动比例", text)
    ):
        raise CorporateActionSourceError(
            "CORPORATE_ACTION_FORMULA_UNSUPPORTED",
            "公告未证明正向股份变动比例及匹配的除权参考公式",
        )

    code = code_match.group(1)
    symbol = f"SZ.{code}"
    notice_id = notice_match.group(1)
    available_at = datetime.combine(
        published_date + timedelta(days=1), time.min, tzinfo=_SHANGHAI
    )
    bonus_per_10 = (share_change_ratio * Decimal(10)).normalize()
    normalized = {
        "action_kind": "CAPITAL_RESERVE_TO_STOCK",
        "available_at_quality": "DATE_ONLY_NEXT_DAY_BOUNDARY",
        "bonus_per_10": str(bonus_per_10),
        "cash_per_10": "0",
        "effective_session": effective_session.isoformat(),
        "published_date": published_date.isoformat(),
        "rights_per_10": "0",
        "rights_price": "0",
        "share_change_ratio": str(share_change_ratio.normalize()),
        "source_market": "SZ",
        "source_notice_id": notice_id,
        "source_ref": source_ref,
        "source_symbol": symbol,
    }
    revision = hashlib.sha256(
        json.dumps(
            normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    return CorporateActionRecord(
        event_ref=f"SZSE:{notice_id}:{symbol}:{effective_session.isoformat()}",
        semantic_revision=revision,
        source_ref=source_ref,
        source_notice_id=notice_id,
        source_market="SZ",
        source_symbol=symbol,
        effective_session=effective_session,
        published_date=published_date,
        available_at=available_at,
        available_at_quality="DATE_ONLY_NEXT_DAY_BOUNDARY",
        captured_at=captured_at,
        action_kind="CAPITAL_RESERVE_TO_STOCK",
        cash_per_10=Decimal(0),
        rights_price=Decimal(0),
        bonus_per_10=bonus_per_10,
        rights_per_10=Decimal(0),
        declared_bonus_per_10=declared_bonus,
        share_change_ratio=share_change_ratio,
        source_quality="VERIFIED_OFFICIAL_NOTICE",
    )
