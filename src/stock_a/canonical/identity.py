"""来源原件身份、字段语义身份与可得时钟身份各自计算。"""
from datetime import date, datetime, timezone
from hashlib import sha256
import json
from collections.abc import Mapping

from stock_a.domain import FieldValue, exact_number


def _encode(value):
    if isinstance(value, FieldValue):
        return ["field", value.quality, value.reason, _encode(value.value)]
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("时钟必须有时区")
        return ["time", value.astimezone(timezone.utc).isoformat()]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("摘要键必须为字符串")
        return ["map", [[key, _encode(value[key])] for key in sorted(value)]]
    if isinstance(value, (list, tuple)):
        return ["sequence", [_encode(item) for item in value]]
    number = exact_number(value)
    return ["number", number.numerator, number.denominator]


def semantic_digest(value) -> str:
    encoded = json.dumps(_encode(value), ensure_ascii=False, separators=(",", ":"))
    return sha256(encoded.encode("utf-8")).hexdigest()


def field_digest(*, entity_id: str, session: date, field_id: str, value: FieldValue,
                 source_authority: str, parser_version: str, unit_version: str,
                 normalization_version: str) -> str:
    """源文件追加/重新探测不改变旧字段；禁止传运行ID或整个source_set。"""
    if (not all(isinstance(x, str) and x for x in
                (entity_id, field_id, source_authority, parser_version, unit_version, normalization_version))
            or type(session) is not date or not isinstance(value, FieldValue)):
        raise ValueError("规范字段摘要缺少精确业务键或版本")
    return semantic_digest(("CANONICAL_FIELD_R1", entity_id, session, field_id, value,
                            source_authority, parser_version, unit_version, normalization_version))


def window_digest(*, field_rows, calendar_revision: str, status_revision: str,
                  coordinate_revision: str, producer_version: str, parameter_digest: str) -> str:
    """只接收已选窗口；revision必须属于被消费的投影，不是每日全局新版本。"""
    if not all(isinstance(x, str) and x for x in
               (calendar_revision, status_revision, coordinate_revision, producer_version, parameter_digest)):
        raise ValueError("窗口缺少显式版本")
    rows = tuple(field_rows)
    keys = []
    for session, field, digest in rows:
        if type(session) is not date or not isinstance(field, str) or not field:
            raise ValueError("窗口键非法")
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("字段摘要非法")
        keys.append((session, field))
    if not rows or keys != sorted(set(keys)):
        raise ValueError("窗口必须非空、按业务键升序且无重复")
    return semantic_digest(("FIELD_WINDOW_R1", rows, calendar_revision, status_revision,
                            coordinate_revision, producer_version, parameter_digest))


def availability_digest(*, selected_capture_id: str, available_at: datetime,
                        durable_at: datetime, accepted_at: datetime, cutoff: datetime) -> str:
    """所选binding的原时钟/as-of身份，不传本轮wall-clock；本函数不签发LIVE。"""
    if not isinstance(selected_capture_id, str) or not selected_capture_id:
        raise ValueError("缺少实际选定capture")
    clocks = (available_at, durable_at, accepted_at, cutoff)
    if any(not isinstance(x, datetime) or x.tzinfo is None or x.utcoffset() is None for x in clocks):
        raise ValueError("真实时钟缺失或无时区")
    if not available_at <= durable_at <= accepted_at:
        raise ValueError("来源、持久及接受时钟顺序非法")
    return semantic_digest(("AVAILABILITY_R1", selected_capture_id, *clocks))
