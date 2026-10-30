"""预览响应使用的稳定JSON投影与摘要函数。"""

from datetime import date, datetime
from decimal import Decimal
from collections.abc import Mapping
from typing import Any
import hashlib
import json


def _json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, float) and (
            value != value or value in (float("inf"), float("-inf"))
        ):
            raise ValueError("JSON拒绝NaN和Infinity")
        return value
    raise TypeError(f"不支持的预览响应类型：{type(value).__name__}")


def canonical_sha(value: Any) -> str:
    encoded = json.dumps(
        _json_value(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
