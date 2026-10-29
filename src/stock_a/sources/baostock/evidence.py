"""BaoStock单日补充证据投影与TDX字段的只读对照。"""
from __future__ import annotations

from datetime import date
from decimal import Decimal, localcontext
from fractions import Fraction
import math
import struct
from typing import Any

from stock_a.domain import ContractError, FieldValue, exact_number, unknown
from stock_a.sources.baostock.daily import BaoUnitContract, normalize_daily_row


_FIELD_MAP = {
    "session_date": "date",
    "source_code": "code",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "amount": "amount",
    "turn": "turn",
    "tradestatus": "tradestatus",
    "isST": "isST",
}
_UNITS = BaoUnitContract(
    "BAOSTOCK_API_DOCUMENTED_UNITS_NOT_INDEPENDENTLY_BOUND_R1",
    "CNY_PER_SHARE_DOCUMENTED",
    1,
    1,
    1,
    "PERCENT",
)


def _render_value(value: object) -> object:
    if isinstance(value, Fraction):
        with localcontext() as context:
            context.prec = 24
            decimal_value = Decimal(value.numerator) / Decimal(value.denominator)
        return {"exact": f"{value.numerator}/{value.denominator}", "decimal": format(decimal_value, "f")}
    return value


def _field(value: FieldValue) -> dict[str, Any]:
    if value.quality != "OBSERVED":
        return {"value": None, "quality": value.quality, "reason": value.reason}
    return {"value": _render_value(value.value), "quality": "OBSERVED", "reason": None}


def _as_fraction(rendered: object) -> Fraction | None:
    try:
        if isinstance(rendered, dict):
            rendered = rendered.get("exact", rendered.get("decimal"))
        if isinstance(rendered, str) and "/" in rendered:
            numerator, denominator = rendered.split("/", 1)
            return Fraction(int(numerator), int(denominator))
        if isinstance(rendered, str):
            return Fraction(Decimal(rendered))
        return exact_number(rendered)
    except (ArithmeticError, TypeError, ValueError, ZeroDivisionError):
        return None


def _quality_field(mapping: object, key: str) -> FieldValue:
    if not isinstance(mapping, dict):
        return unknown("TDX字段容器不可用")
    item = mapping.get(key)
    if not isinstance(item, dict) or item.get("quality") != "OBSERVED":
        return unknown((item or {}).get("reason", "TDX字段未知") if isinstance(item, dict) else "TDX字段缺失")
    value = _as_fraction(item.get("value"))
    return FieldValue(value) if value is not None else unknown("TDX数值格式不可用于精确对照")


def _comparison(bao: FieldValue, tdx: FieldValue, *, units_unverified: bool = False) -> dict[str, Any]:
    if bao.quality != "OBSERVED" or tdx.quality != "OBSERVED":
        status = "NOT_COMPARABLE"
        relation = None
        difference = None
        reason = bao.reason if bao.quality != "OBSERVED" else tdx.reason
    else:
        bao_number = exact_number(bao.value)
        tdx_number = exact_number(tdx.value)
        difference = bao_number - tdx_number
        relation = "MATCH" if difference == 0 else "DIFFERENT"
        status = "UNIT_ASSUMPTION_UNVERIFIED" if units_unverified else "SYMBOL_DATE_ONLY_COMPARISON"
        reason = "金额/数量的TDX倍率仍是调用方声明假设；关系仅描述该假设下的数值差"
        if not units_unverified:
            reason = "仅按来源代码和交易日对照；未绑定稳定security_id或来源权威"
    payload: dict[str, Any] = {
        "status": status,
        "observed_relation_under_declared_units": relation,
        "baostock_value": _field(bao),
        "tdx_value": _field(tdx),
        "difference_baostock_minus_tdx": _render_value(difference) if difference is not None else None,
        "reason": reason,
    }
    if difference is not None and units_unverified and exact_number(tdx.value) != 0:
        payload["relative_difference_over_tdx"] = _render_value(difference / exact_number(tdx.value))
    else:
        payload["relative_difference_over_tdx"] = None
    return payload


def _float32_fraction(value: Fraction) -> Fraction | None:
    try:
        encoded = struct.pack("<f", float(value))
        decoded = struct.unpack("<f", encoded)[0]
    except (OverflowError, struct.error, ValueError):
        return None
    if not math.isfinite(decoded):
        return None
    return Fraction.from_float(decoded)


def _float32_ulp(value: Fraction) -> Fraction | None:
    """返回非负float32数值处的相邻正向量化间距。"""
    encoded = _float32_fraction(value)
    if encoded is None or value < 0:
        return None
    try:
        bits = struct.unpack("<I", struct.pack("<f", float(value)))[0]
        if bits >= 0x7F7FFFFF:
            return None
        following = struct.unpack("<f", struct.pack("<I", bits + 1))[0]
    except (OverflowError, struct.error, ValueError):
        return None
    if not math.isfinite(following):
        return None
    return Fraction.from_float(following) - encoded


def _price_comparison(bao: FieldValue, tdx: FieldValue, *, raw_response_confirmed: bool) -> dict[str, Any]:
    comparison = _comparison(bao, tdx)
    relation = comparison["observed_relation_under_declared_units"]
    status = "NOT_COMPARABLE" if relation is None else "EXACT_MATCH" if relation == "MATCH" else "PRICE_DIFFERENCE_UNEXPLAINED"
    return {
        "status": status,
        "coordinate": "RAW_UNADJUSTED" if raw_response_confirmed else None,
        "requested_adjustflag": "3",
        "observed_adjustflag": "3" if raw_response_confirmed else None,
        "unit_basis": "BAOSTOCK_DOCUMENTED_CNY_PER_SHARE_NOT_INDEPENDENTLY_BOUND",
        "baostock_value": comparison["baostock_value"],
        "tdx_value": comparison["tdx_value"],
        "difference_baostock_minus_tdx": comparison["difference_baostock_minus_tdx"],
        "reason": comparison["reason"],
    }


def _amount_precision_explanation(
    bao_amount: FieldValue,
    tdx_amount: FieldValue,
    amount_scale: Fraction | None,
) -> dict[str, Any]:
    known_difference = None
    known_relative_difference = None
    if bao_amount.quality == "OBSERVED" and tdx_amount.quality == "OBSERVED":
        bao_cny = exact_number(bao_amount.value)
        tdx_cny = exact_number(tdx_amount.value)
        known_difference = bao_cny - tdx_cny
        known_relative_difference = known_difference / tdx_cny if tdx_cny != 0 else None
    if (bao_amount.quality != "OBSERVED" or tdx_amount.quality != "OBSERVED"
            or amount_scale is None or amount_scale <= 0):
        reason = bao_amount.reason if bao_amount.quality != "OBSERVED" else tdx_amount.reason
        return {
            "status": "NOT_COMPARABLE/UNIT_UNVERIFIED",
            "unit_basis": "CALLER_DECLARED_TDX_AMOUNT_TO_CNY; NOT_INDEPENDENTLY_BOUND",
            "tdx_amount_to_cny": _render_value(amount_scale) if amount_scale is not None else None,
            "bao_amount_cny": _field(bao_amount),
            "tdx_amount_cny": _field(tdx_amount),
            "difference_bao_minus_tdx_cny": (
                _render_value(known_difference) if known_difference is not None else None
            ),
            "relative_difference_over_tdx": (
                _render_value(known_relative_difference) if known_relative_difference is not None else None
            ),
            "reason": reason or "金额或单位倍率未知，不能作float32量化判断",
        }

    bao_cny = exact_number(bao_amount.value)
    tdx_cny = exact_number(tdx_amount.value)
    bao_storage_unit = bao_cny / amount_scale
    tdx_storage_unit = tdx_cny / amount_scale
    projected = _float32_fraction(bao_storage_unit)
    encoded_tdx = _float32_fraction(tdx_storage_unit)
    if projected is None or encoded_tdx is None or encoded_tdx != tdx_storage_unit:
        return {
            "status": "NOT_COMPARABLE/UNIT_UNVERIFIED",
            "unit_basis": "CALLER_DECLARED_TDX_AMOUNT_TO_CNY",
            "tdx_amount_to_cny": _render_value(amount_scale),
            "bao_amount_cny": _field(bao_amount),
            "tdx_amount_cny": _field(tdx_amount),
            "difference_bao_minus_tdx_cny": _render_value(known_difference),
            "relative_difference_over_tdx": (
                _render_value(known_relative_difference) if known_relative_difference is not None else None
            ),
            "bao_amount_in_tdx_storage_units": _render_value(bao_storage_unit),
            "tdx_float32_stored_value_in_tdx_storage_units": _render_value(tdx_storage_unit),
            "reason": "金额不是有限float32范围内的可比较值，或TDX值不能还原为其float32存储坐标",
        }

    difference = bao_cny - tdx_cny
    if difference == 0:
        status = "EXACT_MATCH"
    elif projected == tdx_storage_unit:
        status = "FLOAT32_QUANTIZATION_EQUIVALENT_UNDER_DECLARED_UNITS"
    else:
        status = "DIFFERENT_UNEXPLAINED"
    ulp_storage = _float32_ulp(tdx_storage_unit)
    ulp_cny = ulp_storage * amount_scale if ulp_storage is not None else None
    return {
        "status": status,
        "unit_basis": "CALLER_DECLARED_TDX_AMOUNT_TO_CNY; NOT_INDEPENDENTLY_BOUND",
        "tdx_amount_to_cny": _render_value(amount_scale),
        "bao_amount_cny": _field(bao_amount),
        "tdx_amount_cny": _field(tdx_amount),
        "difference_bao_minus_tdx_cny": _render_value(difference),
        "relative_difference_over_tdx": _render_value(difference / tdx_cny) if tdx_cny != 0 else None,
        "bao_amount_in_tdx_storage_units": _render_value(bao_storage_unit),
        "tdx_float32_stored_value_in_tdx_storage_units": _render_value(tdx_storage_unit),
        "bao_amount_float32_projection_in_tdx_storage_units": _render_value(projected),
        "tdx_float32_ulp_cny": _render_value(ulp_cny) if ulp_cny is not None else None,
        "difference_in_tdx_float32_ulps": _render_value(difference / ulp_cny) if ulp_cny not in (None, 0) else None,
        "explanation": (
            "Bao金额在相同调用方单位假设下编码为TDX float32后与TDX存储值相等；这是格式量化解释，不证明来源单位权威。"
            if status == "FLOAT32_QUANTIZATION_EQUIVALENT_UNDER_DECLARED_UNITS"
            else "Bao与TDX金额的精确值相同。" if status == "EXACT_MATCH"
            else "float32投影仍不能解释该金额差异；保留为未解释差异。"
        ),
    }


def _volume_difference(bao_volume: FieldValue, tdx_volume: FieldValue, tdx_volume_raw: FieldValue) -> dict[str, Any]:
    if bao_volume.quality != "OBSERVED" or tdx_volume.quality != "OBSERVED":
        return {
            "status": "NOT_COMPARABLE",
            "baostock_value_shares": _field(bao_volume),
            "tdx_value_shares": _field(tdx_volume),
            "difference_bao_minus_tdx_shares": None,
            "reason": bao_volume.reason if bao_volume.quality != "OBSERVED" else tdx_volume.reason,
        }
    difference = exact_number(bao_volume.value) - exact_number(tdx_volume.value)
    return {
        "status": "EXACT_MATCH" if difference == 0 else "SOURCE_DIFFERENCE_UNRESOLVED",
        "baostock_value_shares": _field(bao_volume),
        "tdx_value_shares": _field(tdx_volume),
        "tdx_stored_integer_before_declared_multiplier": _field(tdx_volume_raw),
        "difference_bao_minus_tdx_shares": _render_value(difference),
        "unit_basis": "TDX_VOLUME_TO_SHARES_CALLER_DECLARED_NOT_INDEPENDENTLY_BOUND",
        "reason": None if difference == 0 else "成交量是独立整数来源字段；差值原因未证实，不归因于float32金额精度或复权。",
    }


def _unknown_daily(reason: str) -> dict[str, FieldValue]:
    return {
        key: unknown(reason)
        for key in ("open", "high", "low", "close", "volume", "amount", "turn", "tradestatus", "isST")
    }


def _semantic_status(value: FieldValue, *, true_label: str, false_label: str) -> dict[str, Any]:
    if value.quality != "OBSERVED":
        return {"quality": "UNKNOWN_DATA", "status": "UNKNOWN", "raw_value": None, "reason": value.reason}
    raw = str(value.value)
    if raw == "1":
        return {"quality": "OBSERVED", "status": true_label, "raw_value": value.value, "reason": None}
    if raw == "0":
        return {"quality": "OBSERVED", "status": false_label, "raw_value": value.value, "reason": None}
    return {"quality": "UNKNOWN_DATA", "status": "UNKNOWN", "raw_value": value.value,
            "reason": "BaoStock状态枚举值不在已知合同范围"}


def project_baostock_evidence(
    *,
    query: dict[str, Any],
    requested_symbol: str,
    target_session: str,
    tdx_result: dict[str, Any],
) -> dict[str, Any]:
    """把一条实际/测试查询结果投影为不改变TDX结果的补充字段。"""
    normalized_symbol = requested_symbol.upper()
    market, code = normalized_symbol.split(".", 1)
    expected_source_code = f"{market.lower()}.{code}"
    status = str(query.get("status", "QUERY_FAILED"))
    response_fields = tuple(query.get("response_fields") or ())
    rows = query.get("rows") or []
    reason = str(query.get("reason") or "BaoStock来源未返回有效结果")
    observation = None
    accepted_row: dict[str, Any] | None = None
    coordinate_status = "NOT_AVAILABLE"
    row_validation_reason = reason

    if status == "RESPONSE_RECEIVED":
        if not rows:
            status = "NO_TARGET_ROW"
            row_validation_reason = "该单日查询未返回行；不能据此推断停牌、退市或上市前"
        elif len(rows) != 1:
            status = "PROTOCOL_INVALID"
            row_validation_reason = f"单日查询返回{len(rows)}行，预期恰为1行；拒绝择行"
        elif not {"date", "code"}.issubset(response_fields):
            status = "PROTOCOL_INVALID"
            row_validation_reason = "响应字段缺少日期或来源证券代码，无法验证行身份"
        else:
            row = rows[0]
            try:
                if date.fromisoformat(str(row.get("date"))) != date.fromisoformat(target_session):
                    raise ValueError("响应日期与目标交易日不符")
                if str(row.get("code", "")).lower() != expected_source_code:
                    raise ValueError("响应来源证券代码与请求不符")
                if "adjustflag" not in response_fields or str(row.get("adjustflag")) != "3":
                    raise ValueError("响应未确认adjustflag=3原始未复权坐标")
                observation = normalize_daily_row(row, response_fields, _FIELD_MAP, _UNITS)
                accepted_row = row
                coordinate_status = "RAW_UNADJUSTED_RESPONSE_CONFIRMED"
                row_validation_reason = "响应日期、来源代码及adjustflag=3符合请求"
            except (ContractError, TypeError, ValueError) as error:
                status = "PROTOCOL_INVALID"
                row_validation_reason = str(error)

    if observation is None:
        fields = _unknown_daily(row_validation_reason)
    else:
        fields = {
            "open": observation.open,
            "high": observation.high,
            "low": observation.low,
            "close": observation.close,
            "volume": observation.volume,
            "amount": observation.amount,
            "turn": observation.turn,
            "tradestatus": observation.tradestatus,
            "isST": observation.is_st,
        }

    tdx_fields = tdx_result.get("target_raw_fields", {})
    tdx_prices = {name: _quality_field(tdx_fields, name) for name in ("open", "high", "low", "close")}
    tdx_amount = _quality_field(tdx_fields, "amount")
    tdx_volume = _quality_field(tdx_fields, "volume")
    tdx_units = tdx_result.get("unit_contract", {})
    amount_scale = _as_fraction(tdx_units.get("amount_to_cny", "")) if isinstance(tdx_units, dict) else None
    volume_scale = _as_fraction(tdx_units.get("volume_to_shares", "")) if isinstance(tdx_units, dict) else None
    if tdx_amount.quality == "OBSERVED" and amount_scale and amount_scale > 0:
        tdx_amount_raw = FieldValue(exact_number(tdx_amount.value) / amount_scale)
    else:
        tdx_amount_raw = unknown(tdx_amount.reason or "TDX金额倍率不可用")
    if tdx_volume.quality == "OBSERVED" and volume_scale and volume_scale > 0:
        tdx_volume_raw = FieldValue(exact_number(tdx_volume.value) / volume_scale)
    else:
        tdx_volume_raw = unknown(tdx_volume.reason or "TDX数量倍率不可用")

    row_is_target = observation is not None and status == "RESPONSE_RECEIVED"
    raw_values = {
        key: (accepted_row.get(column) if accepted_row is not None and column in response_fields else None)
        for key, column in (
            ("session_date", "date"), ("source_code", "code"),
            ("open", "open"), ("high", "high"), ("low", "low"), ("close", "close"),
            ("volume", "volume"), ("amount", "amount"), ("adjustflag", "adjustflag"),
            ("turn", "turn"), ("tradestatus", "tradestatus"), ("isST", "isST"),
        )
    }
    bao_amount_raw = _as_fraction(raw_values["amount"])
    bao_volume_raw = _as_fraction(raw_values["volume"])
    return {
        "status": status,
        "source": "BAOSTOCK",
        "requested_symbol": normalized_symbol,
        "requested_source_code": expected_source_code,
        "target_session": target_session,
        "sdk_version": query.get("sdk_version"),
        "request": query.get("request", {}),
        "request_started_at": query.get("request_started_at"),
        "request_completed_at": query.get("request_completed_at"),
        "capture_clock_role": query.get("capture_clock_role"),
        "login_attempted": query.get("login_attempted", False),
        "login_error_code": query.get("login_error_code"),
        "login_error_message": query.get("login_error_message"),
        "query_error_code": query.get("query_error_code"),
        "query_error_message": query.get("query_error_message"),
        "logout_attempted": query.get("logout_attempted", False),
        "logout_error_code": query.get("logout_error_code"),
        "logout_error_message": query.get("logout_error_message"),
        "error_type": query.get("error_type"),
        "response_fields": list(response_fields),
        "rows_seen": query.get("rows_seen", len(rows)),
        "response_rows": rows,
        "raw_source_values": raw_values,
        "row_validation": {"status": "VALID" if row_is_target else status, "reason": row_validation_reason},
        "coordinate": {
            "status": coordinate_status,
            "adjustflag": "3" if row_is_target else None,
            "role": "RAW_UNADJUSTED",
        },
        "identity": "SOURCE_SYMBOL_ONLY",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "unit_disclosure": {
            "close": "BaoStock API文档记载为元/股，尚未完成独立单位权威验收",
            "amount": "BaoStock API文档记载为人民币元；TDX倍率是假设，跨源单位未独立绑定",
            "volume": "BaoStock API文档记载为股；TDX倍率是假设，跨源单位未独立绑定",
            "turn": "BaoStock API文档turn字段按百分比；此处除以100输出fraction",
            "status": "BaoStock单源状态证据，不是官方交易状态证明",
            "independent_authority_bound": False,
        },
        "fields": {
            "open": _field(fields["open"]),
            "high": _field(fields["high"]),
            "low": _field(fields["low"]),
            "close": _field(fields["close"]),
            "volume": _field(fields["volume"]),
            "amount": _field(fields["amount"]),
            "turn": _field(fields["turn"]),
            "tradestatus": _semantic_status(fields["tradestatus"],
                                             true_label="BAOSTOCK_REPORTED_TRADING",
                                             false_label="BAOSTOCK_REPORTED_SUSPENSION"),
            "isST": _semantic_status(fields["isST"], true_label="BAOSTOCK_REPORTED_ST",
                                      false_label="BAOSTOCK_REPORTED_NOT_ST"),
        },
        "price_comparisons": {
            name: _price_comparison(fields[name], tdx_prices[name], raw_response_confirmed=row_is_target)
            for name in ("open", "high", "low", "close")
        },
        "comparisons": {
            "open": _comparison(fields["open"], tdx_prices["open"]),
            "high": _comparison(fields["high"], tdx_prices["high"]),
            "low": _comparison(fields["low"], tdx_prices["low"]),
            "close": _comparison(fields["close"], tdx_prices["close"]),
            "volume": _comparison(fields["volume"], tdx_volume, units_unverified=True),
            "amount": _comparison(fields["amount"], tdx_amount, units_unverified=True),
            "volume_raw_as_returned": _comparison(
                FieldValue(bao_volume_raw) if bao_volume_raw is not None else unknown(fields["volume"].reason or "Bao原始成交量不可用"),
                tdx_volume_raw, units_unverified=True,
            ),
            "amount_raw_as_returned": _comparison(
                FieldValue(bao_amount_raw) if bao_amount_raw is not None else unknown(fields["amount"].reason or "Bao原始金额不可用"),
                tdx_amount_raw, units_unverified=True,
            ),
        },
        "amount_precision_explanation": _amount_precision_explanation(fields["amount"], tdx_amount, amount_scale),
        "volume_difference": _volume_difference(fields["volume"], tdx_volume, tdx_volume_raw),
        "turnover_percentile": {
            "status": "NOT_EVALUATED",
            "reason": "仅查询目标日一行，未验证同一身份前60个有效交易日，不能计算历史分位",
        },
        "tdx_unchanged": True,
        "production_profile_gate": "BLOCKED_UNCHANGED",
        "source_writes_attempted": False,
        "database_io_attempted": False,
        "database_write_attempted": False,
        "reason": row_validation_reason if row_is_target else query.get("reason") or row_validation_reason,
    }
