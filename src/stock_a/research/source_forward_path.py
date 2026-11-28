"""来源级后验QFQ价格路径；不产生正式结算或交易事实。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from fractions import Fraction
from typing import Any

from stock_a.domain import exact_number
from stock_a.research.entry_context.sessions import _parse_target
from stock_a.research.entry_history.history_timeline import (
    _timeline_actual_qfq_ohlc,
    _timeline_raw_ohlc,
    post_signal_price_facts,
)
from stock_a.sources.baostock.identity import normalize_baostock_symbol


def _calendar_revision_map(value: Any) -> dict[int, str] | None:
    if not isinstance(value, str) or not value:
        return None
    result: dict[int, str] = {}
    for component in value.split("+"):
        year_text, separator, revision = component.partition(":")
        if not separator or not year_text.isdigit() or not revision:
            return None
        year = int(year_text)
        if not 1000 <= year <= 9999 or year in result:
            return None
        result[year] = revision
    return result or None


def forward_calendar_revision_compatible(
    target_session: date,
    endpoint_session: date,
    *,
    rule_revision: str,
    path_revision: str,
    official_revision_chain: str,
) -> bool:
    """按T0与收益端点覆盖年份核对日历修订，不比较完整链字符串。"""
    if (
        type(target_session) is not date
        or type(endpoint_session) is not date
        or endpoint_session < target_session
    ):
        return False
    official = _calendar_revision_map(official_revision_chain)
    if official is None:
        return False
    required_years = tuple(range(target_session.year, endpoint_session.year + 1))
    if any(year not in official for year in required_years):
        return False

    rule = _calendar_revision_map(rule_revision)
    if rule is None:
        if rule_revision != official[target_session.year]:
            return False
        rule = {target_session.year: rule_revision}
    if rule.get(target_session.year) != official[target_session.year]:
        return False

    path = _calendar_revision_map(path_revision)
    if path is None:
        path = {
            year: path_revision
            for year in required_years
            if path_revision == official[year]
        }
    if not path:
        return False
    for revisions in (rule, path):
        if any(
            year not in official or official[year] != revision
            for year, revision in revisions.items()
        ):
            return False
    for year in required_years:
        rule_revision_for_year = rule.get(year)
        path_revision_for_year = path.get(year)
        if rule_revision_for_year is None and path_revision_for_year is None:
            return False
        if (
            rule_revision_for_year is not None
            and path_revision_for_year is not None
            and rule_revision_for_year != path_revision_for_year
        ):
            return False
    return True


def _parse_qfq_capture_coordinate(coord_id: str | None) -> int | None:
    if not isinstance(coord_id, str) or not coord_id:
        return None
    for prefix in (
        "BAOSTOCK_QFQ_CAPTURE_",
        "FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:",
    ):
        if coord_id.startswith(prefix):
            tail = coord_id[len(prefix) :].split(":", 1)[0]
            return int(tail) if tail.isdigit() else None
    return None


def _same_economic_coordinate(coord_a: str | None, coord_b: str | None) -> bool:
    """判断两个坐标标识是否指向同一真实可比经济坐标。"""
    if not coord_a or not coord_b:
        return False
    if coord_a == "FORWARD_ADJUSTED" or coord_b == "FORWARD_ADJUSTED":
        return False
    if coord_a == coord_b:
        return True
    cap_a = _parse_qfq_capture_coordinate(coord_a)
    cap_b = _parse_qfq_capture_coordinate(coord_b)
    return cap_a is not None and cap_a == cap_b


def _row_qfq_coordinate_id(row: Mapping[str, Any] | None, *, symbol: str) -> str | None:
    if row is None:
        return None
    for key in ("qfq_coordinate_id", "coordinate_id"):
        val = row.get(key)
        if isinstance(val, str) and val and val != "FORWARD_ADJUSTED":
            return val
    cap = row.get("qfq_source_capture_id") or row.get("source_capture_id")
    if type(cap) is int and cap > 0:
        return f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{cap}:{symbol}"
    return None


def _qfq_row_metric_field(
    row: Mapping[str, Any] | None, field: str
) -> tuple[Fraction | None, str, str | None]:
    """按单字段依赖提取QFQ价格；不因无关字段缺失抹除合法指标，OHLC冲突保守拒绝。"""
    from stock_a.research.entry_history.support import trade_status_state_from_pg_row

    if row is None:
        return None, "UNKNOWN", "UNKNOWN_HISTORY_SESSION_NOT_STORED"
    trade_state, trade_reason = trade_status_state_from_pg_row(
        row, price_coordinate="FORWARD_ADJUSTED"
    )
    if trade_state != "ACTUAL":
        return None, "UNKNOWN", trade_reason or "QFQ_TRADE_STATUS_NOT_ACTUAL"
    bar_status = row.get("qfq_bar_status")
    if bar_status is not None and bar_status != "TARGET_PRESENT":
        return (
            None,
            "UNKNOWN",
            str(row.get("qfq_bar_reason") or "QFQ_TARGET_BAR_MISSING"),
        )

    # 检查已有非空OHLC是否存在包络冲突或非正数
    present_vals: dict[str, Fraction] = {}
    for name in ("open", "high", "low", "close"):
        raw_val = row.get(f"qfq_{name}")
        if raw_val is not None:
            try:
                num = exact_number(raw_val)
            except (TypeError, ValueError, ZeroDivisionError, OverflowError):
                return None, "BAD_VALUE", "QFQ_PRICE_VALUE_INVALID"
            if num <= 0:
                return None, "BAD_VALUE", "QFQ_PRICE_NON_POSITIVE"
            present_vals[name] = num
    if len(present_vals) == 4:
        if present_vals["low"] > min(
            present_vals["open"], present_vals["close"]
        ) or present_vals["high"] < max(present_vals["open"], present_vals["close"]):
            return None, "BAD_VALUE", "QFQ_OHLC_ENVELOPE_INVALID"
    elif "high" in present_vals and "low" in present_vals:
        if present_vals["high"] < present_vals["low"]:
            return None, "BAD_VALUE", "QFQ_OHLC_ENVELOPE_INVALID"

    field_quality_col = f"qfq_{field}_quality"
    field_reason_col = f"qfq_{field}_reason"
    if field_quality_col in row:
        quality = row.get(field_quality_col)
        reason = row.get(field_reason_col) or row.get("qfq_price_reason")
    else:
        quality = row.get("qfq_price_quality")
        reason = row.get("qfq_price_reason")

    if quality == "OBSERVED" and field in present_vals:
        return present_vals[field], "OBSERVED", None
    if quality not in ("OBSERVED", None):
        return None, "UNKNOWN", str(reason or f"QFQ_{field.upper()}_UNKNOWN")
    if field in present_vals and row.get("qfq_price_quality") in ("OBSERVED", None):
        return present_vals[field], "OBSERVED", None
    return None, "UNKNOWN", str(reason or f"QFQ_{field.upper()}_MISSING")


def exploratory_raw_path(
    sessions: Sequence[date],
    rows: Sequence[Mapping[str, Any]],
    horizon: int,
    *,
    target_session: date | None,
    source_capture_id: int | None = None,
    price_coordinate: str | None = None,
    saved_through: date | None = None,
) -> dict[str, Any]:
    """以QFQ T0收盘计算后验路径；捕获身份仅作溯源，不承担坐标可比性判定。"""
    session_vector = tuple(sessions)
    if not session_vector or type(horizon) is not int or horizon < 1:
        raise ValueError("路径须包含T0且期限为正整数")
    if (
        tuple(sorted(set(session_vector))) != session_vector
        or len(session_vector) > horizon + 1
    ):
        raise ValueError("路径会话必须唯一递增且不超过期限")
    if len(session_vector) == horizon + 1 and session_vector[-1] != target_session:
        raise ValueError("目标会话与路径末端不一致")
    if (
        len(session_vector) <= horizon
        and target_session is not None
        and target_session <= session_vector[-1]
    ):
        raise ValueError("未到期目标须晚于最后可用会话")

    def not_computed(code: str, message: str) -> dict[str, Any]:
        return {
            "scope": "EXPLORATORY_QFQ_PATH_NON_PIT",
            "status": "NOT_COMPUTED",
            "reason_code": code,
            "reason": message,
            "horizon": horizon,
            "target_session": target_session.isoformat() if target_session else None,
            "source_capture_id": source_capture_id
            if type(source_capture_id) is int and source_capture_id > 0
            else None,
            "price_coordinate": price_coordinate,
            "time_role": "NON_PIT_RETROSPECTIVE",
            "maturity": "NOT_COMPUTED",
            "due": len(session_vector) == horizon + 1,
            "endpoint": {
                "qfq_close_relative_change": {
                    "value": None,
                    "quality": "UNKNOWN",
                    "reason": code,
                }
            },
            "points": [],
            "path_quality": "UNKNOWN",
            "path_reasons": [code],
            "path_values": None,
            "metric_qualities": {"mfe": "UNKNOWN", "mae": "UNKNOWN", "mdd": "UNKNOWN"},
            "metric_reasons": {"mfe": code, "mae": code, "mdd": code},
            "database_write": False,
        }

    if type(source_capture_id) is not int or source_capture_id <= 0:
        return not_computed(
            "PG_BAOSTOCK_CAPTURE_REQUIRED",
            "后验路径必须绑定paired BaoStock source_capture_id。",
        )
    if price_coordinate != "FORWARD_ADJUSTED":
        return not_computed(
            "FORWARD_PATH_REQUIRES_QFQ_COORDINATE",
            "跨日收益、MFE、MAE和MDD必须使用QFQ坐标。",
        )

    by_date: dict[date, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("价格行必须是映射")
        session = row.get("session_date")
        if (
            type(session) is not date
            or session not in session_vector
            or session in by_date
        ):
            raise ValueError("价格行重复、非法或越界")
        by_date[session] = row
    symbols = [
        normalize_baostock_symbol(row.get("source_symbol")) for row in by_date.values()
    ]
    if any(symbol is None for symbol in symbols) or len(set(symbols)) > 1:
        return not_computed(
            "SOURCE_SYMBOL_MISMATCH",
            "价格路径行缺少来源代码或混合了不同证券。",
        )
    if any(
        row.get("price_coordinate") not in (None, price_coordinate)
        for row in by_date.values()
    ):
        return not_computed(
            "PRICE_COORDINATE_MISMATCH",
            "价格路径行的坐标与QFQ请求不一致。",
        )

    birth = by_date.get(session_vector[0])
    c0_val, c0_qual, c0_reason = _qfq_row_metric_field(birth, "close")
    if birth is None or c0_qual != "OBSERVED" or c0_val is None:
        return not_computed(
            c0_reason or "QFQ_T0_CLOSE_UNAVAILABLE",
            "路径T0没有可观察QFQ收盘价；不回退未复权收盘。",
        )
    symbol_text = symbols[0]
    t0_coord = _row_qfq_coordinate_id(birth, symbol=symbol_text)
    facts = post_signal_price_facts(session_vector, rows, horizon)
    endpoint = facts["points"][horizon - 1]
    is_due = len(session_vector) == horizon + 1
    if is_due:
        end_row = by_date.get(session_vector[-1])
        end_coord = _row_qfq_coordinate_id(end_row, symbol=symbol_text)
        if not _same_economic_coordinate(t0_coord, end_coord):
            endpoint = dict(endpoint)
            endpoint["qfq_close_relative_change"] = {
                "value": None,
                "quality": "UNKNOWN",
                "reason": "QFQ_CROSS_CAPTURE_CONTINUITY_UNPROVEN",
            }
        else:
            cn_val, cn_qual, cn_reason = _qfq_row_metric_field(end_row, "close")
            if cn_qual == "OBSERVED" and cn_val is not None:
                ret_val = cn_val / c0_val - 1
                endpoint = dict(endpoint)
                endpoint["qfq_close_relative_change"] = {
                    "value": {
                        "numerator": str(ret_val.numerator),
                        "denominator": str(ret_val.denominator),
                        "exact": str(ret_val),
                    },
                    "quality": "OBSERVED",
                    "reason": None,
                }
            else:
                endpoint = dict(endpoint)
                endpoint["qfq_close_relative_change"] = {
                    "value": None,
                    "quality": "UNKNOWN",
                    "reason": cn_reason or "QFQ_ENDPOINT_CLOSE_UNKNOWN",
                }

    if target_session is None:
        maturity = "RIGHT_CENSORED"
    elif not is_due:
        maturity = "NOT_DUE_AT_CUTOFF"
    elif endpoint["qfq_close_relative_change"]["quality"] == "OBSERVED":
        maturity = "ENDPOINT_OBSERVED_NON_PIT"
    elif saved_through is not None and target_session > saved_through:
        maturity = "RIGHT_CENSORED"
    else:
        maturity = "ENDPOINT_UNKNOWN"

    path_reasons: list[str] = []
    if not is_due:
        path_reasons.append("WINDOW_NOT_AVAILABLE")
        metric_qualities = {"mfe": "NOT_DUE", "mae": "NOT_DUE", "mdd": "NOT_DUE"}
        metric_reasons = {
            "mfe": "WINDOW_NOT_AVAILABLE",
            "mae": "WINDOW_NOT_AVAILABLE",
            "mdd": "WINDOW_NOT_AVAILABLE",
        }
        values = None
        path_quality = "UNKNOWN"
    else:
        mfe_vals: list[Fraction] = []
        mae_vals: list[Fraction] = []
        mdd_closes: list[Fraction] = []
        mfe_err: str | None = None
        mae_err: str | None = None
        mdd_err: str | None = None

        for day in session_vector[1:]:
            row = by_date.get(day)
            day_coord = _row_qfq_coordinate_id(row, symbol=symbol_text)
            if not _same_economic_coordinate(t0_coord, day_coord):
                err = "QFQ_CROSS_CAPTURE_CONTINUITY_UNPROVEN"
                if mfe_err is None:
                    mfe_err = err
                    path_reasons.append(f"{day.isoformat()}:{err}")
                if mae_err is None:
                    mae_err = err
                if mdd_err is None:
                    mdd_err = err
                continue
            h_val, h_qual, h_reason = _qfq_row_metric_field(row, "high")
            if h_qual == "OBSERVED" and h_val is not None:
                mfe_vals.append(h_val)
            elif mfe_err is None:
                mfe_err = h_reason or "QFQ_HIGH_UNKNOWN"
                path_reasons.append(f"{day.isoformat()}:{mfe_err}")

            l_val, l_qual, l_reason = _qfq_row_metric_field(row, "low")
            if l_qual == "OBSERVED" and l_val is not None:
                mae_vals.append(l_val)
            elif mae_err is None:
                mae_err = l_reason or "QFQ_LOW_UNKNOWN"
                path_reasons.append(f"{day.isoformat()}:{mae_err}")

            c_val, c_qual, c_reason = _qfq_row_metric_field(row, "close")
            if c_qual == "OBSERVED" and c_val is not None:
                mdd_closes.append(c_val)
            elif mdd_err is None:
                mdd_err = c_reason or "QFQ_CLOSE_UNKNOWN"
                path_reasons.append(f"{day.isoformat()}:{mdd_err}")

        mfe_str: str | None = None
        mae_str: str | None = None
        mdd_str: str | None = None
        if mfe_err is None and len(mfe_vals) == horizon:
            mfe_str = str(max(Fraction(0), *(h / c0_val - 1 for h in mfe_vals)))
        if mae_err is None and len(mae_vals) == horizon:
            mae_str = str(min(Fraction(0), *(low_v / c0_val - 1 for low_v in mae_vals)))
        if mdd_err is None and len(mdd_closes) == horizon:
            peak = c0_val
            mdd_v = Fraction(0)
            for close_v in mdd_closes:
                peak = max(peak, close_v)
                mdd_v = min(mdd_v, close_v / peak - 1)
            mdd_str = str(mdd_v)

        metric_qualities = {
            "mfe": "OBSERVED" if mfe_str is not None else "UNKNOWN",
            "mae": "OBSERVED" if mae_str is not None else "UNKNOWN",
            "mdd": "OBSERVED" if mdd_str is not None else "UNKNOWN",
        }
        metric_reasons = {
            "mfe": None if mfe_str is not None else (mfe_err or "QFQ_HIGH_UNKNOWN"),
            "mae": None if mae_str is not None else (mae_err or "QFQ_LOW_UNKNOWN"),
            "mdd": None if mdd_str is not None else (mdd_err or "QFQ_CLOSE_UNKNOWN"),
        }
        values = (
            {"mfe": mfe_str, "mae": mae_str, "mdd": mdd_str}
            if any(v is not None for v in (mfe_str, mae_str, mdd_str))
            else None
        )
        observed_cnt = sum(q == "OBSERVED" for q in metric_qualities.values())
        path_quality = (
            "OBSERVED"
            if observed_cnt == 3
            else "PARTIAL"
            if observed_cnt > 0
            else "UNKNOWN"
        )

    complete = path_quality == "OBSERVED"
    return {
        "scope": "EXPLORATORY_QFQ_PATH_NON_PIT",
        "status": "AVAILABLE" if complete else "INCOMPLETE",
        "horizon": horizon,
        "target_session": target_session.isoformat() if target_session else None,
        "maturity": maturity,
        "due": is_due,
        "endpoint": endpoint,
        "points": facts["points"],
        "path_quality": path_quality,
        "path_reasons": list(dict.fromkeys(path_reasons)),
        "path_values": values,
        "metric_qualities": metric_qualities,
        "metric_reasons": metric_reasons,
        "source_capture_id": source_capture_id,
        "price_coordinate": price_coordinate,
        "time_role": "NON_PIT_RETROSPECTIVE",
        "raw_t0_ohlc": _timeline_raw_ohlc(birth),
        "definition": "QFQ MFE/MAE为T+1至T+N日内高低相对QFQ T0收盘；MDD为含T0的QFQ逐日收盘先峰后谷最小收益",
    }
