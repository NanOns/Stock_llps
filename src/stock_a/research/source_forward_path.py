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
    """以paired capture的QFQ T0收盘计算后验路径，RAW日线仅作原始报价披露。"""
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
    if any(
        row.get("source_capture_id") != source_capture_id for row in by_date.values()
    ):
        return not_computed(
            "SOURCE_CAPTURE_MISMATCH",
            "价格路径行没有绑定同一个paired source_capture_id。",
        )
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
    if (
        birth is None
        or _timeline_actual_qfq_ohlc(birth)["close"]["quality"] != "OBSERVED"
    ):
        return not_computed(
            "QFQ_T0_CLOSE_UNAVAILABLE",
            "路径T0没有可观察QFQ收盘价；不回退未复权收盘。",
        )
    safe_rows = [
        row
        if row.get("source_capture_id") == source_capture_id
        else {
            "session_date": row["session_date"],
            "bar_status": "SOURCE_CAPTURE_MISMATCH",
        }
        for row in rows
    ]
    facts = post_signal_price_facts(session_vector, safe_rows, horizon)
    endpoint = facts["points"][horizon - 1]
    if target_session is None:
        maturity = "RIGHT_CENSORED"
    elif len(session_vector) <= horizon:
        maturity = "NOT_DUE_AT_CUTOFF"
    elif endpoint["qfq_close_relative_change"]["quality"] == "OBSERVED":
        maturity = "ENDPOINT_OBSERVED_NON_PIT"
    elif saved_through is not None and target_session > saved_through:
        maturity = "RIGHT_CENSORED"
    else:
        maturity = "ENDPOINT_UNKNOWN"

    complete = len(session_vector) == horizon + 1 and all(
        _timeline_actual_qfq_ohlc(by_date.get(session))["quality"] == "OBSERVED"
        for session in session_vector
    )
    path_reasons = []
    if len(session_vector) <= horizon:
        path_reasons.append("WINDOW_NOT_AVAILABLE")
    for session in session_vector:
        row = by_date.get(session)
        qfq = _timeline_actual_qfq_ohlc(row)
        if qfq["quality"] != "OBSERVED":
            path_reasons.append(
                f"{session.isoformat()}:{qfq['reason'] or 'QFQ_PRICE_UNKNOWN'}"
            )
    values = None
    if complete:
        selected = {row["session_date"]: row for row in by_date.values()}
        base = exact_number(selected[session_vector[0]]["qfq_close"])
        mfe = max(
            Fraction(0),
            *(
                exact_number(selected[day]["qfq_high"]) / base - 1
                for day in session_vector[1:]
            ),
        )
        mae = min(
            Fraction(0),
            *(
                exact_number(selected[day]["qfq_low"]) / base - 1
                for day in session_vector[1:]
            ),
        )
        peak = base
        mdd = Fraction(0)
        for day in session_vector[1:]:
            close = exact_number(selected[day]["qfq_close"])
            peak = max(peak, close)
            mdd = min(mdd, close / peak - 1)
        values = {"mfe": str(mfe), "mae": str(mae), "mdd": str(mdd)}

    return {
        "scope": "EXPLORATORY_QFQ_PATH_NON_PIT",
        "status": "AVAILABLE" if complete else "INCOMPLETE",
        "horizon": horizon,
        "target_session": target_session.isoformat() if target_session else None,
        "maturity": maturity,
        "due": len(session_vector) == horizon + 1,
        "endpoint": endpoint,
        "points": facts["points"],
        "path_quality": "OBSERVED" if complete else "UNKNOWN",
        "path_reasons": path_reasons,
        "path_values": values,
        "source_capture_id": source_capture_id,
        "price_coordinate": price_coordinate,
        "time_role": "NON_PIT_RETROSPECTIVE",
        "raw_t0_ohlc": _timeline_raw_ohlc(birth),
        "definition": "QFQ MFE/MAE为T+1至T+N日内高低相对QFQ T0收盘；MDD为含T0的QFQ逐日收盘先峰后谷最小收益",
    }
