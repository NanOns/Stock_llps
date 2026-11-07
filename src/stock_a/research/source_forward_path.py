"""来源级后验原始价格路径；不产生正式结算或交易事实。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from fractions import Fraction
from typing import Any

from stock_a.domain import exact_number
from stock_a.research.entry_history.history_timeline import (
    _timeline_raw_ohlc,
    post_signal_price_facts,
)


def exploratory_raw_path(
    sessions: Sequence[date],
    rows: Sequence[Mapping[str, Any]],
    horizon: int,
    *,
    target_session: date | None,
    saved_through: date | None = None,
) -> dict[str, Any]:
    """以T0收盘为基准；缺内部Bar只使路径未知，不抹去合法端点。

    MFE/MAE分别为后继日最高/最低价相对T0收盘的极值；MDD为
    T0及后继每日收盘序列中的最大先峰后谷跌幅，不推断日内高低顺序。
    调用者须提供截至截止日的连续官方会话，以及日历中的目标会话。
    """
    if not sessions or type(horizon) is not int or horizon < 1:
        raise ValueError("路径须包含T0且期限为正整数")
    if tuple(sorted(set(sessions))) != tuple(sessions) or len(sessions) > horizon + 1:
        raise ValueError("路径会话必须唯一递增且不超过期限")
    if len(sessions) == horizon + 1 and sessions[-1] != target_session:
        raise ValueError("目标会话与路径末端不一致")
    if (
        len(sessions) <= horizon
        and target_session is not None
        and target_session <= sessions[-1]
    ):
        raise ValueError("未到期目标须晚于最后可用会话")
    by_date = {row["session_date"]: row for row in rows}
    if len(by_date) != len(rows) or any(day not in sessions for day in by_date):
        raise ValueError("价格行重复或越界")
    birth = by_date.get(sessions[0])
    safe = []
    binding_valid = birth is not None and all(
        birth.get(key)
        for key in (
            "source_entry_sha256",
            "source_content_id",
            "package_sha256",
            "namespace_id",
        )
    )
    for row in rows:
        present = row.get("bar_status") == "TARGET_PRESENT"
        if present and not binding_valid:
            safe.append(
                {
                    "session_date": row["session_date"],
                    "bar_status": "SOURCE_IDENTITY_UNKNOWN",
                }
            )
        elif (
            present
            and row["session_date"] != sessions[0]
            and (
                row.get("source_entry_sha256") != birth.get("source_entry_sha256")
                or row.get("source_content_id") != birth.get("source_content_id")
                or row.get("package_sha256") != birth.get("package_sha256")
                or row.get("namespace_id") != birth.get("namespace_id")
                or row.get("price_coordinate") != "RAW_UNADJUSTED"
            )
        ):
            safe.append(
                {"session_date": row["session_date"], "bar_status": "SOURCE_MISMATCH"}
            )
        else:
            safe.append(row)
    facts = post_signal_price_facts(sessions, safe, horizon)
    endpoint = facts["points"][horizon - 1]
    if target_session is None:
        maturity = "RIGHT_CENSORED"
    elif len(sessions) <= horizon:
        maturity = "NOT_DUE_AT_CUTOFF"
    elif endpoint["bar_status"] == "SOURCE_MISMATCH" or (
        birth is not None and birth.get("price_coordinate") != "RAW_UNADJUSTED"
    ):
        maturity = "SOURCE_MISMATCH"
    elif endpoint["raw_close_relative_change"]["quality"] == "OBSERVED":
        maturity = "ENDPOINT_OBSERVED"
    elif saved_through is not None and target_session > saved_through:
        maturity = "RIGHT_CENSORED"
    else:
        maturity = "ENDPOINT_UNKNOWN"
    complete = len(sessions) == horizon + 1 and all(
        _timeline_raw_ohlc(
            next((row for row in safe if row["session_date"] == day), None)
        )["quality"]
        == "OBSERVED"
        for day in sessions
    )
    complete = (
        complete
        and birth is not None
        and birth.get("price_coordinate") == "RAW_UNADJUSTED"
    )
    path_reasons = []
    if len(sessions) <= horizon:
        path_reasons.append("WINDOW_NOT_AVAILABLE")
    for day in sessions:
        row = next((row for row in safe if row["session_date"] == day), None)
        quality = _timeline_raw_ohlc(row)
        if quality["quality"] != "OBSERVED":
            path_reasons.append(f"{day.isoformat()}:{quality['reason']}")
    if birth is not None and birth.get("price_coordinate") != "RAW_UNADJUSTED":
        path_reasons.append("T0_PRICE_COORDINATE_MISMATCH")
    values = None
    if complete:
        selected = {row["session_date"]: row for row in safe}
        base = exact_number(birth["raw_close"])
        mfe = max(
            exact_number(selected[day]["raw_high"]) / base - 1 for day in sessions[1:]
        )
        mae = min(
            exact_number(selected[day]["raw_low"]) / base - 1 for day in sessions[1:]
        )
        peak = base
        mdd = Fraction(0)
        for day in sessions[1:]:
            close = exact_number(selected[day]["raw_close"])
            peak = max(peak, close)
            mdd = min(mdd, close / peak - 1)
        values = {"mfe": str(mfe), "mae": str(mae), "mdd": str(mdd)}
    return {
        "scope": "EXPLORATORY_RAW_PATH_NON_PIT",
        "horizon": horizon,
        "target_session": target_session.isoformat() if target_session else None,
        "maturity": maturity,
        "due": len(sessions) == horizon + 1,
        "endpoint": endpoint,
        "points": facts["points"],
        "path_quality": "OBSERVED" if complete else "UNKNOWN",
        "path_reasons": path_reasons,
        "path_values": values,
        "definition": "MFE/MAE为T+1至T+N日内高低相对T0收盘；MDD为含T0收盘的逐日收盘先峰后谷最小收益",
    }
