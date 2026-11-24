"""通用参考指数因子与市场复合环境展示状态 PG 持久化与只读查询（R3）。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any

from stock_a.domain.market_presentation import (
    market_environment_label,
    reduce_market_environment_display_series,
)
from stock_a.research.market_indices import (
    MARKET_INDEX_CONTRACT_VERSION,
    compute_style_spreads,
)
from stock_a.storage.postgres.source_raw import _transaction


def _to_dec(val: Fraction | Decimal | None, scale: int) -> Decimal | None:
    if val is None:
        return None
    if isinstance(val, Fraction):
        return (Decimal(val.numerator) / Decimal(val.denominator)).quantize(
            Decimal(10) ** (-scale)
        )
    return val.quantize(Decimal(10) ** (-scale))


def persist_market_index_day_facts(
    config: Mapping[str, Any],
    *,
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """幂等写入通用参考指数日级因子（同键同内容 NO_CHANGE，异内容拒绝回滚）。"""
    if not records:
        return {"status": "NO_CHANGE", "row_count": 0}
    with _transaction(config, write=True, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        inserted = 0
        unchanged = 0
        for rec in records:
            target_session = rec["target_session"]
            sym = rec["source_symbol"]
            idx_cap = int(rec["index_capture_id"])
            cal_rev = str(rec["calendar_revision"])
            contract_ver = str(
                rec.get("contract_version") or MARKET_INDEX_CONTRACT_VERSION
            )
            vals = (
                _to_dec(rec.get("close_points"), 6),
                _to_dec(rec.get("ma20_points"), 10),
                _to_dec(rec.get("ma20_5d_ago_points"), 10),
                _to_dec(rec.get("ret1_fraction"), 12),
                rec["ret1_quality"],
                rec.get("ret1_reason"),
                _to_dec(rec.get("ret5_fraction"), 12),
                rec["ret5_quality"],
                rec.get("ret5_reason"),
                _to_dec(rec.get("ret20_fraction"), 12),
                rec["ret20_quality"],
                rec.get("ret20_reason"),
                _to_dec(rec.get("close_vs_ma20_fraction"), 12),
                rec["close_vs_ma20_quality"],
                rec.get("close_vs_ma20_reason"),
                rec["trend_state"],
                rec.get("trend_reason"),
            )
            cursor.execute(
                """SELECT close_points, ma20_points, ma20_5d_ago_points,
                          ret1_fraction, ret1_quality, ret1_reason,
                          ret5_fraction, ret5_quality, ret5_reason,
                          ret20_fraction, ret20_quality, ret20_reason,
                          close_vs_ma20_fraction, close_vs_ma20_quality, close_vs_ma20_reason,
                          trend_state, trend_reason
                   FROM stock.source_market_index_day_facts
                   WHERE target_session = %s AND source_symbol = %s
                     AND index_capture_id = %s AND calendar_revision = %s
                     AND contract_version = %s""",
                (target_session, sym, idx_cap, cal_rev, contract_ver),
            )
            existing = cursor.fetchone()
            if existing is not None:
                if existing == vals:
                    unchanged += 1
                    continue
                raise ValueError(f"MARKET_INDEX_FACTS_SAME_KEY_DIFFERENT_CONTENT:{sym}")

            cursor.execute(
                """INSERT INTO stock.source_market_index_day_facts (
                       target_session, source_symbol, official_code, index_capture_id,
                       config_version, index_role_at_run, calendar_id, calendar_revision,
                       contract_version,
                       close_points, ma20_points, ma20_5d_ago_points,
                       ret1_fraction, ret1_quality, ret1_reason,
                       ret5_fraction, ret5_quality, ret5_reason,
                       ret20_fraction, ret20_quality, ret20_reason,
                       close_vs_ma20_fraction, close_vs_ma20_quality, close_vs_ma20_reason,
                       trend_state, trend_reason
                   ) VALUES (
                       %s, %s, %s, %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                   )""",
                (
                    target_session,
                    sym,
                    rec["official_code"],
                    idx_cap,
                    rec["config_version"],
                    rec["index_role_at_run"],
                    rec["calendar_id"],
                    cal_rev,
                    contract_ver,
                    *vals,
                ),
            )
            inserted += 1
    return {
        "status": "NO_CHANGE" if inserted == 0 else "PERSISTED",
        "inserted_count": inserted,
        "unchanged_count": unchanged,
    }


def persist_market_environment_series(
    config: Mapping[str, Any],
    *,
    official_sessions: Sequence[date],
    day_axis_inputs: Sequence[Mapping[str, Any]],
    stock_capture_id: int,
    index_capture_id: int | None,
    primary_index_symbol: str,
    index_config_version: str,
    calendar_id: str,
    calendar_revision: str,
    rule_version: str = "MARKET_PRESENTATION_R1",
) -> dict[str, Any]:
    """按严格官方相邻会话离线归约市场复合环境展示状态并幂等持久化。"""
    raw_obs = []
    axes_by_session: dict[str, Mapping[str, Any]] = {}
    for row in day_axis_inputs:
        sess = row["target_session"]
        sess_key = sess.isoformat() if hasattr(sess, "isoformat") else str(sess)
        axes_by_session[sess_key] = row
        env = market_environment_label(
            row.get("trend_axis"),
            row.get("breadth_axis"),
            row.get("participation_axis"),
            row.get("pressure_axis"),
        )
        raw_obs.append(
            {
                "session": sess_key,
                "raw_status": env["status"],
                "raw_label": env["label"],
                "rule_version": rule_version,
                "primary_index_symbol": primary_index_symbol,
            }
        )

    reduced = reduce_market_environment_display_series(
        raw_obs, official_sessions=official_sessions
    )
    inserted = 0
    unchanged = 0
    with _transaction(config, write=True, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        for item in reduced:
            sess_str = str(item["session"])
            sess_dt = date.fromisoformat(sess_str)
            ax = axes_by_session.get(sess_str, {})
            vals = (
                str(ax.get("trend_axis") or "UNKNOWN"),
                str(ax.get("breadth_axis") or "UNKNOWN"),
                str(ax.get("participation_axis") or "UNKNOWN"),
                str(ax.get("pressure_axis") or "UNKNOWN"),
                item["raw_status"],
                item["raw_label"],
                item["displayed_label"],
                item["display_status"],
                item["pending_label"],
                int(item["pending_count"]),
            )
            cursor.execute(
                """SELECT trend_axis, breadth_axis, participation_axis, pressure_axis,
                          raw_status, raw_label, displayed_label, display_status,
                          pending_label, pending_count
                   FROM stock.source_market_environment_day_facts
                   WHERE target_session = %s AND stock_capture_id = %s
                     AND primary_index_symbol = %s AND calendar_revision = %s
                     AND rule_version = %s""",
                (
                    sess_dt,
                    stock_capture_id,
                    primary_index_symbol,
                    calendar_revision,
                    rule_version,
                ),
            )
            existing = cursor.fetchone()
            if existing is not None:
                if existing == vals:
                    unchanged += 1
                    continue
                raise ValueError(
                    f"MARKET_ENVIRONMENT_SAME_KEY_DIFFERENT_CONTENT:{sess_str}"
                )
            cursor.execute(
                """INSERT INTO stock.source_market_environment_day_facts (
                       target_session, stock_capture_id, index_capture_id,
                       primary_index_symbol, index_config_version,
                       calendar_id, calendar_revision, rule_version,
                       trend_axis, breadth_axis, participation_axis, pressure_axis,
                       raw_status, raw_label, displayed_label, display_status,
                       pending_label, pending_count
                   ) VALUES (
                       %s, %s, %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                   )""",
                (
                    sess_dt,
                    stock_capture_id,
                    index_capture_id,
                    primary_index_symbol,
                    index_config_version,
                    calendar_id,
                    calendar_revision,
                    rule_version,
                    *vals,
                ),
            )
            inserted += 1
    return {
        "status": "NO_CHANGE" if inserted == 0 else "PERSISTED",
        "inserted_count": inserted,
        "unchanged_count": unchanged,
    }


def read_market_indices_and_style_summary(
    config: Mapping[str, Any],
    *,
    target_session: date,
    calendar_revision: str,
    index_contract: Mapping[str, Any],
) -> dict[str, Any]:
    """只读查询目标日各参考指数已存因子与风格剪刀差（未落库时诚实返回 NOT_COMPUTED）。"""
    with _transaction(config, write=False, statement_timeout_ms=10000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT DISTINCT ON (source_symbol)
                      source_symbol, official_code, index_capture_id,
                      index_role_at_run, close_points,
                      ret1_fraction, ret1_quality, ret1_reason,
                      ret5_fraction, ret5_quality, ret5_reason,
                      ret20_fraction, ret20_quality, ret20_reason,
                      close_vs_ma20_fraction, close_vs_ma20_quality, close_vs_ma20_reason,
                      trend_state, trend_reason
               FROM stock.source_market_index_day_facts
               WHERE target_session = %s
                 AND calendar_revision = %s
                 AND contract_version = %s
               ORDER BY source_symbol, index_capture_id DESC""",
            (target_session, calendar_revision, MARKET_INDEX_CONTRACT_VERSION),
        )
        rows = cursor.fetchall()

    if not rows:
        return {
            "status": "NOT_COMPUTED",
            "target_session": target_session.isoformat(),
            "calendar_revision": calendar_revision,
            "contract_version": MARKET_INDEX_CONTRACT_VERSION,
            "indices": [],
            "style_spreads": [],
            "reason": "MARKET_INDEX_FACTS_NOT_PERSISTED",
        }

    by_official: dict[str, dict[str, Any]] = {}
    items: list[dict[str, Any]] = []
    for r in rows:
        item = {
            "source_symbol": r[0],
            "official_code": r[1],
            "index_capture_id": int(r[2]),
            "index_role_at_run": r[3],
            "close_points": float(r[4]) if r[4] is not None else None,
            "ret1_fraction": r[5],
            "ret1_quality": r[6],
            "ret1_reason": r[7],
            "ret5_fraction": r[8],
            "ret5_quality": r[9],
            "ret5_reason": r[10],
            "ret20_fraction": r[11],
            "ret20_quality": r[12],
            "ret20_reason": r[13],
            "close_vs_ma20_fraction": r[14],
            "close_vs_ma20_quality": r[15],
            "close_vs_ma20_reason": r[16],
            "trend_state": r[17],
            "trend_reason": r[18],
        }
        by_official[r[1]] = item
        items.append(
            {
                **item,
                "ret1_fraction": float(r[5]) if r[5] is not None else None,
                "ret5_fraction": float(r[8]) if r[8] is not None else None,
                "ret20_fraction": float(r[11]) if r[11] is not None else None,
                "close_vs_ma20_fraction": float(r[14]) if r[14] is not None else None,
            }
        )

    spreads = compute_style_spreads(
        index_facts_by_official_code=by_official,
        style_spread_pairs=index_contract.get("style_spread_pairs", ()),
    )
    return {
        "status": "AVAILABLE",
        "target_session": target_session.isoformat(),
        "calendar_revision": calendar_revision,
        "contract_version": MARKET_INDEX_CONTRACT_VERSION,
        "indices": items,
        "style_spreads": spreads,
    }


def read_market_environment_display(
    config: Mapping[str, Any],
    *,
    target_session: date,
    stock_capture_id: int,
    primary_index_symbol: str,
    calendar_revision: str,
    rule_version: str = "MARKET_PRESENTATION_R1",
) -> dict[str, Any]:
    """只读查询目标日已存市场复合环境展示状态（未落库时诚实返回 NOT_COMPUTED）。"""
    with _transaction(config, write=False, statement_timeout_ms=10000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT index_capture_id, index_config_version,
                      trend_axis, breadth_axis, participation_axis, pressure_axis,
                      raw_status, raw_label, displayed_label, display_status,
                      pending_label, pending_count
               FROM stock.source_market_environment_day_facts
               WHERE target_session = %s
                 AND stock_capture_id = %s
                 AND primary_index_symbol = %s
                 AND calendar_revision = %s
                 AND rule_version = %s""",
            (
                target_session,
                stock_capture_id,
                primary_index_symbol,
                calendar_revision,
                rule_version,
            ),
        )
        row = cursor.fetchone()
    if row is None:
        return {
            "status": "NOT_COMPUTED",
            "target_session": target_session.isoformat(),
            "stock_capture_id": stock_capture_id,
            "primary_index_symbol": primary_index_symbol,
            "calendar_revision": calendar_revision,
            "rule_version": rule_version,
            "reason": "MARKET_ENVIRONMENT_DISPLAY_NOT_PERSISTED",
        }
    return {
        "status": "AVAILABLE",
        "target_session": target_session.isoformat(),
        "stock_capture_id": stock_capture_id,
        "index_capture_id": int(row[0]) if row[0] is not None else None,
        "primary_index_symbol": primary_index_symbol,
        "index_config_version": row[1],
        "calendar_revision": calendar_revision,
        "rule_version": rule_version,
        "axes": {
            "trend": row[2],
            "breadth": row[3],
            "participation": row[4],
            "pressure": row[5],
        },
        "raw_status": row[6],
        "raw_label": row[7],
        "displayed_label": row[8],
        "display_status": row[9],
        "pending_label": row[10],
        "pending_count": int(row[11]),
    }
