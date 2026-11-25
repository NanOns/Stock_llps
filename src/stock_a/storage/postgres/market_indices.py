"""通用参考指数因子与市场复合环境展示状态 PG 持久化与只读查询（R3）。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

from stock_a.domain.market_presentation import (
    classify_market_breadth,
    market_environment_label,
    reduce_market_environment_display_series,
)
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.market_indices import (
    MARKET_INDEX_CONTRACT_VERSION,
    IndexDailyBar,
    compute_style_spreads,
    evaluate_index_day_factors,
)
from stock_a.storage.postgres.source_raw import RawSourcePgError, _transaction


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
    initial_state: Mapping[str, Any] | None = None,
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
        raw_obs,
        official_sessions=official_sessions,
        initial_state=dict(initial_state)
        if isinstance(initial_state, Mapping)
        else None,
    )
    inserted = 0
    unchanged = 0
    try:
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
                    index_capture_id,
                    index_config_version,
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
                    """SELECT index_capture_id, index_config_version,
                              trend_axis, breadth_axis, participation_axis, pressure_axis,
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
                        *vals[2:],
                    ),
                )
                inserted += 1
    except RawSourcePgError as exc:
        if isinstance(exc.__cause__, ValueError):
            raise exc.__cause__ from None
        raise
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
    index_capture_id: int | None = None,
    config_version: str | None = None,
) -> dict[str, Any]:
    """只读查询目标日单一选定捕获与配置版本下的参考指数因子与风格剪刀差（杜绝跨捕获混算）。"""
    with _transaction(config, write=False, statement_timeout_ms=10000) as (
        _,
        cursor,
        _,
    ):
        selected_capture_id = index_capture_id
        selected_config_version = config_version
        if selected_capture_id is None or selected_config_version is None:
            # 优先锁定主参考指数（PRIMARY_MARKET）的最新 (index_capture_id, config_version)
            cursor.execute(
                """SELECT index_capture_id, config_version
                   FROM stock.source_market_index_day_facts
                   WHERE target_session = %s
                     AND calendar_revision = %s
                     AND contract_version = %s
                     AND (%s::bigint IS NULL OR index_capture_id = %s)
                     AND (%s::text IS NULL OR config_version = %s)
                   ORDER BY
                     CASE WHEN index_role_at_run = 'PRIMARY_MARKET' THEN 0 ELSE 1 END,
                     index_capture_id DESC,
                     config_version DESC
                   LIMIT 1""",
                (
                    target_session,
                    calendar_revision,
                    MARKET_INDEX_CONTRACT_VERSION,
                    selected_capture_id,
                    selected_capture_id,
                    selected_config_version,
                    selected_config_version,
                ),
            )
            scope_row = cursor.fetchone()
            if scope_row is None:
                return {
                    "status": "NOT_COMPUTED",
                    "target_session": target_session.isoformat(),
                    "calendar_revision": calendar_revision,
                    "contract_version": MARKET_INDEX_CONTRACT_VERSION,
                    "index_capture_id": selected_capture_id,
                    "config_version": selected_config_version,
                    "indices": [],
                    "style_spreads": [],
                    "reason": "MARKET_INDEX_FACTS_NOT_PERSISTED",
                }
            selected_capture_id = int(scope_row[0])
            selected_config_version = str(scope_row[1])

        cursor.execute(
            """SELECT source_symbol, official_code, index_capture_id,
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
                 AND index_capture_id = %s
                 AND config_version = %s
               ORDER BY source_symbol""",
            (
                target_session,
                calendar_revision,
                MARKET_INDEX_CONTRACT_VERSION,
                selected_capture_id,
                selected_config_version,
            ),
        )
        rows = cursor.fetchall()

    if not rows:
        return {
            "status": "NOT_COMPUTED",
            "target_session": target_session.isoformat(),
            "calendar_revision": calendar_revision,
            "contract_version": MARKET_INDEX_CONTRACT_VERSION,
            "index_capture_id": selected_capture_id,
            "config_version": selected_config_version,
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
        "index_capture_id": selected_capture_id,
        "config_version": selected_config_version,
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


def _opt_dec(value: Any, scale: int) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text:
        return None
    dec = value if isinstance(value, Decimal) else Decimal(text)
    if not dec.is_finite():
        return None
    return dec.quantize(Decimal(10) ** (-scale))


def persist_market_index_daily_bars(
    config: Mapping[str, Any],
    *,
    source_capture_id: int,
    rows: Iterable[Mapping[str, Any]],
    captured_at: datetime | None = None,
    pit_status: str = "NON_PIT_RETROSPECTIVE",
) -> dict[str, Any]:
    """单事务幂等写入通用参考指数日线表（INDEX_POINT/RAW_UNADJUSTED）；同键异内容拒绝回滚。"""
    if type(source_capture_id) is not int or source_capture_id <= 0:
        raise ValueError("INVALID_INDEX_CAPTURE_ID")
    if pit_status not in {"NON_PIT_RETROSPECTIVE", "OBSERVED_AT_CAPTURE"}:
        raise ValueError("INVALID_INDEX_PIT_STATUS")
    effective_captured_at = captured_at or datetime.now(timezone.utc)
    if effective_captured_at.tzinfo is None:
        raise ValueError("CAPTURED_AT_MUST_BE_TIMEZONE_AWARE")

    normalized: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, date]] = set()
    for row in rows:
        sym = str(row["source_symbol"]).strip().upper()
        official_code = str(row.get("official_code") or sym.split(".")[-1]).strip()
        sess = row["session_date"]
        if isinstance(sess, str):
            sess = date.fromisoformat(sess)
        if type(sess) is not date:
            raise ValueError("INVALID_INDEX_SESSION_DATE")
        key = (sym, sess)
        if key in seen_keys:
            raise ValueError(f"DUPLICATE_INDEX_DAILY_KEY:{sym}:{sess}")
        seen_keys.add(key)
        normalized.append(
            {
                "source_symbol": sym,
                "official_code": official_code,
                "session_date": sess,
                "bar_status": str(row.get("bar_status") or "TARGET_PRESENT"),
                "open_points": _opt_dec(row.get("open_points"), 6),
                "high_points": _opt_dec(row.get("high_points"), 6),
                "low_points": _opt_dec(row.get("low_points"), 6),
                "close_points": _opt_dec(row.get("close_points"), 6),
                "preclose_points": _opt_dec(row.get("preclose_points"), 6),
                "volume_raw": _opt_dec(row.get("volume_raw"), 4),
                "amount_raw": _opt_dec(row.get("amount_raw"), 4),
                "pct_chg_reported": _opt_dec(row.get("pct_chg_reported"), 8),
                "price_quality": str(row.get("price_quality") or "OBSERVED"),
                "price_reason": row.get("price_reason"),
            }
        )
    if not normalized:
        return {
            "status": "NO_CHANGE",
            "source_capture_id": source_capture_id,
            "row_count": 0,
        }

    with _transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        inserted = 0
        unchanged = 0
        for item in normalized:
            vals = (
                item["official_code"],
                item["bar_status"],
                item["open_points"],
                item["high_points"],
                item["low_points"],
                item["close_points"],
                item["preclose_points"],
                item["volume_raw"],
                item["amount_raw"],
                item["pct_chg_reported"],
                item["price_quality"],
                item["price_reason"],
                pit_status,
            )
            cursor.execute(
                """SELECT official_code, bar_status, open_points, high_points,
                          low_points, close_points, preclose_points,
                          volume_raw, amount_raw, pct_chg_reported,
                          price_quality, price_reason, pit_status
                   FROM stock.source_market_index_daily
                   WHERE source_capture_id = %s
                     AND source_symbol = %s
                     AND session_date = %s""",
                (source_capture_id, item["source_symbol"], item["session_date"]),
            )
            existing = cursor.fetchone()
            if existing is not None:
                if tuple(existing) == vals:
                    unchanged += 1
                    continue
                raise ValueError("MARKET_INDEX_DAILY_SAME_KEY_DIFFERENT_CONTENT")
            cursor.execute(
                """INSERT INTO stock.source_market_index_daily (
                       source_capture_id, source_symbol, official_code, session_date,
                       price_coordinate, price_unit, bar_status,
                       open_points, high_points, low_points, close_points, preclose_points,
                       volume_raw, amount_raw, pct_chg_reported,
                       price_quality, price_reason, pit_status, captured_at
                   ) VALUES (
                       %s, %s, %s, %s,
                       'RAW_UNADJUSTED', 'INDEX_POINT', %s,
                       %s, %s, %s, %s, %s,
                       %s, %s, %s,
                       %s, %s, %s, %s
                   )""",
                (
                    source_capture_id,
                    item["source_symbol"],
                    item["official_code"],
                    item["session_date"],
                    item["bar_status"],
                    item["open_points"],
                    item["high_points"],
                    item["low_points"],
                    item["close_points"],
                    item["preclose_points"],
                    item["volume_raw"],
                    item["amount_raw"],
                    item["pct_chg_reported"],
                    item["price_quality"],
                    item["price_reason"],
                    pit_status,
                    effective_captured_at,
                ),
            )
            inserted += 1
    return {
        "status": "NO_CHANGE" if inserted == 0 else "PERSISTED",
        "source_capture_id": source_capture_id,
        "inserted_count": inserted,
        "unchanged_count": unchanged,
        "row_count": len(normalized),
    }


def persist_market_indices_and_environment_for_session(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    stock_capture_id: int,
    index_capture_id: int,
    index_contract: Mapping[str, Any] | None = None,
    rule_version: str = "MARKET_PRESENTATION_R1",
) -> dict[str, Any]:
    """离线生产单日全部启用参考指数日因子与目标日四轴复合市场环境展示状态。"""
    if target_session > research_cutoff:
        raise ValueError("市场指数与环境目标日不得晚于研究截止日")
    calendar_data, _, calendar_revision, calendar_sessions = _load_calendar(
        research_cutoff
    )
    if target_session not in calendar_sessions:
        raise ValueError("市场指数与环境目标日不在官方日历中")
    calendar_id = str(calendar_data["calendar_id"])

    if index_contract is None:
        from stock_a.contracts.loader import (
            load_market_reference_indices_contract,
        )

        index_contract = load_market_reference_indices_contract(
            Path(config["paths"]["configuration_root"])
        )
    config_version = str(
        index_contract.get("config_version") or index_contract["version"]
    )
    enabled_indices = [
        item for item in index_contract.get("indices", ()) if item.get("enabled")
    ]
    if not enabled_indices:
        raise ValueError("NO_ENABLED_MARKET_REFERENCE_INDICES")
    primary_item = next(
        (item for item in enabled_indices if item.get("role") == "PRIMARY_MARKET"),
        None,
    )
    if primary_item is None:
        raise ValueError("PRIMARY_MARKET_INDEX_NOT_CONFIGURED")
    primary_symbol = str(primary_item["source_symbol"])
    enabled_symbols = [str(item["source_symbol"]) for item in enabled_indices]

    # 1. 读取该 index_capture_id 下全部启用指数 <= target_session 的日线点位
    bars_by_symbol: dict[str, dict[date, IndexDailyBar]] = {
        sym: {} for sym in enabled_symbols
    }
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT source_symbol, session_date, close_points, preclose_points,
                      price_quality, bar_status
               FROM stock.source_market_index_daily
               WHERE source_capture_id = %s
                 AND source_symbol = ANY(%s)
                 AND session_date <= %s
               ORDER BY source_symbol, session_date""",
            (index_capture_id, enabled_symbols, target_session),
        )
        for sym, sess, close_p, preclose_p, pq, bs in cursor.fetchall():
            bars_by_symbol[str(sym)][sess] = IndexDailyBar(
                session_date=sess,
                close_points=close_p,
                preclose_points=preclose_p,
                price_quality=str(pq),
                bar_status=str(bs),
            )

        cursor.execute(
            """SELECT classification, status
               FROM stock.source_market_participation_days
               WHERE target_session = %s
                 AND source_capture_id = %s
                 AND calendar_revision = %s
               ORDER BY id DESC
               LIMIT 1""",
            (target_session, stock_capture_id, calendar_revision),
        )
        part_row = cursor.fetchone()
        participation_axis = (
            str(part_row[0])
            if part_row is not None
            and part_row[1] not in {"NOT_COMPUTED", "NOT_COMPARABLE", "INCOMPLETE"}
            and part_row[0] in {"EXPANDING", "NORMAL", "THIN"}
            else "UNKNOWN"
        )

        # E02：读取紧邻前一官方交易日的已存环境状态作为 initial_state，保障跨日两日确认/UNKNOWN保留语义
        target_idx = calendar_sessions.index(target_session)
        prior_env_state: dict[str, Any] | None = None
        if target_idx > 0:
            prev_session = calendar_sessions[target_idx - 1]
            cursor.execute(
                """SELECT displayed_label, pending_label, pending_count,
                          rule_version, primary_index_symbol
                   FROM stock.source_market_environment_day_facts
                   WHERE target_session = %s
                     AND stock_capture_id = %s
                     AND calendar_revision = %s
                   ORDER BY (primary_index_symbol = %s AND rule_version = %s) DESC,
                            id DESC
                   LIMIT 1""",
                (
                    prev_session,
                    stock_capture_id,
                    calendar_revision,
                    primary_symbol,
                    rule_version,
                ),
            )
            prev_row = cursor.fetchone()
            if prev_row is not None:
                prior_env_state = {
                    "displayed_label": prev_row[0],
                    "pending_label": prev_row[1],
                    "pending_count": int(prev_row[2]),
                    "rule_version": str(prev_row[3]),
                    "primary_index_symbol": str(prev_row[4]),
                }

    # E03：直接从主宽度事实（read_source_market_context）读取 B_fixed_source，不依赖相邻变化表
    breadth_axis = "UNKNOWN"
    try:
        from stock_a.application.source_market_context import read_source_market_context

        mkt_ctx = read_source_market_context(
            config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=stock_capture_id,
            include_symbols=False,
        )
        breadth_axis = classify_market_breadth(mkt_ctx.calculation.b_fixed_source)
    except Exception:  # noqa: BLE001
        breadth_axis = "UNKNOWN"

    from stock_a.storage.postgres.price_limits import read_market_pressure_summary

    pressure_report = read_market_pressure_summary(
        config,
        target_session=target_session,
        source_capture_id=stock_capture_id,
        calendar_revision=calendar_revision,
    )
    pressure_axis = (
        str(pressure_report.get("classification"))
        if pressure_report.get("status") == "OBSERVED"
        and pressure_report.get("classification") in {"HIGH", "ELEVATED", "LOW"}
        else "UNKNOWN"
    )

    # 2. 计算全部启用指数目标日因子并落库
    factor_records: list[dict[str, Any]] = []
    primary_trend_state = "UNKNOWN"
    for item in enabled_indices:
        sym = str(item["source_symbol"])
        evaluated = evaluate_index_day_factors(
            target_session=target_session,
            official_sessions=calendar_sessions,
            bars_by_session=bars_by_symbol[sym],
        )
        if sym == primary_symbol:
            primary_trend_state = str(evaluated["trend_state"])
        factor_records.append(
            {
                "target_session": target_session,
                "source_symbol": sym,
                "official_code": str(item["official_code"]),
                "index_capture_id": index_capture_id,
                "config_version": config_version,
                "index_role_at_run": str(item["role"]),
                "calendar_id": calendar_id,
                "calendar_revision": calendar_revision,
                "contract_version": MARKET_INDEX_CONTRACT_VERSION,
                **evaluated,
            }
        )
    idx_report = persist_market_index_day_facts(config, records=factor_records)

    # 3. 落库目标日四轴复合市场环境展示状态（严格继承紧邻前一官方交易日前态）
    env_report = persist_market_environment_series(
        config,
        official_sessions=(target_session,),
        day_axis_inputs=(
            {
                "target_session": target_session,
                "trend_axis": primary_trend_state,
                "breadth_axis": breadth_axis,
                "participation_axis": participation_axis,
                "pressure_axis": pressure_axis,
            },
        ),
        stock_capture_id=stock_capture_id,
        index_capture_id=index_capture_id,
        primary_index_symbol=primary_symbol,
        index_config_version=config_version,
        calendar_id=calendar_id,
        calendar_revision=calendar_revision,
        rule_version=rule_version,
        initial_state=prior_env_state,
    )
    return {
        "status": "PERSISTED"
        if (
            idx_report.get("status") == "PERSISTED"
            or env_report.get("status") == "PERSISTED"
        )
        else "NO_CHANGE",
        "target_session": target_session.isoformat(),
        "stock_capture_id": stock_capture_id,
        "index_capture_id": index_capture_id,
        "config_version": config_version,
        "calendar_revision": calendar_revision,
        "index_factor_rows": int(idx_report.get("inserted_count", 0))
        + int(idx_report.get("unchanged_count", 0)),
        "environment_rows": int(env_report.get("inserted_count", 0))
        + int(env_report.get("unchanged_count", 0)),
        "axes": {
            "trend": primary_trend_state,
            "breadth": breadth_axis,
            "participation": participation_axis,
            "pressure": pressure_axis,
        },
    }
