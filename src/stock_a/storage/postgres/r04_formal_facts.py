"""R04正式D2生命周期、股票风险、Priority、Radar与Near-Miss的PG唯一读写边界。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any

from stock_a.storage.postgres import source_raw


class R04FormalPgError(RuntimeError):
    """R04正式PG读写失败。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _num_den(value: object) -> tuple[Decimal | None, Decimal | None]:
    if value is None or isinstance(value, bool):
        return None, None
    if isinstance(value, Fraction):
        return Decimal(value.numerator), Decimal(value.denominator)
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None, None
        frac = Fraction(value)
        return Decimal(frac.numerator), Decimal(frac.denominator)
    if isinstance(value, int):
        return Decimal(value), Decimal(1)
    return None, None


def _to_fraction(num: Any, den: Any) -> Fraction | None:
    if num is None or den is None:
        return None
    d = Decimal(den)
    if d <= 0:
        return None
    return Fraction(Decimal(num)) / Fraction(d)


def persist_d2_day_bundle(
    config: Mapping[str, Any],
    *,
    session_date: date,
    calendar_id: str,
    rule_identity: str,
    episodes: Sequence[Mapping[str, Any]],
    day_states: Sequence[Mapping[str, Any]],
    primary_projections: Sequence[Mapping[str, Any]],
    peak_events: Sequence[Mapping[str, Any]],
    entity_type: str = "STOCK",
) -> dict[str, Any]:
    """在单一SERIALIZABLE事务中原子持久化当日D2 episode、状态、选主投影与新父峰。"""
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _conn,
        cursor,
        _scope,
    ):
        for ep in episodes:
            fl_num, fl_den = _num_den(ep.get("failure_level"))
            cursor.execute(
                """
                INSERT INTO stock.d2_episodes (
                    episode_id, entity_type, entity_id, calendar_id, rule_identity,
                    scenario, kind, birth_session, birth_ordinal, parent_episode_id,
                    event_id, failure_level_numerator, failure_level_denominator,
                    failure_coordinate, failure_source, effective_from_session, linked_anchor_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (episode_id) DO UPDATE SET
                    failure_level_numerator = EXCLUDED.failure_level_numerator,
                    failure_level_denominator = EXCLUDED.failure_level_denominator,
                    failure_coordinate = EXCLUDED.failure_coordinate,
                    failure_source = EXCLUDED.failure_source,
                    effective_from_session = EXCLUDED.effective_from_session,
                    linked_anchor_id = EXCLUDED.linked_anchor_id
                """,
                (
                    ep["episode_id"],
                    ep.get("entity_type", entity_type),
                    ep["entity_id"],
                    ep.get("calendar_id", calendar_id),
                    ep.get("rule_identity", rule_identity),
                    ep["scenario"],
                    ep["kind"],
                    ep["birth_session"],
                    int(ep["birth_ordinal"]),
                    ep.get("parent_episode_id"),
                    ep.get("event_id"),
                    fl_num,
                    fl_den,
                    ep.get("failure_coordinate"),
                    ep.get("failure_source"),
                    ep.get("effective_from_session"),
                    ep.get("linked_anchor_id"),
                ),
            )

        # 按(entity_type, calendar_id, session_date, rule_identity)替换当日逐日状态与选主
        cursor.execute(
            """
            DELETE FROM stock.d2_episode_day_states
            WHERE entity_type = %s AND calendar_id = %s AND session_date = %s AND rule_identity = %s
            """,
            (entity_type, calendar_id, session_date, rule_identity),
        )
        for st in day_states:
            ib_num, ib_den = _num_den(st.get("improvement_baseline"))
            cursor.execute(
                """
                INSERT INTO stock.d2_episode_day_states (
                    entity_type, entity_id, calendar_id, session_date, ordinal, rule_identity,
                    episode_id, coordinate_id, input_ref, prior_mode, event_id, scenario, kind,
                    birth_ordinal, parent_episode_id, exact_maturity, lower_bound_maturity,
                    display_maturity, effective_eligibility_value, effective_eligibility_quality,
                    effective_eligibility_reason, effective_bucket, validity_value,
                    validity_quality, validity_reason, freshness, health_value, health_quality,
                    health_reason, tracking, downgrade_candidate, downgrade_count, expiry_count,
                    improvement_baseline_numerator, improvement_baseline_denominator,
                    exit_ordinal, exit_reason, reentry_armed, transitions, preserved_episode_ids
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    st.get("entity_type", entity_type),
                    st["entity_id"],
                    st.get("calendar_id", calendar_id),
                    session_date,
                    int(st["ordinal"]),
                    st.get("rule_identity", rule_identity),
                    st["episode_id"],
                    st["coordinate_id"],
                    st["input_ref"],
                    st["prior_mode"],
                    st.get("event_id"),
                    st["scenario"],
                    st["kind"],
                    int(st["birth_ordinal"]),
                    st.get("parent_episode_id"),
                    st.get("exact_maturity"),
                    st["lower_bound_maturity"],
                    st["display_maturity"],
                    st.get("effective_eligibility_value"),
                    st["effective_eligibility_quality"],
                    st.get("effective_eligibility_reason"),
                    st.get("effective_bucket"),
                    st.get("validity_value"),
                    st["validity_quality"],
                    st.get("validity_reason"),
                    st["freshness"],
                    st.get("health_value"),
                    st["health_quality"],
                    st.get("health_reason"),
                    st["tracking"],
                    st.get("downgrade_candidate"),
                    int(st["downgrade_count"]),
                    int(st["expiry_count"]),
                    ib_num,
                    ib_den,
                    st.get("exit_ordinal"),
                    st.get("exit_reason"),
                    bool(st["reentry_armed"]),
                    list(st.get("transitions") or ()),
                    list(st.get("preserved_episode_ids") or ()),
                ),
            )

        cursor.execute(
            """
            DELETE FROM stock.d2_primary_episode_day_facts
            WHERE entity_type = %s AND calendar_id = %s AND session_date = %s AND rule_identity = %s
            """,
            (entity_type, calendar_id, session_date, rule_identity),
        )
        for pr in primary_projections:
            cursor.execute(
                """
                INSERT INTO stock.d2_primary_episode_day_facts (
                    entity_type, entity_id, calendar_id, session_date, ordinal, rule_identity,
                    primary_status, primary_reason, primary_episode_id, candidate_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    pr.get("entity_type", entity_type),
                    pr["entity_id"],
                    pr.get("calendar_id", calendar_id),
                    session_date,
                    int(pr["ordinal"]),
                    pr.get("rule_identity", rule_identity),
                    pr["primary_status"],
                    pr.get("primary_reason"),
                    pr.get("primary_episode_id"),
                    int(pr["candidate_count"]),
                ),
            )

        cursor.execute(
            "DELETE FROM stock.d2_parent_peak_events WHERE peak_session = %s",
            (session_date,),
        )
        for pk in peak_events:
            hi_num, hi_den = _num_den(pk["peak_high"])
            cl_num, cl_den = _num_den(pk["peak_close"])
            cursor.execute(
                """
                INSERT INTO stock.d2_parent_peak_events (
                    parent_episode_id, entity_id, peak_session, peak_ordinal, coordinate_id,
                    peak_high_numerator, peak_high_denominator,
                    peak_close_numerator, peak_close_denominator
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (parent_episode_id, peak_session) DO UPDATE SET
                    peak_ordinal = EXCLUDED.peak_ordinal,
                    coordinate_id = EXCLUDED.coordinate_id,
                    peak_high_numerator = EXCLUDED.peak_high_numerator,
                    peak_high_denominator = EXCLUDED.peak_high_denominator,
                    peak_close_numerator = EXCLUDED.peak_close_numerator,
                    peak_close_denominator = EXCLUDED.peak_close_denominator
                """,
                (
                    pk["parent_episode_id"],
                    pk["entity_id"],
                    session_date,
                    int(pk["peak_ordinal"]),
                    pk["coordinate_id"],
                    hi_num,
                    hi_den,
                    cl_num,
                    cl_den,
                ),
            )

        return {
            "status": "PERSISTED",
            "session_date": session_date.isoformat(),
            "episodes_upserted": len(episodes),
            "day_states_persisted": len(day_states),
            "primary_projections_persisted": len(primary_projections),
            "peak_events_persisted": len(peak_events),
        }


def read_d2_day_states(
    config: Mapping[str, Any],
    *,
    session_date: date,
    calendar_id: str,
    rule_identity: str,
    entity_type: str = "STOCK",
    entity_id: str | None = None,
) -> list[dict[str, Any]]:
    """只读查询指定交易日的D2 episode逐日状态。"""
    with source_raw._transaction(config, write=False) as (_conn, cursor, _scope):
        where = [
            "s.entity_type = %s",
            "s.calendar_id = %s",
            "s.session_date = %s",
            "s.rule_identity = %s",
        ]
        params: list[Any] = [entity_type, calendar_id, session_date, rule_identity]
        if entity_id is not None:
            where.append("s.entity_id = %s")
            params.append(entity_id)
        rows = cursor.execute(
            f"""
            SELECT s.entity_type, s.entity_id, s.calendar_id, s.session_date, s.ordinal,
                   s.rule_identity, s.episode_id, s.coordinate_id, s.input_ref, s.prior_mode,
                   s.event_id, s.scenario, s.kind, s.birth_ordinal, s.parent_episode_id,
                   s.exact_maturity, s.lower_bound_maturity, s.display_maturity,
                   s.effective_eligibility_value, s.effective_eligibility_quality,
                   s.effective_eligibility_reason, s.effective_bucket,
                   s.validity_value, s.validity_quality, s.validity_reason,
                   s.freshness, s.health_value, s.health_quality, s.health_reason,
                   s.tracking, s.downgrade_candidate, s.downgrade_count, s.expiry_count,
                   s.improvement_baseline_numerator, s.improvement_baseline_denominator,
                   s.exit_ordinal, s.exit_reason, s.reentry_armed, s.transitions,
                   s.preserved_episode_ids,
                   e.birth_session, e.failure_level_numerator, e.failure_level_denominator,
                   e.failure_coordinate, e.failure_source, e.effective_from_session, e.linked_anchor_id
            FROM stock.d2_episode_day_states s
            JOIN stock.d2_episodes e ON e.episode_id = s.episode_id
            WHERE {" AND ".join(where)}
            ORDER BY s.entity_id, s.birth_ordinal DESC, s.episode_id
            """,
            tuple(params),
        ).fetchall()
        cols = [d[0] for d in cursor.description]
        result = []
        for r in rows:
            item = dict(zip(cols, r))
            item["improvement_baseline"] = _to_fraction(
                item.pop("improvement_baseline_numerator"),
                item.pop("improvement_baseline_denominator"),
            )
            item["failure_level"] = _to_fraction(
                item.pop("failure_level_numerator"),
                item.pop("failure_level_denominator"),
            )
            result.append(item)
        return result


def read_d2_primary_projections(
    config: Mapping[str, Any],
    *,
    session_date: date,
    calendar_id: str,
    rule_identity: str,
    entity_type: str = "STOCK",
    entity_id: str | None = None,
) -> list[dict[str, Any]]:
    """只读查询指定交易日D2唯一主选择投影。"""
    with source_raw._transaction(config, write=False) as (_conn, cursor, _scope):
        where = [
            "entity_type = %s",
            "calendar_id = %s",
            "session_date = %s",
            "rule_identity = %s",
        ]
        params: list[Any] = [entity_type, calendar_id, session_date, rule_identity]
        if entity_id is not None:
            where.append("entity_id = %s")
            params.append(entity_id)
        rows = cursor.execute(
            f"""
            SELECT entity_type, entity_id, calendar_id, session_date, ordinal, rule_identity,
                   primary_status, primary_reason, primary_episode_id, candidate_count
            FROM stock.d2_primary_episode_day_facts
            WHERE {" AND ".join(where)}
            ORDER BY entity_id
            """,
            tuple(params),
        ).fetchall()
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, r)) for r in rows]


def read_d2_parent_peaks_before(
    config: Mapping[str, Any],
    *,
    before_ordinal: int,
    entity_id: str | None = None,
) -> list[dict[str, Any]]:
    """只读查询严格早于当日（peak_ordinal < before_ordinal）的冻结父峰事件。"""
    with source_raw._transaction(config, write=False) as (_conn, cursor, _scope):
        where = ["peak_ordinal < %s"]
        params: list[Any] = [before_ordinal]
        if entity_id is not None:
            where.append("entity_id = %s")
            params.append(entity_id)
        rows = cursor.execute(
            f"""
            SELECT parent_episode_id, entity_id, peak_session, peak_ordinal, coordinate_id,
                   peak_high_numerator, peak_high_denominator,
                   peak_close_numerator, peak_close_denominator
            FROM stock.d2_parent_peak_events
            WHERE {" AND ".join(where)}
            ORDER BY entity_id, parent_episode_id, peak_ordinal ASC
            """,
            tuple(params),
        ).fetchall()
        cols = [d[0] for d in cursor.description]
        result = []
        for r in rows:
            item = dict(zip(cols, r))
            item["peak_high"] = _to_fraction(
                item.pop("peak_high_numerator"), item.pop("peak_high_denominator")
            )
            item["peak_close"] = _to_fraction(
                item.pop("peak_close_numerator"), item.pop("peak_close_denominator")
            )
            result.append(item)
        return result


def persist_downstream_projections_day(
    config: Mapping[str, Any],
    *,
    session_date: date,
    risk_rows: Sequence[Mapping[str, Any]],
    priority_rows: Sequence[Mapping[str, Any]],
    radar_events: Sequence[Mapping[str, Any]],
    near_miss_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """在单一SERIALIZABLE事务中原子持久化股票风险、Priority、Radar与Near-Miss日事实。"""
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _conn,
        cursor,
        _scope,
    ):
        cursor.execute(
            "DELETE FROM stock.stock_risk_day_facts WHERE target_session = %s",
            (session_date,),
        )
        for rr in risk_rows:
            cursor.execute(
                """
                INSERT INTO stock.stock_risk_day_facts (
                    target_session, source_symbol, coordinate_id, selected_episode_id,
                    structure_applicability, support_applicability, risk_level, risk_quality,
                    risk_reason, lower_bound, upper_bound, primary_reason,
                    confirmed_reasons, possible_reasons, damage_value, damage_quality,
                    extension_value, extension_quality, invalidated_value, invalidated_quality,
                    shallow_breach_value, shallow_breach_quality
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    session_date,
                    rr["source_symbol"],
                    rr["coordinate_id"],
                    rr.get("selected_episode_id"),
                    rr["structure_applicability"],
                    rr["support_applicability"],
                    rr.get("risk_level"),
                    rr["risk_quality"],
                    rr.get("risk_reason"),
                    rr.get("lower_bound"),
                    rr.get("upper_bound"),
                    rr.get("primary_reason"),
                    list(rr.get("confirmed_reasons") or ()),
                    list(rr.get("possible_reasons") or ()),
                    rr.get("damage_value"),
                    rr["damage_quality"],
                    rr.get("extension_value"),
                    rr["extension_quality"],
                    rr.get("invalidated_value"),
                    rr["invalidated_quality"],
                    rr.get("shallow_breach_value"),
                    rr["shallow_breach_quality"],
                ),
            )

        cursor.execute(
            "DELETE FROM stock.stock_priority_day_facts WHERE target_session = %s",
            (session_date,),
        )
        for pr in priority_rows:
            rps_num, rps_den = _num_den(pr.get("rps5_delta3_pp"))
            amt_num, amt_den = _num_den(pr.get("prior20_amount_mean"))
            cursor.execute(
                """
                INSERT INTO stock.stock_priority_day_facts (
                    target_session, source_symbol, priority_status, exclusion_reasons,
                    bucket, emergence_rank, structure_rank,
                    rps5_delta3_pp_numerator, rps5_delta3_pp_denominator,
                    risk_rank, improvement_age, improvement_age_category,
                    prior20_amount_mean_numerator, prior20_amount_mean_denominator,
                    stable_id, priority_rank
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    session_date,
                    pr["source_symbol"],
                    pr["priority_status"],
                    list(pr.get("exclusion_reasons") or ()),
                    pr.get("bucket"),
                    pr.get("emergence_rank"),
                    pr.get("structure_rank"),
                    rps_num,
                    rps_den,
                    pr.get("risk_rank"),
                    pr.get("improvement_age"),
                    pr["improvement_age_category"],
                    amt_num,
                    amt_den,
                    pr["stable_id"],
                    pr.get("priority_rank"),
                ),
            )

        cursor.execute(
            "DELETE FROM stock.stock_radar_day_events WHERE target_session = %s",
            (session_date,),
        )
        for ev in radar_events:
            cursor.execute(
                """
                INSERT INTO stock.stock_radar_day_events (
                    target_session, source_symbol, event_type, episode_id,
                    display_order, is_primary_event, why_now, waiting_for,
                    invalid_if, annotation_flags, evidence_summary
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    session_date,
                    ev["source_symbol"],
                    ev["event_type"],
                    ev["episode_id"],
                    int(ev["display_order"]),
                    bool(ev["is_primary_event"]),
                    ev["why_now"],
                    ev.get("waiting_for"),
                    ev.get("invalid_if"),
                    list(ev.get("annotation_flags") or ()),
                    ev["evidence_summary"],
                ),
            )

        cursor.execute(
            "DELETE FROM stock.stock_near_miss_day_facts WHERE target_session = %s",
            (session_date,),
        )
        for nm in near_miss_rows:
            g_num, g_den = _num_den(nm["gap"])
            t_num, t_den = _num_den(nm["tolerance"])
            r_num, r_den = _num_den(nm["gap_ratio"])
            d_num, d_den = _num_den(nm["delta3"])
            cursor.execute(
                """
                INSERT INTO stock.stock_near_miss_day_facts (
                    target_session, source_symbol, scenario, path_id, failed_predicate,
                    gap_numerator, gap_denominator, tolerance_numerator, tolerance_denominator,
                    gap_ratio_numerator, gap_ratio_denominator, emergence_rank,
                    delta3_numerator, delta3_denominator, stable_id, display_order,
                    hard_gates_summary
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    session_date,
                    nm["source_symbol"],
                    nm["scenario"],
                    nm["path_id"],
                    nm["failed_predicate"],
                    g_num,
                    g_den,
                    t_num,
                    t_den,
                    r_num,
                    r_den,
                    int(nm["emergence_rank"]),
                    d_num,
                    d_den,
                    nm["stable_id"],
                    int(nm["display_order"]),
                    nm["hard_gates_summary"],
                ),
            )

        return {
            "status": "PERSISTED",
            "session_date": session_date.isoformat(),
            "risk_rows": len(risk_rows),
            "priority_rows": len(priority_rows),
            "radar_events": len(radar_events),
            "near_miss_rows": len(near_miss_rows),
        }


def read_stock_priority_page(
    config: Mapping[str, Any],
    *,
    session_date: date,
    status_filter: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """只读分页查询指定交易日股票正式Priority排序结果。"""
    with source_raw._transaction(config, write=False) as (_conn, cursor, _scope):
        where = ["target_session = %s"]
        params: list[Any] = [session_date]
        if status_filter is not None:
            where.append("priority_status = %s")
            params.append(status_filter)
        total = cursor.execute(
            f"SELECT COUNT(*) FROM stock.stock_priority_day_facts WHERE {' AND '.join(where)}",
            tuple(params),
        ).fetchone()[0]
        offset = max(0, (page - 1) * page_size)
        rows = cursor.execute(
            f"""
            SELECT target_session, source_symbol, priority_status, exclusion_reasons,
                   bucket, emergence_rank, structure_rank,
                   rps5_delta3_pp_numerator, rps5_delta3_pp_denominator,
                   risk_rank, improvement_age, improvement_age_category,
                   prior20_amount_mean_numerator, prior20_amount_mean_denominator,
                   stable_id, priority_rank
            FROM stock.stock_priority_day_facts
            WHERE {" AND ".join(where)}
            ORDER BY
                (priority_status = 'RANKABLE') DESC,
                priority_rank ASC NULLS LAST,
                (priority_status = 'EXCLUDED') DESC,
                source_symbol ASC
            LIMIT %s OFFSET %s
            """,
            tuple(params) + (page_size, offset),
        ).fetchall()
        cols = [d[0] for d in cursor.description]
        items = []
        for r in rows:
            item = dict(zip(cols, r))
            item["rps5_delta3_pp"] = _to_fraction(
                item.pop("rps5_delta3_pp_numerator"),
                item.pop("rps5_delta3_pp_denominator"),
            )
            item["prior20_amount_mean"] = _to_fraction(
                item.pop("prior20_amount_mean_numerator"),
                item.pop("prior20_amount_mean_denominator"),
            )
            items.append(item)
        return {
            "session_date": session_date.isoformat(),
            "total": int(total),
            "page": page,
            "page_size": page_size,
            "items": items,
        }


def read_stock_risk_summary_and_rows(
    config: Mapping[str, Any],
    *,
    session_date: date,
    source_symbol: str | None = None,
) -> dict[str, Any]:
    """只读查询指定交易日股票风险明细与覆盖率汇总。"""
    with source_raw._transaction(config, write=False) as (_conn, cursor, _scope):
        summary_row = cursor.execute(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE risk_quality = 'OBSERVED'),
                   COUNT(*) FILTER (WHERE risk_quality = 'UNKNOWN')
            FROM stock.stock_risk_day_facts
            WHERE target_session = %s
            """,
            (session_date,),
        ).fetchone()
        total, known, unknown_cnt = (
            int(summary_row[0]),
            int(summary_row[1]),
            int(summary_row[2]),
        )
        where = ["target_session = %s"]
        params: list[Any] = [session_date]
        if source_symbol is not None:
            where.append("source_symbol = %s")
            params.append(source_symbol)
        rows = cursor.execute(
            f"""
            SELECT target_session, source_symbol, coordinate_id, selected_episode_id,
                   structure_applicability, support_applicability, risk_level, risk_quality,
                   risk_reason, lower_bound, upper_bound, primary_reason,
                   confirmed_reasons, possible_reasons
            FROM stock.stock_risk_day_facts
            WHERE {" AND ".join(where)}
            ORDER BY source_symbol
            """,
            tuple(params),
        ).fetchall()
        cols = [d[0] for d in cursor.description]
        return {
            "session_date": session_date.isoformat(),
            "total": total,
            "known": known,
            "unknown": unknown_cnt,
            "coverage": (known / total) if total > 0 else None,
            "items": [dict(zip(cols, r)) for r in rows],
        }


def read_stock_radar_and_near_miss(
    config: Mapping[str, Any],
    *,
    session_date: date,
    source_symbol: str | None = None,
) -> dict[str, Any]:
    """只读查询指定交易日Radar事件与Near-Miss解释。"""
    with source_raw._transaction(config, write=False) as (_conn, cursor, _scope):
        where = ["target_session = %s"]
        params: list[Any] = [session_date]
        if source_symbol is not None:
            where.append("source_symbol = %s")
            params.append(source_symbol)
        radar_rows = cursor.execute(
            f"""
            SELECT target_session, source_symbol, event_type, episode_id,
                   display_order, is_primary_event, why_now, waiting_for,
                   invalid_if, annotation_flags, evidence_summary
            FROM stock.stock_radar_day_events
            WHERE {" AND ".join(where)}
            ORDER BY display_order ASC
            """,
            tuple(params),
        ).fetchall()
        r_cols = [d[0] for d in cursor.description]
        nm_rows = cursor.execute(
            f"""
            SELECT target_session, source_symbol, scenario, path_id, failed_predicate,
                   gap_numerator, gap_denominator, tolerance_numerator, tolerance_denominator,
                   gap_ratio_numerator, gap_ratio_denominator, emergence_rank,
                   delta3_numerator, delta3_denominator, stable_id, display_order,
                   hard_gates_summary
            FROM stock.stock_near_miss_day_facts
            WHERE {" AND ".join(where)}
            ORDER BY display_order ASC
            """,
            tuple(params),
        ).fetchall()
        nm_cols = [d[0] for d in cursor.description]
        nm_items = []
        for r in nm_rows:
            item = dict(zip(nm_cols, r))
            item["gap"] = _to_fraction(
                item.pop("gap_numerator"), item.pop("gap_denominator")
            )
            item["tolerance"] = _to_fraction(
                item.pop("tolerance_numerator"), item.pop("tolerance_denominator")
            )
            item["gap_ratio"] = _to_fraction(
                item.pop("gap_ratio_numerator"), item.pop("gap_ratio_denominator")
            )
            item["delta3"] = _to_fraction(
                item.pop("delta3_numerator"), item.pop("delta3_denominator")
            )
            nm_items.append(item)
        return {
            "session_date": session_date.isoformat(),
            "radar_events": [dict(zip(r_cols, r)) for r in radar_rows],
            "near_miss_items": nm_items,
        }
