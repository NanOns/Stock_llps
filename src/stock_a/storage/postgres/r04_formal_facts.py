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


def _persist_d2_day_bundle_in_cursor(
    cursor: Any,
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
    for ep in episodes:
        fl_num, fl_den = _num_den(ep.get("failure_level"))
        cursor.execute(
            """
            INSERT INTO stock.d2_episodes (
                episode_id, entity_type, entity_id, calendar_id, rule_identity,
                scenario, kind, birth_session, birth_ordinal, parent_episode_id,
                event_id, failure_level_numerator, failure_level_denominator,
                failure_coordinate, failure_source, effective_from_session, linked_anchor_id,
                failure_window, evidence_ref
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (episode_id) DO UPDATE SET
                failure_level_numerator = CASE
                    WHEN stock.d2_episodes.birth_session = %s THEN EXCLUDED.failure_level_numerator
                    ELSE COALESCE(stock.d2_episodes.failure_level_numerator, EXCLUDED.failure_level_numerator)
                END,
                failure_level_denominator = CASE
                    WHEN stock.d2_episodes.birth_session = %s THEN EXCLUDED.failure_level_denominator
                    ELSE COALESCE(stock.d2_episodes.failure_level_denominator, EXCLUDED.failure_level_denominator)
                END,
                failure_coordinate = CASE
                    WHEN stock.d2_episodes.birth_session = %s THEN EXCLUDED.failure_coordinate
                    ELSE COALESCE(stock.d2_episodes.failure_coordinate, EXCLUDED.failure_coordinate)
                END,
                failure_source = CASE
                    WHEN stock.d2_episodes.birth_session = %s THEN EXCLUDED.failure_source
                    ELSE COALESCE(stock.d2_episodes.failure_source, EXCLUDED.failure_source)
                END,
                effective_from_session = CASE
                    WHEN stock.d2_episodes.birth_session = %s THEN EXCLUDED.effective_from_session
                    ELSE COALESCE(stock.d2_episodes.effective_from_session, EXCLUDED.effective_from_session)
                END,
                linked_anchor_id = CASE
                    WHEN stock.d2_episodes.birth_session = %s THEN EXCLUDED.linked_anchor_id
                    ELSE COALESCE(stock.d2_episodes.linked_anchor_id, EXCLUDED.linked_anchor_id)
                END,
                failure_window = CASE
                    WHEN stock.d2_episodes.birth_session = %s THEN EXCLUDED.failure_window
                    ELSE COALESCE(stock.d2_episodes.failure_window, EXCLUDED.failure_window)
                END,
                evidence_ref = CASE
                    WHEN stock.d2_episodes.birth_session = %s THEN EXCLUDED.evidence_ref
                    ELSE COALESCE(stock.d2_episodes.evidence_ref, EXCLUDED.evidence_ref)
                END
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
                ep.get("failure_window"),
                ep.get("evidence_ref"),
                session_date,
                session_date,
                session_date,
                session_date,
                session_date,
                session_date,
                session_date,
                session_date,
            ),
        )

    # 按单一有效业务键(entity_type, calendar_id, session_date)替换当日逐日状态与选主（rule_identity仅作元数据）
    cursor.execute(
        """
        DELETE FROM stock.d2_episode_day_states
        WHERE entity_type = %s AND calendar_id = %s AND session_date = %s
        """,
        (entity_type, calendar_id, session_date),
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
        WHERE entity_type = %s AND calendar_id = %s AND session_date = %s
        """,
        (entity_type, calendar_id, session_date),
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
        return _persist_d2_day_bundle_in_cursor(
            cursor,
            session_date=session_date,
            calendar_id=calendar_id,
            rule_identity=rule_identity,
            episodes=episodes,
            day_states=day_states,
            primary_projections=primary_projections,
            peak_events=peak_events,
            entity_type=entity_type,
        )


def read_d2_day_states(
    config: Mapping[str, Any],
    *,
    session_date: date,
    calendar_id: str,
    rule_identity: str | None = None,
    entity_type: str = "STOCK",
    entity_id: str | None = None,
) -> list[dict[str, Any]]:
    """只读查询指定交易日的D2 episode逐日状态（按单一业务键读取，rule_identity仅作元数据）。"""
    with source_raw._transaction(config, write=False) as (_conn, cursor, _scope):
        where = [
            "s.entity_type = %s",
            "s.calendar_id = %s",
            "s.session_date = %s",
        ]
        params: list[Any] = [entity_type, calendar_id, session_date]
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
                   e.failure_coordinate, e.failure_source, e.effective_from_session, e.linked_anchor_id,
                   e.failure_window, e.evidence_ref
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
    rule_identity: str | None = None,
    entity_type: str = "STOCK",
    entity_id: str | None = None,
) -> list[dict[str, Any]]:
    """只读查询指定交易日D2唯一主选择投影（按单一业务键读取，rule_identity仅作元数据）。"""
    with source_raw._transaction(config, write=False) as (_conn, cursor, _scope):
        where = [
            "entity_type = %s",
            "calendar_id = %s",
            "session_date = %s",
        ]
        params: list[Any] = [entity_type, calendar_id, session_date]
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


def _persist_downstream_projections_in_cursor(
    cursor: Any,
    *,
    session_date: date,
    risk_rows: Sequence[Mapping[str, Any]],
    priority_rows: Sequence[Mapping[str, Any]],
    radar_events: Sequence[Mapping[str, Any]],
    near_miss_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
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
        return _persist_downstream_projections_in_cursor(
            cursor,
            session_date=session_date,
            risk_rows=risk_rows,
            priority_rows=priority_rows,
            radar_events=radar_events,
            near_miss_rows=near_miss_rows,
        )


def _persist_d0_pb_continue_in_cursor(
    cursor: Any,
    *,
    session_date: date,
    pb_cont_facts: Sequence[Mapping[str, Any]],
    pb_cont_details: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    """在同事务内持久化PB/CONTINUE证券级场景事实、正式准入及逐父/峰明细。"""
    cursor.execute(
        """
        DELETE FROM stock.d0_parent_scenario_detail_facts
        WHERE target_session = %s
        """,
        (session_date,),
    )
    cursor.execute(
        """
        DELETE FROM stock.source_d0_formal_admission_day_facts
        WHERE target_session = %s
          AND scenario IN ('STRONG_PULLBACK', 'TREND_CONTINUE')
        """,
        (session_date,),
    )
    cursor.execute(
        """
        DELETE FROM stock.source_d0_scenario_day_facts
        WHERE target_session = %s
          AND scenario IN ('STRONG_PULLBACK', 'TREND_CONTINUE')
        """,
        (session_date,),
    )
    scen_upserted = 0
    adm_upserted = 0
    for item in pb_cont_facts:
        p_ids = list(item["predicate_ids"])
        p_states = list(item["predicate_states"])
        p_quals = list(item["predicate_qualities"])
        p_reasons = list(item["predicate_reason_codes"])
        p_nums = list(item["predicate_value_numerators"])
        p_dens = list(item["predicate_value_denominators"])
        cursor.execute(
            """
            INSERT INTO stock.source_d0_scenario_day_facts (
                target_session, source_symbol, source_capture_id, parser_contract,
                scenario, producer_version, algorithm_contract, session_contract,
                history_window_start, history_session_count, source_count,
                scenario_state, scenario_reason,
                predicate_ids, predicate_states, predicate_qualities,
                predicate_reason_codes, predicate_value_numerators, predicate_value_denominators,
                source_time_quality, source_clock_role, source_available_at,
                calendar_id, calendar_revision, research_mode, price_coordinate
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING id
            """,
            (
                session_date,
                item["source_symbol"],
                int(item["source_capture_id"]),
                item["parser_contract"],
                item["scenario"],
                item["producer_version"],
                item["algorithm_contract"],
                item["session_contract"],
                item["history_window_start"],
                int(item["history_session_count"]),
                int(item["source_count"]),
                item["scenario_state"],
                item["scenario_reason"],
                p_ids,
                p_states,
                p_quals,
                p_reasons,
                p_nums,
                p_dens,
                item["source_time_quality"],
                item["source_clock_role"],
                item["source_available_at"],
                item["calendar_id"],
                item["calendar_revision"],
                item["research_mode"],
                item["price_coordinate"],
            ),
        )
        raw_fact_id = int(cursor.fetchone()[0])
        scen_upserted += 1

        fl_num, fl_den = _num_den(item.get("failure_level"))
        cursor.execute(
            """
            INSERT INTO stock.source_d0_formal_admission_day_facts (
                raw_fact_id, seed_fact_id, source_capture_id, target_session,
                source_symbol, scenario, raw_role, raw_state, raw_reason,
                raw_producer_version, algorithm_contract, session_contract,
                history_window_start, history_session_count, source_count,
                calendar_id, calendar_revision, source_time_quality, source_clock_role,
                source_available_at, capture_started_at, capture_validated_at,
                profile_revision, seed_producer_version, identity_scope, research_mode,
                price_coordinate, admission_contract, admission_status, admission_reason,
                research_universe_state, research_universe_reason,
                actual_quote_state, actual_quote_reason,
                coordinate_window_state, coordinate_window_reason,
                d0_liquidity_state, d0_liquidity_reason,
                non_core_damage_state, non_core_damage_reason,
                non_extreme_extension_state, non_extreme_extension_reason,
                non_first_day_damage_state, non_first_day_damage_reason,
                non_severe_drop_state, non_severe_drop_reason,
                reason_codes, event_id, failure_level_numerator, failure_level_denominator,
                failure_coordinate, failure_source, failure_window_start,
                failure_window_session_count, failure_source_event_ref,
                effective_from_session, prior_episode_id, linked_anchor_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                raw_fact_id,
                int(item["seed_fact_id"]),
                int(item["source_capture_id"]),
                session_date,
                item["source_symbol"],
                item["scenario"],
                "FORMAL",
                item["scenario_state"],
                item["scenario_reason"],
                item["producer_version"],
                item["algorithm_contract"],
                item["session_contract"],
                item["history_window_start"],
                int(item["history_session_count"]),
                int(item["source_count"]),
                item["calendar_id"],
                item["calendar_revision"],
                item["source_time_quality"],
                item["source_clock_role"],
                item["source_available_at"],
                item.get("capture_started_at"),
                item.get("capture_validated_at"),
                item["profile_revision"],
                item["seed_producer_version"],
                item["identity_scope"],
                item["research_mode"],
                item["price_coordinate"],
                item["admission_contract"],
                item["admission_status"],
                item["admission_reason"],
                item["research_universe_state"],
                item.get("research_universe_reason"),
                item["actual_quote_state"],
                item.get("actual_quote_reason"),
                item["coordinate_window_state"],
                item.get("coordinate_window_reason"),
                item["d0_liquidity_state"],
                item.get("d0_liquidity_reason"),
                item["non_core_damage_state"],
                item.get("non_core_damage_reason"),
                item["non_extreme_extension_state"],
                item.get("non_extreme_extension_reason"),
                item["non_first_day_damage_state"],
                item.get("non_first_day_damage_reason"),
                item["non_severe_drop_state"],
                item.get("non_severe_drop_reason"),
                list(item.get("reason_codes") or ()),
                item.get("event_id"),
                fl_num,
                fl_den,
                item.get("failure_coordinate"),
                item.get("failure_source"),
                item.get("failure_window_start"),
                item.get("failure_window_session_count"),
                item.get("failure_source_event_ref"),
                item.get("effective_from_session"),
                item.get("prior_episode_id"),
                item.get("linked_anchor_id"),
            ),
        )
        adm_upserted += 1

    detail_inserted = 0
    for d in pb_cont_details:
        fl_num, fl_den = _num_den(d.get("failure_level"))
        cursor.execute(
            """
            INSERT INTO stock.d0_parent_scenario_detail_facts (
                target_session, source_symbol, scenario, parent_episode_id,
                peak_event_id, peak_ordinal_key, raw_state, raw_reason,
                predicate_ids, predicate_states, predicate_qualities,
                predicate_reason_codes, predicate_value_numerators, predicate_value_denominators,
                admission_status, admission_reason, reason_codes,
                event_id, linked_anchor_id,
                failure_level_numerator, failure_level_denominator,
                failure_coordinate, failure_source, failure_window_start,
                failure_window_session_count, effective_from_session
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                session_date,
                d["source_symbol"],
                d["scenario"],
                d["parent_episode_id"],
                d.get("peak_event_id"),
                int(d.get("peak_ordinal_key", -1)),
                d["raw_state"],
                d["raw_reason"],
                list(d["predicate_ids"]),
                list(d["predicate_states"]),
                list(d["predicate_qualities"]),
                list(d["predicate_reason_codes"]),
                list(d["predicate_value_numerators"]),
                list(d["predicate_value_denominators"]),
                d["admission_status"],
                d["admission_reason"],
                list(d.get("reason_codes") or ()),
                d.get("event_id"),
                d.get("linked_anchor_id"),
                fl_num,
                fl_den,
                d.get("failure_coordinate"),
                d.get("failure_source"),
                d.get("failure_window_start"),
                d.get("failure_window_session_count"),
                d.get("effective_from_session"),
            ),
        )
        detail_inserted += 1

    return {
        "scenario_facts": scen_upserted,
        "admission_facts": adm_upserted,
        "parent_details": detail_inserted,
    }


def persist_d2_and_downstream_day_bundle(
    config: Mapping[str, Any],
    *,
    session_date: date,
    calendar_id: str,
    rule_identity: str,
    episodes: Sequence[Mapping[str, Any]],
    day_states: Sequence[Mapping[str, Any]],
    primary_projections: Sequence[Mapping[str, Any]],
    peak_events: Sequence[Mapping[str, Any]],
    risk_rows: Sequence[Mapping[str, Any]],
    priority_rows: Sequence[Mapping[str, Any]],
    radar_events: Sequence[Mapping[str, Any]],
    near_miss_rows: Sequence[Mapping[str, Any]],
    pb_cont_facts: Sequence[Mapping[str, Any]] = (),
    pb_cont_details: Sequence[Mapping[str, Any]] = (),
    entity_type: str = "STOCK",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """在单一SERIALIZABLE事务中原子持久化当日PB/CONT四场景、D2生命周期与全部下游事实，阻断半成品暴露。"""
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _conn,
        cursor,
        _scope,
    ):
        pb_cont_report = _persist_d0_pb_continue_in_cursor(
            cursor,
            session_date=session_date,
            pb_cont_facts=pb_cont_facts,
            pb_cont_details=pb_cont_details,
        )
        d2_report = _persist_d2_day_bundle_in_cursor(
            cursor,
            session_date=session_date,
            calendar_id=calendar_id,
            rule_identity=rule_identity,
            episodes=episodes,
            day_states=day_states,
            primary_projections=primary_projections,
            peak_events=peak_events,
            entity_type=entity_type,
        )
        proj_report = _persist_downstream_projections_in_cursor(
            cursor,
            session_date=session_date,
            risk_rows=risk_rows,
            priority_rows=priority_rows,
            radar_events=radar_events,
            near_miss_rows=near_miss_rows,
        )
        d2_report["pb_continue"] = pb_cont_report
        return d2_report, proj_report


def read_stock_priority_page(
    config: Mapping[str, Any],
    *,
    session_date: date,
    status_filter: str | None = None,
    source_symbol: str | None = None,
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
        if source_symbol is not None:
            where.append("source_symbol = %s")
            params.append(source_symbol)
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
                   confirmed_reasons, possible_reasons,
                   damage_value, damage_quality, extension_value, extension_quality,
                   invalidated_value, invalidated_quality, shallow_breach_value, shallow_breach_quality
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


def read_d0_pb_continue_day_facts(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_symbol: str | None = None,
) -> dict[str, Any]:
    """只读读取指定交易日PB/CONTINUE四场景日事实、正式准入与多父/峰明细。"""
    with source_raw._transaction(config, write=False) as (_conn, cursor, _scope):
        where = ["target_session = %s"]
        params: list[Any] = [target_session]
        if source_symbol is not None:
            where.append("source_symbol = %s")
            params.append(source_symbol)
        where_sql = " AND ".join(where)

        scen_rows = cursor.execute(
            f"""
            SELECT target_session, source_symbol, scenario, scenario_state, scenario_reason,
                   predicate_ids, predicate_states, predicate_qualities, predicate_reason_codes,
                   predicate_value_numerators, predicate_value_denominators
            FROM stock.source_d0_scenario_day_facts
            WHERE {where_sql}
              AND scenario IN ('STRONG_PULLBACK', 'TREND_CONTINUE')
            ORDER BY source_symbol, scenario
            """,
            tuple(params),
        ).fetchall()
        scen_cols = [d[0] for d in cursor.description]

        adm_rows = cursor.execute(
            f"""
            SELECT target_session, source_symbol, scenario, raw_state, raw_reason,
                   admission_status, admission_reason, reason_codes, event_id,
                   prior_episode_id, linked_anchor_id,
                   failure_level_numerator, failure_level_denominator,
                   failure_coordinate, failure_source, failure_window_start,
                   failure_window_session_count, effective_from_session
            FROM stock.source_d0_formal_admission_day_facts
            WHERE {where_sql}
              AND scenario IN ('STRONG_PULLBACK', 'TREND_CONTINUE')
            ORDER BY source_symbol, scenario
            """,
            tuple(params),
        ).fetchall()
        adm_cols = [d[0] for d in cursor.description]
        adm_items = []
        for r in adm_rows:
            item = dict(zip(adm_cols, r))
            item["failure_level"] = _to_fraction(
                item["failure_level_numerator"], item["failure_level_denominator"]
            )
            adm_items.append(item)

        detail_rows = cursor.execute(
            f"""
            SELECT target_session, source_symbol, scenario, parent_episode_id,
                   peak_event_id, peak_ordinal_key, raw_state, raw_reason,
                   predicate_ids, predicate_states, predicate_qualities,
                   predicate_reason_codes, predicate_value_numerators, predicate_value_denominators,
                   admission_status, admission_reason, reason_codes,
                   event_id, linked_anchor_id,
                   failure_level_numerator, failure_level_denominator,
                   failure_coordinate, failure_source, failure_window_start,
                   failure_window_session_count, effective_from_session
            FROM stock.d0_parent_scenario_detail_facts
            WHERE {where_sql}
            ORDER BY source_symbol, scenario, parent_episode_id, peak_ordinal_key
            """,
            tuple(params),
        ).fetchall()
        detail_cols = [d[0] for d in cursor.description]
        detail_items = []
        for r in detail_rows:
            item = dict(zip(detail_cols, r))
            item["failure_level"] = _to_fraction(
                item["failure_level_numerator"], item["failure_level_denominator"]
            )
            detail_items.append(item)

        return {
            "session_date": target_session.isoformat(),
            "scenario_facts": [dict(zip(scen_cols, r)) for r in scen_rows],
            "admission_facts": adm_items,
            "parent_details": detail_items,
        }
