"""全市场10项横截面因子（无损精确Fraction + 日级共享10端点依据）PG 持久化与只读查询（ALG-09/10/11）。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any

from stock_a.domain.types import FieldValue, exact_number, unknown
from stock_a.research.cross_section import CROSS_SECTION_CONTRACT_VERSION
from stock_a.storage.postgres.source_raw import RawSourcePgError, _transaction

_FIELD_DB_COLS = (
    ("cross.rps5", "rps5"),
    ("cross.rps20", "rps20"),
    ("cross.rps5_delta1_pp", "rps5_delta1_pp"),
    ("cross.rps5_delta3_pp", "rps5_delta3_pp"),
    ("cross.rps20_delta1_pp", "rps20_delta1_pp"),
    ("cross.rps20_delta3_pp", "rps20_delta3_pp"),
    ("cross.rel_market_1", "rel_market_1"),
    ("cross.rel_market_3", "rel_market_3"),
    ("cross.rel_market_5", "rel_market_5"),
    ("cross.rel_market_20", "rel_market_20"),
)

_ENDPOINT_SPECS = (
    ("rps5_current", "RANK_RPS", 5, 0),
    ("rps5_minus1", "RANK_RPS", 5, 1),
    ("rps5_minus3", "RANK_RPS", 5, 3),
    ("rps20_current", "RANK_RPS", 20, 0),
    ("rps20_minus1", "RANK_RPS", 20, 1),
    ("rps20_minus3", "RANK_RPS", 20, 3),
    ("rel_market_1", "RELATIVE_MARKET", 1, 0),
    ("rel_market_3", "RELATIVE_MARKET", 3, 0),
    ("rel_market_5", "RELATIVE_MARKET", 5, 0),
    ("rel_market_20", "RELATIVE_MARKET", 20, 0),
)


def _to_db_numeric(
    fv: FieldValue, *, scale: int
) -> tuple[Decimal | None, str | None, str, str | None]:
    """同时生成展示/排序用定长 Decimal（带零边界保号）与无损精确有理分数文本。"""
    if fv.quality != "OBSERVED" or fv.value is None:
        return None, None, "UNKNOWN_DATA", fv.reason or "UNKNOWN_DATA"
    frac = exact_number(fv.value)
    exact_text = (
        f"{frac.numerator}/{frac.denominator}"
        if frac.denominator != 1
        else str(frac.numerator)
    )
    quantum = Decimal(10) ** (-scale)
    dec = (Decimal(frac.numerator) / Decimal(frac.denominator)).quantize(quantum)
    exact_sign = 1 if frac > 0 else (-1 if frac < 0 else 0)
    if exact_sign != 0 and dec == 0:
        dec = quantum if exact_sign > 0 else -quantum
    return dec, exact_text, "OBSERVED", None


def _build_endpoint_rows(
    *,
    target_session: date,
    price_capture_id: int,
    universe_capture_id: int | None,
    universe_version: str,
    calendar_id: str,
    calendar_revision: str,
    contract_version: str,
    rank_meta: Mapping[str, Any],
    rel_meta: Mapping[str, Any],
) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for ep_key, ep_kind, period, offset in _ENDPOINT_SPECS:
        info = (
            rank_meta.get(ep_key, {})
            if ep_kind == "RANK_RPS"
            else rel_meta.get(ep_key, {})
        )
        ep_date = info.get("endpoint_date")
        start_date = (
            info.get("return_start_date")
            if ep_kind == "RANK_RPS"
            else info.get("start_session")
        )
        u_id = info.get("universe_id")
        u_rev = info.get("universe_revision")
        denom = int(info.get("denominator") or 0)
        known = int(info.get("known_count") or 0)
        unk = int(
            info.get("unknown_count")
            if info.get("unknown_count") is not None
            else max(0, denom - known)
        )
        avail = (
            ep_date is not None
            and start_date is not None
            and isinstance(u_id, str)
            and bool(u_id)
            and isinstance(u_rev, str)
            and bool(u_rev)
        )
        rows.append(
            (
                target_session,
                price_capture_id,
                universe_capture_id,
                universe_version,
                calendar_id,
                calendar_revision,
                contract_version,
                ep_key,
                ep_kind,
                period,
                offset,
                "AVAILABLE" if avail else "UNAVAILABLE",
                ep_date if avail else None,
                start_date if avail else None,
                u_id if avail else None,
                u_rev if avail else None,
                denom,
                known,
                unk,
            )
        )
    return rows


def persist_cross_section_day_facts(
    config: Mapping[str, Any],
    *,
    batch_result: Mapping[str, Any],
    price_capture_id: int,
    universe_capture_id: int | None,
    universe_version: str,
    calendar_id: str,
    calendar_revision: str,
    pit_status: str = "NON_PIT_RETROSPECTIVE",
) -> dict[str, Any]:
    """在单事务内幂等持久化目标日全市场10项无损横截面因子与日级共享10端点依据；同键异内容拒绝回滚。"""
    target_session: date = batch_result["target_date"]
    contract_version = str(
        batch_result.get("contract_version") or CROSS_SECTION_CONTRACT_VERSION
    )
    by_entity: Mapping[str, Mapping[str, FieldValue]] = batch_result["by_entity"]
    rank_meta = batch_result.get("rank_metadata") or {}
    rel_meta = batch_result["relative_metadata"]

    rows_to_insert: list[tuple[Any, ...]] = []
    for sym in sorted(by_entity):
        fields = by_entity[sym]
        vals: list[Any] = [
            target_session,
            sym,
            price_capture_id,
            universe_capture_id,
            universe_version,
            "FORWARD_ADJUSTED",
            calendar_id,
            calendar_revision,
            contract_version,
            pit_status,
        ]
        exact_vals: list[str | None] = []
        for fid, col in _FIELD_DB_COLS:
            scale = 12 if col.startswith("rel_market_") else 10
            num, exact_text, qual, reason = _to_db_numeric(fields[fid], scale=scale)
            vals.extend((num, qual, reason))
            exact_vals.append(exact_text)
        vals.extend(
            (
                int(batch_result["target_universe_count"]),
                int(batch_result["rps5_known_count"]),
                int(batch_result["rps20_known_count"]),
                rel_meta["rel_market_1"]["start_session"],
                int(rel_meta["rel_market_1"]["denominator"]),
                int(rel_meta["rel_market_1"]["known_count"]),
                rel_meta["rel_market_3"]["start_session"],
                int(rel_meta["rel_market_3"]["denominator"]),
                int(rel_meta["rel_market_3"]["known_count"]),
                rel_meta["rel_market_5"]["start_session"],
                int(rel_meta["rel_market_5"]["denominator"]),
                int(rel_meta["rel_market_5"]["known_count"]),
                rel_meta["rel_market_20"]["start_session"],
                int(rel_meta["rel_market_20"]["denominator"]),
                int(rel_meta["rel_market_20"]["known_count"]),
                *exact_vals,
            )
        )
        rows_to_insert.append(tuple(vals))

    endpoint_rows = _build_endpoint_rows(
        target_session=target_session,
        price_capture_id=price_capture_id,
        universe_capture_id=universe_capture_id,
        universe_version=universe_version,
        calendar_id=calendar_id,
        calendar_revision=calendar_revision,
        contract_version=contract_version,
        rank_meta=rank_meta,
        rel_meta=rel_meta,
    )

    try:
        with _transaction(config, write=True, statement_timeout_ms=30000) as (
            _,
            cursor,
            _,
        ):
            cursor.execute(
                """SELECT source_symbol,
                          universe_capture_id, pit_status,
                          rps5, rps5_quality, rps5_reason,
                          rps20, rps20_quality, rps20_reason,
                          rps5_delta1_pp, rps5_delta1_pp_quality, rps5_delta1_pp_reason,
                          rps5_delta3_pp, rps5_delta3_pp_quality, rps5_delta3_pp_reason,
                          rps20_delta1_pp, rps20_delta1_pp_quality, rps20_delta1_pp_reason,
                          rps20_delta3_pp, rps20_delta3_pp_quality, rps20_delta3_pp_reason,
                          rel_market_1, rel_market_1_quality, rel_market_1_reason,
                          rel_market_3, rel_market_3_quality, rel_market_3_reason,
                          rel_market_5, rel_market_5_quality, rel_market_5_reason,
                          rel_market_20, rel_market_20_quality, rel_market_20_reason,
                          target_universe_count, rps5_known_count, rps20_known_count,
                          rel_market_1_start_session, rel_market_1_denominator, rel_market_1_known_count,
                          rel_market_3_start_session, rel_market_3_denominator, rel_market_3_known_count,
                          rel_market_5_start_session, rel_market_5_denominator, rel_market_5_known_count,
                          rel_market_20_start_session, rel_market_20_denominator, rel_market_20_known_count,
                          rps5_exact, rps20_exact, rps5_delta1_pp_exact, rps5_delta3_pp_exact,
                          rps20_delta1_pp_exact, rps20_delta3_pp_exact,
                          rel_market_1_exact, rel_market_3_exact, rel_market_5_exact, rel_market_20_exact
                   FROM stock.source_cross_section_day_facts
                   WHERE target_session = %s
                     AND price_capture_id = %s
                     AND universe_version = %s
                     AND calendar_revision = %s
                     AND contract_version = %s""",
                (
                    target_session,
                    price_capture_id,
                    universe_version,
                    calendar_revision,
                    contract_version,
                ),
            )
            existing = {r[0]: r for r in cursor.fetchall()}

            cursor.execute(
                """SELECT endpoint_key, universe_capture_id, calendar_id,
                          endpoint_kind, return_period, offset_sessions,
                          endpoint_status, endpoint_date, return_start_date,
                          universe_id, universe_revision,
                          denominator_count, known_count, unknown_count
                   FROM stock.source_cross_section_endpoints
                   WHERE target_session = %s
                     AND price_capture_id = %s
                     AND universe_version = %s
                     AND calendar_revision = %s
                     AND contract_version = %s""",
                (
                    target_session,
                    price_capture_id,
                    universe_version,
                    calendar_revision,
                    contract_version,
                ),
            )
            existing_eps = {r[0]: r for r in cursor.fetchall()}
            expected_eps = {
                r[7]: (
                    r[7],
                    r[2],
                    r[4],
                    r[8],
                    r[9],
                    r[10],
                    r[11],
                    r[12],
                    r[13],
                    r[14],
                    r[15],
                    r[16],
                    r[17],
                    r[18],
                )
                for r in endpoint_rows
            }

            if existing or existing_eps:
                expected = {
                    r[1]: (
                        r[1],
                        r[3],
                        r[9],
                        *r[10:65],
                    )
                    for r in rows_to_insert
                }
                if existing == expected and existing_eps == expected_eps:
                    return {
                        "status": "NO_CHANGE",
                        "target_session": target_session.isoformat(),
                        "row_count": len(rows_to_insert),
                        "endpoint_count": len(endpoint_rows),
                    }
                if existing_eps != expected_eps and existing == expected:
                    raise ValueError(
                        "CROSS_SECTION_ENDPOINTS_SAME_KEY_DIFFERENT_CONTENT"
                    )
                raise ValueError("CROSS_SECTION_SAME_KEY_DIFFERENT_CONTENT")

            placeholders = ", ".join(["%s"] * 65)
            cursor.executemany(
                f"""INSERT INTO stock.source_cross_section_day_facts (
                        target_session, source_symbol, price_capture_id, universe_capture_id,
                        universe_version, price_coordinate, calendar_id, calendar_revision,
                        contract_version, pit_status,
                        rps5, rps5_quality, rps5_reason,
                        rps20, rps20_quality, rps20_reason,
                        rps5_delta1_pp, rps5_delta1_pp_quality, rps5_delta1_pp_reason,
                        rps5_delta3_pp, rps5_delta3_pp_quality, rps5_delta3_pp_reason,
                        rps20_delta1_pp, rps20_delta1_pp_quality, rps20_delta1_pp_reason,
                        rps20_delta3_pp, rps20_delta3_pp_quality, rps20_delta3_pp_reason,
                        rel_market_1, rel_market_1_quality, rel_market_1_reason,
                        rel_market_3, rel_market_3_quality, rel_market_3_reason,
                        rel_market_5, rel_market_5_quality, rel_market_5_reason,
                        rel_market_20, rel_market_20_quality, rel_market_20_reason,
                        target_universe_count, rps5_known_count, rps20_known_count,
                        rel_market_1_start_session, rel_market_1_denominator, rel_market_1_known_count,
                        rel_market_3_start_session, rel_market_3_denominator, rel_market_3_known_count,
                        rel_market_5_start_session, rel_market_5_denominator, rel_market_5_known_count,
                        rel_market_20_start_session, rel_market_20_denominator, rel_market_20_known_count,
                        rps5_exact, rps20_exact, rps5_delta1_pp_exact, rps5_delta3_pp_exact,
                        rps20_delta1_pp_exact, rps20_delta3_pp_exact,
                        rel_market_1_exact, rel_market_3_exact, rel_market_5_exact, rel_market_20_exact
                    ) VALUES ({placeholders})""",
                rows_to_insert,
            )

            ep_placeholders = ", ".join(["%s"] * 19)
            cursor.executemany(
                f"""INSERT INTO stock.source_cross_section_endpoints (
                        target_session, price_capture_id, universe_capture_id,
                        universe_version, calendar_id, calendar_revision,
                        contract_version, endpoint_key, endpoint_kind,
                        return_period, offset_sessions, endpoint_status,
                        endpoint_date, return_start_date,
                        universe_id, universe_revision,
                        denominator_count, known_count, unknown_count
                    ) VALUES ({ep_placeholders})""",
                endpoint_rows,
            )
    except RawSourcePgError as exc:
        if isinstance(exc.__cause__, ValueError):
            raise exc.__cause__ from None
        raise

    return {
        "status": "PERSISTED",
        "target_session": target_session.isoformat(),
        "row_count": len(rows_to_insert),
        "endpoint_count": len(endpoint_rows),
    }


def read_cross_section_day_bundle(
    config: Mapping[str, Any],
    *,
    target_session: date,
    price_capture_id: int,
    universe_version: str,
    calendar_revision: str,
    contract_version: str = CROSS_SECTION_CONTRACT_VERSION,
) -> dict[str, Any]:
    """只读加载目标日全市场10项无损精确Fraction因子与日级共享10端点真实元数据。"""
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT source_symbol,
                      rps5, rps5_exact, rps5_quality, rps5_reason,
                      rps20, rps20_exact, rps20_quality, rps20_reason,
                      rps5_delta1_pp, rps5_delta1_pp_exact, rps5_delta1_pp_quality, rps5_delta1_pp_reason,
                      rps5_delta3_pp, rps5_delta3_pp_exact, rps5_delta3_pp_quality, rps5_delta3_pp_reason,
                      rps20_delta1_pp, rps20_delta1_pp_exact, rps20_delta1_pp_quality, rps20_delta1_pp_reason,
                      rps20_delta3_pp, rps20_delta3_pp_exact, rps20_delta3_pp_quality, rps20_delta3_pp_reason,
                      rel_market_1, rel_market_1_exact, rel_market_1_quality, rel_market_1_reason,
                      rel_market_3, rel_market_3_exact, rel_market_3_quality, rel_market_3_reason,
                      rel_market_5, rel_market_5_exact, rel_market_5_quality, rel_market_5_reason,
                      rel_market_20, rel_market_20_exact, rel_market_20_quality, rel_market_20_reason
               FROM stock.source_cross_section_day_facts
               WHERE target_session = %s
                 AND price_capture_id = %s
                 AND universe_version = %s
                 AND calendar_revision = %s
                 AND contract_version = %s
               ORDER BY source_symbol""",
            (
                target_session,
                price_capture_id,
                universe_version,
                calendar_revision,
                contract_version,
            ),
        )
        rows = cursor.fetchall()

        cursor.execute(
            """SELECT endpoint_key, endpoint_kind, return_period, offset_sessions,
                      endpoint_status, endpoint_date, return_start_date,
                      universe_id, universe_revision,
                      denominator_count, known_count, unknown_count,
                      calendar_id, universe_capture_id
               FROM stock.source_cross_section_endpoints
               WHERE target_session = %s
                 AND price_capture_id = %s
                 AND universe_version = %s
                 AND calendar_revision = %s
                 AND contract_version = %s
               ORDER BY endpoint_key""",
            (
                target_session,
                price_capture_id,
                universe_version,
                calendar_revision,
                contract_version,
            ),
        )
        ep_rows = cursor.fetchall()

    by_entity: dict[str, dict[str, FieldValue]] = {}
    for row in rows:
        sym = str(row[0])
        fields: dict[str, FieldValue] = {}
        for idx, (fid, _) in enumerate(_FIELD_DB_COLS):
            base = 1 + idx * 4
            num_val, exact_val, qual, reason = (
                row[base],
                row[base + 1],
                row[base + 2],
                row[base + 3],
            )
            if qual == "OBSERVED" and (exact_val is not None or num_val is not None):
                frac = (
                    Fraction(str(exact_val))
                    if exact_val is not None
                    else Fraction(num_val)
                )
                fields[fid] = FieldValue(frac)
            else:
                fields[fid] = unknown(str(reason or "UNKNOWN_DATA"))
        by_entity[sym] = fields

    rank_meta: dict[str, dict[str, Any]] = {}
    rel_meta: dict[str, dict[str, Any]] = {}
    calendar_id_val: str | None = None
    universe_capture_id_val: int | None = None
    for er in ep_rows:
        (
            ep_key,
            ep_kind,
            period,
            offset,
            ep_status,
            ep_date,
            start_d,
            u_id,
            u_rev,
            denom,
            known,
            unk,
            cal_id,
            u_cap,
        ) = er
        calendar_id_val = str(cal_id)
        universe_capture_id_val = int(u_cap) if u_cap is not None else None
        payload = {
            "endpoint_status": str(ep_status),
            "endpoint_date": ep_date,
            "return_start_date": start_d,
            "start_session": start_d,
            "universe_id": u_id,
            "universe_revision": u_rev,
            "denominator": int(denom),
            "known_count": int(known),
            "unknown_count": int(unk),
            "return_period": int(period),
            "offset_sessions": int(offset),
        }
        if ep_kind == "RANK_RPS":
            rank_meta[str(ep_key)] = payload
        else:
            rel_meta[str(ep_key)] = payload

    return {
        "status": "AVAILABLE" if by_entity else "NOT_COMPUTED",
        "target_session": target_session,
        "price_capture_id": price_capture_id,
        "universe_capture_id": universe_capture_id_val,
        "universe_version": universe_version,
        "calendar_id": calendar_id_val,
        "calendar_revision": calendar_revision,
        "contract_version": contract_version,
        "rank_metadata": rank_meta,
        "relative_metadata": rel_meta,
        "by_entity": by_entity,
    }


def read_cross_section_day_facts(
    config: Mapping[str, Any],
    *,
    target_session: date,
    price_capture_id: int,
    universe_version: str,
    calendar_revision: str,
    contract_version: str = CROSS_SECTION_CONTRACT_VERSION,
) -> dict[str, dict[str, FieldValue]]:
    """只读加载目标日全市场10项无损精确Fraction横截面因子。"""
    bundle = read_cross_section_day_bundle(
        config,
        target_session=target_session,
        price_capture_id=price_capture_id,
        universe_version=universe_version,
        calendar_revision=calendar_revision,
        contract_version=contract_version,
    )
    return bundle["by_entity"]
