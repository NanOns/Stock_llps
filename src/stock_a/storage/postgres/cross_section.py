"""全市场10项横截面因子 PG 持久化与只读查询（ALG-09/10/11）。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any

from stock_a.domain.types import FieldValue, unknown
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


def _to_db_numeric(
    fv: FieldValue, *, scale: int
) -> tuple[Decimal | None, str, str | None]:
    if fv.quality != "OBSERVED" or fv.value is None:
        return None, "UNKNOWN_DATA", fv.reason or "UNKNOWN_DATA"
    val = fv.value
    if isinstance(val, Fraction):
        dec = (Decimal(val.numerator) / Decimal(val.denominator)).quantize(
            Decimal(10) ** (-scale)
        )
    elif isinstance(val, Decimal):
        dec = val.quantize(Decimal(10) ** (-scale))
    else:
        dec = Decimal(str(val)).quantize(Decimal(10) ** (-scale))
    return dec, "OBSERVED", None


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
    """在单事务内幂等持久化目标日全市场10项横截面因子；同键异内容拒绝回滚。"""
    target_session: date = batch_result["target_date"]
    contract_version = str(
        batch_result.get("contract_version") or CROSS_SECTION_CONTRACT_VERSION
    )
    by_entity: Mapping[str, Mapping[str, FieldValue]] = batch_result["by_entity"]
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
        for fid, col in _FIELD_DB_COLS:
            scale = 12 if col.startswith("rel_market_") else 10
            num, qual, reason = _to_db_numeric(fields[fid], scale=scale)
            vals.extend((num, qual, reason))
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
            )
        )
        rows_to_insert.append(tuple(vals))

    try:
        with _transaction(config, write=True, statement_timeout_ms=30000) as (
            _,
            cursor,
            _,
        ):
            cursor.execute(
                """SELECT source_symbol,
                          rps5, rps5_quality, rps20, rps20_quality,
                          rps5_delta1_pp, rps5_delta1_pp_quality,
                          rps5_delta3_pp, rps5_delta3_pp_quality,
                          rps20_delta1_pp, rps20_delta1_pp_quality,
                          rps20_delta3_pp, rps20_delta3_pp_quality,
                          rel_market_1, rel_market_1_quality,
                          rel_market_3, rel_market_3_quality,
                          rel_market_5, rel_market_5_quality,
                          rel_market_20, rel_market_20_quality
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
            if existing:
                expected = {
                    r[1]: (
                        r[1],
                        r[10],
                        r[11],
                        r[13],
                        r[14],
                        r[16],
                        r[17],
                        r[19],
                        r[20],
                        r[22],
                        r[23],
                        r[25],
                        r[26],
                        r[28],
                        r[29],
                        r[31],
                        r[32],
                        r[34],
                        r[35],
                        r[37],
                        r[38],
                    )
                    for r in rows_to_insert
                }
                if existing == expected:
                    return {
                        "status": "NO_CHANGE",
                        "target_session": target_session.isoformat(),
                        "row_count": len(rows_to_insert),
                    }
                raise ValueError("CROSS_SECTION_SAME_KEY_DIFFERENT_CONTENT")

            placeholders = ", ".join(["%s"] * 55)
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
                        rel_market_20_start_session, rel_market_20_denominator, rel_market_20_known_count
                    ) VALUES ({placeholders})""",
                rows_to_insert,
            )
    except RawSourcePgError as exc:
        if isinstance(exc.__cause__, ValueError):
            raise exc.__cause__ from None
        raise

    return {
        "status": "PERSISTED",
        "target_session": target_session.isoformat(),
        "row_count": len(rows_to_insert),
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
    """只读加载目标日全市场10项横截面因子（供二阶 Profile.relative 与 D1 IMPULSE 消费）。"""
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT source_symbol,
                      rps5, rps5_quality, rps5_reason,
                      rps20, rps20_quality, rps20_reason,
                      rps5_delta1_pp, rps5_delta1_pp_quality, rps5_delta1_pp_reason,
                      rps5_delta3_pp, rps5_delta3_pp_quality, rps5_delta3_pp_reason,
                      rps20_delta1_pp, rps20_delta1_pp_quality, rps20_delta1_pp_reason,
                      rps20_delta3_pp, rps20_delta3_pp_quality, rps20_delta3_pp_reason,
                      rel_market_1, rel_market_1_quality, rel_market_1_reason,
                      rel_market_3, rel_market_3_quality, rel_market_3_reason,
                      rel_market_5, rel_market_5_quality, rel_market_5_reason,
                      rel_market_20, rel_market_20_quality, rel_market_20_reason
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
        rows = cursor.fetchall()

    out: dict[str, dict[str, FieldValue]] = {}
    for row in rows:
        sym = row[0]
        fields: dict[str, FieldValue] = {}
        idx = 1
        for fid, _col in _FIELD_DB_COLS:
            val, qual, reason = row[idx], row[idx + 1], row[idx + 2]
            idx += 3
            if qual == "OBSERVED" and val is not None:
                fields[fid] = FieldValue(Fraction(val))
            else:
                fields[fid] = unknown(reason or "UNKNOWN_DATA")
        out[sym] = fields
    return out
