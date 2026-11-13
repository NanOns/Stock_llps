"""保存并读取精确来源范围的成交参与中位数事实。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any, Mapping

from stock_a.storage.postgres import source_raw


MARKET_PARTICIPATION_CONTRACT = "MARKET_PARTICIPATION_R1"
_DAY_TABLE = "source_market_participation_days"
_REASON_TABLE = "source_market_participation_unknown_reasons"
_PROFILE_COLUMNS = (
    "source_symbol",
    "market",
    "board",
    "source_capture_id",
    "source_time_quality",
    "source_available_at",
    "calculation_scope",
    "producer_contract",
    "producer_version",
    "price_coordinate",
    "pit_status",
    "profile_revision",
    "history_calendar_id",
    "history_calendar_revision",
    "history_window_start",
    "history_window_end",
    "history_requested_session_count",
    "history_stored_session_count",
    "history_records_complete",
    "history_records_status",
    "amount_unit_status",
    "amount_ratio20",
    "amount_ratio20_exact_numerator",
    "amount_ratio20_exact_denominator",
    "amount_ratio20_quality",
    "amount_ratio20_reason",
)
_DAY_COLUMNS = (
    "target_session",
    "source_package_id",
    "source_capture_id",
    "contract_version",
    "profile_revision",
    "producer_contract",
    "producer_version",
    "source_dataset_id",
    "source_time_quality",
    "source_available_at",
    "source_clock_role",
    "calendar_id",
    "calendar_revision",
    "profile_history_calendar_id",
    "profile_history_calendar_revision",
    "history_window_start",
    "history_window_end",
    "history_requested_session_count",
    "history_stored_session_count",
    "amount_unit_status",
    "price_coordinate",
    "pit_status",
    "source_pool_count",
    "profile_count",
    "assessable_count",
    "profile_unknown_count",
    "profile_missing_count",
    "median_numerator",
    "median_denominator",
    "median_value",
    "classification",
    "status",
)
_KEY_COLUMNS = (
    "target_session",
    "source_package_id",
    "source_capture_id",
    "contract_version",
    "profile_revision",
    "producer_version",
)
_REASON_COLUMNS = ("participation_day_id", "reason_code", "unknown_count")


class MarketParticipationPgError(Exception):
    """成交参与事实读取或保存未闭合。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        self.database_write_attempted = False
        super().__init__(message)


def read_market_participation_profiles(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
) -> list[dict[str, Any]]:
    """离线生产时一次性读取指定日期、来源包和捕获的已存Profile。"""
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(source_capture_id) is not int
        or source_capture_id <= 0
    ):
        raise MarketParticipationPgError(
            "MARKET_PARTICIPATION_INPUT_KEY_INVALID",
            "画像读取键必须是明确的日期、包和捕获ID",
        )
    columns = ", ".join(_PROFILE_COLUMNS)
    try:
        with source_raw._transaction(
            config, write=False, statement_timeout_ms=30000
        ) as (_, cursor, _):
            cursor.execute(
                f"""SELECT {columns}
FROM source_symbol_price_profiles
WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s
ORDER BY source_symbol""",
                (target_session, source_package_id, source_capture_id),
            )
            return [
                dict(zip(_PROFILE_COLUMNS, row, strict=True))
                for row in cursor.fetchall()
            ]
    except source_raw.RawSourcePgError:
        raise


def _clock(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _expected_day_values(result: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(result[name] for name in _DAY_COLUMNS)


def persist_market_participation(
    config: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    """在一个SERIALIZABLE事务内幂等保存父事实及UNKNOWN原因分解。"""
    day = dict(result)
    day["contract_version"] = MARKET_PARTICIPATION_CONTRACT
    day["source_available_at"] = _clock(day.get("source_available_at"))
    if day["median_value"] is None:
        day["median_value"] = None
    expected_day = _expected_day_values(day)
    key = tuple(day[name] for name in _KEY_COLUMNS)
    reason_counts = {
        str(reason): int(count)
        for reason, count in sorted(day["unknown_reason_counts"].items())
        if count
    }
    if any(not reason or count <= 0 for reason, count in reason_counts.items()):
        raise MarketParticipationPgError(
            "MARKET_PARTICIPATION_REASON_INVALID", "未知原因必须具有非空代码和正数计数"
        )
    if (
        day["source_pool_count"] != day["profile_count"] + day["profile_missing_count"]
        or day["profile_count"]
        != day["assessable_count"] + day["profile_unknown_count"]
        or sum(reason_counts.values())
        != day["profile_unknown_count"] + day["profile_missing_count"]
    ):
        raise MarketParticipationPgError(
            "MARKET_PARTICIPATION_DENOMINATOR_OPEN",
            "画像评估、UNKNOWN和来源池分母未闭合",
        )

    try:
        with source_raw._transaction(
            config, write=True, statement_timeout_ms=30000
        ) as (_, cursor, _):
            cursor.execute(
                f"""INSERT INTO {_DAY_TABLE} ({", ".join(_DAY_COLUMNS)})
VALUES ({", ".join(["%s"] * len(_DAY_COLUMNS))})
ON CONFLICT ({", ".join(_KEY_COLUMNS)}) DO NOTHING
RETURNING id""",
                expected_day,
            )
            inserted_id = cursor.fetchone()
            inserted = inserted_id is not None
            if inserted:
                participation_day_id = int(inserted_id[0])
                if reason_counts:
                    cursor.executemany(
                        f"""INSERT INTO {_REASON_TABLE} ({", ".join(_REASON_COLUMNS)})
VALUES (%s, %s, %s)""",
                        [
                            (participation_day_id, reason, count)
                            for reason, count in reason_counts.items()
                        ],
                    )
            else:
                cursor.execute(
                    f"""SELECT id, {", ".join(_DAY_COLUMNS)} FROM {_DAY_TABLE}
WHERE {" AND ".join(f"{name}=%s" for name in _KEY_COLUMNS)}""",
                    key,
                )
                existing = cursor.fetchall()
                if len(existing) != 1 or tuple(existing[0][1:]) != expected_day:
                    raise MarketParticipationPgError(
                        "MARKET_PARTICIPATION_CONTENT_CONFLICT",
                        "同一成交参与业务键已有不同内容；事务已回滚",
                    )
                participation_day_id = int(existing[0][0])
                cursor.execute(
                    f"""SELECT reason_code, unknown_count FROM {_REASON_TABLE}
WHERE participation_day_id=%s ORDER BY reason_code""",
                    (participation_day_id,),
                )
                existing_reasons = dict(cursor.fetchall())
                if existing_reasons != reason_counts:
                    raise MarketParticipationPgError(
                        "MARKET_PARTICIPATION_REASON_CONFLICT",
                        "同一成交参与业务键的UNKNOWN原因分解不一致；事务已回滚",
                    )

            cursor.execute(
                f"""SELECT {", ".join(_DAY_COLUMNS)} FROM {_DAY_TABLE}
WHERE id=%s""",
                (participation_day_id,),
            )
            persisted_day = cursor.fetchone()
            if persisted_day is None or tuple(persisted_day) != expected_day:
                raise MarketParticipationPgError(
                    "MARKET_PARTICIPATION_READBACK_MISMATCH",
                    "成交参与父事实事务内读回不一致",
                )
            cursor.execute(
                f"""SELECT reason_code, unknown_count FROM {_REASON_TABLE}
WHERE participation_day_id=%s ORDER BY reason_code""",
                (participation_day_id,),
            )
            persisted_reasons = dict(cursor.fetchall())
            if persisted_reasons != reason_counts:
                raise MarketParticipationPgError(
                    "MARKET_PARTICIPATION_REASON_READBACK_MISMATCH",
                    "成交参与UNKNOWN原因事务内读回不一致",
                )
    except MarketParticipationPgError as error:
        error.database_write_attempted = True
        raise
    except source_raw.RawSourcePgError as error:
        error.database_write_attempted = True
        raise
    return {
        "status": "WRITTEN" if inserted else "IDEMPOTENT_ALREADY_PRESENT",
        "target_session": day["target_session"].isoformat(),
        "source_package_id": day["source_package_id"],
        "source_capture_id": day["source_capture_id"],
        "profile_revision": day["profile_revision"],
        "profile_count": day["profile_count"],
        "assessable_count": day["assessable_count"],
        "unknown_count": day["profile_unknown_count"] + day["profile_missing_count"],
        "inserted_reason_rows": len(reason_counts) if inserted else 0,
        "transaction": "SERIALIZABLE_ATOMIC",
        "database_readback": "SAME_TRANSACTION_EXACT",
    }


def _history_calendar_covers_target(
    history_calendar_id: str,
    history_calendar_revision: str,
    *,
    calendar_id: str,
    calendar_revision: str,
    target_session: date,
) -> bool:
    if history_calendar_id != calendar_id:
        return False
    if history_calendar_revision == calendar_revision:
        return True
    components: dict[str, str] = {}
    for component in history_calendar_revision.split("+"):
        year, separator, revision = component.partition(":")
        if not separator or len(year) != 4 or year in components:
            return False
        components[year] = revision
    return components.get(str(target_session.year)) == calendar_revision


def _market_clock(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return f"NAIVE:{parsed.isoformat()}"
    return parsed.isoformat()


def read_market_participation(
    config: Mapping[str, Any],
    *,
    market: Any,
) -> dict[str, Any]:
    """只读指定宽度来源包、捕获与日历下唯一已保存成交参与结果。"""
    target_session = market.target_session
    package_id = market.cohort.source_package_id
    capture_id = market.cohort.source_capture_id
    try:
        with source_raw._transaction(
            config, write=False, statement_timeout_ms=10000
        ) as (_, cursor, _):
            cursor.execute(
                f"""SELECT id, {", ".join(_DAY_COLUMNS)} FROM {_DAY_TABLE}
WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s
  AND contract_version=%s ORDER BY profile_revision, producer_version""",
                (
                    target_session,
                    package_id,
                    capture_id,
                    MARKET_PARTICIPATION_CONTRACT,
                ),
            )
            rows = cursor.fetchall()
            if not rows:
                return {
                    "status": "NOT_COMPUTED",
                    "reason": "此精确日期、来源包和捕获尚无已保存成交参与事实。",
                }
            if len(rows) != 1:
                return {
                    "status": "INCOMPLETE",
                    "reason": "同一市场来源范围存在多个画像修订的成交参与结果。",
                }
            parent_id = int(rows[0][0])
            day = dict(zip(_DAY_COLUMNS, rows[0][1:], strict=True))
            cursor.execute(
                f"""SELECT reason_code, unknown_count FROM {_REASON_TABLE}
WHERE participation_day_id=%s ORDER BY reason_code""",
                (parent_id,),
            )
            reasons = dict(cursor.fetchall())
    except source_raw.RawSourcePgError:
        raise

    calculation = market.calculation
    cohort = market.cohort
    if (
        day["target_session"] != target_session
        or day["source_package_id"] != package_id
        or day["source_capture_id"] != capture_id
        or day["source_pool_count"] != calculation.n_source
        or day["source_dataset_id"] != cohort.source_dataset_id
        or day["source_time_quality"] != cohort.source_time_quality
        or _market_clock(day["source_available_at"])
        != _market_clock(cohort.source_available_at)
        or day["source_clock_role"] != cohort.source_clock_role
        or day["calendar_id"] != market.calendar["calendar_id"]
        or day["calendar_revision"] != market.calendar["calendar_revision"]
        or day["price_coordinate"] != "RAW_UNADJUSTED"
        or day["pit_status"] != "NON_PIT_RETROSPECTIVE"
        or not _history_calendar_covers_target(
            day["profile_history_calendar_id"],
            day["profile_history_calendar_revision"],
            calendar_id=market.calendar["calendar_id"],
            calendar_revision=market.calendar["calendar_revision"],
            target_session=target_session,
        )
    ):
        return {
            "status": "NOT_COMPARABLE",
            "reason": "已保存成交参与结果与当前宽度的来源捕获、日历或质量绑定不一致。",
        }

    source_count = int(day["source_pool_count"])
    profile_count = int(day["profile_count"])
    assessable_count = int(day["assessable_count"])
    ratio_unknown_count = int(day["profile_unknown_count"])
    profile_missing_count = int(day["profile_missing_count"])
    if (
        profile_count + profile_missing_count != source_count
        or assessable_count + ratio_unknown_count != profile_count
        or sum(int(count) for count in reasons.values())
        != ratio_unknown_count + profile_missing_count
        or any(int(count) <= 0 for count in reasons.values())
    ):
        return {
            "status": "INCOMPLETE",
            "reason": "成交参与画像、UNKNOWN原因或来源池分母未闭合。",
        }

    median: Fraction | None
    if day["median_numerator"] is None:
        median = None
    else:
        if day["median_denominator"] is None or int(day["median_denominator"]) <= 0:
            return {
                "status": "INCOMPLETE",
                "reason": "已保存成交参与中位数的精确分母无效。",
            }
        median = Fraction(int(day["median_numerator"]), int(day["median_denominator"]))
        if median < 0 or day["classification"] not in {
            "EXPANDING",
            "NORMAL",
            "THIN",
        }:
            return {
                "status": "INCOMPLETE",
                "reason": "已保存成交参与数值域或分类非法。",
            }
        if day["classification"] != _participation_class(median):
            return {
                "status": "INCOMPLETE",
                "reason": "已保存分类与精确中位数不一致。",
            }
        with localcontext() as context:
            context.prec = 60
            expected_value = Decimal(median.numerator) / Decimal(median.denominator)
        if day["median_value"] is None or abs(
            day["median_value"] - expected_value
        ) > Decimal("1e-22"):
            return {
                "status": "INCOMPLETE",
                "reason": "已保存中位数NUMERIC与精确有理数不一致。",
            }

    return {
        "status": day["status"],
        "contract_version": day["contract_version"],
        "target_session": target_session.isoformat(),
        "analysis_scope": "SOURCE_SYMBOL_COHORT_FIXED_AT_T",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": day["pit_status"],
        "price_coordinate": day["price_coordinate"],
        "source_cohort": {
            "source_package_id": package_id,
            "source_capture_id": capture_id,
            "source_dataset_id": day["source_dataset_id"],
            "source_time_quality": day["source_time_quality"],
            "source_available_at": (
                day["source_available_at"].isoformat()
                if day["source_available_at"] is not None
                else None
            ),
        },
        "metric": {
            "name": "amr20_median",
            "formula": "MEDIAN(当前原生金额/严格前20个actual原生金额均值)",
            "exact": None
            if median is None
            else {
                "numerator": median.numerator,
                "denominator": median.denominator,
                "fraction": str(median),
                "display_decimal_12dp": f"{Decimal(median.numerator) / Decimal(median.denominator):.12f}",
                "unit": "ratio",
            },
            "classification": day["classification"],
        },
        "denominator": {
            "source_pool_count": source_count,
            "profile_count": profile_count,
            "assessable_count": assessable_count,
            "profile_unknown_count": ratio_unknown_count,
            "profile_missing_count": profile_missing_count,
            "unknown_count": ratio_unknown_count + profile_missing_count,
            "assessable_coverage": {
                "numerator": assessable_count,
                "denominator": source_count,
                "fraction": str(Fraction(assessable_count, source_count))
                if source_count
                else None,
            },
            "unknown_reason_counts": reasons,
        },
        "profile": {
            "profile_revision": day["profile_revision"],
            "producer_contract": day["producer_contract"],
            "producer_version": day["producer_version"],
            "history_calendar_id": day["profile_history_calendar_id"],
            "history_calendar_revision": day["profile_history_calendar_revision"],
            "history_window_start": day["history_window_start"].isoformat(),
            "history_window_end": day["history_window_end"].isoformat(),
            "requested_sessions": day["history_requested_session_count"],
            "stored_sessions": day["history_stored_session_count"],
            "amount_unit_status": day["amount_unit_status"],
        },
    }


def _participation_class(value: Fraction) -> str:
    if value >= Fraction(6, 5):
        return "EXPANDING"
    if value < Fraction(4, 5):
        return "THIN"
    return "NORMAL"
