"""将真实P03 SEALED PG Raw窗口接入来源代码价格画像批处理。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from fractions import Fraction
from hashlib import sha256
import json
from typing import Any, Mapping

from stock_a import __version__
from stock_a.application.pg_research import (
    PgResearchBindingError,
    read_sealed_source_context,
)
from stock_a.application.research_window import resolve_pg_history_window
from stock_a.research.entry import (
    ResearchEntryError,
    research_universe_pg_history_preview,
)
from stock_a.storage.postgres.price_profiles import (
    PriceProfilePgError,
    list_profile_snapshots,
    persist_profile_records,
    read_profile_page,
)
from stock_a.storage.postgres.source_raw import (
    RawSourcePgError,
    read_raw_daily_history_page,
)


SOURCE_SYMBOL_PROFILE_SCOPE = "SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE"
_PRODUCER_CONTRACT = "STOCK_A_PG_SOURCE_SYMBOL_CORE_PROFILE_PREVIEW_R1"
_CORE_FIELDS = {
    "core.ma5": "ma5",
    "core.ma10": "ma10",
    "core.ma20": "ma20",
    "core.ma60": "ma60",
    "core.atr20": "atr20",
    "core.clv": "clv",
    "core.prior_high20": "prior_high20",
    "core.ret20": "ret20",
    "core.pos60": "pos60",
    "core.bias20_atr": "bias20_atr",
}
_HISTORY_HASH_FIELDS = (
    "session_date",
    "bar_status",
    "bar_reason",
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "price_quality",
    "price_reason",
    "raw_amount",
    "raw_amount_quality",
    "raw_amount_reason",
    "raw_volume",
    "raw_volume_quality",
    "raw_volume_reason",
    "source_content_id",
    "source_capture_id",
    "source_entry_sha256",
    "package_sha256",
)


class PgPriceProfileError(Exception):
    """来源代码级价格画像批处理未闭合。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _canonical(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def _history_window_sha256(rows: list[Mapping[str, Any]]) -> str:
    inputs = [
        {name: _canonical(row.get(name)) for name in _HISTORY_HASH_FIELDS}
        for row in rows
    ]
    payload = json.dumps(
        inputs,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return sha256(payload).hexdigest()


def _profile_revision(
    *,
    scope: str,
    contract_sha256: str,
    calendar_id: str,
    calendar_revision: str,
    session_dates: tuple[date, ...],
) -> str:
    identity = {
        "scope": scope,
        "producer_contract": _PRODUCER_CONTRACT,
        "producer_version": __version__,
        "producer_contract_sha256": contract_sha256,
        "price_coordinate": "RAW_UNADJUSTED",
        "window_contract": "TECH_ACTUAL_R1",
        "calendar_id": calendar_id,
        "calendar_revision": calendar_revision,
        "session_dates": [item.isoformat() for item in session_dates],
        "core_fields": sorted(_CORE_FIELDS),
        "profile_component_contract": "DAY_CHAIN_PROFILE_EXPRESSION_REGISTRY_R1",
    }
    return sha256(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _records_for_page(
    *,
    page: Mapping[str, Any],
    preview: Mapping[str, Any],
    target_session: date,
    package_sha256: str,
    profile_revision: str,
    producer_contract_sha256: str,
    source_context: Mapping[str, Any],
) -> list[dict[str, Any]]:
    candidates = {item["source_symbol"]: item for item in page.get("candidates", ())}
    records: list[dict[str, Any]] = []
    for result in preview.get("results", ()):
        symbol = result["symbol"]
        candidate = candidates.get(symbol)
        if candidate is None:
            raise PgPriceProfileError(
                "PROFILE_RESULT_SOURCE_MISMATCH", "Core/Profile结果无法绑定原始分页候选"
            )
        rows = candidate["history_rows"]
        target_row = next(
            (row for row in rows if row.get("session_date") == target_session), None
        )
        if target_row is None:
            raise PgPriceProfileError(
                "PROFILE_TARGET_RAW_MISSING", "来源分页没有精确目标日Raw行"
            )
        window = result.get("window") or {}
        core_fields = result.get("core_price_fields") or {}
        profile_components = sorted(
            result.get("profile_components") or (), key=lambda item: item["field_id"]
        )
        if len(profile_components) != 11:
            raise PgPriceProfileError(
                "PROFILE_COMPONENT_COUNT_INVALID", "现役Profile11分项没有完整返回"
            )
        source_time_quality = source_context.get("source_time_quality") or "UNKNOWN"
        source_available_at = source_context.get("source_available_at")
        if isinstance(source_available_at, str):
            source_available_at = datetime.fromisoformat(source_available_at)
        record: dict[str, Any] = {
            "namespace_id": page["namespace_id"],
            "source_set_id": source_context["source_set_id"],
            "source_set_revision": int(source_context["revision_id"]),
            "source_capture_id": str(target_row["source_capture_id"]),
            "source_symbol": symbol,
            "market": candidate["market"],
            "board": candidate["board"],
            "target_session": target_session,
            "package_sha256": package_sha256,
            "profile_revision": profile_revision,
            "calculation_scope": SOURCE_SYMBOL_PROFILE_SCOPE,
            "producer_contract": _PRODUCER_CONTRACT,
            "producer_version": __version__,
            "producer_contract_sha256": producer_contract_sha256,
            "price_coordinate": "RAW_UNADJUSTED",
            "identity_status": "SOURCE_SYMBOL_ONLY",
            "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
            "listing_status": "LISTING_STATUS_UNKNOWN",
            "suspension_status": "SUSPENSION_STATUS_UNKNOWN",
            "pit_status": "NON_PIT_RETROSPECTIVE",
            "source_time_quality": source_time_quality,
            "source_available_at": source_available_at,
            "history_calendar_id": window.get("calendar_id") or "",
            "history_calendar_revision": window.get("calendar_revision") or "",
            "history_window_start": date.fromisoformat(
                window.get("start_session") or page["session_start"]
            ),
            "history_window_end": date.fromisoformat(
                window.get("end_session") or page["session_end"]
            ),
            "history_requested_session_count": int(
                window.get("requested_sessions") or page["session_count"]
            ),
            "history_stored_session_count": int(
                window.get("stored_sessions", len(rows))
            ),
            "history_records_complete": bool(result["history_records_complete"]),
            "history_records_status": result["history_records_status"],
            "history_actual_price_bar_count": int(window.get("real_bar_count", 0)),
            "target_bar_status": result["target_bar_status"],
            "target_price_quality": target_row["price_quality"],
            "ma20_window_valid_bar_count": int(result["ma20_window_valid_bar_count"]),
            "ma20_window_expected_bar_count": int(
                result["ma20_window_expected_bar_count"]
            ),
            "atr20_window_valid_bar_count": int(result["price_window_valid_bar_count"]),
            "atr20_window_expected_bar_count": int(
                result["price_window_expected_bar_count"]
            ),
            "price_window_usable": bool(result["price_window_usable"]),
            "price_window_status": result["price_window_status"],
            "price_window_reason": result["price_window_reason"],
            "history_window_sha256": _history_window_sha256(rows),
            "amount_unit_status": "UNIT_UNVERIFIED",
            "volume_unit_status": "UNIT_UNVERIFIED",
        }
        for field_id, column in _CORE_FIELDS.items():
            field = core_fields.get(field_id)
            if not isinstance(field, Mapping):
                field = {
                    "value": None,
                    "quality": "UNKNOWN",
                    "reason": result.get("consumer_error_code") or "CORE_FIELD_OMITTED",
                }
            value = field.get("value")
            if isinstance(value, Mapping):
                decimal_value = value.get("decimal")
                exact_value = value.get("exact")
                if exact_value is not None:
                    try:
                        exact_fraction = Fraction(str(exact_value))
                    except (ValueError, ZeroDivisionError) as error:
                        raise PgPriceProfileError(
                            "PROFILE_CORE_VALUE_INVALID",
                            f"{field_id}的精确分数表示无效",
                        ) from error
                elif decimal_value is not None:
                    exact_fraction = Fraction(Decimal(str(decimal_value)))
                else:
                    exact_fraction = None
                value = decimal_value
            elif value is not None:
                exact_fraction = Fraction(Decimal(str(value)))
            else:
                exact_fraction = None
            if value is not None and not isinstance(value, (str, int, Decimal)):
                raise PgPriceProfileError(
                    "PROFILE_CORE_VALUE_INVALID",
                    f"{field_id}的Core数值表示不符合有界NUMERIC合同",
                )
            record[column] = Decimal(str(value)) if value is not None else None
            record[f"{column}_exact_numerator"] = (
                exact_fraction.numerator if exact_fraction is not None else None
            )
            record[f"{column}_exact_denominator"] = (
                exact_fraction.denominator if exact_fraction is not None else None
            )
            quality = field.get("quality") or "UNKNOWN"
            if quality == "UNKNOWN_DATA":
                quality = "UNKNOWN"
            record[f"{column}_quality"] = quality
            record[f"{column}_reason"] = field.get("reason")
        record["profile_field_ids"] = [item["field_id"] for item in profile_components]
        record["profile_statuses"] = [
            item.get("status") or "UNKNOWN" for item in profile_components
        ]
        record["profile_qualities"] = [
            item.get("quality") or item.get("status") or "UNKNOWN"
            for item in profile_components
        ]
        record["profile_reasons"] = [
            item.get("reason") or "" for item in profile_components
        ]
        records.append(record)
    return records


def run_source_symbol_price_profile(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_set_id: str,
    package_sha256: str,
    scope: str,
    session_count: int,
    page_size: int,
    profile_contract: dict[str, Any],
    profile_contract_sha256: str,
) -> dict[str, Any]:
    """按SQL分页读取已封口Raw，复用现役Core43/Profile11并保存typed摘要。"""
    if scope != SOURCE_SYMBOL_PROFILE_SCOPE:
        raise PgPriceProfileError(
            "PROFILE_SCOPE_INVALID",
            "画像计算范围必须显式为SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE",
        )
    if type(session_count) is not int or not 1 <= session_count <= 75:
        raise PgPriceProfileError(
            "PROFILE_WINDOW_COUNT_INVALID", "历史窗口会话数必须在1至75之间"
        )
    if type(page_size) is not int or not 1 <= page_size <= 100:
        raise PgPriceProfileError(
            "PROFILE_PAGE_SIZE_INVALID", "来源分页批大小必须在1至100之间"
        )
    foundation = profile_contract.get("scope_bindings", {}).get("FOUNDATION_EOD", {})
    field_count = sum(
        isinstance(item, dict)
        and isinstance(item.get("field_id"), str)
        and item["field_id"].startswith("core.")
        for item in foundation.get("field_registry", ())
    )
    profile_count = sum(
        isinstance(item, dict) and isinstance(item.get("field_id"), str)
        for item in foundation.get("profile_expression_registry", {}).values()
    )
    if (field_count, profile_count) != (43, 11):
        raise PgPriceProfileError(
            "PROFILE_CONTRACT_INVALID", "现役FOUNDATION合同不是Core43/Profile11"
        )
    try:
        resolved_target, session_dates, calendar_revision = resolve_pg_history_window(
            target_session.isoformat(), session_count
        )
        if resolved_target != target_session:
            raise PgPriceProfileError(
                "PROFILE_TARGET_NOT_TRADING_SESSION", "输入日期不是已登记官方交易会话"
            )
        source_context = read_sealed_source_context(
            config,
            target_session=target_session,
            package_sha256=package_sha256,
            namespace_id=None,
        )
        if source_context["source_set_id"] != source_set_id:
            raise PgPriceProfileError(
                "PROFILE_SOURCE_SET_ID_MISMATCH", "输入source_set_id与精确P04读回不一致"
            )
        offset = 0
        raw_total: int | None = None
        records: list[dict[str, Any]] = []
        consumer_failures = 0
        while True:
            page = read_raw_daily_history_page(
                config,
                target_session=target_session,
                package_sha256=package_sha256,
                sessions=session_dates,
                offset=offset,
                limit=page_size,
                market="ALL",
                board="ALL",
            )
            if raw_total is None:
                raw_total = int(page["total_count"])
                if (
                    page["version_status"] != "CLOSED"
                    or not page["partition_closed"]
                    or raw_total != source_context["raw_row_count"]
                    or raw_total != source_context["capture_expected_row_count"]
                ):
                    raise PgPriceProfileError(
                        "PROFILE_SOURCE_PARTITION_OPEN", "PG Raw或P03来源分母未闭合"
                    )
            if page["total_count"] != raw_total or page["version_status"] != "CLOSED":
                raise PgPriceProfileError(
                    "PROFILE_SOURCE_PARTITION_CHANGED", "分页期间PG Raw分母发生变化"
                )
            if not page["candidates"]:
                break
            preview = research_universe_pg_history_preview(
                target_session=target_session.isoformat(),
                package_sha256=package_sha256,
                session_dates=session_dates,
                page=page,
                profile_contract=profile_contract,
            )
            page_records = _records_for_page(
                page=page,
                preview=preview,
                target_session=target_session,
                package_sha256=package_sha256,
                profile_revision="0" * 64,
                producer_contract_sha256=profile_contract_sha256,
                source_context=source_context,
            )
            consumer_failures += sum(
                item.get("consumer_status") == "CONSUMER_FAILED"
                for item in preview.get("results", ())
            )
            records.extend(page_records)
            offset = int(page["next_offset"] or raw_total)
            if page["next_offset"] is None:
                break
        if (
            raw_total is None
            or len(records) != raw_total
            or len({item["source_symbol"] for item in records}) != raw_total
        ):
            raise PgPriceProfileError(
                "PROFILE_BATCH_DENOMINATOR_MISMATCH", "全来源代码画像未一对一闭合"
            )
        first_window = resolve_pg_history_window(
            target_session.isoformat(), session_count
        )[1]
        calendar_id = records[0]["history_calendar_id"] if records else ""
        if not calendar_id:
            raise PgPriceProfileError(
                "PROFILE_CALENDAR_ID_MISSING", "现役Core窗口未返回正式日历身份"
            )
        profile_revision = _profile_revision(
            scope=scope,
            contract_sha256=profile_contract_sha256,
            calendar_id=calendar_id,
            calendar_revision=calendar_revision,
            session_dates=tuple(first_window),
        )
        for record in records:
            record["profile_revision"] = profile_revision
        result = persist_profile_records(config, records)
        result.update(
            {
                "status": "COMPLETE_WITH_SYMBOL_FAILURES"
                if consumer_failures
                else result["status"],
                "source_candidate_count": raw_total,
                "consumer_failure_count": consumer_failures,
                "source_status_counts": page.get("bar_status_counts"),
                "scope": scope,
                "session_count": session_count,
                "session_start": session_dates[0].isoformat(),
                "session_end": session_dates[-1].isoformat(),
                "calendar_revision": calendar_revision,
                "source_context": source_context,
                "source_zip_opened": False,
                "baostock_called": False,
                "formal_universe_status": "UNKNOWN_NOT_PROVED_BY_SOURCE_SYMBOLS",
                "pit_status": "NON_PIT_RETROSPECTIVE",
            }
        )
        return result
    except (
        RawSourcePgError,
        PgResearchBindingError,
        PriceProfilePgError,
        ResearchEntryError,
    ) as error:
        code = getattr(error, "code", "PG_PRICE_PROFILE_FAILED")
        raise PgPriceProfileError(code, str(error)) from error


def source_symbol_price_profile_snapshots(config: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return list_profile_snapshots(config)
    except (RawSourcePgError, PriceProfilePgError) as error:
        raise PgPriceProfileError(
            getattr(error, "code", "PROFILE_READ_FAILED"), str(error)
        ) from error


def query_source_symbol_price_profiles(
    config: Mapping[str, Any],
    **kwargs: Any,
) -> dict[str, Any]:
    try:
        return read_profile_page(config, **kwargs)
    except (RawSourcePgError, PriceProfilePgError) as error:
        raise PgPriceProfileError(
            getattr(error, "code", "PROFILE_READ_FAILED"), str(error)
        ) from error
