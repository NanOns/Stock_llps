"""Stock A研究single_stock职责模块。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import time
from typing import Any, Iterable, Mapping, Sequence
from uuid import uuid4
from stock_a.canonical.selection import CanonicalReadSelection, build_tdx_read_selection
from stock_a.domain import ContractError, FieldValue, exact_number, unknown
from stock_a.research.core import (
    CORE_MAX_ACTUAL_LOOKBACK,
    CoreObservation,
    MarketSessionCalendar,
    calculate_core,
)
from stock_a.sources.baostock.identity import (
    IdentityLifecycleProjection,
    reconcile_identity_bar_status,
)
from stock_a.sources.tdx.day import DAY_STRUCT, DecodedDay, UnitContract
from stock_a.sources.tdx.capture import TdxCaptureRequest, build_tdx_capture_envelope
from stock_a.sources.tdx.day_source import open_day_file
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits
from stock_a.sources.tdx.vipdata import VipdataEntryCapture, VipdataTargetQualityReport

from stock_a.research.entry_core.fields import (
    _apply_calendar_coverage_to_core,
    _basic_fact_dimensions,
    _core_history_from_selection,
    _observation_summary,
    _render_canonical_selection,
    _render_field,
    _render_raw,
    _unit_trust_status,
)
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_core.profile_preview import (
    _build_profile_preview,
    _source_path,
)
from stock_a.research.entry_context.sessions import _load_calendar, _parse_target
from stock_a.research.entry_context.source_discovery import (
    _availability_record,
    _normalize_symbol,
)
from stock_a.research.entry_core.fields import _RAW_FIELDS
from stock_a.research.entry_context.sessions import _CALENDAR_EVIDENCE_VERIFIED


def apply_identity_observation(
    result: dict[str, Any],
    *,
    identity_projection: IdentityLifecycleProjection | None,
    identity_query_status: str,
) -> dict[str, Any]:
    """将一次Bao观察附加到已完成的TDX/Core结果，不触发重读或重算。"""
    if (
        not isinstance(result, dict)
        or not isinstance(identity_query_status, str)
        or not identity_query_status
    ):
        raise ResearchEntryError(
            "IDENTITY_QUERY_STATUS_INVALID", "Bao身份结果或来源状态无效"
        )
    if identity_projection is not None and (
        identity_projection.source_symbol != result.get("requested_symbol")
        or identity_projection.target_session.isoformat()
        != result.get("target_session")
    ):
        raise ResearchEntryError(
            "IDENTITY_PROJECTION_MISMATCH", "BaoStock身份投影与已计算TDX/Core结果不匹配"
        )
    if identity_projection is None:
        result.setdefault("identity", {}).update(
            {
                "status": "UNAVAILABLE",
                "reason": f"BAOSTOCK_IDENTITY_SOURCE_{identity_query_status}",
            }
        )
    else:
        result["identity"] = {
            **identity_projection.as_dict(),
            "security_id": None,
            "stable_security_id_issued": False,
        }
    selection = result.get("canonical_selection")
    target_bar_status = (
        selection.get("target_bar_status") if isinstance(selection, dict) else None
    )
    if target_bar_status not in {
        "PRESENT",
        "TARGET_MISSING",
        "READ_FAILED",
        "NOT_EVALUATED",
    }:
        target_bar_status = (
            "PRESENT" if result.get("target_bar_present") is True else "TARGET_MISSING"
        )
    result["identity_reconciliation"] = reconcile_identity_bar_status(
        identity_projection,
        target_bar_status=target_bar_status,
        source_status="RESPONSE_RECEIVED"
        if identity_projection is not None
        else identity_query_status,
    )
    return result


def research_stock(
    *,
    symbol: str,
    target_session: str,
    source_roots: Iterable[str],
    units: UnitContract,
    limits: ReadLimits,
    profile_contract: dict[str, Any] | None = None,
    include_baostock_evidence: bool = False,
    vipdata_capture: VipdataEntryCapture | None = None,
    identity_projection: IdentityLifecycleProjection | None = None,
    identity_query_status: str = "NOT_REQUESTED",
    timing_sink: dict[str, float] | None = None,
) -> dict[str, Any]:
    """验证目标会话后保留可读Raw；仅在可用前态上调用现有Core。"""
    target = _parse_target(target_session)
    calendar_payload, calendar_metadata, calendar_revision, calendar_sessions = (
        _load_calendar(target)
    )
    coverage_context = calendar_metadata[-1]["coverage_context"]
    calendar_years = {item["year"] for item in calendar_metadata}
    session_set = set(calendar_sessions)
    normalized, market_directory, board = _normalize_symbol(symbol)
    if identity_projection is not None and (
        not isinstance(identity_projection, IdentityLifecycleProjection)
        or identity_projection.source_symbol != normalized
        or identity_projection.target_session != target
    ):
        raise ResearchEntryError(
            "IDENTITY_PROJECTION_MISMATCH", "BaoStock身份投影与研究代码或目标日期不匹配"
        )
    if not isinstance(identity_query_status, str) or not identity_query_status:
        raise ResearchEntryError(
            "IDENTITY_QUERY_STATUS_INVALID", "Bao身份查询状态必须为非空文本"
        )
    code = normalized.split(".")[1]
    selected: dict[date, DecodedDay] = {}
    future_source_rows_excluded = 0
    file_summary = None
    root: Path | None = None
    path: Path | None = None
    if vipdata_capture is not None:
        if (
            not isinstance(vipdata_capture, VipdataEntryCapture)
            or vipdata_capture.source_symbol != normalized
            or vipdata_capture.target_session != target
            or vipdata_capture.capture.source_package_entry is None
        ):
            raise ResearchEntryError(
                "VIPDATA_CAPTURE_MISMATCH", "官网ZIP来源捕获与请求代码或目标日不匹配"
            )
        scan_summary = vipdata_capture.scan_summary
        if not scan_summary.complete:
            raise ResearchEntryError(
                "VIPDATA_CAPTURE_INCOMPLETE", "官网ZIP条目尚未完成只读扫描"
            )
        for decoded in vipdata_capture.rows:
            day = decoded.session_date
            if day.year in calendar_years and day not in session_set:
                raise ResearchEntryError(
                    "SOURCE_SESSION_NOT_IN_CALENDAR",
                    f"TDX来源在已核验年度包含非交易会话记录：{day.isoformat()}",
                )
            if day > target:
                future_source_rows_excluded += 1
            elif day in session_set:
                selected[day] = decoded
        if future_source_rows_excluded != vipdata_capture.future_source_rows_excluded:
            raise ResearchEntryError(
                "VIPDATA_CAPTURE_MISMATCH", "来源捕获的未来行计数与读取行不一致"
            )
        capture_envelope = vipdata_capture.capture
    else:
        root, path = _source_path(source_roots, market_directory, code)
        try:
            with open_day_file(path, (root,), units, limits) as scan:
                for located in scan:
                    day = located.decoded.session_date
                    if day.year in calendar_years and day not in session_set:
                        raise ResearchEntryError(
                            "SOURCE_SESSION_NOT_IN_CALENDAR",
                            f"TDX来源在已核验年度包含非交易会话记录：{day.isoformat()}",
                            context={
                                "byte_count": scan.progress[0],
                                "record_count": scan.progress[1],
                            },
                        )
                    if day > target:
                        future_source_rows_excluded += 1
                    elif day in session_set:
                        selected[day] = located.decoded
                file_summary = scan.summary
        except DayReadError:
            raise
        except PermissionError as error:
            raise ResearchEntryError(
                "SOURCE_ACCESS_DENIED", "操作系统拒绝读取配置的TDX日线文件"
            ) from error
        except OSError as error:
            raise ResearchEntryError(
                "SOURCE_IO_ERROR", "只读TDX扫描遇到操作系统I/O错误"
            ) from error
        scan_summary = file_summary.scan

    session_slice = tuple(day for day in calendar_sessions if day <= target)
    if vipdata_capture is None:
        capture_request = TdxCaptureRequest(
            capture_id=str(uuid4()),
            dataset_id="TDX_DAILY_BARS",
            dataset_version="TDX_DAY_RECORD_32B_CANDIDATE",
            query_scope={
                "source_symbol": normalized,
                "target_session": target.isoformat(),
                "selection_mode": "OFFICIAL_SESSIONS_NOT_AFTER_TARGET",
                "research_mode": "NON_PIT_RETROSPECTIVE",
            },
            query_start=min(scan_summary.first_date, target),
            query_end=max(scan_summary.last_date, target),
            parser_version="stock_a.sources.tdx.day.decode_day_record",
            validation_contract="TDX_LOCAL_LDAY_32_BYTE_LAYOUT_R00",
        )
        capture_envelope = build_tdx_capture_envelope(file_summary, capture_request)
    canonical_selection = build_tdx_read_selection(
        source_symbol=normalized,
        target_session=target,
        official_sessions=session_slice,
        selected_rows=selected,
        capture=capture_envelope,
        calendar_id=calendar_payload["calendar_id"],
        calendar_revision=calendar_revision,
        calendar_evidence_status=_CALENDAR_EVIDENCE_VERIFIED,
        units=units,
        future_source_rows_excluded=future_source_rows_excluded,
        identity_projection=identity_projection,
    )
    bounded_calendar = MarketSessionCalendar(
        calendar_id=calendar_payload["calendar_id"],
        revision=calendar_revision,
        sessions=session_slice,
    )
    history_started = time.perf_counter()
    history = _core_history_from_selection(canonical_selection)
    if timing_sink is not None:
        timing_sink["core_history_build_elapsed_ms"] = round(
            (time.perf_counter() - history_started) * 1000, 3
        )
    core_started = time.perf_counter()
    core = calculate_core(history, official_calendar=bounded_calendar)
    if timing_sink is not None:
        timing_sink["core_calculation_elapsed_ms"] = round(
            (time.perf_counter() - core_started) * 1000, 3
        )
    _apply_calendar_coverage_to_core(core, history, coverage_context)
    target_bar = selected.get(target)
    unknown_core = sum(field.quality != "OBSERVED" for field in core.values())
    raw_fields = _render_raw(target_bar, units)
    raw_close = (
        target_bar.raw_close
        if target_bar is not None
        else unknown("TARGET_BAR_MISSING")
    )
    rendered_core_fields = {
        name: _render_field(value) for name, value in sorted(core.items())
    }
    raw_availability = _availability_record(raw_fields, subject="Raw")
    core_availability = _availability_record(rendered_core_fields, subject="Core")
    observation_summary = _observation_summary(
        symbol=normalized,
        target_session=target,
        source_last_session=scan_summary.last_date,
        raw_fields=raw_fields,
        raw_close=raw_close,
        core=core,
    )
    unknown_raw = sum(raw_fields[name]["quality"] != "OBSERVED" for name in _RAW_FIELDS)
    unit_dependent_core = {
        "core.amount_ratio5": {
            "unit_trust": _unit_trust_status(units.amount_to_cny),
            "interpretation": "同一窗口内的金额比率；金额单位倍率需在窗口内保持一致",
        },
        "core.amount_ratio20": {
            "unit_trust": _unit_trust_status(units.amount_to_cny),
            "interpretation": "同一窗口内的金额比率；金额单位倍率需在窗口内保持一致",
        },
        "core.liquidity_mean20": {
            "unit_trust": _unit_trust_status(units.amount_to_cny),
            "interpretation": "绝对金额均值使用调用方声明的amount_to_cny倍率",
        },
        "core.volume_ratio5": {
            "unit_trust": _unit_trust_status(units.volume_to_shares),
            "interpretation": "同一窗口内的数量比率；数量单位倍率需在窗口内保持一致",
        },
        "core.volume_ratio20": {
            "unit_trust": _unit_trust_status(units.volume_to_shares),
            "interpretation": "同一窗口内的数量比率；数量单位倍率需在窗口内保持一致",
        },
    }
    target_prices_usable = (
        target_bar is not None and target_bar.prices.quality == "OBSERVED"
    )
    basic_fact_dimensions, availability_explanation = _basic_fact_dimensions(
        symbol=normalized,
        target_session=target,
        source_last_session=scan_summary.last_date,
        read_time={
            "started_at": scan_summary.read_started_at.isoformat(),
            "completed_at": scan_summary.read_completed_at.isoformat(),
            "clock_role": (
                vipdata_capture.capture.source_package_entry.source_clock_role
                if vipdata_capture is not None
                else "LOCAL_FILE_READ_TIME_ONLY"
            ),
        },
        source={
            "budgets": {
                "max_bytes": limits.max_bytes,
                "max_records": limits.max_records,
                "chunk_records": limits.chunk_records,
                "max_seconds": limits.max_seconds,
            }
        },
        raw_fields=raw_fields,
        core=core,
        observation_summary=observation_summary,
        raw_availability=raw_availability,
        core_availability=core_availability,
        window_context={
            "calendar_revision": calendar_revision,
            "calendar_session_count": len(session_slice),
            "actual_price_bars": sum(row.state == "ACTUAL" for row in history),
            "missing_bar_sessions": sum(row.state == "UNKNOWN" for row in history),
            "lookback_complete": coverage_context["core_lookback_complete"],
            "lookback_reason": coverage_context["core_lookback_reason"],
            "future_rows_used": False,
        },
        target_bar_present=target_bar is not None,
        target_prices_usable=target_prices_usable,
    )
    result = {
        "status": "AVAILABLE" if target_prices_usable else "PARTIAL_UNKNOWN",
        "status_explanation": "兼容状态仅按目标日原始价格组判定；Raw/Core整体可用性及字段原因分别见raw_availability、core_availability和逐字段quality/reason。",
        "raw_availability": raw_availability,
        "core_availability": core_availability,
        "requested_symbol": normalized,
        "board_scope": board,
        "analysis_scope": "TDX_SOURCE_SYMBOL_RAW_SERIES_ONLY",
        "target_session": target.isoformat(),
        "source_last_session": scan_summary.last_date.isoformat(),
        "source_first_session": scan_summary.first_date.isoformat(),
        "read_time": {
            "started_at": scan_summary.read_started_at.isoformat(),
            "completed_at": scan_summary.read_completed_at.isoformat(),
            "clock_role": (
                vipdata_capture.capture.source_package_entry.source_clock_role
                if vipdata_capture is not None
                else "LOCAL_FILE_READ_TIME_ONLY"
            ),
        },
        "source": (
            {
                "configured_root": None,
                "final_path": None,
                "format_contract": "TDX_HSJDAY_ZIP_ENTRY_32_BYTE_LAYOUT_R1",
                "source_contract_status": "OFFICIAL_PACKAGE_BYTES_REVALIDATED; MEMBERSHIP_PIT_AND_UNIT_AUTHORITY_UNBOUND",
                "package_id": vipdata_capture.capture.source_package_entry.package_id,
                "package_sha256": vipdata_capture.capture.source_package_entry.package_sha256,
                "official_package_url": vipdata_capture.capture.source_package_entry.official_package_url,
                "package_verified_at": vipdata_capture.capture.source_package_entry.package_verified_at.isoformat(),
                "zip_entry_path": vipdata_capture.capture.source_package_entry.zip_entry_path,
                "zip_entry_sha256": vipdata_capture.capture.source_package_entry.entry_sha256,
                "raw_byte_sha256": scan_summary.raw_byte_sha256,
                "record_count": scan_summary.record_count,
                "byte_count": scan_summary.byte_count,
                "read_started_at": scan_summary.read_started_at.isoformat(),
                "read_completed_at": scan_summary.read_completed_at.isoformat(),
                "read_clock_role": vipdata_capture.capture.source_package_entry.source_clock_role,
                "published_at": "UNKNOWN",
                "first_available_at": "UNKNOWN",
                "complete_read": scan_summary.complete,
                "budgets": {
                    "max_bytes": limits.max_bytes,
                    "max_records": limits.max_records,
                    "chunk_records": limits.chunk_records,
                    "max_seconds": limits.max_seconds,
                },
            }
            if vipdata_capture is not None
            else {
                "configured_root": str(root),
                "final_path": file_summary.source.handle_path,
                "format_contract": "TDX_LOCAL_LDAY_32_BYTE_LAYOUT_R00",
                "source_contract_status": "RESEARCH_LAYOUT_ONLY_AUTHORITY_AND_CAPTURE_CLOCK_UNRESOLVED",
                "raw_byte_sha256": scan_summary.raw_byte_sha256,
                "record_count": scan_summary.record_count,
                "byte_count": scan_summary.byte_count,
                "read_started_at": scan_summary.read_started_at.isoformat(),
                "read_completed_at": scan_summary.read_completed_at.isoformat(),
                "read_clock_role": "LOCAL_FILE_READ_TIME_ONLY",
                "complete_read": scan_summary.complete,
                "budgets": {
                    "max_bytes": limits.max_bytes,
                    "max_records": limits.max_records,
                    "chunk_records": limits.chunk_records,
                    "max_seconds": limits.max_seconds,
                },
            }
        ),
        "unit_contract": {
            "version": units.version,
            "amount_to_cny": str(units.amount_to_cny)
            if units.amount_to_cny is not None
            else None,
            "volume_to_shares": units.volume_to_shares,
            "amount_authority": _unit_trust_status(units.amount_to_cny),
            "volume_authority": _unit_trust_status(units.volume_to_shares),
            "binding_status": "CALLER_DECLARED_RESEARCH_ASSUMPTION"
            if units.amount_to_cny is not None or units.volume_to_shares is not None
            else "UNKNOWN_UNIT_AUTHORITY",
            "amount_quality_is_unit_authority": False,
            "volume_quality_is_unit_authority": False,
        },
        "calendar": {
            "calendar_id": calendar_payload["calendar_id"],
            "revision": calendar_revision,
            "resources": list(calendar_metadata),
            "coverage": [calendar_metadata[0]["coverage"][0], target.isoformat()],
            "evidence_status": _CALENDAR_EVIDENCE_VERIFIED,
            "resource_continuity_status": coverage_context[
                "resource_continuity_status"
            ],
            "known_resource_years": list(coverage_context["known_resource_years"]),
            "calculation_years": list(coverage_context["calculation_years"]),
            "missing_intermediate_years": list(
                coverage_context["missing_intermediate_years"]
            ),
            "core_lookback_complete": coverage_context["core_lookback_complete"],
            "core_lookback_reason": coverage_context["core_lookback_reason"],
            "required_actual_sessions": coverage_context["required_actual_sessions"],
            "available_calendar_sessions_to_target": coverage_context[
                "available_calendar_sessions_to_target"
            ],
            "session_count_in_calculation": len(session_slice),
            "calculation_first_session": session_slice[0].isoformat(),
            "missing_bar_session_count": sum(
                day not in selected for day in session_slice
            ),
            "missing_bar_state": "UNKNOWN; 不推断为停牌或上市前",
        },
        "identity": {
            **(
                identity_projection.as_dict()
                if identity_projection is not None
                else {
                    "status": "SOURCE_SYMBOL_ONLY",
                    "security_id": None,
                    "reason": "OFFICIAL_STABLE_SECURITY_ID_SOURCE_NOT_BOUND",
                }
            ),
            "security_id": None,
            "stable_security_id_issued": False,
        },
        "canonical_selection": _render_canonical_selection(canonical_selection),
        "adjustment": {
            "coordinate": "RAW_UNADJUSTED",
            "status": "NOT_APPLIED_IDENTITY_AND_EVENT_TIMING_UNRESOLVED",
        },
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "target_bar_present": target_bar is not None,
        "target_prices_usable": target_prices_usable,
        "target_raw_fields": raw_fields,
        "core_fields": rendered_core_fields,
        "observation_summary": observation_summary,
        "basic_fact_dimensions": basic_fact_dimensions,
        "availability_explanation": availability_explanation,
        "core_field_disclosures": {
            "price_coordinate": "RAW_UNADJUSTED；身份和公司行为时钟未绑定",
            "unit_assumptions": unit_dependent_core,
        },
        "coverage": {
            "calendar_sessions": len(session_slice),
            "actual_price_bars": sum(row.state == "ACTUAL" for row in history),
            "unknown_sessions": sum(row.state == "UNKNOWN" for row in history),
            "unknown_core_fields": unknown_core,
            "unknown_target_raw_fields": unknown_raw,
        },
    }
    if identity_projection is not None or identity_query_status != "NOT_REQUESTED":
        apply_identity_observation(
            result,
            identity_projection=identity_projection,
            identity_query_status=identity_query_status,
        )
    if profile_contract is not None:
        result["profile_preview"] = _build_profile_preview(
            symbol=normalized,
            target_session=target,
            source_last_session=scan_summary.last_date,
            source_sha256=scan_summary.raw_byte_sha256,
            target_bar=target_bar,
            history=history,
            core=core,
            calendar_revision=calendar_revision,
            contract=profile_contract,
        )
    if vipdata_capture is not None:
        source_ref = vipdata_capture.capture.source_package_entry
        result["source_package"] = {
            "package_id": source_ref.package_id,
            "sha256": source_ref.package_sha256,
            "official_package_url": source_ref.official_package_url,
            "file_size": source_ref.package_file_size,
            "zip_entry_count": source_ref.zip_entry_count,
            "zip_crc": source_ref.zip_crc,
            "verified_at": source_ref.package_verified_at.isoformat(),
            "zip_entry_path": source_ref.zip_entry_path,
            "zip_entry_sha256": source_ref.entry_sha256,
            "zip_entry_bytes": source_ref.entry_byte_count,
            "target_session": source_ref.target_session.isoformat(),
            "entry_observed_started_at": source_ref.observed_started_at.isoformat(),
            "entry_observed_completed_at": source_ref.observed_completed_at.isoformat(),
            "published_at": "UNKNOWN",
            "first_available_at": "UNKNOWN",
            "identity_status": "SOURCE_SYMBOL_ONLY",
            "membership_and_listing_status": "UNKNOWN_NOT_OFFICIALLY_BOUND",
            "price_coordinate": "RAW_UNADJUSTED",
            "amount_unit_authority": _unit_trust_status(units.amount_to_cny),
            "volume_unit_authority": _unit_trust_status(units.volume_to_shares),
        }
    if type(include_baostock_evidence) is not bool:
        raise ResearchEntryError(
            "BAOSTOCK_OPTION_INVALID", "BaoStock证据开关必须为布尔值"
        )
    if include_baostock_evidence:
        # 可选SDK仅在用户显式请求research-stock补充证据时延迟导入。
        from stock_a.sources.baostock.client import query_daily_rows
        from stock_a.sources.baostock.evidence import project_baostock_evidence

        query = query_daily_rows(normalized, target.isoformat())
        result["baostock_evidence"] = project_baostock_evidence(
            query=query,
            requested_symbol=normalized,
            target_session=target.isoformat(),
            tdx_result=result,
        )
    return result
