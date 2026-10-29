"""已完整读取的TDX来源事实选择；不读取文件、不证明来源权威或PIT。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Mapping

from stock_a.domain import FieldValue, unknown
from stock_a.sources.tdx.capture import TdxCaptureEnvelope
from stock_a.sources.tdx.day import DecodedDay, UnitContract


_FIELD_NAMES = ("open", "high", "low", "close", "amount", "volume")


@dataclass(frozen=True)
class CanonicalSelectedRow:
    """一个官方会话上的来源行；缺bar保持UNKNOWN，不推断交易状态。"""

    session: date
    state: str
    coordinate_id: str | None
    open: FieldValue
    high: FieldValue
    low: FieldValue
    close: FieldValue
    amount: FieldValue
    volume: FieldValue


@dataclass(frozen=True)
class CanonicalFieldFact:
    """目标会话单字段的值、质量、来源与调用方单位假设。"""

    field_id: str
    target_session: date
    source_session: date | None
    value: FieldValue
    unit: str
    unit_basis: str
    source_sha256: str


@dataclass(frozen=True)
class CanonicalReadSelection:
    """仅供本次只读研究调用消费的规范选择，不是已接受或持久化事实。"""

    source_symbol: str
    target_session: date
    capture: TdxCaptureEnvelope
    calendar_id: str
    calendar_revision: str
    calendar_evidence_status: str
    units: UnitContract
    rows: tuple[CanonicalSelectedRow, ...]
    target_fields: tuple[CanonicalFieldFact, ...]
    future_source_rows_excluded: int


def _row_fields(decoded: DecodedDay | None) -> dict[str, FieldValue]:
    if decoded is None:
        return {name: unknown("NO_TDX_BAR_FOR_OFFICIAL_SESSION") for name in _FIELD_NAMES}
    return {
        "open": decoded.raw_open,
        "high": decoded.raw_high,
        "low": decoded.raw_low,
        "close": decoded.raw_close,
        "amount": decoded.amount,
        "volume": decoded.volume,
    }


def build_tdx_read_selection(
    *,
    source_symbol: str,
    target_session: date,
    official_sessions: tuple[date, ...],
    selected_rows: Mapping[date, DecodedDay],
    capture: TdxCaptureEnvelope,
    calendar_id: str,
    calendar_revision: str,
    calendar_evidence_status: str,
    units: UnitContract,
    future_source_rows_excluded: int,
) -> CanonicalReadSelection:
    """从现有完整扫描和已选行构造选择结果；此函数不执行任何IO。"""
    if not isinstance(capture, TdxCaptureEnvelope) or type(target_session) is not date:
        raise ValueError("规范选择必须绑定完整TDX捕获封套和目标会话")
    if (not isinstance(source_symbol, str) or not source_symbol
            or not isinstance(calendar_id, str) or not calendar_id
            or not isinstance(calendar_revision, str) or not calendar_revision
            or not isinstance(calendar_evidence_status, str) or not calendar_evidence_status
            or not isinstance(units, UnitContract)):
        raise ValueError("规范选择缺少来源代码、日历、证据状态或单位合同")
    file_identity = capture.source_identity
    package_entry = capture.source_package_entry
    file_identity_valid = (
        file_identity is not None
        and file_identity.path_binding_verified is True
        and file_identity.handle_path_verified is True
        and file_identity.requested_access == "GENERIC_READ"
        and file_identity.size_before == capture.byte_count
        and file_identity.size_after == capture.byte_count
        and file_identity.mtime_ns_before == file_identity.mtime_ns_after
    )
    package_entry_valid = (
        package_entry is not None
        and package_entry.source_symbol == source_symbol
        and package_entry.target_session == target_session
        and package_entry.entry_sha256 == capture.content_sha256
        and package_entry.entry_byte_count == capture.byte_count
    )
    if (capture.record_count <= 0 or capture.byte_count != capture.record_count * 32
            or capture.first_date > capture.last_date
            or (file_identity_valid == package_entry_valid)):
        raise ValueError("规范选择拒绝不完整或未核验的TDX来源扫描")
    if type(future_source_rows_excluded) is not int or future_source_rows_excluded < 0:
        raise ValueError("未来来源记录排除数必须是非负整数")
    if (not official_sessions or official_sessions != tuple(sorted(set(official_sessions)))
            or official_sessions[-1] != target_session
            or any(type(session) is not date for session in official_sessions)):
        raise ValueError("规范选择会话必须严格递增并且截至目标会话")
    if any(type(session) is not date or session > target_session for session in selected_rows):
        raise ValueError("规范选择拒绝目标会话之后的来源行")
    for session, decoded in selected_rows.items():
        if decoded.session_date != session or session not in official_sessions:
            raise ValueError("规范选择来源行与交易会话不一致")

    coordinate_id = f"RAW:TDX_SOURCE_SYMBOL:{source_symbol}"
    rows: list[CanonicalSelectedRow] = []
    for session in official_sessions:
        decoded = selected_rows.get(session)
        values = _row_fields(decoded)
        rows.append(CanonicalSelectedRow(
            session=session,
            state="ACTUAL" if decoded is not None else "UNKNOWN",
            coordinate_id=coordinate_id if decoded is not None else None,
            **values,
        ))

    target = selected_rows.get(target_session)
    target_values = _row_fields(target)
    source_sha256 = capture.content_sha256
    amount_basis = f"CALLER_DECLARED_SCALE:{units.amount_to_cny}"
    volume_basis = f"CALLER_DECLARED_SCALE:{units.volume_to_shares}"
    field_units = {
        "open": ("CNY_PER_SHARE", "TDX_RAW_PRICE_INTEGER_DIV_100"),
        "high": ("CNY_PER_SHARE", "TDX_RAW_PRICE_INTEGER_DIV_100"),
        "low": ("CNY_PER_SHARE", "TDX_RAW_PRICE_INTEGER_DIV_100"),
        "close": ("CNY_PER_SHARE", "TDX_RAW_PRICE_INTEGER_DIV_100"),
        "amount": ("CNY", amount_basis),
        "volume": ("SHARES", volume_basis),
    }
    target_fields = tuple(
        CanonicalFieldFact(
            field_id=f"raw.{name}",
            target_session=target_session,
            source_session=target_session if target is not None else None,
            value=target_values[name],
            unit=field_units[name][0],
            unit_basis=field_units[name][1],
            source_sha256=source_sha256,
        )
        for name in _FIELD_NAMES
    )
    return CanonicalReadSelection(
        source_symbol=source_symbol,
        target_session=target_session,
        capture=capture,
        calendar_id=calendar_id,
        calendar_revision=calendar_revision,
        calendar_evidence_status=calendar_evidence_status,
        units=units,
        rows=tuple(rows),
        target_fields=target_fields,
        future_source_rows_excluded=future_source_rows_excluded,
    )
