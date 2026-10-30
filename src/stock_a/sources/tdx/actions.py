"""GBBQ 协议解析及类别 1 字段解释；只处理调用方提供的有界字节。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from fractions import Fraction
import math
import struct

from stock_a.domain import ContractError
from stock_a.sources.tdx._gbbq_key import GBBQ_KEY_BYTES


GBBQ_HEADER_LENGTH = 4
GBBQ_RECORD_LENGTH = 29
GBBQ_PROTOCOL_VERSION = "TDX_GBBQ_29B_V1"
GBBQ_UNIT_VERSION = "TDX_XRXD_UNIT_R1"
GBBQ_KEY_SHA256 = "2cdbe88a76fd70bdb3338d1cf5e2400e16412994c8cf5e1eafbefe2dcabd2e45"

MARKET_BY_BYTE = {0: "SZ", 1: "SH", 2: "BJ"}
CATEGORY_NAMES = {
    1: "XRXD",
    2: "LISTED_BONUS_OR_RIGHTS_SHARES",
    3: "NON_TRADABLE_SHARES_LISTED",
    4: "UNKNOWN_EQUITY_CHANGE",
    5: "EQUITY_CHANGE",
    6: "PRIVATE_PLACEMENT",
    7: "SHARE_REPURCHASE",
    8: "PRIVATE_PLACEMENT_SHARES_LISTED",
    9: "TRANSFERRED_RIGHTS_SHARES_LISTED",
    10: "CONVERTIBLE_BOND_LISTED",
    11: "SHARE_CONSOLIDATION_OR_SPLIT",
    12: "NON_TRADABLE_SHARE_CONSOLIDATION",
    13: "CALL_WARRANT_DISTRIBUTION",
    14: "PUT_WARRANT_DISTRIBUTION",
}

_FLOAT32 = struct.Struct("<f")
_UINT32 = struct.Struct("<I")
_KEY_WORDS = struct.unpack(f"<{len(GBBQ_KEY_BYTES) // 4}I", GBBQ_KEY_BYTES)
_CATEGORY1_FIELD_NAMES = (
    "cash_dividend_per_10",
    "rights_price",
    "bonus_transfer_per_10",
    "rights_ratio_per_10",
)
_CATEGORY1_FIELD_OFFSETS = (13, 17, 21, 25)
_CATEGORY1_REASONS = ("C1_NEGATIVE", "C2_NEGATIVE", "C3_NEGATIVE", "C4_NEGATIVE")


class GbbqDecodeError(ValueError):
    """结构、字节预算或类别语义错误，带稳定代码及可用记录位置。"""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        record_index: int | None = None,
        byte_offset: int | None = None,
    ) -> None:
        self.code = code
        self.record_index = record_index
        self.byte_offset = byte_offset
        if record_index is not None:
            message = f"{message}（记录序号 {record_index}，字节偏移 {byte_offset}）"
        super().__init__(message)


@dataclass(frozen=True)
class GbbqParseResult:
    """完整解析结果；记录数与输入顺序一一对应。"""

    source_ref: str
    protocol_version: str
    unit_version: str
    declared_count: int
    records: tuple[GbbqRecord, ...]

    @property
    def parsed_count(self) -> int:
        return len(self.records)


@dataclass(frozen=True)
class GbbqRecord:
    """单条来源记录；source_symbol 不是稳定 security_id。"""

    source_ref: str
    protocol_version: str
    unit_version: str
    source_record_index: int
    source_byte_offset: int
    market_byte: int
    market_code: str | None
    raw_code_bytes: bytes
    raw_code: str | None
    source_symbol: str | None
    event_date_value: int
    event_date: date | None
    category: int
    category_name: str
    semantic_status: str
    c1: Fraction | None
    c2: Fraction | None
    c3: Fraction | None
    c4: Fraction | None
    parameter_quality: tuple[str, str, str, str]
    parameter_reasons: tuple[str | None, str | None, str | None, str | None]
    data_quality: str
    reasons: tuple[str, ...]
    identity_status: str
    pit_status: str

    @property
    def is_xrxd(self) -> bool:
        return self.category == 1

    @property
    def can_interpret_xrxd(self) -> bool:
        return (
            self.is_xrxd
            and self.data_quality == "VALID"
            and self.source_symbol is not None
            and self.event_date is not None
        )

    def xrxd_fields(self) -> dict[str, Fraction]:
        """返回类别1的精确字段；不支持类别和坏数据均明确拒绝。"""
        if self.category != 1:
            code = (
                "CATEGORY_SEMANTICS_UNSUPPORTED"
                if self.category in CATEGORY_NAMES
                else "UNKNOWN_CATEGORY"
            )
            raise GbbqDecodeError(
                code,
                "该记录没有已采用的类别1字段语义",
                record_index=self.source_record_index,
                byte_offset=self.source_byte_offset,
            )
        if not self.can_interpret_xrxd:
            raise GbbqDecodeError(
                "CATEGORY1_RECORD_INVALID",
                "类别1记录的来源标识、日期或参数未通过校验",
                record_index=self.source_record_index,
                byte_offset=self.source_byte_offset,
            )
        values = (self.c1, self.c2, self.c3, self.c4)
        # can_interpret_xrxd 保证四个字段均为 Fraction。
        return dict(zip(_CATEGORY1_FIELD_NAMES, values, strict=True))  # type: ignore[arg-type]


def _require_positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ContractError(f"{name} 必须为正整数且不能为布尔值")
    return value


def _validate_metadata(
    *,
    source_ref: object,
    protocol_version: object,
    unit_version: object,
) -> tuple[str, str, str]:
    if (
        not isinstance(source_ref, str)
        or not source_ref.strip()
        or len(source_ref) > 1024
    ):
        raise ContractError("source_ref 必须是非空且不超过1024字符的引用")
    if (
        not isinstance(protocol_version, str)
        or protocol_version != GBBQ_PROTOCOL_VERSION
    ):
        raise ContractError("protocol_version 缺失或不受支持")
    if not isinstance(unit_version, str) or unit_version != GBBQ_UNIT_VERSION:
        raise ContractError("unit_version 缺失或不受支持")
    return source_ref, GBBQ_PROTOCOL_VERSION, GBBQ_UNIT_VERSION


def decrypt_block(encrypted: bytes) -> bytes:
    """解密一个8字节块；16轮查表/XOR/32位掩码顺序保留自旧内核。"""
    if not isinstance(encrypted, bytes):
        raise ContractError("加密块必须是 bytes")
    if len(encrypted) != 8:
        raise ContractError("加密块必须恰好为8字节")
    # 保留旧算法的8字节小端双uint32、16轮索引、掩码及输出顺序。
    words = _KEY_WORDS
    left, right = struct.unpack("<II", encrypted)
    current = words[17] ^ left
    previous = right
    for round_index in range(16, 0, -1):
        value = words[274 + ((current >> 16) & 0xFF)]
        value = (value + words[18 + ((current >> 24) & 0xFF)]) & 0xFFFFFFFF
        value ^= words[530 + ((current >> 8) & 0xFF)]
        value = (value + words[786 + (current & 0xFF)]) & 0xFFFFFFFF
        value ^= words[round_index]
        current, previous = (previous ^ value) & 0xFFFFFFFF, current
    previous ^= words[0]
    return struct.pack("<II", previous, current)


def _parse_date(value: int) -> date | None:
    try:
        return date(value // 10000, (value // 100) % 100, value % 100)
    except ValueError:
        return None


def _decode_parameter(clear: bytes, offset: int) -> Fraction | None:
    value = _FLOAT32.unpack_from(clear, offset)[0]
    if not math.isfinite(value):
        return None
    # float32值能被Python float精确表示，Fraction.from_float保留其二进制值。
    return Fraction.from_float(value)


def decode_record(
    encrypted_record: bytes,
    *,
    source_ref: str | None = None,
    source_record_index: int | None = None,
    protocol_version: str | None = None,
    unit_version: str | None = None,
) -> GbbqRecord:
    """按29字节协议解析一条记录，保留坏行并附带其精确来源位置。"""
    source_ref, protocol_version, unit_version = _validate_metadata(
        source_ref=source_ref,
        protocol_version=protocol_version,
        unit_version=unit_version,
    )
    if (
        isinstance(source_record_index, bool)
        or not isinstance(source_record_index, int)
        or source_record_index < 0
    ):
        raise ContractError("source_record_index 必须为非负整数且不能为布尔值")
    source_index_offset = GBBQ_HEADER_LENGTH + GBBQ_RECORD_LENGTH * source_record_index
    if not isinstance(encrypted_record, bytes):
        raise ContractError("加密记录必须是 bytes")
    if len(encrypted_record) != GBBQ_RECORD_LENGTH:
        raise GbbqDecodeError(
            "RECORD_LENGTH_INVALID",
            "GBBQ记录必须恰好为29字节",
            record_index=source_record_index,
            byte_offset=source_index_offset,
        )

    # 协议布局：解密前三个8字节块，末5字节保留原样。
    clear = (
        decrypt_block(encrypted_record[0:8])
        + decrypt_block(encrypted_record[8:16])
        + decrypt_block(encrypted_record[16:24])
        + encrypted_record[24:29]
    )
    market_byte = clear[0]
    market_code = MARKET_BY_BYTE.get(market_byte)
    raw_code_bytes = clear[1:8]
    reasons: list[str] = []
    try:
        ascii_code = raw_code_bytes.decode("ascii", errors="strict")
    except UnicodeDecodeError:
        ascii_code = None
        reasons.append("INVALID_CODE_ASCII")

    raw_code: str | None = None
    if ascii_code is not None:
        code_body = raw_code_bytes[:6]
        if code_body.isdigit() and len(code_body) == 6 and raw_code_bytes[6] == 0:
            raw_code = ascii_code[:6]
        else:
            reasons.append("INVALID_SOURCE_CODE")
    if market_code is None:
        reasons.append("INVALID_MARKET")
    source_symbol = (
        f"{market_code}.{raw_code}"
        if market_code is not None and raw_code is not None
        else None
    )

    event_date_value = _UINT32.unpack_from(clear, 8)[0]
    event_date = _parse_date(event_date_value)
    if event_date is None:
        reasons.append("INVALID_GREGORIAN_DATE")

    category = clear[12]
    if category == 1:
        semantic_status = "SUPPORTED_CATEGORY_1"
    elif category in CATEGORY_NAMES:
        semantic_status = "KNOWN_UNSUPPORTED_CATEGORY"
    else:
        semantic_status = "UNKNOWN_CATEGORY"

    parameters = tuple(
        _decode_parameter(clear, offset) for offset in _CATEGORY1_FIELD_OFFSETS
    )
    parameter_reasons: list[str | None] = []
    for index, parameter in enumerate(parameters):
        reason = None if parameter is not None else f"C{index + 1}_NON_FINITE"
        parameter_reasons.append(reason)
        if reason is not None:
            reasons.append(reason)

    if category == 1:
        for index, (parameter, reason) in enumerate(
            zip(parameters, _CATEGORY1_REASONS, strict=True)
        ):
            if parameter is not None and parameter < 0:
                parameter_reasons[index] = reason
                reasons.append(reason)
        if all(parameter is not None for parameter in parameters):
            c3 = parameters[2]
            c4 = parameters[3]
            if c3 is not None and c4 is not None and 10 + c3 + c4 <= 0:
                reasons.append("CATEGORY1_DENOMINATOR_NON_POSITIVE")

    data_quality = "INVALID_DATA" if reasons else "VALID"
    # OBSERVED表示有限值已从float32解码，不单独表示该值通过类别语义约束。
    parameter_quality = tuple(
        "OBSERVED" if parameter is not None else "UNKNOWN_DATA"
        for parameter in parameters
    )
    return GbbqRecord(
        source_ref=source_ref,
        protocol_version=protocol_version,
        unit_version=unit_version,
        source_record_index=source_record_index,
        source_byte_offset=source_index_offset,
        market_byte=market_byte,
        market_code=market_code,
        raw_code_bytes=raw_code_bytes,
        raw_code=raw_code,
        source_symbol=source_symbol,
        event_date_value=event_date_value,
        event_date=event_date,
        category=category,
        category_name=CATEGORY_NAMES.get(category, "UNKNOWN_CATEGORY"),
        semantic_status=semantic_status,
        c1=parameters[0],
        c2=parameters[1],
        c3=parameters[2],
        c4=parameters[3],
        parameter_quality=parameter_quality,  # type: ignore[arg-type]
        parameter_reasons=tuple(parameter_reasons),  # type: ignore[arg-type]
        data_quality=data_quality,
        reasons=tuple(reasons),
        identity_status=(
            "SOURCE_IDENTITY_ONLY"
            if source_symbol is not None
            else "UNRESOLVED_SOURCE_IDENTITY"
        ),
        pit_status="NOT_ASSERTED",
    )


def decode_gbbq_bytes(
    content: bytes,
    *,
    source_ref: str | None = None,
    protocol_version: str | None = None,
    unit_version: str | None = None,
    max_bytes: int | None = None,
    max_records: int | None = None,
) -> GbbqParseResult:
    """解析完整、有界的GBBQ字节；不读取文件、配置、网络或数据库。"""
    source_ref, protocol_version, unit_version = _validate_metadata(
        source_ref=source_ref,
        protocol_version=protocol_version,
        unit_version=unit_version,
    )
    max_bytes = _require_positive_integer(max_bytes, "max_bytes")
    max_records = _require_positive_integer(max_records, "max_records")
    if not isinstance(content, bytes):
        raise ContractError("GBBQ输入必须是 bytes")
    if len(content) > max_bytes:
        raise GbbqDecodeError("BYTE_BUDGET_EXCEEDED", "GBBQ字节数超过显式预算")
    if len(content) < GBBQ_HEADER_LENGTH:
        raise GbbqDecodeError("HEADER_TOO_SHORT", "GBBQ包头不足4字节")

    declared_count = _UINT32.unpack_from(content, 0)[0]
    if declared_count > max_records:
        raise GbbqDecodeError("RECORD_BUDGET_EXCEEDED", "GBBQ声明记录数超过显式预算")
    expected_size = GBBQ_HEADER_LENGTH + declared_count * GBBQ_RECORD_LENGTH
    if len(content) != expected_size:
        raise GbbqDecodeError(
            "PACKAGE_SIZE_MISMATCH",
            f"GBBQ包长度不闭合：声明{declared_count}条，应为{expected_size}字节，实为{len(content)}字节",
        )

    # 通过整体长度与预算校验后才构造有限记录集合，保持原序及重复行。
    records = tuple(
        decode_record(
            content[offset : offset + GBBQ_RECORD_LENGTH],
            source_ref=source_ref,
            source_record_index=index,
            protocol_version=protocol_version,
            unit_version=unit_version,
        )
        for index, offset in enumerate(
            range(GBBQ_HEADER_LENGTH, expected_size, GBBQ_RECORD_LENGTH)
        )
    )
    return GbbqParseResult(
        source_ref=source_ref,
        protocol_version=protocol_version,
        unit_version=unit_version,
        declared_count=declared_count,
        records=records,
    )
