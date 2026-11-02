"""读取本机TDX行业与概念成员原件并生成非PIT来源快照。"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import re
from typing import Mapping


SECTOR_PARSE_CONTRACT = "STOCK_A_TDX_SECTOR_SOURCE_MEMBERS_R1"
_MARKET_PREFIX = {"0": "SZ", "1": "SH", "2": "BJ"}
_INDUSTRY_CODE = re.compile(r"T[0-9]{2,6}\Z")
_CONCEPT_MEMBER = re.compile(rb"([012])#([0-9]{6})\Z")
_MAX_SECTOR_FILE_BYTES = 8 * 1024 * 1024


class TdxSectorSourceError(ValueError):
    """TDX板块来源结构或路径不符合合同。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class SourceFileEvidence:
    """一次只读来源文件观察。"""

    relative_path: str
    absolute_path: str
    raw: bytes
    sha256: str
    size_bytes: int
    last_write_time_utc: str
    source_label: str | None = None
    source_protocol: str | None = None
    source_archive_sha256: str | None = None


@dataclass(frozen=True)
class SectorCatalogRow:
    board_kind: str
    board_native_id: str
    board_name: str


@dataclass(frozen=True)
class SectorMemberRow:
    board_kind: str
    board_native_id: str
    source_symbol: str
    membership_quality: str
    source_record_ordinal: int


@dataclass(frozen=True)
class SectorSourceSnapshot:
    snapshot_id: str
    source_kind: str
    source_file_sha256: str
    source_path_role: str
    source_file_size: int
    observed_at: datetime
    source_valid_on: None
    source_known_at: None
    source_available_at: None
    clock_quality: str
    research_mode: str
    parse_contract_version: str
    rows_declared: int
    source_quality: str
    catalog_rows: tuple[SectorCatalogRow, ...]
    member_rows: tuple[SectorMemberRow, ...]
    unresolved_rows: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class SectorSourceInputs:
    root: str
    files: Mapping[str, SourceFileEvidence]


@dataclass(frozen=True)
class RemoteSectorParseOutcome:
    snapshots: tuple[SectorSourceSnapshot, ...]
    rejected_sources: tuple[dict[str, str], ...]


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _source_label(item: SourceFileEvidence) -> str:
    return item.source_label or f"TDX_INSTALL/{item.relative_path}"


def _file_evidence(root: Path, relative_path: str) -> SourceFileEvidence:
    root_resolved = root.resolve(strict=True)
    source_path = root_resolved.joinpath(*relative_path.split("/"))
    try:
        resolved = source_path.resolve(strict=True)
        resolved.relative_to(root_resolved)
        stat_before = resolved.stat()
    except FileNotFoundError as error:
        raise TdxSectorSourceError(
            "SOURCE_FILE_MISSING", "配置TDX根下缺少行业或概念原始文件"
        ) from error
    except PermissionError as error:
        raise TdxSectorSourceError(
            "SOURCE_ACCESS_DENIED", "操作系统拒绝只读行业或概念来源文件"
        ) from error
    except (OSError, ValueError) as error:
        raise TdxSectorSourceError(
            "SOURCE_PATH_INVALID", "TDX来源最终路径无效或越出配置根目录"
        ) from error
    if not resolved.is_file() or stat_before.st_size > _MAX_SECTOR_FILE_BYTES:
        raise TdxSectorSourceError(
            "SOURCE_FILE_LIMIT", "行业或概念来源文件类型或字节数超出上限"
        )
    try:
        first = resolved.read_bytes()
        stat_after = resolved.stat()
        second = resolved.read_bytes()
    except PermissionError as error:
        raise TdxSectorSourceError(
            "SOURCE_ACCESS_DENIED", "操作系统拒绝只读行业或概念来源文件"
        ) from error
    except OSError as error:
        raise TdxSectorSourceError(
            "SOURCE_IO_ERROR", "只读行业或概念来源文件时发生工程I/O错误"
        ) from error
    if (
        first != second
        or len(first) != stat_before.st_size
        or stat_after.st_size != stat_before.st_size
        or stat_after.st_mtime_ns != stat_before.st_mtime_ns
        or getattr(stat_after, "st_ino", None) != getattr(stat_before, "st_ino", None)
    ):
        raise TdxSectorSourceError(
            "SOURCE_CHANGED_DURING_READ", "来源文件在只读观察期间发生变化"
        )
    return SourceFileEvidence(
        relative_path=relative_path,
        absolute_path=str(resolved),
        raw=first,
        sha256=_sha256(first),
        size_bytes=len(first),
        last_write_time_utc=datetime.fromtimestamp(
            stat_after.st_mtime, timezone.utc
        ).isoformat(),
    )


def read_sector_source_inputs(source_root: str | Path) -> SectorSourceInputs:
    """从一个已登记TDX根只读读取行业、名称目录与概念成员文件。"""
    try:
        root = Path(source_root).resolve(strict=True)
    except FileNotFoundError as error:
        raise TdxSectorSourceError(
            "SOURCE_ROOT_MISSING", "配置TDX来源根不存在"
        ) from error
    except (OSError, ValueError) as error:
        raise TdxSectorSourceError(
            "SOURCE_ROOT_INVALID", "配置TDX来源根无法解析"
        ) from error
    if not root.is_dir():
        raise TdxSectorSourceError("SOURCE_ROOT_INVALID", "配置TDX来源根不是目录")
    relative_paths = (
        "T0002/hq_cache/tdxhy.cfg",
        "T0002/hq_cache/tdxzs.cfg",
        "T0002/hq_cache/infoharbor_block.dat",
    )
    files = {relative: _file_evidence(root, relative) for relative in relative_paths}
    if sum(item.size_bytes for item in files.values()) > 16 * 1024 * 1024:
        raise TdxSectorSourceError(
            "SOURCE_TOTAL_SIZE_LIMIT", "行业与概念来源总字节数超出上限"
        )
    return SectorSourceInputs(root=str(root), files=files)


def _strict_text_lines(raw: bytes, source_name: str) -> list[str]:
    if b"\x00" in raw:
        raise TdxSectorSourceError(
            "SOURCE_TEXT_NUL", f"{source_name}包含不允许的NUL字节"
        )
    try:
        text = raw.decode("gb18030", errors="strict")
    except UnicodeDecodeError as error:
        raise TdxSectorSourceError(
            "SOURCE_TEXT_DECODE", f"{source_name}不能按GB18030严格解码"
        ) from error
    if "\r" in text.replace("\r\n", ""):
        raise TdxSectorSourceError(
            "SOURCE_LINE_ENDING_INVALID", f"{source_name}包含孤立CR换行"
        )
    return text.splitlines()


def _industry_directory(raw: bytes) -> dict[str, tuple[str, str, int]]:
    lines = _strict_text_lines(raw, "tdxzs.cfg")
    directory: dict[str, tuple[str, str, int]] = {}
    native_ids: set[str] = set()
    for ordinal, line in enumerate(lines, 1):
        fields = line.split("|")
        if len(fields) != 6:
            raise TdxSectorSourceError(
                "TDXZS_FIELD_COUNT", f"tdxzs.cfg第{ordinal}条记录字段数不为6"
            )
        name, native_id, record_type, _level, _parent, taxonomy_code = fields
        if record_type != "2" or not taxonomy_code.startswith("T"):
            continue
        if not name or re.fullmatch(r"[0-9]{6}", native_id) is None:
            raise TdxSectorSourceError(
                "TDXZS_INDUSTRY_HEADER_INVALID",
                f"tdxzs.cfg第{ordinal}条行业目录记录缺少名称或native id",
            )
        if _INDUSTRY_CODE.fullmatch(taxonomy_code) is None:
            raise TdxSectorSourceError(
                "TDXZS_INDUSTRY_CODE_INVALID",
                f"tdxzs.cfg第{ordinal}条行业分类代码格式无效",
            )
        if taxonomy_code in directory or native_id in native_ids:
            raise TdxSectorSourceError(
                "TDXZS_INDUSTRY_DUPLICATE",
                f"tdxzs.cfg第{ordinal}条行业目录记录发生重复",
            )
        directory[taxonomy_code] = (native_id, name, ordinal)
        native_ids.add(native_id)
    return directory


def _industry_snapshot(
    assignment_file: SourceFileEvidence,
    directory_file: SourceFileEvidence,
    observed_at: datetime,
) -> SectorSourceSnapshot:
    directory = _industry_directory(directory_file.raw)
    lines = _strict_text_lines(assignment_file.raw, "tdxhy.cfg")
    assignments: list[tuple[str, str, str, int]] = []
    unresolved = Counter()
    seen_source_symbols: set[str] = set()
    for ordinal, line in enumerate(lines, 1):
        fields = line.split("|")
        if len(fields) != 6:
            raise TdxSectorSourceError(
                "TDXHY_FIELD_COUNT", f"tdxhy.cfg第{ordinal}条记录字段数不为6"
            )
        market_number, code, taxonomy_code = fields[:3]
        if market_number not in _MARKET_PREFIX:
            raise TdxSectorSourceError(
                "TDXHY_MARKET_INVALID", f"tdxhy.cfg第{ordinal}条市场编号无效"
            )
        if re.fullmatch(r"[0-9]{6}", code) is None:
            raise TdxSectorSourceError(
                "TDXHY_SECURITY_CODE_INVALID", f"tdxhy.cfg第{ordinal}条证券代码无效"
            )
        source_symbol = f"{_MARKET_PREFIX[market_number]}.{code}"
        if source_symbol in seen_source_symbols:
            raise TdxSectorSourceError(
                "TDXHY_SOURCE_SYMBOL_DUPLICATE",
                f"tdxhy.cfg第{ordinal}条来源代码重复",
            )
        seen_source_symbols.add(source_symbol)
        if not taxonomy_code:
            unresolved["INDUSTRY_CODE_EMPTY"] += 1
            continue
        if taxonomy_code == "T00":
            unresolved["INDUSTRY_CODE_T00"] += 1
            continue
        if _INDUSTRY_CODE.fullmatch(taxonomy_code) is None:
            raise TdxSectorSourceError(
                "TDXHY_INDUSTRY_CODE_INVALID",
                f"tdxhy.cfg第{ordinal}条行业分类代码格式无效",
            )
        directory_row = directory.get(taxonomy_code)
        if directory_row is None:
            unresolved["INDUSTRY_CODE_NOT_IN_TDXZS"] += 1
            continue
        native_id, name, _directory_ordinal = directory_row
        assignments.append((taxonomy_code, native_id, name, ordinal))

    used_boards: dict[str, str] = {}
    members: list[SectorMemberRow] = []
    for taxonomy_code, native_id, name, ordinal in assignments:
        used_boards[native_id] = name
        line = lines[ordinal - 1]
        market_number, code = line.split("|")[:2]
        members.append(
            SectorMemberRow(
                board_kind="INDUSTRY",
                board_native_id=native_id,
                source_symbol=f"{_MARKET_PREFIX[market_number]}.{code}",
                membership_quality="OBSERVED_SOURCE_RELATION",
                source_record_ordinal=ordinal,
            )
        )
    members.sort(key=lambda row: (row.board_native_id, row.source_symbol))
    catalog = tuple(
        SectorCatalogRow("INDUSTRY", native_id, name)
        for native_id, name in sorted(used_boards.items(), key=lambda item: item[0])
    )
    observed_at = _require_observed_at(observed_at)
    source_quality = "PARTIAL_NON_PIT;UNMAPPED_SOURCE_ROWS=" + str(
        sum(unresolved.values())
    )
    if unresolved:
        source_quality += ";" + ";".join(
            f"{reason}={count}" for reason, count in sorted(unresolved.items())
        )
    source_role = (
        f"PRIMARY={_source_label(assignment_file)};"
        f"INDUSTRY_NAME_AND_NATIVE_ID_DIRECTORY={_source_label(directory_file)};"
        f"DIRECTORY_SHA256={directory_file.sha256}"
    )
    if directory_file.source_protocol is not None:
        source_role += f";DIRECTORY_PROTOCOL={directory_file.source_protocol}"
    if directory_file.source_archive_sha256 is not None:
        source_role += f";ARCHIVE_SHA256={directory_file.source_archive_sha256}"
    source_role += ";TYPE_FILTER=2"
    return SectorSourceSnapshot(
        snapshot_id=_snapshot_uuid(
            "TDX_TDXHY_CFG", assignment_file.sha256, "TDX_INDUSTRY_MEMBERSHIP_R1"
        ),
        source_kind="TDX_TDXHY_CFG",
        source_file_sha256=assignment_file.sha256,
        source_path_role=source_role,
        source_file_size=assignment_file.size_bytes,
        observed_at=observed_at,
        source_valid_on=None,
        source_known_at=None,
        source_available_at=None,
        clock_quality="UNKNOWN",
        research_mode="NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT",
        parse_contract_version="TDX_INDUSTRY_MEMBERSHIP_R1",
        rows_declared=len(lines),
        source_quality=source_quality,
        catalog_rows=catalog,
        member_rows=tuple(members),
        unresolved_rows=tuple(sorted(unresolved.items())),
    )


def _concept_snapshot(
    concept_file: SourceFileEvidence, observed_at: datetime
) -> SectorSourceSnapshot:
    raw = concept_file.raw
    lines = raw.splitlines()
    catalogs: list[SectorCatalogRow] = []
    member_occurrences: list[SectorMemberRow] = []
    current: tuple[str, str, str] | None = None
    known_prefixes = {"GN", "FG", "ZS"}
    seen_concept_ids: set[str] = set()
    seen_concept_members: set[tuple[str, str]] = set()
    unresolved = Counter()
    occurrence_ordinal = 0
    style_member_count = 0
    index_member_count = 0
    concept_prefix = {"GN": "CONCEPT", "FG": "STYLE", "ZS": "INDEX_GROUP"}

    for line_number, raw_line in enumerate(lines, 1):
        if not raw_line:
            continue
        if b"\x00" in raw_line:
            raise TdxSectorSourceError(
                "INFOHARBOR_NUL", f"infoharbor_block.dat第{line_number}行含NUL字节"
            )
        if raw_line.startswith(b"#"):
            try:
                fields = raw_line[1:].decode("gb18030", errors="strict").split(",")
            except UnicodeDecodeError as error:
                raise TdxSectorSourceError(
                    "INFOHARBOR_HEADER_DECODE",
                    f"infoharbor_block.dat第{line_number}行头部编码非法",
                ) from error
            if len(fields) != 7 or "_" not in fields[0]:
                raise TdxSectorSourceError(
                    "INFOHARBOR_HEADER_SHAPE",
                    f"infoharbor_block.dat第{line_number}行头部结构无效",
                )
            prefix, name = fields[0].split("_", 1)
            if prefix not in known_prefixes or not name:
                raise TdxSectorSourceError(
                    "INFOHARBOR_BOARD_TYPE_INVALID",
                    f"infoharbor_block.dat第{line_number}行板块类型未知",
                )
            native_id = fields[2].strip()
            if prefix == "GN":
                if re.fullmatch(r"[0-9]{6}", native_id) is None:
                    raise TdxSectorSourceError(
                        "INFOHARBOR_CONCEPT_ID_INVALID",
                        f"infoharbor_block.dat第{line_number}行概念native id无效",
                    )
                if native_id in seen_concept_ids:
                    raise TdxSectorSourceError(
                        "INFOHARBOR_CONCEPT_ID_DUPLICATE",
                        f"infoharbor_block.dat第{line_number}行概念native id重复",
                    )
                seen_concept_ids.add(native_id)
                current_id = native_id
                catalogs.append(SectorCatalogRow("CONCEPT", native_id, name))
            else:
                current_id = native_id or name
            current = (prefix, current_id, name)
            continue

        if current is None:
            raise TdxSectorSourceError(
                "INFOHARBOR_MEMBER_WITHOUT_HEADER",
                f"infoharbor_block.dat第{line_number}行成员缺少板块头",
            )
        parts = raw_line.split(b",")
        if parts and parts[-1] == b"":
            parts = parts[:-1]
        if not parts:
            continue
        for token_ordinal, part in enumerate(parts, 1):
            match = _CONCEPT_MEMBER.fullmatch(part)
            if match is None:
                raise TdxSectorSourceError(
                    "INFOHARBOR_MEMBER_TOKEN_INVALID",
                    f"infoharbor_block.dat第{line_number}行成员token无效",
                )
            market_number = match.group(1).decode("ascii")
            code = match.group(2).decode("ascii")
            occurrence_ordinal += 1
            prefix, native_id, _name = current
            source_symbol = f"{_MARKET_PREFIX[market_number]}.{code}"
            if prefix == "GN":
                key = (native_id, source_symbol)
                if key in seen_concept_members:
                    raise TdxSectorSourceError(
                        "INFOHARBOR_CONCEPT_MEMBER_DUPLICATE",
                        f"infoharbor_block.dat第{line_number}行概念成员重复",
                    )
                seen_concept_members.add(key)
                member_occurrences.append(
                    SectorMemberRow(
                        board_kind="CONCEPT",
                        board_native_id=native_id,
                        source_symbol=source_symbol,
                        membership_quality="OBSERVED_SOURCE_RELATION",
                        source_record_ordinal=occurrence_ordinal,
                    )
                )
            elif prefix == "FG":
                style_member_count += 1
            else:
                index_member_count += 1

    observed_at = _require_observed_at(observed_at)
    if not catalogs:
        raise TdxSectorSourceError(
            "INFOHARBOR_CONCEPT_CATALOG_EMPTY", "概念来源没有GN类型目录"
        )
    catalog_ids = {row.board_native_id for row in catalogs}
    member_board_ids = {row.board_native_id for row in member_occurrences}
    empty_board_count = len(catalog_ids - member_board_ids)
    source_quality = (
        "COMPLETE_NON_PIT;UNMAPPED_SOURCE_ROWS=0;"
        f"EMPTY_BOARDS={empty_board_count};STYLE_MEMBERS_EXCLUDED={style_member_count};"
        f"INDEX_GROUP_MEMBERS_EXCLUDED={index_member_count}"
    )
    return SectorSourceSnapshot(
        snapshot_id=_snapshot_uuid(
            "TDX_INFOHARBOR_BLOCK_DAT",
            concept_file.sha256,
            "TDX_GN_CONCEPT_MEMBERSHIP_R1",
        ),
        source_kind="TDX_INFOHARBOR_BLOCK_DAT",
        source_file_sha256=concept_file.sha256,
        source_path_role=(
            f"PRIMARY={_source_label(concept_file)};"
            "INCLUDED_PREFIX=GN;EXCLUDED_PREFIX=FG,ZS"
            + (
                f";SOURCE_PROTOCOL={concept_file.source_protocol}"
                if concept_file.source_protocol is not None
                else ""
            )
        ),
        source_file_size=concept_file.size_bytes,
        observed_at=observed_at,
        source_valid_on=None,
        source_known_at=None,
        source_available_at=None,
        clock_quality="UNKNOWN",
        research_mode="NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT",
        parse_contract_version="TDX_GN_CONCEPT_MEMBERSHIP_R1",
        rows_declared=len(member_occurrences),
        source_quality=source_quality,
        catalog_rows=tuple(
            sorted(
                catalogs,
                key=lambda row: (row.board_kind, row.board_native_id),
            )
        ),
        member_rows=tuple(
            sorted(
                member_occurrences,
                key=lambda row: (row.board_native_id, row.source_symbol),
            )
        ),
        unresolved_rows=tuple(sorted(unresolved.items())),
    )


def _snapshot_uuid(source_kind: str, source_sha256: str, contract: str) -> str:
    import uuid

    key = f"STOCK_A_TDX_SECTOR|{source_kind}|{source_sha256}|{contract}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def _require_observed_at(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise TdxSectorSourceError(
            "OBSERVED_TIME_INVALID", "来源观察时间必须是带时区的datetime"
        )
    return value


def parse_sector_source_inputs(
    inputs: SectorSourceInputs, *, observed_at: datetime
) -> tuple[SectorSourceSnapshot, SectorSourceSnapshot]:
    """按明示观察时间解析两个真实来源，不读取或修改文件。"""
    assignment = inputs.files["T0002/hq_cache/tdxhy.cfg"]
    directory = inputs.files["T0002/hq_cache/tdxzs.cfg"]
    concept = inputs.files["T0002/hq_cache/infoharbor_block.dat"]
    industry_snapshot = _industry_snapshot(assignment, directory, observed_at)
    concept_snapshot = _concept_snapshot(concept, observed_at)
    return industry_snapshot, concept_snapshot


def parse_remote_sector_source_inputs(
    inputs: SectorSourceInputs, *, observed_at: datetime
) -> tuple[SectorSourceSnapshot, ...]:
    """按远端实际可得类别分别解析，不回退到本地TDX原件。"""
    outcome = parse_remote_sector_source_inputs_with_status(
        inputs, observed_at=observed_at
    )
    if not outcome.snapshots:
        if outcome.rejected_sources:
            first = outcome.rejected_sources[0]
            raise TdxSectorSourceError(first["code"], first["message"])
        raise TdxSectorSourceError(
            "BLOCKED_REMOTE_SOURCE", "TDX远端没有可完整验证的行业或概念成员来源"
        )
    return outcome.snapshots


def parse_remote_sector_source_inputs_with_status(
    inputs: SectorSourceInputs, *, observed_at: datetime
) -> RemoteSectorParseOutcome:
    """分别解析行业与概念，单类坏源不阻塞另一类独立成员来源。"""
    snapshots: list[SectorSourceSnapshot] = []
    rejected: list[dict[str, str]] = []
    assignment = inputs.files.get("T0002/hq_cache/tdxhy.cfg")
    directory = inputs.files.get("T0002/hq_cache/tdxzs.cfg")
    concept = inputs.files.get("T0002/hq_cache/infoharbor_block.dat")
    if assignment is not None and directory is not None:
        try:
            snapshots.append(_industry_snapshot(assignment, directory, observed_at))
        except TdxSectorSourceError as error:
            rejected.append(
                {
                    "source_kind": "TDX_TDXHY_CFG",
                    "code": error.code,
                    "message": str(error),
                }
            )
    elif assignment is not None or directory is not None:
        rejected.append(
            {
                "source_kind": "TDX_TDXHY_CFG",
                "code": "REMOTE_INDUSTRY_SOURCE_INCOMPLETE",
                "message": "tdxhy.cfg与tdxzs.cfg必须来自同一远端服务器并同时完整",
            }
        )
    if concept is not None:
        try:
            snapshots.append(_concept_snapshot(concept, observed_at))
        except TdxSectorSourceError as error:
            rejected.append(
                {
                    "source_kind": "TDX_INFOHARBOR_BLOCK_DAT",
                    "code": error.code,
                    "message": str(error),
                }
            )
    return RemoteSectorParseOutcome(tuple(snapshots), tuple(rejected))
