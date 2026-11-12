"""只读解析本机TDX证券名称字段，输出非PIT展示标签。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import stat


DISPLAY_NAME_PARSE_CONTRACT = "TDX_TNF_DISPLAY_NAME_R1"
_FILES = {"SH": "shs.tnf", "SZ": "szs.tnf", "BJ": "bjs.tnf"}
_TNF_RECORD = re.compile(rb"(?<![0-9])([0-9]{6})\x00{25}([^\x00]{1,16})\x00")
_MAX_FILE_BYTES = 16 * 1024 * 1024
_MAX_TOTAL_BYTES = 40 * 1024 * 1024
_NAME_AUTHORITY = "UNVERIFIED_LOCAL_TDX_INSTALLATION"


class TdxDisplayNameError(ValueError):
    """本机名称文件路径、结构或编码不满足展示标签合同。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class DisplayNameRow:
    source_symbol: str
    market: str
    display_name: str
    source_file: str


@dataclass(frozen=True)
class DisplayNameSnapshot:
    captured_at: datetime
    source_root: str
    source_file_sizes: dict[str, int]
    parsed_record_counts: dict[str, int]
    rejected_truncated_name_counts: dict[str, int]
    rows: tuple[DisplayNameRow, ...]


def _read_file(root: Path, relative_path: str) -> tuple[bytes, int]:
    root_path = root.resolve(strict=True)
    candidate = root_path.joinpath(*relative_path.split("/"))
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root_path)
        before = resolved.stat()
    except FileNotFoundError as error:
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SOURCE_MISSING", f"缺少名称来源文件：{relative_path}"
        ) from error
    except PermissionError as error:
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SOURCE_ACCESS_DENIED", "操作系统拒绝只读名称文件"
        ) from error
    except (OSError, ValueError) as error:
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SOURCE_PATH_INVALID", "名称文件路径无效或越出配置TDX根"
        ) from error
    if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SOURCE_FILE_INVALID", "名称来源必须是非空普通文件"
        )
    if before.st_size > _MAX_FILE_BYTES:
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SOURCE_FILE_TOO_LARGE", "单个名称来源文件超过16MiB"
        )
    try:
        raw = resolved.read_bytes()
        after = resolved.stat()
    except PermissionError as error:
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SOURCE_ACCESS_DENIED", "操作系统拒绝只读名称文件"
        ) from error
    except OSError as error:
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SOURCE_IO_ERROR", "读取本机名称来源文件发生I/O错误"
        ) from error
    if (
        len(raw) != before.st_size
        or after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
        or getattr(after, "st_ino", None) != getattr(before, "st_ino", None)
    ):
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SOURCE_CHANGED", "名称来源文件在读取期间发生变化"
        )
    return raw, len(raw)


def parse_tdx_display_name_snapshot(
    source_root: str | Path, *, captured_at: datetime
) -> DisplayNameSnapshot:
    """按市场文件解析GBK名称；不为损坏或截断字段补字。"""
    if (
        not isinstance(captured_at, datetime)
        or captured_at.tzinfo is None
        or captured_at.utcoffset() is None
    ):
        raise TdxDisplayNameError(
            "DISPLAY_NAME_CAPTURE_TIME_INVALID", "名称观察时间必须带时区"
        )
    try:
        root = Path(source_root).resolve(strict=True)
    except FileNotFoundError as error:
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SOURCE_ROOT_MISSING", "配置的TDX名称来源根不存在"
        ) from error
    except (OSError, ValueError) as error:
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SOURCE_ROOT_INVALID", "配置的TDX名称来源根不可解析"
        ) from error
    if not root.is_dir():
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SOURCE_ROOT_INVALID", "配置的TDX名称来源根不是目录"
        )

    rows: list[DisplayNameRow] = []
    file_sizes: dict[str, int] = {}
    record_counts: dict[str, int] = {}
    rejected_counts: dict[str, int] = {}
    seen_symbols: set[str] = set()
    for market, filename in _FILES.items():
        relative_path = f"T0002/hq_cache/{filename}"
        raw, size = _read_file(root, relative_path)
        file_sizes[filename] = size
        matches = tuple(_TNF_RECORD.finditer(raw))
        if len(matches) < (100 if market == "BJ" else 1000):
            raise TdxDisplayNameError(
                "DISPLAY_NAME_TNF_PARTITION_INCOMPLETE",
                f"{filename}解析到的名称记录数异常偏少",
            )
        record_counts[filename] = len(matches)
        rejected = 0
        file_symbols: set[str] = set()
        for match in matches:
            code = match.group(1).decode("ascii")
            symbol = f"{market}.{code}"
            if symbol in file_symbols:
                raise TdxDisplayNameError(
                    "DISPLAY_NAME_TNF_DUPLICATE_SYMBOL",
                    f"{filename}含重复来源代码，无法确定唯一显示标签",
                )
            file_symbols.add(symbol)
            try:
                name = match.group(2).decode("gbk").strip()
            except UnicodeDecodeError:
                rejected += 1
                continue
            if not name or any(ord(char) < 32 for char in name) or len(name) > 40:
                rejected += 1
                continue
            rows.append(
                DisplayNameRow(
                    source_symbol=symbol,
                    market=market,
                    display_name=name,
                    source_file=filename,
                )
            )
            seen_symbols.add(symbol)
        rejected_counts[filename] = rejected

    if sum(file_sizes.values()) > _MAX_TOTAL_BYTES:
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SOURCE_TOTAL_TOO_LARGE", "名称来源文件合计超过40MiB"
        )
    if not rows or len(seen_symbols) != len(rows):
        raise TdxDisplayNameError(
            "DISPLAY_NAME_SNAPSHOT_NOT_UNIQUE", "名称快照没有可用行或含重复来源键"
        )
    return DisplayNameSnapshot(
        captured_at=captured_at.astimezone(timezone.utc),
        source_root=str(root),
        source_file_sizes=file_sizes,
        parsed_record_counts=record_counts,
        rejected_truncated_name_counts=rejected_counts,
        rows=tuple(sorted(rows, key=lambda item: item.source_symbol)),
    )


__all__ = [
    "DISPLAY_NAME_PARSE_CONTRACT",
    "DisplayNameRow",
    "DisplayNameSnapshot",
    "TdxDisplayNameError",
    "parse_tdx_display_name_snapshot",
]
