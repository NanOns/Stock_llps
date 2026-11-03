"""Stock A研究source_discovery职责模块。"""

from __future__ import annotations

import os
from pathlib import Path
import re
import stat
import time
from typing import Any, Iterable, Mapping, Sequence

from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.windows import (
    UniverseDiscoveryBudget,
    _DEFAULT_MAX_DIRECTORY_ENTRIES,
    _DEFAULT_MAX_DISCOVERY_SECONDS,
)

_SYMBOL_PATTERN = re.compile(r"^(SH|SZ)[.]?([0-9]{6})$", re.IGNORECASE)
_DAY_FILENAME_PATTERN = re.compile(r"^(SH|SZ)([0-9]{6})[.]DAY$", re.IGNORECASE)


def _normalize_symbol(value: str) -> tuple[str, str, str]:
    if not isinstance(value, str):
        raise ResearchEntryError("SYMBOL_INVALID", "证券代码必须带SH或SZ市场前缀")
    match = _SYMBOL_PATTERN.fullmatch(value.strip())
    if not match:
        raise ResearchEntryError(
            "SYMBOL_INVALID", "证券代码格式必须是SH.六位代码或SZ.六位代码"
        )
    market, code = match.groups()
    market = market.upper()
    if market == "SH":
        board = "MAIN_BOARD" if code.startswith(("600", "601", "603", "605")) else None
        directory = "sh"
    else:
        if code.startswith(("000", "001", "002", "003")):
            board = "MAIN_BOARD"
        elif code.startswith(("300", "301")):
            board = "GEM"
        else:
            board = None
        directory = "sz"
    if board is None:
        raise ResearchEntryError(
            "SYMBOL_OUT_OF_SCOPE", "代码类别不属于本入口的沪深主板/创业板范围"
        )
    symbol = f"{market}.{code}"
    return symbol, directory, board


def _availability_record(fields: dict[str, Any], *, subject: str) -> dict[str, Any]:
    total = len(fields)
    observed = sum(item.get("quality") == "OBSERVED" for item in fields.values())
    unknown_reasons: dict[str, int] = {}
    for item in fields.values():
        if item.get("quality") != "OBSERVED":
            reason = item.get("reason") or "UNKNOWN_REASON_UNSPECIFIED"
            unknown_reasons[reason] = unknown_reasons.get(reason, 0) + 1
    if observed == total:
        status = "AVAILABLE"
        message = f"{subject}字段全部可用"
    elif observed == 0:
        status = "UNAVAILABLE"
        message = f"{subject}字段均不可用；逐项原因见unknown_reasons"
    else:
        status = "PARTIAL_UNKNOWN"
        message = f"{subject}部分字段可用；本状态不表示全部指标已知"
    result = {
        "status": status,
        "observed_fields": observed,
        "unknown_fields": total - observed,
        "total_fields": total,
        "unknown_reasons": dict(sorted(unknown_reasons.items())),
        "message": message,
    }
    return result


def _aggregate_availability(
    results: list[dict[str, Any]], field_name: str
) -> dict[str, Any]:
    counts = {
        "AVAILABLE": 0,
        "PARTIAL_UNKNOWN": 0,
        "UNAVAILABLE": 0,
        "FAILED": 0,
        "NOT_EVALUATED": 0,
    }
    for item in results:
        item_status = item.get("status")
        if item_status == "FAILED":
            counts["FAILED"] += 1
        elif item_status == "NOT_EVALUATED":
            counts["NOT_EVALUATED"] += 1
        else:
            availability = item.get(field_name, {})
            status = availability.get("status")
            if status in counts:
                counts[status] += 1
            else:
                counts["UNAVAILABLE"] += 1
    return {
        "counts": counts,
        "requested": len(results),
        "message": "仅汇总本次已选页对象；AVAILABLE不代表另一组Raw/Core字段全部已知",
    }


def _discover_source_candidates(
    source_roots: Iterable[str],
    budget: UniverseDiscoveryBudget,
) -> tuple[tuple[dict[str, str], ...], dict[str, Any]]:
    if not isinstance(budget, UniverseDiscoveryBudget):
        raise ResearchEntryError("UNIVERSE_BUDGET_INVALID", "缺少有效的来源发现预算")
    if isinstance(source_roots, (str, bytes, os.PathLike)):
        raise ResearchEntryError("SOURCE_PATH_INVALID", "source_roots必须是根路径序列")
    try:
        configured_roots = tuple(source_roots)
    except TypeError as error:
        raise ResearchEntryError(
            "SOURCE_PATH_INVALID", "source_roots必须是根路径序列"
        ) from error
    if not configured_roots:
        raise ResearchEntryError("SOURCE_NOT_CONFIGURED", "没有配置只读TDX保护根")

    started = time.monotonic()
    roots: list[Path] = []
    for value in configured_roots:
        try:
            root = Path(value).resolve(strict=True)
        except FileNotFoundError as error:
            raise ResearchEntryError(
                "SOURCE_ROOT_INVALID", "配置的TDX来源根不存在"
            ) from error
        except PermissionError as error:
            raise ResearchEntryError(
                "SOURCE_ACCESS_DENIED", "操作系统拒绝核验配置的TDX来源根"
            ) from error
        except (RuntimeError, RecursionError, ValueError) as error:
            raise ResearchEntryError(
                "SOURCE_ROOT_INVALID", "配置的TDX来源根无法安全解析"
            ) from error
        except (OSError, TypeError) as error:
            raise ResearchEntryError(
                "SOURCE_ROOT_INVALID", "配置的TDX来源根无法核验"
            ) from error
        if not root.is_dir():
            raise ResearchEntryError("SOURCE_ROOT_INVALID", "配置的TDX来源根不是目录")
        roots.append(root)

    candidates_by_symbol: dict[str, dict[str, str]] = {}
    candidate_sources: dict[str, str] = {}
    directory_summaries: list[dict[str, Any]] = []
    directory_entries_scanned = 0
    day_file_entries = 0
    excluded_out_of_scope = 0
    invalid_day_filenames = 0
    invalid_day_filename_samples: list[str] = []
    by_market = {"SH": 0, "SZ": 0}
    by_board = {"MAIN_BOARD": 0, "GEM": 0}
    for root in roots:
        for market, directory in (("SH", "sh"), ("SZ", "sz")):
            logical_directory = root / "vipdoc" / directory / "lday"
            try:
                resolved_directory = logical_directory.resolve(strict=True)
            except FileNotFoundError as error:
                raise ResearchEntryError(
                    "SOURCE_DIRECTORY_UNAVAILABLE",
                    f"缺少必需的TDX来源目录vipdoc/{directory}/lday；无法给出完整候选分母",
                    context={
                        "directory": f"vipdoc/{directory}/lday",
                        "discovery_complete": False,
                    },
                ) from error
            except PermissionError as error:
                raise ResearchEntryError(
                    "SOURCE_ACCESS_DENIED", "操作系统拒绝核验TDX来源目录"
                ) from error
            except (RuntimeError, RecursionError, ValueError) as error:
                raise ResearchEntryError(
                    "SOURCE_PATH_INVALID",
                    f"TDX来源目录vipdoc/{directory}/lday无法安全解析",
                    context={
                        "directory": f"vipdoc/{directory}/lday",
                        "discovery_complete": False,
                    },
                ) from error
            except OSError as error:
                raise ResearchEntryError(
                    "SOURCE_IO_ERROR", "核验TDX来源目录时发生I/O错误"
                ) from error
            try:
                resolved_directory.relative_to(root)
            except ValueError as error:
                raise ResearchEntryError(
                    "SOURCE_PATH_OUTSIDE_ROOT",
                    "TDX行情目录最终路径离开已配置来源保护根",
                    context={
                        "directory": f"vipdoc/{directory}/lday",
                        "discovery_complete": False,
                    },
                ) from error
            try:
                directory_stat_before = resolved_directory.stat()
            except PermissionError as error:
                raise ResearchEntryError(
                    "SOURCE_ACCESS_DENIED", "操作系统拒绝读取TDX来源目录"
                ) from error
            except OSError as error:
                raise ResearchEntryError(
                    "SOURCE_IO_ERROR", "读取TDX来源目录状态时发生I/O错误"
                ) from error
            if not stat.S_ISDIR(directory_stat_before.st_mode):
                raise ResearchEntryError(
                    "SOURCE_DIRECTORY_UNAVAILABLE",
                    f"TDX来源路径vipdoc/{directory}/lday不是目录",
                    context={
                        "directory": f"vipdoc/{directory}/lday",
                        "discovery_complete": False,
                    },
                )

            entry_count = 0
            day_count = 0
            directory_candidates = 0
            directory_excluded = 0
            directory_invalid_names = 0
            try:
                with os.scandir(resolved_directory) as iterator:
                    for entry in iterator:
                        entry_count += 1
                        directory_entries_scanned += 1
                        if directory_entries_scanned > budget.max_directory_entries:
                            raise ResearchEntryError(
                                "UNIVERSE_DISCOVERY_BUDGET_EXCEEDED",
                                "来源候选目录项超过发现预算；已停止且不声明完整候选总数",
                                context={
                                    "discovery_complete": False,
                                    "directory_entries_scanned": directory_entries_scanned,
                                    "day_file_entries_seen": day_file_entries,
                                    "max_directory_entries": budget.max_directory_entries,
                                },
                            )
                        if (
                            entry_count % 256 == 0
                            and time.monotonic() - started >= budget.max_seconds
                        ):
                            raise ResearchEntryError(
                                "UNIVERSE_DISCOVERY_BUDGET_EXCEEDED",
                                "来源候选发现超过耗时预算；已停止且不声明完整候选总数",
                                context={
                                    "discovery_complete": False,
                                    "directory_entries_scanned": directory_entries_scanned,
                                    "day_file_entries_seen": day_file_entries,
                                    "max_seconds": budget.max_seconds,
                                },
                            )
                        if not entry.name.casefold().endswith(".day"):
                            continue
                        day_file_entries += 1
                        day_count += 1
                        entry_path = Path(entry.path)
                        try:
                            resolved_entry = entry_path.resolve(strict=True)
                            entry_stat = resolved_entry.stat()
                        except FileNotFoundError as error:
                            raise ResearchEntryError(
                                "SOURCE_CANDIDATE_CHANGED",
                                "枚举期间TDX日线来源文件消失；候选总分母不完整",
                                context={
                                    "directory": f"vipdoc/{directory}/lday",
                                    "discovery_complete": False,
                                },
                            ) from error
                        except PermissionError as error:
                            raise ResearchEntryError(
                                "SOURCE_ACCESS_DENIED", "操作系统拒绝核验TDX日线文件"
                            ) from error
                        except (RuntimeError, RecursionError, ValueError) as error:
                            raise ResearchEntryError(
                                "SOURCE_PATH_INVALID",
                                "TDX日线候选最终路径无法安全解析",
                                context={
                                    "filename": entry.name,
                                    "discovery_complete": False,
                                },
                            ) from error
                        except OSError as error:
                            raise ResearchEntryError(
                                "SOURCE_IO_ERROR", "核验TDX日线文件时发生I/O错误"
                            ) from error
                        try:
                            resolved_entry.relative_to(root)
                        except ValueError as error:
                            raise ResearchEntryError(
                                "SOURCE_PATH_OUTSIDE_ROOT",
                                "TDX日线候选最终路径离开已配置来源保护根",
                                context={
                                    "filename": entry.name,
                                    "discovery_complete": False,
                                },
                            ) from error
                        if not stat.S_ISREG(entry_stat.st_mode):
                            raise ResearchEntryError(
                                "SOURCE_CANDIDATE_INVALID",
                                "后缀为.day的来源候选不是普通文件",
                                context={
                                    "filename": entry.name,
                                    "discovery_complete": False,
                                },
                            )
                        match = _DAY_FILENAME_PATTERN.fullmatch(entry.name)
                        if match is None or match.group(1).upper() != market:
                            invalid_day_filenames += 1
                            directory_invalid_names += 1
                            if len(invalid_day_filename_samples) < 100:
                                invalid_day_filename_samples.append(
                                    f"vipdoc/{directory}/lday/{entry.name}"
                                )
                            continue
                        code = match.group(2)
                        try:
                            symbol, _resolved_market_dir, board = _normalize_symbol(
                                f"{market}.{code}"
                            )
                        except ResearchEntryError as error:
                            if error.code != "SYMBOL_OUT_OF_SCOPE":
                                raise
                            excluded_out_of_scope += 1
                            directory_excluded += 1
                            continue
                        if symbol in candidates_by_symbol:
                            raise ResearchEntryError(
                                "UNIVERSE_DUPLICATE_SYMBOL",
                                "多个TDX目录项归一为同一来源代码；拒绝静默去重",
                                context={
                                    "symbol": symbol,
                                    "first_source": candidate_sources[symbol],
                                    "duplicate_source": f"vipdoc/{directory}/lday/{entry.name}",
                                    "discovery_complete": False,
                                },
                            )
                        candidates_by_symbol[symbol] = {
                            "symbol": symbol,
                            "market": market,
                            "board": board,
                        }
                        candidate_sources[symbol] = (
                            f"vipdoc/{directory}/lday/{entry.name}"
                        )
                        directory_candidates += 1
                        by_market[market] += 1
                        by_board[board] += 1
            except ResearchEntryError:
                raise
            except PermissionError as error:
                raise ResearchEntryError(
                    "SOURCE_ACCESS_DENIED", "操作系统拒绝枚举TDX来源目录"
                ) from error
            except OSError as error:
                raise ResearchEntryError(
                    "SOURCE_IO_ERROR", "枚举TDX来源目录时发生I/O错误"
                ) from error
            try:
                directory_stat_after = resolved_directory.stat()
            except PermissionError as error:
                raise ResearchEntryError(
                    "SOURCE_ACCESS_DENIED", "操作系统拒绝复核TDX来源目录"
                ) from error
            except OSError as error:
                raise ResearchEntryError(
                    "SOURCE_IO_ERROR", "复核TDX来源目录时发生I/O错误"
                ) from error
            identity_before = (
                directory_stat_before.st_dev,
                directory_stat_before.st_ino,
                directory_stat_before.st_mtime_ns,
            )
            identity_after = (
                directory_stat_after.st_dev,
                directory_stat_after.st_ino,
                directory_stat_after.st_mtime_ns,
            )
            if identity_before != identity_after:
                raise ResearchEntryError(
                    "SOURCE_DIRECTORY_CHANGED",
                    "候选发现期间TDX目录发生变化；拒绝声明稳定分母",
                    context={
                        "directory": f"vipdoc/{directory}/lday",
                        "discovery_complete": False,
                    },
                )
            directory_summaries.append(
                {
                    "market": market,
                    "directory": f"vipdoc/{directory}/lday",
                    "directory_entries": entry_count,
                    "day_file_entries": day_count,
                    "in_scope_candidates": directory_candidates,
                    "excluded_out_of_scope": directory_excluded,
                    "invalid_day_filenames": directory_invalid_names,
                }
            )
            if time.monotonic() - started >= budget.max_seconds:
                raise ResearchEntryError(
                    "UNIVERSE_DISCOVERY_BUDGET_EXCEEDED",
                    "来源候选发现超过耗时预算；已停止且不声明完整候选总数",
                    context={
                        "discovery_complete": False,
                        "directory_entries_scanned": directory_entries_scanned,
                        "day_file_entries_seen": day_file_entries,
                        "max_seconds": budget.max_seconds,
                    },
                )

    candidates = tuple(
        candidates_by_symbol[name] for name in sorted(candidates_by_symbol)
    )
    return candidates, {
        "contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
        "complete": True,
        "configured_root_count": len(roots),
        "directories": directory_summaries,
        "directory_entries_scanned": directory_entries_scanned,
        "day_file_entries": day_file_entries,
        "candidate_count": len(candidates),
        "excluded_out_of_scope": excluded_out_of_scope,
        "invalid_day_filenames": invalid_day_filenames,
        "invalid_day_filename_samples": sorted(invalid_day_filename_samples),
        "invalid_day_filename_sample_limit": 100,
        "candidate_counts_by_market": by_market,
        "candidate_counts_by_board": by_board,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "identity_disclosure": "文件名仅形成TDX来源代码候选；不证明稳定实体、上市、当日可交易、ST状态或来源更新时点",
    }
