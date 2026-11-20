from __future__ import annotations

import hashlib
import json
import os
from datetime import date
from pathlib import Path
import threading
from typing import Any, Mapping
from uuid import UUID


_JOURNAL_CONTRACT = "STOCK_A_BAOSTOCK_RAW_RESPONSE_JOURNAL/V1"
_JOURNAL_ROOT = Path(
    "G:/codex work/Stock A 交付/过程资料/SOURCE-SWITCH-01/BaoStock原始响应"
)
_COORDINATE_BY_FLAG = {"3": "RAW_UNADJUSTED", "2": "FORWARD_ADJUSTED"}


class BaoStockRawResponseJournalError(RuntimeError):
    """BaoStock原始响应档案写入或完整性校验失败。"""


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


class BaoStockRawResponseJournal:
    """本批reset/修复resume专用、逐代码坐标追加的原始响应JSONL档案。"""

    def __init__(
        self,
        *,
        job_id: UUID,
        capture_uuid: UUID,
        cutoff: date,
        range_start_session: date,
        range_end_session: date,
        root: Path | None = None,
    ) -> None:
        if (
            not isinstance(job_id, UUID)
            or not isinstance(capture_uuid, UUID)
            or type(cutoff) is not date
            or type(range_start_session) is not date
            or type(range_end_session) is not date
            or range_start_session > range_end_session
            or range_end_session > cutoff
        ):
            raise BaoStockRawResponseJournalError("原始响应档案范围或身份无效")
        configured_root = _JOURNAL_ROOT if root is None else root
        if not isinstance(configured_root, Path):
            raise BaoStockRawResponseJournalError("原始响应档案根目录类型无效")
        try:
            configured_root.mkdir(parents=True, exist_ok=True)
            self.root = configured_root.resolve(strict=True)
            if configured_root.is_symlink():
                raise BaoStockRawResponseJournalError("原始响应档案目录不能是符号链接")
            self.path = self.root / (
                f"BaoStock原始响应_{capture_uuid.hex}_{cutoff:%Y%m%d}.jsonl"
            )
            if self.path.exists() and self.path.is_symlink():
                raise BaoStockRawResponseJournalError("原始响应档案文件不能是符号链接")
        except BaoStockRawResponseJournalError:
            raise
        except OSError as error:
            raise BaoStockRawResponseJournalError(
                "无法准备G盘原始响应档案目录"
            ) from error
        self.job_id = job_id
        self.capture_uuid = capture_uuid
        self.cutoff = cutoff
        self.range_start_session = range_start_session
        self.range_end_session = range_end_session
        self._lock = threading.RLock()
        self._records: dict[str, tuple[int, int, str]] = {}
        self._record_success: dict[str, bool] = {}
        self._load_existing_records()

    @property
    def response_count(self) -> int:
        """返回已校验并可复用的成功原始响应数。"""
        return len(self._records)

    def has_other_capture_archives(self) -> bool:
        """报告同一任务目录是否已有其它捕获身份的响应档案。"""
        return any(
            item.is_file() and item != self.path
            for item in self.root.glob("BaoStock原始响应_*.jsonl")
        )

    def all_requests_complete(self, source_symbols: tuple[str, ...]) -> bool:
        """仅用构造时逐行校验过的索引核验双坐标请求，不重复读取大响应。"""
        if (
            not source_symbols
            or len(source_symbols) * 2 != len(self._records)
            or len(set(source_symbols)) != len(source_symbols)
        ):
            return False
        return all(
            self._record_success.get(self._request_key(symbol, flag)) is True
            for symbol in source_symbols
            for flag in ("3", "2")
        )

    def _request_key(self, source_symbol: str, adjustment_flag: str) -> str:
        material = {
            "contract": _JOURNAL_CONTRACT,
            "capture_uuid": str(self.capture_uuid),
            "source_symbol": source_symbol,
            "adjustment_flag": adjustment_flag,
            "price_coordinate": _COORDINATE_BY_FLAG[adjustment_flag],
            "range_start_session": self.range_start_session.isoformat(),
            "range_end_session": self.range_end_session.isoformat(),
            "cutoff": self.cutoff.isoformat(),
        }
        return hashlib.sha256(_canonical_bytes(material)).hexdigest()

    @staticmethod
    def _record_digest(record: Mapping[str, Any]) -> str:
        material = {key: value for key, value in record.items() if key != "sha256"}
        return hashlib.sha256(_canonical_bytes(material)).hexdigest()

    def _validate_record(self, record: Any) -> dict[str, Any]:
        if not isinstance(record, dict) or record.get("contract") != _JOURNAL_CONTRACT:
            raise BaoStockRawResponseJournalError("JSONL记录合同版本无效")
        source_symbol = record.get("source_symbol")
        adjustment_flag = record.get("adjustment_flag")
        request_key = record.get("request_key")
        fields = record.get("response_fields")
        raw_fields = record.get("raw_response_fields")
        rows = record.get("rows")
        raw_row_values = record.get("raw_row_values")
        if (
            not isinstance(source_symbol, str)
            or adjustment_flag not in _COORDINATE_BY_FLAG
            or not isinstance(request_key, str)
            or request_key != self._request_key(source_symbol, adjustment_flag)
            or record.get("capture_uuid") != str(self.capture_uuid)
            or record.get("range_start_session") != self.range_start_session.isoformat()
            or record.get("range_end_session") != self.range_end_session.isoformat()
            or record.get("cutoff") != self.cutoff.isoformat()
            or record.get("status") not in {"RESPONSE_RECEIVED", "QUERY_FAILED"}
            or type(record.get("complete")) is not bool
            or record.get("query_error_code") != "0"
            or not isinstance(fields, list)
            or any(not isinstance(field, str) for field in fields)
            or raw_fields != fields
            or not isinstance(rows, list)
            or any(
                not isinstance(row, dict)
                or any(
                    not isinstance(key, str) or not isinstance(value, str)
                    for key, value in row.items()
                )
                for row in rows
            )
            or not isinstance(raw_row_values, list)
            or any(
                not isinstance(raw_row, list)
                or any(not isinstance(value, str) for value in raw_row)
                for raw_row in raw_row_values
            )
            or record.get("row_count") != len(raw_row_values)
            or record.get("normalized_candidate_row_count") != len(rows)
            or not isinstance(record.get("request_started_at"), str)
            or not isinstance(record.get("request_completed_at"), str)
            or (
                record.get("query_error_message") is not None
                and not isinstance(record.get("query_error_message"), str)
            )
            or record.get("local_error") is not None
            and not isinstance(record.get("local_error"), str)
            or type(record.get("request_attempt_number")) is not int
            or record.get("request_attempt_number", 0) < 1
            or not isinstance(record.get("job_id"), str)
            or record.get("sha256") != self._record_digest(record)
        ):
            raise BaoStockRawResponseJournalError(
                "JSONL记录键、原始字段、行、时钟或摘要校验失败"
            )
        date_indexes = [
            index for index, field in enumerate(raw_fields) if field == "date"
        ]
        if raw_row_values and not date_indexes:
            raise BaoStockRawResponseJournalError(
                "SOURCE_RANGE_VIOLATION:原始行缺少date字段"
            )
        for raw_row in raw_row_values:
            for date_index in date_indexes:
                if date_index >= len(raw_row):
                    raise BaoStockRawResponseJournalError(
                        "SOURCE_RANGE_VIOLATION:原始行缺少date值"
                    )
                raw_session = raw_row[date_index]
                parsed_session = None
                for candidate in (raw_session[:10], raw_session):
                    try:
                        parsed_session = date.fromisoformat(candidate)
                        break
                    except ValueError:
                        continue
                if parsed_session is not None and (
                    parsed_session < self.range_start_session
                    or parsed_session > self.range_end_session
                    or parsed_session > self.cutoff
                ):
                    raise BaoStockRawResponseJournalError(
                        "SOURCE_RANGE_VIOLATION:JSONL原始行超出request range/cutoff"
                    )
        return record

    def _load_existing_records(self) -> None:
        if not self.path.exists():
            return
        try:
            offset = 0
            last_complete_offset = 0
            file_size = self.path.stat().st_size
            with self.path.open("rb") as handle:
                while True:
                    record_offset = handle.tell()
                    line = handle.readline()
                    if not line:
                        break
                    offset += len(line)
                    if not line.endswith(b"\n"):
                        break
                    try:
                        record = self._validate_record(json.loads(line.decode("utf-8")))
                    except Exception as error:
                        raise BaoStockRawResponseJournalError(
                            "原始响应JSONL存在完整但无效的记录"
                        ) from error
                    request_key = record["request_key"]
                    digest = record["sha256"]
                    existing = self._records.get(request_key)
                    if existing is not None and existing[2] != digest:
                        raise BaoStockRawResponseJournalError(
                            "同一请求键在原始响应JSONL中对应不同成功响应"
                        )
                    self._records[request_key] = (record_offset, len(line), digest)
                    self._record_success[request_key] = (
                        record.get("status") == "RESPONSE_RECEIVED"
                        and record.get("complete") is True
                        and record.get("query_error_code") == "0"
                    )
                    last_complete_offset = offset
            if last_complete_offset != file_size:
                with self.path.open("r+b") as handle:
                    handle.truncate(last_complete_offset)
                    handle.flush()
                    os.fsync(handle.fileno())
        except BaoStockRawResponseJournalError:
            raise
        except OSError as error:
            raise BaoStockRawResponseJournalError(
                "读取或修复原始响应JSONL失败"
            ) from error

    def _read_indexed_record(
        self, request_key: str, reference: tuple[int, int, str]
    ) -> dict[str, Any]:
        offset, length, expected_digest = reference
        try:
            with self.path.open("rb") as handle:
                handle.seek(offset)
                line = handle.read(length)
            if len(line) != length or not line.endswith(b"\n"):
                raise BaoStockRawResponseJournalError("原始响应档案索引指向不完整记录")
            record = self._validate_record(json.loads(line.decode("utf-8")))
        except BaoStockRawResponseJournalError:
            raise
        except Exception as error:
            raise BaoStockRawResponseJournalError(
                "原始响应档案索引记录无法读回或校验"
            ) from error
        if (
            record.get("request_key") != request_key
            or record.get("sha256") != expected_digest
        ):
            raise BaoStockRawResponseJournalError(
                "原始响应档案索引与记录键或摘要不匹配"
            )
        return record

    def read_response(
        self, *, source_symbol: str, adjustment_flag: str
    ) -> dict[str, Any] | None:
        with self._lock:
            request_key = self._request_key(source_symbol, adjustment_flag)
            reference = self._records.get(request_key)
            if reference is None:
                return None
            record = self._read_indexed_record(request_key, reference)
            return {
                "status": record["status"],
                "complete": record["complete"],
                "adjustflag": adjustment_flag,
                "coordinate": _COORDINATE_BY_FLAG[adjustment_flag],
                "query_error_code": record["query_error_code"],
                "query_error_message": record["query_error_message"],
                "response_fields": list(record["response_fields"]),
                "raw_response_fields": list(record["raw_response_fields"]),
                "rows": [dict(row) for row in record["rows"]],
                "raw_row_values": [list(row) for row in record["raw_row_values"]],
                "request_started_at": record["request_started_at"],
                "request_completed_at": record["request_completed_at"],
                "local_error": record.get("local_error"),
                "request_attempt_number": record["request_attempt_number"],
                "response_row_count": record["row_count"],
                "journal_reused": True,
            }

    def archive_response(
        self,
        *,
        source_symbol: str,
        adjustment_flag: str,
        response: Mapping[str, Any],
        request_attempt_number: int,
    ) -> dict[str, Any]:
        query_error_code = response.get("query_error_code")
        if (
            adjustment_flag not in _COORDINATE_BY_FLAG
            or not isinstance(response, Mapping)
            or response.get("status") not in {"RESPONSE_RECEIVED", "QUERY_FAILED"}
            or type(response.get("complete")) is not bool
            or str(query_error_code) != "0"
            or response.get("adjustflag") != adjustment_flag
            or response.get("coordinate") != _COORDINATE_BY_FLAG[adjustment_flag]
            or type(request_attempt_number) is not int
            or request_attempt_number < 1
        ):
            raise BaoStockRawResponseJournalError(
                "拒绝归档未完整或坐标不符的BaoStock响应"
            )
        fields = response.get("response_fields")
        rows = response.get("rows")
        raw_fields = response.get("raw_response_fields", fields)
        raw_row_values = response.get("raw_row_values")
        if (
            not isinstance(source_symbol, str)
            or not isinstance(fields, (list, tuple))
            or any(not isinstance(field, str) for field in fields)
            or not isinstance(raw_fields, (list, tuple))
            or any(not isinstance(field, str) for field in raw_fields)
            or not isinstance(rows, list)
            or not isinstance(raw_row_values, list)
        ):
            raise BaoStockRawResponseJournalError("BaoStock原始响应字段或行结构无效")
        date_indexes = [
            index for index, field in enumerate(raw_fields) if field == "date"
        ]
        if raw_row_values and not date_indexes:
            raise BaoStockRawResponseJournalError(
                "SOURCE_RANGE_VIOLATION:原始行缺少date字段，拒绝归档或写PG"
            )
        for raw_row in raw_row_values:
            for date_index in date_indexes:
                if date_index >= len(raw_row):
                    raise BaoStockRawResponseJournalError(
                        "SOURCE_RANGE_VIOLATION:原始行缺少date值，拒绝归档或写PG"
                    )
                raw_session = raw_row[date_index]
                try:
                    parsed_session = date.fromisoformat(raw_session)
                except ValueError:
                    continue
                if (
                    parsed_session.isoformat() != raw_session
                    or parsed_session < self.range_start_session
                    or parsed_session > self.range_end_session
                    or parsed_session > self.cutoff
                ):
                    raise BaoStockRawResponseJournalError(
                        "SOURCE_RANGE_VIOLATION:原始行情行超出request range/cutoff，已拒绝归档"
                    )
        request_key = self._request_key(source_symbol, adjustment_flag)
        record: dict[str, Any] = {
            "contract": _JOURNAL_CONTRACT,
            "request_key": request_key,
            "job_id": str(self.job_id),
            "capture_uuid": str(self.capture_uuid),
            "source_symbol": source_symbol,
            "adjustment_flag": adjustment_flag,
            "price_coordinate": _COORDINATE_BY_FLAG[adjustment_flag],
            "range_start_session": self.range_start_session.isoformat(),
            "range_end_session": self.range_end_session.isoformat(),
            "cutoff": self.cutoff.isoformat(),
            "request_attempt_number": request_attempt_number,
            "status": response.get("status"),
            "complete": response.get("complete"),
            "request_started_at": response.get("request_started_at"),
            "request_completed_at": response.get("request_completed_at"),
            "query_error_code": str(query_error_code),
            "query_error_message": response.get("query_error_message"),
            "local_error": response.get("local_error"),
            "response_fields": list(raw_fields),
            "raw_response_fields": list(raw_fields),
            "row_count": len(raw_row_values),
            "normalized_candidate_row_count": len(rows),
            "rows": [dict(row) for row in rows],
            "raw_row_values": [list(row) for row in raw_row_values],
        }
        record["sha256"] = self._record_digest(record)
        self._validate_record(record)
        with self._lock:
            existing_reference = self._records.get(request_key)
            if existing_reference is not None:
                existing = self._read_indexed_record(request_key, existing_reference)
                if existing.get("sha256") != record.get("sha256"):
                    raise BaoStockRawResponseJournalError(
                        "同一请求键出现不同成功响应，拒绝覆盖原始档案"
                    )
                return {
                    "request_key": request_key,
                    "sha256": existing["sha256"],
                    "request_attempt_number": existing["request_attempt_number"],
                    "reused": True,
                }
            encoded = _canonical_bytes(record) + b"\n"
            try:
                with self.path.open("ab") as handle:
                    record_offset = handle.tell()
                    handle.write(encoded)
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError as error:
                raise BaoStockRawResponseJournalError(
                    "原始响应未能flush+fsync到G盘，拒绝继续PG价格持久化"
                ) from error
            self._records[request_key] = (
                record_offset,
                len(encoded),
                record["sha256"],
            )
            self._record_success[request_key] = (
                record.get("status") == "RESPONSE_RECEIVED"
                and record.get("complete") is True
                and record.get("query_error_code") == "0"
            )
        return {
            "request_key": request_key,
            "sha256": record["sha256"],
            "request_attempt_number": request_attempt_number,
            "reused": False,
        }
