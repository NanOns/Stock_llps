"""BaoStock单股历史日线的有界只读查询边界。"""
from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timezone
import importlib
from importlib import metadata
from dataclasses import dataclass
from hashlib import sha256
import io
import json
import re
import socket
from typing import Any
from uuid import uuid4

from stock_a.domain import ContractError


_SYMBOL_PATTERN = re.compile(r"^(SH|SZ)\.([0-9]{6})$")
_PINNED_SDK_VERSION = "0.9.3"
_QUERY_FIELDS = "date,code,open,high,low,close,volume,amount,adjustflag,turn,tradestatus,isST"
_MAX_QUERY_ROWS = 8
_DEFAULT_SOCKET_TIMEOUT_SECONDS = 30.0
_VERIFIED_DAILY_CAPTURES: dict[str, "BaoStockDailySourceCapture"] = {}
_VERIFIED_IDENTITY_CAPTURES: dict[str, "BaoStockIdentitySourceCapture"] = {}


@dataclass(frozen=True)
class BaoStockDailySourceCapture:
    """本进程内由真实SDK成功响应签发的单代码/单日原始行绑定。"""

    capture_id: str
    source_symbol: str
    target_session: date
    adjustflag: str
    amount_raw: str
    volume_raw: str
    source_sha256: str
    captured_at: datetime


@dataclass(frozen=True)
class BaoStockIdentitySourceCapture:
    """本进程内由真实SDK成功身份接口响应签发的内容摘要绑定。"""

    capture_id: str
    source_sha256: str
    canonical_digest: str
    normalized_count: int
    request_started_at: datetime
    request_completed_at: datetime


def resolve_verified_baostock_capture(capture_id: str) -> BaoStockDailySourceCapture | None:
    """解析本进程内真实SDK签发的来源引用；合成transport不会进入注册表。"""
    if not isinstance(capture_id, str):
        return None
    return _VERIFIED_DAILY_CAPTURES.get(capture_id)


def resolve_verified_baostock_identity_capture(capture_id: str) -> BaoStockIdentitySourceCapture | None:
    """解析真实SDK完成的身份查询封套；伪造Mapping与测试transport不签发。"""
    if not isinstance(capture_id, str):
        return None
    return _VERIFIED_IDENTITY_CAPTURES.get(capture_id)


def _issue_verified_identity_capture(snapshot: dict[str, Any], *, injected_transport: bool) -> str | None:
    if injected_transport or snapshot.get("status") != "RESPONSE_RECEIVED":
        return None
    calls = snapshot.get("calls")
    if not isinstance(calls, dict) or not calls:
        return None
    flat_calls: list[dict[str, Any]] = []
    for value in calls.values():
        if isinstance(value, dict) and "query" in value:
            flat_calls.append(value)
        elif isinstance(value, dict):
            flat_calls.extend(item for item in value.values() if isinstance(item, dict))
    if (not flat_calls or any(item.get("status") != "RESPONSE_RECEIVED" or item.get("complete") is not True
                              for item in flat_calls)):
        return None
    rows_count = 0
    for item in flat_calls:
        rows, fields = item.get("rows"), item.get("response_fields")
        if not isinstance(rows, list) or not isinstance(fields, list) or item.get("rows_seen") != len(rows):
            return None
        rows_count += len(rows)
    if rows_count <= 0:
        return None
    projection = {
        "contract": snapshot.get("contract"), "source": snapshot.get("source"),
        "calls": [{"query": item.get("query"), "target_session": item.get("target_session"),
                   "response_fields": item.get("response_fields"), "rows": item.get("rows")}
                  for item in flat_calls],
    }
    try:
        evidence = json.dumps(projection, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False).encode("utf-8")
        started = min(datetime.fromisoformat(item["request_started_at"]) for item in flat_calls)
        completed = max(datetime.fromisoformat(item["request_completed_at"]) for item in flat_calls)
        if (started.tzinfo is None or completed.tzinfo is None or started > completed
                or snapshot.get("capture_clock_role") != "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE"):
            return None
    except (TypeError, ValueError, KeyError, OverflowError):
        return None
    source_digest = sha256(evidence).hexdigest()
    canonical_digest = sha256(json.dumps(projection, sort_keys=True, separators=(",", ":"),
                                         ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()
    capture = BaoStockIdentitySourceCapture(str(uuid4()), source_digest, canonical_digest,
                                            rows_count, started, completed)
    _VERIFIED_IDENTITY_CAPTURES[capture.capture_id] = capture
    while len(_VERIFIED_IDENTITY_CAPTURES) > 64:
        _VERIFIED_IDENTITY_CAPTURES.pop(next(iter(_VERIFIED_IDENTITY_CAPTURES)))
    return capture.capture_id


def _issue_verified_daily_capture(
    *, symbol: str, target_session: str, fields: tuple[str, ...], rows: list[dict[str, str]],
    completed_at: str, injected_transport: bool,
) -> BaoStockDailySourceCapture | None:
    if injected_transport or len(rows) != 1:
        return None
    row = rows[0]
    required = {"date", "code", "open", "high", "low", "close", "volume", "amount", "adjustflag"}
    if (not required.issubset(fields) or row.get("code", "").lower() != symbol.lower()
            or row.get("date") != target_session or row.get("adjustflag") != "3"):
        return None
    try:
        captured_at = datetime.fromisoformat(completed_at)
        if captured_at.tzinfo is None or captured_at.utcoffset() is None:
            return None
        payload = {"fields": fields, "row": row, "source_symbol": symbol.lower(),
                   "target_session": target_session, "adjustflag": "3", "captured_at": completed_at}
        digest = sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                   separators=(",", ":")).encode("utf-8")).hexdigest()
        capture = BaoStockDailySourceCapture(
            capture_id=str(uuid4()), source_symbol=symbol.upper(),
            target_session=date.fromisoformat(target_session), adjustflag="3",
            amount_raw=row["amount"], volume_raw=row["volume"], source_sha256=digest,
            captured_at=captured_at,
        )
    except (KeyError, TypeError, ValueError):
        return None
    _VERIFIED_DAILY_CAPTURES[capture.capture_id] = capture
    while len(_VERIFIED_DAILY_CAPTURES) > 256:
        _VERIFIED_DAILY_CAPTURES.pop(next(iter(_VERIFIED_DAILY_CAPTURES)))
    return capture


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _message(value: object, *, limit: int = 320) -> str:
    text = str(value or "").strip()
    return text[:limit]


def _code(result: object) -> str | None:
    value = getattr(result, "error_code", None)
    return None if value is None else str(value)


def _error_message(result: object) -> str | None:
    value = getattr(result, "error_msg", None)
    return None if value is None else _message(value)


def _load_sdk(transport: object | None) -> tuple[object, str]:
    if transport is None:
        sdk = importlib.import_module("baostock")
        return sdk, metadata.version("baostock")
    return transport, str(getattr(transport, "version", "TEST_TRANSPORT"))


def _endpoint_snapshot(*, timeout_seconds: float, injected_transport: bool) -> dict[str, Any]:
    endpoint: dict[str, Any] = {
        "auth_mode": "TEST_TRANSPORT" if injected_transport else "PUBLIC_ANONYMOUS",
        "configured_host": None, "configured_port": None,
        "connected_peer_ip": None, "connected_peer_port": None,
    }
    if injected_transport:
        return endpoint
    try:
        constants = importlib.import_module("baostock.common.contants")
        context = importlib.import_module("baostock.common.context")
        endpoint["configured_host"] = getattr(constants, "BAOSTOCK_SERVER_IP", None)
        port = getattr(constants, "BAOSTOCK_SERVER_PORT", None)
        endpoint["configured_port"] = int(port) if port is not None else None
        sock = getattr(context, "default_socket", None)
        if sock is not None:
            try:
                peer = sock.getpeername()
                endpoint["connected_peer_ip"] = peer[0]
                endpoint["connected_peer_port"] = peer[1]
            except OSError:
                pass
            try:
                sock.settimeout(timeout_seconds)
            except OSError:
                pass
    except (ImportError, AttributeError, OSError, TypeError, ValueError):
        pass
    return endpoint


def _clear_anonymous_api_key(*, injected_transport: bool) -> None:
    if injected_transport:
        return
    try:
        context = importlib.import_module("baostock.common.context")
        if hasattr(context, "apiKey"):
            delattr(context, "apiKey")
    except ImportError:
        pass


def _close_sdk_socket(*, injected_transport: bool) -> None:
    if injected_transport:
        return
    try:
        context = importlib.import_module("baostock.common.context")
        sock = getattr(context, "default_socket", None)
        if sock is not None:
            sock.close()
    except (ImportError, AttributeError, OSError, TypeError):
        pass


def _unavailable_result(
    *, status: str, symbol: str, target_session: str, started_at: str, sdk_version: str | None,
    message: str, error_type: str | None = None, login_code: str | None = None,
    login_message: str | None = None, login_attempted: bool = False,
    query_code: str | None = None, query_message: str | None = None,
    logout_attempted: bool = False, logout_code: str | None = None,
    logout_message: str | None = None, response_fields: tuple[str, ...] = (),
    rows: tuple[dict[str, str], ...] = (), completed_at: str | None = None,
    endpoint: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "source": "BAOSTOCK",
        "requested_symbol": symbol,
        "requested_source_code": None,
        "target_session": target_session,
        "request": {
            "start_date": target_session,
            "end_date": target_session,
            "frequency": "d",
            "adjustflag": "3",
            "fields": _QUERY_FIELDS.split(","),
            "socket_timeout_seconds": _DEFAULT_SOCKET_TIMEOUT_SECONDS,
            "max_rows": _MAX_QUERY_ROWS,
            "retry_count": 0,
        },
        "sdk_version": sdk_version,
        "endpoint": endpoint or {
            "auth_mode": "PUBLIC_ANONYMOUS", "configured_host": None, "configured_port": None,
            "connected_peer_ip": None, "connected_peer_port": None,
        },
        "request_started_at": started_at,
        "request_completed_at": completed_at or _timestamp(),
        "capture_clock_role": "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
        "login_attempted": login_attempted,
        "login_error_code": login_code,
        "login_error_message": login_message,
        "query_error_code": query_code,
        "query_error_message": query_message,
        "logout_attempted": logout_attempted,
        "logout_error_code": logout_code,
        "logout_error_message": logout_message,
        "response_fields": list(response_fields),
        "rows_seen": len(rows),
        "rows": list(rows),
        "error_type": error_type,
        "reason": _message(message),
        "authority": "SUPPLEMENTAL_SOURCE_ONLY_NOT_OFFICIAL_IDENTITY_OR_PIT",
    }


def query_daily_rows(
    symbol: str,
    target_session: str,
    *,
    timeout_seconds: float = _DEFAULT_SOCKET_TIMEOUT_SECONDS,
    max_rows: int = _MAX_QUERY_ROWS,
    transport: object | None = None,
) -> dict[str, Any]:
    """查询一个显式来源代码和单个历史交易日；不重试、不写文件、不改TDX。"""
    if not isinstance(symbol, str) or _SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise ContractError("BaoStock查询代码必须是SH.六位代码或SZ.六位代码")
    if not isinstance(target_session, str):
        raise ContractError("BaoStock目标交易日必须是YYYY-MM-DD字符串")
    try:
        parsed_session = date.fromisoformat(target_session)
    except ValueError as error:
        raise ContractError("BaoStock目标交易日必须是有效YYYY-MM-DD日期") from error
    if parsed_session.isoformat() != target_session:
        raise ContractError("BaoStock目标交易日必须使用规范YYYY-MM-DD格式")
    if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
        raise ContractError("BaoStock网络超时必须是有限秒数")
    if not 0.2 <= float(timeout_seconds) <= 30.0:
        raise ContractError("BaoStock网络超时必须在0.2至30秒之间")
    if type(max_rows) is not int or not 1 <= max_rows <= _MAX_QUERY_ROWS:
        raise ContractError(f"BaoStock单次响应行数上限必须在1至{_MAX_QUERY_ROWS}之间")

    market, code = _SYMBOL_PATTERN.fullmatch(symbol).groups()
    source_code = f"{market.lower()}.{code}"
    started_at = _timestamp()
    sdk_version: str | None = None
    try:
        sdk, sdk_version = _load_sdk(transport)
    except (ImportError, metadata.PackageNotFoundError) as error:
        return _unavailable_result(
            status="DEPENDENCY_UNAVAILABLE", symbol=symbol, target_session=target_session,
            started_at=started_at, sdk_version=sdk_version,
            message="未安装可选BaoStock客户端；TDX结果保持不变", error_type=type(error).__name__,
            completed_at=_timestamp(),
        )
    if transport is None and sdk_version != _PINNED_SDK_VERSION:
        return _unavailable_result(
            status="UNSUPPORTED_SDK_VERSION", symbol=symbol, target_session=target_session,
            started_at=started_at, sdk_version=sdk_version,
            message=f"BaoStock运行包版本不符合固定合同{_PINNED_SDK_VERSION}；未发送网络请求",
            completed_at=_timestamp(),
        )

    previous_timeout = socket.getdefaulttimeout()
    login_attempted = False
    login_code: str | None = None
    login_message: str | None = None
    query_code: str | None = None
    query_message: str | None = None
    logout_attempted = False
    logout_code: str | None = None
    logout_message: str | None = None
    response_fields: tuple[str, ...] = ()
    rows: list[dict[str, str]] = []
    status = "QUERY_FAILED"
    reason = "BaoStock查询尚未完成"
    error_type: str | None = None
    output_capture = io.StringIO()
    endpoint: dict[str, Any] = {"auth_mode": "PUBLIC_ANONYMOUS", "configured_host": None,
                                "configured_port": None, "connected_peer_ip": None,
                                "connected_peer_port": None}

    try:
        # BaoStock 0.9.3通过标准socket建立连接；默认超时会赋给登录时创建的socket。
        socket.setdefaulttimeout(float(timeout_seconds))
        login_attempted = True
        _clear_anonymous_api_key(injected_transport=transport is not None)
        with redirect_stdout(output_capture), redirect_stderr(output_capture):
            login_result = sdk.login()
        endpoint = _endpoint_snapshot(timeout_seconds=float(timeout_seconds),
                                      injected_transport=transport is not None)
        login_code = _code(login_result)
        login_message = _error_message(login_result)
        if login_code != "0":
            status = "LOGIN_FAILED"
            reason = login_message or "BaoStock登录未成功"
        else:
            with redirect_stdout(output_capture), redirect_stderr(output_capture):
                response = sdk.query_history_k_data_plus(
                    source_code,
                    _QUERY_FIELDS,
                    start_date=target_session,
                    end_date=target_session,
                    frequency="d",
                    adjustflag="3",
                )
            query_code = _code(response)
            query_message = _error_message(response)
            fields = getattr(response, "fields", ())
            if isinstance(fields, (list, tuple)) and all(isinstance(field, str) for field in fields):
                response_fields = tuple(fields)
            if query_code != "0":
                status = "QUERY_FAILED"
                reason = query_message or "BaoStock查询未成功"
            else:
                while response.next():
                    raw_values = response.get_row_data()
                    if len(raw_values) != len(response_fields):
                        status = "PROTOCOL_INVALID"
                        reason = "BaoStock响应行宽与实际字段数不一致"
                        break
                    rows.append(dict(zip(response_fields, raw_values)))
                    if len(rows) > max_rows:
                        status = "ROW_LIMIT_EXCEEDED"
                        reason = f"BaoStock响应超过{max_rows}行上限"
                        break
                else:
                    status = "RESPONSE_RECEIVED"
                    reason = "单日请求已收到响应；行数及日期/代码仍需逐项核验"
    except (socket.timeout, TimeoutError) as error:
        status = "NETWORK_TIMEOUT"
        reason = "BaoStock网络socket达到有限超时；未重试"
        error_type = type(error).__name__
    except Exception as error:  # SDK可能抛出协议或socket异常；不污染TDX主结果。
        status = "NETWORK_OR_PROTOCOL_ERROR"
        reason = _message(error) or "BaoStock客户端发生网络或协议异常"
        error_type = type(error).__name__
    finally:
        if login_code == "0":
            logout_attempted = True
            try:
                with redirect_stdout(output_capture), redirect_stderr(output_capture):
                    logout_result = sdk.logout()
                logout_code = _code(logout_result)
                logout_message = _error_message(logout_result)
            except Exception as error:  # 登出失败仅作为来源清理披露。
                logout_code = "EXCEPTION"
                logout_message = _message(error)
                if status in ("RESPONSE_RECEIVED", "QUERY_FAILED", "LOGIN_FAILED"):
                    status = "LOGOUT_FAILED"
                    reason = "BaoStock登出异常；查询结果按来源清理失败披露"
                error_type = error_type or type(error).__name__
        if login_attempted:
            _close_sdk_socket(injected_transport=transport is not None)
        socket.setdefaulttimeout(previous_timeout)

    completed_at = _timestamp()
    verified_capture = None
    if status == "RESPONSE_RECEIVED" and login_code == "0" and query_code == "0":
        verified_capture = _issue_verified_daily_capture(
            symbol=source_code, target_session=target_session, fields=response_fields, rows=rows,
            completed_at=completed_at, injected_transport=transport is not None,
        )
    return _unavailable_result(
        status=status, symbol=symbol, target_session=target_session, started_at=started_at,
        sdk_version=sdk_version, message=reason, error_type=error_type,
        login_code=login_code, login_message=login_message, login_attempted=login_attempted,
        query_code=query_code, query_message=query_message, logout_attempted=logout_attempted,
        logout_code=logout_code, logout_message=logout_message, response_fields=response_fields,
        rows=tuple(rows), completed_at=completed_at, endpoint=endpoint,
    ) | {
        "requested_source_code": source_code,
        "request": {
            "start_date": target_session,
            "end_date": target_session,
            "frequency": "d",
            "adjustflag": "3",
            "fields": _QUERY_FIELDS.split(","),
            "socket_timeout_seconds": float(timeout_seconds),
            "max_rows": max_rows,
            "retry_count": 0,
        },
        "captured_sdk_output_chars": len(output_capture.getvalue()),
        "verified_source_capture_id": verified_capture.capture_id if verified_capture else None,
        "verified_source_sha256": verified_capture.source_sha256 if verified_capture else None,
    }


def query_identity_reference(
    target_sessions: tuple[str, ...],
    *,
    timeout_seconds: float = _DEFAULT_SOCKET_TIMEOUT_SECONDS,
    max_rows_per_query: int = 10000,
    transport: object | None = None,
) -> dict[str, Any]:
    """一次登录内有限读取证券基本资料与最多两个明确日期名录，不写文件或推断历史PIT。"""
    if (not isinstance(target_sessions, tuple) or not 1 <= len(target_sessions) <= 2
            or any(not isinstance(value, str) for value in target_sessions)
            or len(set(target_sessions)) != len(target_sessions)):
        raise ContractError("BaoStock身份探针必须显式提供一至两个不同目标日")
    try:
        for value in target_sessions:
            if date.fromisoformat(value).isoformat() != value:
                raise ValueError("non-canonical date")
    except ValueError as error:
        raise ContractError("BaoStock身份探针日期必须是YYYY-MM-DD") from error
    if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
            or not 0 < float(timeout_seconds) <= 30):
        raise ContractError("BaoStock身份探针超时必须大于0且不超过30秒")
    if type(max_rows_per_query) is not int or not 1 <= max_rows_per_query <= 10000:
        raise ContractError("BaoStock身份探针每次响应行数上限必须在1至10000之间")

    started_at = _timestamp()
    output_capture = io.StringIO()
    try:
        sdk, sdk_version = _load_sdk(transport)
    except Exception as error:
        return {
            "contract": "STOCK-A-BAOSTOCK-IDENTITY-READ/R1",
            "status": "SOURCE_UNAVAILABLE", "source": "BAOSTOCK",
            "target_sessions": list(target_sessions), "request_started_at": started_at,
            "request_completed_at": _timestamp(),
            "capture_clock_role": "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
            "available_at": None, "published_at": None, "sdk_version": None,
            "login_attempted": False, "login_error_code": None,
            "query_calls": 0, "max_rows_per_query": max_rows_per_query,
            "retry_count": 0, "error_type": type(error).__name__,
            "reason": "BaoStock客户端不可导入；未发送网络请求",
        }
    if transport is None and sdk_version != _PINNED_SDK_VERSION:
        return {
            "contract": "STOCK-A-BAOSTOCK-IDENTITY-READ/R1",
            "status": "UNSUPPORTED_SDK_VERSION", "source": "BAOSTOCK",
            "target_sessions": list(target_sessions), "request_started_at": started_at,
            "request_completed_at": _timestamp(),
            "capture_clock_role": "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
            "available_at": None, "published_at": None, "sdk_version": sdk_version,
            "login_attempted": False, "login_error_code": None,
            "query_calls": 0, "max_rows_per_query": max_rows_per_query,
            "retry_count": 0, "calls": {}, "error_type": None,
            "reason": f"BaoStock运行包版本不符合固定合同{_PINNED_SDK_VERSION}；未发送网络请求",
        }

    previous_timeout = socket.getdefaulttimeout()
    login_attempted = False
    login_code = None
    login_message = None
    logout_attempted = False
    logout_code = None
    logout_message = None
    calls: dict[str, Any] = {}
    query_calls = 0
    error_type = None
    status = "RESPONSE_RECEIVED"
    reason = "BaoStock只读身份接口探针完成"
    endpoint: dict[str, Any] = {"auth_mode": "PUBLIC_ANONYMOUS", "configured_host": None,
                                "configured_port": None, "connected_peer_ip": None,
                                "connected_peer_port": None}

    def read_result(
        result: object, *, label: str, query_started: str, target_session: str | None = None,
    ) -> dict[str, Any]:
        fields = tuple(str(value) for value in (getattr(result, "fields", None) or ()))
        code = _code(result)
        message = _error_message(result)
        rows: list[dict[str, str]] = []
        complete = code == "0"
        local_error = None
        if code == "0":
            try:
                while result.next():
                    if len(rows) >= max_rows_per_query:
                        complete = False
                        break
                    values = result.get_row_data()
                    if not isinstance(values, (list, tuple)) or len(values) != len(fields):
                        complete = False
                        local_error = "ROW_SCHEMA_LENGTH_MISMATCH"
                        break
                    rows.append({name: str(value) for name, value in zip(fields, values, strict=True)})
            except Exception as error:
                complete = False
                local_error = type(error).__name__
        if not complete and local_error is None and code == "0":
            local_error = "ROW_BUDGET_EXHAUSTED"
        return {
            "query": label,
            "target_session": target_session,
            "status": "RESPONSE_RECEIVED" if complete else "QUERY_FAILED",
            "query_error_code": code,
            "query_error_message": message,
            "response_fields": list(fields),
            "rows_seen": len(rows), "rows": rows,
            "complete": complete,
            "request_started_at": query_started,
            "request_completed_at": _timestamp(),
            "capture_clock_role": "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
            "local_error": local_error,
        }

    try:
        socket.setdefaulttimeout(float(timeout_seconds))
        _clear_anonymous_api_key(injected_transport=transport is not None)
        login_attempted = True
        with redirect_stdout(output_capture), redirect_stderr(output_capture):
            login = sdk.login()
            login_code = _code(login)
            login_message = _error_message(login)
            endpoint = _endpoint_snapshot(timeout_seconds=float(timeout_seconds),
                                          injected_transport=transport is not None)
            if login_code != "0":
                status = "LOGIN_FAILED"
                reason = "BaoStock登录失败；未发起身份查询"
            else:
                try:
                    query_calls += 1
                    query_started = _timestamp()
                    calls["stock_basic"] = read_result(
                        sdk.query_stock_basic(), label="query_stock_basic", query_started=query_started,
                    )
                    if calls["stock_basic"]["status"] != "RESPONSE_RECEIVED":
                        status = "QUERY_FAILED"
                        reason = "BaoStock证券基本资料查询未完整返回"
                    else:
                        for target_session in target_sessions:
                            query_calls += 1
                            query_started = _timestamp()
                            result = sdk.query_all_stock(day=target_session)
                            calls.setdefault("all_stock", {})[target_session] = read_result(
                                result, label="query_all_stock", query_started=query_started,
                                target_session=target_session,
                            )
                            if calls["all_stock"][target_session]["status"] != "RESPONSE_RECEIVED":
                                status = "QUERY_FAILED"
                                reason = "BaoStock指定日证券列表未完整返回"
                                break
                except Exception as error:
                    status = "QUERY_FAILED"
                    error_type = type(error).__name__
                    reason = "BaoStock身份接口调用失败；异常文本已省略"
    except Exception as error:
        status = "SOURCE_UNAVAILABLE"
        error_type = type(error).__name__
        reason = "BaoStock身份探针未能完成登录或网络响应；异常文本已省略"
    finally:
        if login_code == "0":
            logout_attempted = True
            try:
                with redirect_stdout(output_capture), redirect_stderr(output_capture):
                    logout = sdk.logout()
                logout_code = _code(logout)
                logout_message = _error_message(logout)
                if logout_code != "0" and status == "RESPONSE_RECEIVED":
                    status = "LOGOUT_FAILED"
                    reason = "BaoStock查询完成但登出返回失败"
            except Exception as error:
                logout_code = "EXCEPTION"
                error_type = error_type or type(error).__name__
                if status == "RESPONSE_RECEIVED":
                    status = "LOGOUT_FAILED"
                    reason = "BaoStock查询完成但登出异常；异常文本已省略"
        if login_attempted:
            _close_sdk_socket(injected_transport=transport is not None)
        socket.setdefaulttimeout(previous_timeout)

    snapshot = {
        "contract": "STOCK-A-BAOSTOCK-IDENTITY-READ/R1",
        "status": status, "source": "BAOSTOCK",
        "target_sessions": list(target_sessions),
        "request_started_at": started_at, "request_completed_at": _timestamp(),
        "capture_clock_role": "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
        "available_at": None, "published_at": None,
        "sdk_version": sdk_version,
        "endpoint": endpoint,
        "socket_timeout_seconds": float(timeout_seconds),
        "login_attempted": login_attempted,
        "login_error_code": login_code, "login_error_message": login_message,
        "logout_attempted": logout_attempted,
        "logout_error_code": logout_code, "logout_error_message": logout_message,
        "query_calls": query_calls, "max_rows_per_query": max_rows_per_query,
        "retry_count": 0, "calls": calls,
        "captured_sdk_output_chars": len(output_capture.getvalue()),
        "error_type": error_type, "reason": reason,
    }
    snapshot["verified_source_capture_id"] = _issue_verified_identity_capture(
        snapshot, injected_transport=transport is not None,
    )
    return snapshot


def query_daily_range_rows(
    symbols: tuple[str, ...],
    start_date: str,
    end_date: str,
    *,
    timeout_seconds: float = _DEFAULT_SOCKET_TIMEOUT_SECONDS,
    max_rows_per_symbol: int = 20,
    transport: object | None = None,
) -> dict[str, Any]:
    """一次登录有界查询少量显式证券的未复权日线，不重试或写入来源数据。"""
    if (not isinstance(symbols, tuple) or not 1 <= len(symbols) <= 20
            or len(set(symbols)) != len(symbols)
            or any(not isinstance(symbol, str) or _SYMBOL_PATTERN.fullmatch(symbol) is None
                   for symbol in symbols)):
        raise ContractError("BaoStock日线范围查询必须显式提供1至20个不重复沪深代码")
    if not isinstance(start_date, str) or not isinstance(end_date, str):
        raise ContractError("BaoStock日线范围必须提供YYYY-MM-DD起止日期")
    try:
        start_day, end_day = date.fromisoformat(start_date), date.fromisoformat(end_date)
    except ValueError as error:
        raise ContractError("BaoStock日线范围日期必须为YYYY-MM-DD") from error
    if start_day.isoformat() != start_date or end_day.isoformat() != end_date:
        raise ContractError("BaoStock日线范围日期必须使用规范YYYY-MM-DD格式")
    if end_day < start_day or (end_day - start_day).days > 14:
        raise ContractError("BaoStock日线范围必须正序且跨度不超过14天")
    if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
            or not 0 < float(timeout_seconds) <= 30):
        raise ContractError("BaoStock日线查询超时必须大于0且不超过30秒")
    if type(max_rows_per_symbol) is not int or not 1 <= max_rows_per_symbol <= 20:
        raise ContractError("BaoStock每证券响应行数上限必须在1至20之间")
    started_at = _timestamp()
    try:
        sdk, sdk_version = _load_sdk(transport)
    except Exception as error:
        return {
            "contract": "STOCK-A-BAOSTOCK-DAILY-RANGE-READ/R1",
            "status": "SOURCE_UNAVAILABLE", "source": "BAOSTOCK", "symbols": list(symbols),
            "start_date": start_date, "end_date": end_date,
            "request_started_at": started_at, "request_completed_at": _timestamp(),
            "capture_clock_role": "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
            "available_at": None, "published_at": None, "sdk_version": None,
            "login_attempted": False, "query_calls": 0, "retry_count": 0,
            "error_type": type(error).__name__, "reason": "BaoStock客户端不可导入；未发送网络请求",
        }
    if transport is None and sdk_version != _PINNED_SDK_VERSION:
        return {
            "contract": "STOCK-A-BAOSTOCK-DAILY-RANGE-READ/R1",
            "status": "UNSUPPORTED_SDK_VERSION", "source": "BAOSTOCK", "symbols": list(symbols),
            "start_date": start_date, "end_date": end_date,
            "request_started_at": started_at, "request_completed_at": _timestamp(),
            "capture_clock_role": "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
            "available_at": None, "published_at": None, "sdk_version": sdk_version,
            "login_attempted": False, "query_calls": 0, "retry_count": 0,
            "error_type": None, "reason": f"BaoStock运行包版本不符合固定合同{_PINNED_SDK_VERSION}；未发送网络请求",
        }
    previous_timeout = socket.getdefaulttimeout()
    output_capture = io.StringIO()
    login_attempted = False
    login_code = None
    login_message = None
    logout_attempted = False
    logout_code = None
    logout_message = None
    rows_by_symbol: dict[str, Any] = {}
    query_calls = 0
    status = "RESPONSE_RECEIVED"
    reason = "BaoStock未复权日线范围查询完成"
    error_type = None
    endpoint: dict[str, Any] = {"auth_mode": "PUBLIC_ANONYMOUS", "configured_host": None,
                                "configured_port": None, "connected_peer_ip": None,
                                "connected_peer_port": None}
    try:
        socket.setdefaulttimeout(float(timeout_seconds))
        login_attempted = True
        _clear_anonymous_api_key(injected_transport=transport is not None)
        with redirect_stdout(output_capture), redirect_stderr(output_capture):
            login = sdk.login()
            login_code = _code(login)
            login_message = _error_message(login)
        endpoint = _endpoint_snapshot(timeout_seconds=float(timeout_seconds),
                                      injected_transport=transport is not None)
        if login_code != "0":
            status = "LOGIN_FAILED"
            reason = "BaoStock登录失败；未发起日线查询"
        else:
            for symbol in symbols:
                query_calls += 1
                query_started = _timestamp()
                source_code = f"{symbol[:2].lower()}.{symbol[3:]}"
                try:
                    result = sdk.query_history_k_data_plus(
                        source_code, _QUERY_FIELDS, start_date=start_date,
                        end_date=end_date, frequency="d", adjustflag="3",
                    )
                    fields = tuple(str(value) for value in (getattr(result, "fields", None) or ()))
                    code = _code(result)
                    message = _error_message(result)
                    rows: list[dict[str, str]] = []
                    complete = code == "0"
                    local_error = None
                    if complete:
                        while result.next():
                            if len(rows) >= max_rows_per_symbol:
                                complete = False
                                local_error = "ROW_BUDGET_EXHAUSTED"
                                break
                            values = result.get_row_data()
                            if not isinstance(values, (list, tuple)) or len(values) != len(fields):
                                complete = False
                                local_error = "ROW_SCHEMA_LENGTH_MISMATCH"
                                break
                            rows.append({name: str(value) for name, value in zip(fields, values, strict=True)})
                    rows_by_symbol[symbol] = {
                        "status": "RESPONSE_RECEIVED" if complete else "QUERY_FAILED",
                        "query_error_code": code, "query_error_message": message,
                        "response_fields": list(fields), "rows_seen": len(rows), "rows": rows,
                        "complete": complete, "adjustflag": "3", "coordinate": "RAW_UNADJUSTED",
                        "request_started_at": query_started, "request_completed_at": _timestamp(),
                        "capture_clock_role": "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
                        "local_error": local_error,
                    }
                    if not complete:
                        status = "QUERY_FAILED"
                        reason = "BaoStock未复权日线未完整返回"
                        break
                except Exception as error:
                    status = "QUERY_FAILED"
                    error_type = type(error).__name__
                    reason = "BaoStock未复权日线查询异常；异常文本已省略"
                    break
    except Exception as error:
        status = "SOURCE_UNAVAILABLE"
        error_type = type(error).__name__
        reason = "BaoStock日线来源未能完成登录或响应；异常文本已省略"
    finally:
        if login_code == "0":
            logout_attempted = True
            try:
                with redirect_stdout(output_capture), redirect_stderr(output_capture):
                    logout = sdk.logout()
                logout_code = _code(logout)
                logout_message = _error_message(logout)
                if logout_code != "0" and status == "RESPONSE_RECEIVED":
                    status = "LOGOUT_FAILED"
                    reason = "BaoStock日线读取完成但登出返回失败"
            except Exception as error:
                logout_code = "EXCEPTION"
                error_type = error_type or type(error).__name__
                if status == "RESPONSE_RECEIVED":
                    status = "LOGOUT_FAILED"
                    reason = "BaoStock日线读取完成但登出异常；异常文本已省略"
        if login_attempted:
            _close_sdk_socket(injected_transport=transport is not None)
        socket.setdefaulttimeout(previous_timeout)
    return {
        "contract": "STOCK-A-BAOSTOCK-DAILY-RANGE-READ/R1",
        "status": status, "source": "BAOSTOCK", "symbols": list(symbols),
        "start_date": start_date, "end_date": end_date,
        "request_started_at": started_at, "request_completed_at": _timestamp(),
        "capture_clock_role": "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
        "available_at": None, "published_at": None, "sdk_version": sdk_version,
        "endpoint": endpoint,
        "socket_timeout_seconds": float(timeout_seconds),
        "fields": _QUERY_FIELDS.split(","),
        "adjustflag": "3", "coordinate": "RAW_UNADJUSTED",
        "max_rows_per_symbol": max_rows_per_symbol,
        "login_attempted": login_attempted, "login_error_code": login_code,
        "login_error_message": login_message, "logout_attempted": logout_attempted,
        "logout_error_code": logout_code, "logout_error_message": logout_message,
        "query_calls": query_calls, "retry_count": 0, "responses": rows_by_symbol,
        "captured_sdk_output_chars": len(output_capture.getvalue()),
        "error_type": error_type, "reason": reason,
    }
