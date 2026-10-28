"""BaoStock单股历史日线的有界只读查询边界。"""
from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timezone
import importlib
from importlib import metadata
import io
import re
import socket
from typing import Any

from stock_a.domain import ContractError


_SYMBOL_PATTERN = re.compile(r"^(SH|SZ)\.([0-9]{6})$")
_QUERY_FIELDS = "date,code,close,volume,amount,adjustflag,turn,tradestatus,isST"
_MAX_QUERY_ROWS = 8
_DEFAULT_SOCKET_TIMEOUT_SECONDS = 5.0


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


def _unavailable_result(
    *, status: str, symbol: str, target_session: str, started_at: str, sdk_version: str | None,
    message: str, error_type: str | None = None, login_code: str | None = None,
    login_message: str | None = None, login_attempted: bool = False,
    query_code: str | None = None, query_message: str | None = None,
    logout_attempted: bool = False, logout_code: str | None = None,
    logout_message: str | None = None, response_fields: tuple[str, ...] = (),
    rows: tuple[dict[str, str], ...] = (), completed_at: str | None = None,
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
    if not 0.2 <= float(timeout_seconds) <= 15.0:
        raise ContractError("BaoStock网络超时必须在0.2至15秒之间")
    if type(max_rows) is not int or not 1 <= max_rows <= _MAX_QUERY_ROWS:
        raise ContractError(f"BaoStock单次响应行数上限必须在1至{_MAX_QUERY_ROWS}之间")

    market, code = _SYMBOL_PATTERN.fullmatch(symbol).groups()
    source_code = f"{market.lower()}.{code}"
    started_at = _timestamp()
    sdk_version: str | None = None
    try:
        if transport is None:
            sdk = importlib.import_module("baostock")
            sdk_version = metadata.version("baostock")
        else:
            sdk = transport
            sdk_version = str(getattr(transport, "version", "TEST_TRANSPORT"))
    except (ImportError, metadata.PackageNotFoundError) as error:
        return _unavailable_result(
            status="DEPENDENCY_UNAVAILABLE", symbol=symbol, target_session=target_session,
            started_at=started_at, sdk_version=sdk_version,
            message="未安装可选BaoStock客户端；TDX结果保持不变", error_type=type(error).__name__,
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

    try:
        # BaoStock 0.9.3通过标准socket建立连接；默认超时会赋给登录时创建的socket。
        socket.setdefaulttimeout(float(timeout_seconds))
        login_attempted = True
        with redirect_stdout(output_capture), redirect_stderr(output_capture):
            login_result = sdk.login()
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
        if login_attempted:
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
        socket.setdefaulttimeout(previous_timeout)

    return _unavailable_result(
        status=status, symbol=symbol, target_session=target_session, started_at=started_at,
        sdk_version=sdk_version, message=reason, error_type=error_type,
        login_code=login_code, login_message=login_message, login_attempted=login_attempted,
        query_code=query_code, query_message=query_message, logout_attempted=logout_attempted,
        logout_code=logout_code, logout_message=logout_message, response_fields=response_fields,
        rows=tuple(rows), completed_at=_timestamp(),
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
    }
