"""BaoStock 证券主资料、逐日名录持久化及来源级 NON_PIT 日期有效总体解析（GAP-01/GAP-05）。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime, timezone
import re
from typing import Any

from stock_a.storage.postgres.source_raw import _transaction


_SYMBOL_PATTERN = re.compile(r"^(SH|SZ|BJ)\.([0-9]{6})$")
_RESEARCH_ELIGIBLE_BOARDS = frozenset({"MAIN_BOARD", "GEM"})
_VALID_MARKETS = frozenset({"SH", "SZ", "BJ", "OTHER"})
_VALID_BOARDS = frozenset(
    {
        "MAIN_BOARD",
        "GEM",
        "STAR_BOARD",
        "BEIJING_BOARD",
        "B_SHARE",
        "INDEX",
        "OTHER",
    }
)


def normalize_baostock_symbol(raw_code: Any) -> tuple[str, str, str] | None:
    """将 BaoStock 市场代码字符串规范化为 (source_symbol, market, digits)。"""
    if not isinstance(raw_code, str):
        return None
    cleaned = raw_code.strip().upper()
    match = _SYMBOL_PATTERN.fullmatch(cleaned)
    if match is None:
        return None
    market, digits = match.groups()
    return (f"{market}.{digits}", market, digits)


def classify_baostock_board(
    market: str, digits: str, *, security_type: str = "STOCK"
) -> str:
    """按市场与6位证券代码段解析板块枚举，严格区分研究池与非研究池/指数/B股。"""
    if security_type == "INDEX":
        return "INDEX"
    if market == "SH":
        if digits.startswith("688") or digits.startswith("689"):
            return "STAR_BOARD"
        if (
            digits.startswith("600")
            or digits.startswith("601")
            or digits.startswith("603")
            or digits.startswith("605")
        ):
            return "MAIN_BOARD"
        if digits.startswith("900"):
            return "B_SHARE"
        if digits.startswith("000"):
            return "INDEX"
        return "OTHER"
    if market == "SZ":
        if digits.startswith("300") or digits.startswith("301"):
            return "GEM"
        if (
            digits.startswith("000")
            or digits.startswith("001")
            or digits.startswith("002")
            or digits.startswith("003")
        ):
            return "MAIN_BOARD"
        if digits.startswith("200"):
            return "B_SHARE"
        if digits.startswith("399"):
            return "INDEX"
        return "OTHER"
    if market == "BJ":
        if digits.startswith(("4", "8", "92")):
            return "BEIJING_BOARD"
        return "OTHER"
    return "OTHER"


def _parse_iso_date(raw: Any) -> tuple[date | None, bool]:
    """解析 YYYY-MM-DD；空值返回 (None, False)，非法非空文本返回 (None, True)。"""
    if raw is None:
        return (None, False)
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return (raw, False)
    text = str(raw).strip()
    if not text:
        return (None, False)
    try:
        parsed = date.fromisoformat(text)
    except ValueError:
        return (None, True)
    if parsed.isoformat() != text:
        return (None, True)
    return (parsed, False)


def normalize_baostock_stock_basic_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    captured_at: datetime,
    pit_status: str = "NON_PIT_RETROSPECTIVE",
    source_clock_role: str = "BAOSTOCK_STOCK_BASIC_CAPTURE",
) -> tuple[dict[str, Any], ...]:
    """规范化 BaoStock query_stock_basic 行，严格执行缺日期降级与冲突隔离。"""
    if captured_at.tzinfo is None:
        raise ValueError("CAPTURED_AT_MUST_BE_TIMEZONE_AWARE")
    if pit_status not in {"NON_PIT_RETROSPECTIVE", "OBSERVED_AT_CAPTURE"}:
        raise ValueError("INVALID_STOCK_BASIC_PIT_STATUS")

    dedup: dict[str, dict[str, Any]] = {}
    for row in rows:
        parsed_sym = normalize_baostock_symbol(
            row.get("code") or row.get("source_symbol")
        )
        if parsed_sym is None:
            continue
        source_symbol, market, digits = parsed_sym
        raw_type = str(row.get("type", "")).strip()
        if raw_type == "1":
            security_type = "STOCK"
        elif raw_type == "2":
            security_type = "INDEX"
        else:
            security_type = "OTHER"

        raw_status = str(row.get("status", "")).strip()
        if raw_status == "1":
            listing_status = "LISTED"
        elif raw_status == "0":
            listing_status = "DELISTED"
        else:
            listing_status = "UNKNOWN"

        board = classify_baostock_board(market, digits, security_type=security_type)
        code_name = str(row.get("code_name") or "").strip()

        ipo_date, ipo_invalid = _parse_iso_date(
            row.get("ipoDate") if "ipoDate" in row else row.get("ipo_date")
        )
        out_date, out_invalid = _parse_iso_date(
            row.get("outDate") if "outDate" in row else row.get("out_date")
        )

        field_quality = "OBSERVED"
        quality_reason: str | None = None
        if ipo_invalid or out_invalid:
            ipo_date = None
            out_date = None
            field_quality = "CONFLICT_QUARANTINED"
            quality_reason = "IPO_OR_OUT_DATE_INVALID_FORMAT"
        elif ipo_date is not None and out_date is not None and out_date < ipo_date:
            ipo_date = None
            out_date = None
            field_quality = "CONFLICT_QUARANTINED"
            quality_reason = "OUT_DATE_BEFORE_IPO_DATE"
        elif security_type == "STOCK" and ipo_date is None:
            field_quality = "PARTIAL_UNKNOWN"
            quality_reason = "IPO_DATE_MISSING"
        elif listing_status == "DELISTED" and out_date is None:
            field_quality = "PARTIAL_UNKNOWN"
            quality_reason = "DELISTED_OUT_DATE_MISSING"

        normalized = {
            "source_symbol": source_symbol,
            "market": market if market in _VALID_MARKETS else "OTHER",
            "board": board if board in _VALID_BOARDS else "OTHER",
            "code_name": code_name,
            "security_type": security_type,
            "listing_status": listing_status,
            "ipo_date": ipo_date,
            "out_date": out_date,
            "field_quality": field_quality,
            "quality_reason": quality_reason,
            "source_clock_role": source_clock_role,
            "pit_status": pit_status,
            "captured_at": captured_at,
        }
        prev = dedup.get(source_symbol)
        if prev is not None and prev != normalized:
            raise ValueError(
                f"DUPLICATE_CONFLICTING_STOCK_BASIC_SYMBOL:{source_symbol}"
            )
        dedup[source_symbol] = normalized
    return tuple(dedup[sym] for sym in sorted(dedup))


def persist_baostock_stock_basic(
    config: Mapping[str, Any],
    *,
    source_capture_id: int,
    rows: Iterable[Mapping[str, Any]],
    captured_at: datetime | None = None,
    pit_status: str = "NON_PIT_RETROSPECTIVE",
    source_clock_role: str = "BAOSTOCK_STOCK_BASIC_CAPTURE",
) -> dict[str, Any]:
    """单事务幂等写入 stock.source_baostock_stock_basic；同键异内容拒绝回滚。"""
    if type(source_capture_id) is not int or source_capture_id <= 0:
        raise ValueError("INVALID_SOURCE_CAPTURE_ID")
    effective_captured_at = captured_at or datetime.now(timezone.utc)
    normalized = normalize_baostock_stock_basic_rows(
        rows,
        captured_at=effective_captured_at,
        pit_status=pit_status,
        source_clock_role=source_clock_role,
    )
    if not normalized:
        raise ValueError("BAOSTOCK_STOCK_BASIC_ROWS_EMPTY")

    with _transaction(config, write=True, statement_timeout_ms=30000) as (_, cursor, _):
        cursor.execute(
            """SELECT source_symbol, market, board, code_name, security_type,
                      listing_status, ipo_date, out_date, field_quality,
                      quality_reason, source_clock_role, pit_status
               FROM stock.source_baostock_stock_basic"""
        )
        existing_rows = cursor.fetchall()
        expected = {
            item["source_symbol"]: (
                item["market"],
                item["board"],
                item["code_name"],
                item["security_type"],
                item["listing_status"],
                item["ipo_date"],
                item["out_date"],
                item["field_quality"],
                item["quality_reason"],
                item["source_clock_role"],
                item["pit_status"],
            )
            for item in normalized
        }
        if existing_rows:
            existing = {row[0]: tuple(row[1:]) for row in existing_rows}
            if existing == expected:
                cursor.executemany(
                    """UPDATE stock.source_baostock_stock_basic
                          SET source_capture_id=%s, captured_at=%s
                        WHERE source_symbol=%s""",
                    [
                        (source_capture_id, item["captured_at"], item["source_symbol"])
                        for item in normalized
                    ],
                )
                return {
                    "status": "NO_CHANGE",
                    "source_capture_id": source_capture_id,
                    "row_count": len(normalized),
                }

        cursor.executemany(
            """INSERT INTO stock.source_baostock_stock_basic (
                   source_capture_id, source_symbol, market, board, code_name,
                   security_type, listing_status, ipo_date, out_date,
                   field_quality, quality_reason, source_clock_role,
                   pit_status, captured_at
               ) VALUES (
                   %s, %s, %s, %s, %s,
                   %s, %s, %s, %s,
                   %s, %s, %s,
                   %s, %s
               )
               ON CONFLICT (source_symbol) DO UPDATE SET
                   source_capture_id = EXCLUDED.source_capture_id,
                   market = EXCLUDED.market,
                   board = EXCLUDED.board,
                   code_name = EXCLUDED.code_name,
                   security_type = EXCLUDED.security_type,
                   listing_status = EXCLUDED.listing_status,
                   ipo_date = EXCLUDED.ipo_date,
                   out_date = EXCLUDED.out_date,
                   field_quality = EXCLUDED.field_quality,
                   quality_reason = EXCLUDED.quality_reason,
                   source_clock_role = EXCLUDED.source_clock_role,
                   pit_status = EXCLUDED.pit_status,
                   captured_at = EXCLUDED.captured_at""",
            [
                (
                    source_capture_id,
                    item["source_symbol"],
                    item["market"],
                    item["board"],
                    item["code_name"],
                    item["security_type"],
                    item["listing_status"],
                    item["ipo_date"],
                    item["out_date"],
                    item["field_quality"],
                    item["quality_reason"],
                    item["source_clock_role"],
                    item["pit_status"],
                    item["captured_at"],
                )
                for item in normalized
            ],
        )

    return {
        "status": "PERSISTED",
        "source_capture_id": source_capture_id,
        "row_count": len(normalized),
    }


def normalize_baostock_session_directory_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    session_date: date,
    captured_at: datetime,
) -> tuple[dict[str, Any], ...]:
    """规范化 BaoStock query_all_stock(day=d) 返回的逐日全证券在市名录。"""
    if type(session_date) is not date:
        raise ValueError("INVALID_SESSION_DATE")
    if captured_at.tzinfo is None:
        raise ValueError("CAPTURED_AT_MUST_BE_TIMEZONE_AWARE")

    dedup: dict[str, dict[str, Any]] = {}
    for row in rows:
        parsed_sym = normalize_baostock_symbol(
            row.get("code") or row.get("source_symbol")
        )
        if parsed_sym is None:
            continue
        source_symbol, market, digits = parsed_sym
        board = classify_baostock_board(market, digits, security_type="STOCK")
        code_name = str(
            row.get("code_name") or row.get("session_code_name") or ""
        ).strip()
        raw_ts = str(
            row.get("tradeStatus")
            if "tradeStatus" in row
            else row.get("trade_status", "")
        ).strip()
        if raw_ts in {"1", "TRADING"}:
            trade_status = "TRADING"
        elif raw_ts in {"0", "SUSPENDED"}:
            trade_status = "SUSPENDED"
        else:
            trade_status = "UNKNOWN"

        normalized = {
            "session_date": session_date,
            "source_symbol": source_symbol,
            "market": market if market in _VALID_MARKETS else "OTHER",
            "board": board if board in _VALID_BOARDS else "OTHER",
            "session_code_name": code_name,
            "trade_status": trade_status,
            "captured_at": captured_at,
        }
        prev = dedup.get(source_symbol)
        if prev is not None and prev != normalized:
            raise ValueError(
                f"DUPLICATE_CONFLICTING_SESSION_DIRECTORY_SYMBOL:{session_date}:{source_symbol}"
            )
        dedup[source_symbol] = normalized
    return tuple(dedup[sym] for sym in sorted(dedup))


def persist_baostock_session_directory(
    config: Mapping[str, Any],
    *,
    source_capture_id: int,
    session_date: date,
    rows: Iterable[Mapping[str, Any]],
    complete_response: bool,
    captured_at: datetime | None = None,
) -> dict[str, Any]:
    """以完整的逐日供应方响应原子替换名录；不完整响应不触碰既有集合。"""
    if type(source_capture_id) is not int or source_capture_id <= 0:
        raise ValueError("INVALID_SOURCE_CAPTURE_ID")
    if type(complete_response) is not bool:
        raise ValueError("INVALID_SESSION_DIRECTORY_COMPLETENESS")
    if not complete_response:
        return {
            "status": "INCOMPLETE",
            "source_capture_id": source_capture_id,
            "session_date": session_date.isoformat()
            if type(session_date) is date
            else None,
            "row_count": 0,
            "database_write_attempted": False,
        }
    effective_captured_at = captured_at or datetime.now(timezone.utc)
    normalized = normalize_baostock_session_directory_rows(
        rows,
        session_date=session_date,
        captured_at=effective_captured_at,
    )
    if not normalized:
        raise ValueError("BAOSTOCK_SESSION_DIRECTORY_ROWS_EMPTY")

    with _transaction(config, write=True, statement_timeout_ms=30000) as (_, cursor, _):
        cursor.execute(
            """SELECT source_symbol, market, board, session_code_name, trade_status
               FROM stock.source_baostock_session_directory
               WHERE session_date = %s""",
            (session_date,),
        )
        existing_rows = cursor.fetchall()
        expected = {
            item["source_symbol"]: (
                item["market"],
                item["board"],
                item["session_code_name"],
                item["trade_status"],
            )
            for item in normalized
        }
        if existing_rows:
            existing = {row[0]: tuple(row[1:]) for row in existing_rows}
            if existing == expected:
                cursor.executemany(
                    """UPDATE stock.source_baostock_session_directory
                          SET source_capture_id=%s, captured_at=%s
                        WHERE session_date=%s AND source_symbol=%s""",
                    [
                        (
                            source_capture_id,
                            item["captured_at"],
                            item["session_date"],
                            item["source_symbol"],
                        )
                        for item in normalized
                    ],
                )
                return {
                    "status": "NO_CHANGE",
                    "source_capture_id": source_capture_id,
                    "session_date": session_date.isoformat(),
                    "row_count": len(normalized),
                }

        incoming_symbols = [item["source_symbol"] for item in normalized]
        cursor.execute(
            """DELETE FROM stock.source_baostock_session_directory
                WHERE session_date=%s AND NOT (source_symbol=ANY(%s::text[]))""",
            (session_date, incoming_symbols),
        )

        cursor.executemany(
            """INSERT INTO stock.source_baostock_session_directory (
                   source_capture_id, session_date, source_symbol, market,
                   board, session_code_name, trade_status, captured_at
               ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (session_date, source_symbol) DO UPDATE SET
                   source_capture_id = EXCLUDED.source_capture_id,
                   market = EXCLUDED.market,
                   board = EXCLUDED.board,
                   session_code_name = EXCLUDED.session_code_name,
                   trade_status = EXCLUDED.trade_status,
                   captured_at = EXCLUDED.captured_at""",
            [
                (
                    source_capture_id,
                    item["session_date"],
                    item["source_symbol"],
                    item["market"],
                    item["board"],
                    item["session_code_name"],
                    item["trade_status"],
                    item["captured_at"],
                )
                for item in normalized
            ],
        )

    return {
        "status": "PERSISTED",
        "source_capture_id": source_capture_id,
        "session_date": session_date.isoformat(),
        "row_count": len(normalized),
    }


def read_stock_basic_ipo_dates(
    config: Mapping[str, Any],
    *,
    source_capture_id: int | None = None,
) -> tuple[int | None, dict[str, date]]:
    """只读读取权威非冲突隔离的上市日期映射（用于限价 IPO 特殊期判定）。"""
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        if source_capture_id is not None:
            cursor.execute(
                """SELECT source_symbol, ipo_date
                   FROM stock.source_baostock_stock_basic
                   WHERE source_capture_id = %s
                     AND field_quality <> 'CONFLICT_QUARANTINED'
                     AND ipo_date IS NOT NULL""",
                (source_capture_id,),
            )
            return (
                source_capture_id,
                {str(sym): ipo for sym, ipo in cursor.fetchall()},
            )

        cursor.execute(
            """SELECT MAX(source_capture_id)
               FROM stock.source_baostock_stock_basic"""
        )
        row = cursor.fetchone()
        latest_capture_id = int(row[0]) if row and row[0] is not None else None
        if latest_capture_id is None:
            return (None, {})

        cursor.execute(
            """SELECT source_symbol, ipo_date
               FROM stock.source_baostock_stock_basic
               WHERE field_quality <> 'CONFLICT_QUARANTINED'
                 AND ipo_date IS NOT NULL"""
        )
        return (
            latest_capture_id,
            {str(sym): ipo for sym, ipo in cursor.fetchall()},
        )


def resolve_date_effective_universe(
    config: Mapping[str, Any],
    *,
    session_date: date,
    directory_capture_id: int | None = None,
    stock_basic_capture_id: int | None = None,
    research_eligible_only: bool = True,
) -> dict[str, Any]:
    """解析目标交易日的来源级 NON_PIT 日期有效全市场总体（绝不用当前名单回填历史）。

    规则：
    - 必须存在目标日 session_date 的 source_baostock_session_directory 事实，缺失返回 NOT_COMPUTED；
    - 结合 source_baostock_stock_basic（如有）执行半开区间 [ipo_date, out_date) 过滤：
      剔除 session_date < ipo_date 与 session_date >= out_date 的证券；
    - research_eligible_only=True 时仅保留 MAIN_BOARD / GEM 普通股票（自动排除 B 股/指数/北交/科创）。
    """
    if type(session_date) is not date:
        raise ValueError("INVALID_SESSION_DATE")

    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        if directory_capture_id is not None:
            cursor.execute(
                """SELECT source_symbol, market, board, session_code_name, trade_status, source_capture_id
                   FROM stock.source_baostock_session_directory
                   WHERE session_date = %s AND source_capture_id = %s
                   ORDER BY source_symbol""",
                (session_date, directory_capture_id),
            )
        else:
            cursor.execute(
                """SELECT source_symbol, market, board, session_code_name, trade_status, source_capture_id
                   FROM stock.source_baostock_session_directory
                   WHERE session_date = %s
                   ORDER BY source_symbol""",
                (session_date,),
            )
        dir_rows = cursor.fetchall()
        if not dir_rows:
            return {
                "status": "NOT_COMPUTED",
                "reason": "SESSION_DIRECTORY_NOT_AVAILABLE"
                if directory_capture_id is None
                else "SESSION_DIRECTORY_CAPTURE_EMPTY",
                "session_date": session_date.isoformat(),
                "directory_capture_id": directory_capture_id,
                "stock_basic_capture_id": None,
                "pit_status": "NON_PIT_RETROSPECTIVE",
                "members": (),
                "trade_status_by_symbol": {},
                "excluded_by_date_bounds": 0,
            }
        dir_cap = (
            directory_capture_id
            if directory_capture_id is not None
            else max(int(row[5]) for row in dir_rows)
        )

        if stock_basic_capture_id is not None:
            cursor.execute(
                """SELECT source_symbol, security_type, ipo_date, out_date, field_quality, source_capture_id
                   FROM stock.source_baostock_stock_basic
                   WHERE source_capture_id = %s""",
                (stock_basic_capture_id,),
            )
        else:
            cursor.execute(
                """SELECT source_symbol, security_type, ipo_date, out_date, field_quality, source_capture_id
                   FROM stock.source_baostock_stock_basic"""
            )
        basic_rows = cursor.fetchall()
        basic_cap: int | None = stock_basic_capture_id
        if basic_cap is None and basic_rows:
            basic_cap = max(int(row[5]) for row in basic_rows)

        basic_by_symbol: dict[str, tuple[str, date | None, date | None, str]] = {}
        for sym, sec_type, ipo_d, out_d, fq, _cap_id in basic_rows:
            basic_by_symbol[str(sym)] = (str(sec_type), ipo_d, out_d, str(fq))

    members: list[str] = []
    quarantined_symbols: list[str] = []
    partial_unknown_symbols: list[str] = []
    missing_basic_symbols: list[str] = []
    trade_status_by_symbol: dict[str, str] = {}
    excluded_by_date_bounds = 0
    for sym, _market, board, _name, trade_status, _dir_cap in dir_rows:
        symbol = str(sym)
        if research_eligible_only and board not in _RESEARCH_ELIGIBLE_BOARDS:
            continue
        basic_info = basic_by_symbol.get(symbol)
        if basic_info is not None:
            sec_type, ipo_d, out_d, fq = basic_info
            if research_eligible_only and sec_type != "STOCK":
                continue
            if fq == "CONFLICT_QUARANTINED":
                # R04/B02：目录已确证在市且研究板别/STOCK资格合法时，不因日期字段冲突静默缩小正式排名总体；
                # 冲突日期已清空为 NULL（不用于日期边界排除），并单列 quarantined_symbols 供下游质量摘要消费。
                quarantined_symbols.append(symbol)
            if fq == "PARTIAL_UNKNOWN":
                partial_unknown_symbols.append(symbol)
            if ipo_d is not None and session_date < ipo_d:
                excluded_by_date_bounds += 1
                continue
            if out_d is not None and session_date >= out_d:
                excluded_by_date_bounds += 1
                continue
        else:
            missing_basic_symbols.append(symbol)
        members.append(symbol)
        trade_status_by_symbol[symbol] = str(trade_status)

    return {
        "status": "AVAILABLE",
        "reason": None,
        "session_date": session_date.isoformat(),
        "directory_capture_id": dir_cap,
        "stock_basic_capture_id": basic_cap,
        "universe_capture_id": dir_cap,
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "members": tuple(members),
        "member_count": len(members),
        "quarantined_symbols": tuple(quarantined_symbols),
        "partial_unknown_symbols": tuple(partial_unknown_symbols),
        "missing_basic_symbols": tuple(missing_basic_symbols),
        "trade_status_by_symbol": trade_status_by_symbol,
        "excluded_by_date_bounds": excluded_by_date_bounds,
    }


def read_baostock_stock_basic_scope(
    config: Mapping[str, Any], *, source_capture_id: int, cutoff: date
) -> tuple[dict[str, Any], ...]:
    """只读回精确BaoStock基本资料捕获的证券身份与规范化关键字段。"""
    if (
        type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(cutoff) is not date
    ):
        raise ValueError("BAOSTOCK_STOCK_BASIC_READ_SCOPE_INVALID")
    with _transaction(config, write=False, statement_timeout_ms=10000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT b.source_symbol,b.market,b.board,b.security_type,
                      b.listing_status,b.ipo_date,b.out_date,b.field_quality
               FROM stock.source_baostock_stock_basic b
               JOIN stock.source_captures c ON c.capture_id=b.source_capture_id
               WHERE b.source_capture_id=%s AND c.provider='BAOSTOCK'
                 AND c.range_end_session<=%s
               ORDER BY b.source_symbol""",
            (source_capture_id, cutoff),
        )
        rows = cursor.fetchall()
    return tuple(
        {
            "source_symbol": str(row[0]),
            "market": str(row[1]),
            "board": str(row[2]),
            "security_type": str(row[3]),
            "listing_status": str(row[4]),
            "ipo_date": row[5],
            "out_date": row[6],
            "field_quality": str(row[7]),
        }
        for row in rows
    )


def read_baostock_session_directory_scope(
    config: Mapping[str, Any],
    *,
    source_capture_id: int,
    session_date: date,
    cutoff: date,
) -> tuple[dict[str, Any], ...]:
    """只读回指定捕获和官方会话的逐日BaoStock名录成员。"""
    if (
        type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(session_date) is not date
        or type(cutoff) is not date
        or session_date > cutoff
    ):
        raise ValueError("BAOSTOCK_SESSION_DIRECTORY_READ_SCOPE_INVALID")
    with _transaction(config, write=False, statement_timeout_ms=10000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT d.source_symbol,d.market,d.board,d.trade_status
               FROM stock.source_baostock_session_directory d
               JOIN stock.source_captures c ON c.capture_id=d.source_capture_id
               WHERE d.source_capture_id=%s AND d.session_date=%s
                 AND d.session_date<=%s AND c.provider='BAOSTOCK'
                 AND c.range_start_session<=d.session_date
                 AND c.range_end_session>=d.session_date
                 AND c.range_end_session<=%s
               ORDER BY d.source_symbol""",
            (source_capture_id, session_date, cutoff, cutoff),
        )
        rows = cursor.fetchall()
    return tuple(
        {
            "source_symbol": str(row[0]),
            "market": str(row[1]),
            "board": str(row[2]),
            "trade_status": str(row[3]),
        }
        for row in rows
    )
