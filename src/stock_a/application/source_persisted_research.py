"""从冻结研究事实直接计算三态分母及后验路径统计。"""

from collections import Counter
from datetime import date
from fractions import Fraction
from statistics import median
from typing import Any, Mapping

from stock_a.application.source_cohort_forward import _frozen_line_followup
from stock_a.application.source_d0_raw import attach_stock_risk_readiness
from stock_a.application.source_single_followup import observe_frozen_scenario
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.source_forward_path import exploratory_raw_path
from stock_a.storage.postgres.research_events import read_source_events
from stock_a.storage.postgres.cohort_stats import read_cohort_page
from stock_a.storage.postgres.research_facts import (
    read_research_keys,
)
from stock_a.storage.postgres.baostock_price_facts import (
    RAW_COORDINATE,
    QFQ_COORDINATE,
    read_baostock_capture_roster,
    read_baostock_price_history_batch,
)


class ResearchSymbolNotFound(ValueError):
    """所选来源代码不在指定的冻结来源池中。"""


def read_persisted_cohort_keys(config, *, target, research_cutoff, source_capture_id):
    """目录和结果使用同一个截止日所绑定的规则与日历修订。"""
    if target > research_cutoff:
        raise ValueError("目标日不得晚于研究截止日")
    _, _, revision, calendar = _load_calendar(research_cutoff)
    if target not in calendar:
        raise ResearchEntryError("TARGET_NOT_TRADING_SESSION", "目标日不在官方日历中")
    return read_research_keys(
        config,
        target=target,
        source_capture_id=source_capture_id,
        calendar_revision=revision,
    )


def _read_qfq_forward_rows(
    config,
    *,
    source_symbol: str,
    sessions: tuple[date, ...],
    research_cutoff: date,
    source_capture_id: int | None = None,
) -> tuple[list[dict[str, Any]], date | None]:
    """按短业务键与实际QFQ坐标读取路径行及完整请求水位，不强绑paired capture。"""
    batch = read_baostock_price_history_batch(
        config,
        source_symbols=(source_symbol,),
        sessions=sessions,
        source_capture_id=None,
        price_coordinate=QFQ_COORDINATE,
        cutoff=research_cutoff,
        offset=0,
        limit=max(1, len(sessions)),
    )
    if (
        batch.get("status") != "AVAILABLE"
        or batch.get("price_coordinate") != QFQ_COORDINATE
        or batch.get("page", {}).get("next_offset") is not None
    ):
        raise ValueError("BaoStock QFQ后继窗口未从PG一次完整读回")

    rows_by_session = {
        row["session_date"]: row
        for row in batch.get("rows", ())
        if row.get("source_symbol") == source_symbol
    }
    coverage_by_session = {
        date.fromisoformat(row["session_date"]): row
        for row in batch.get("coverage", ())
        if isinstance(row.get("session_date"), str)
    }
    output: list[dict[str, Any]] = []
    saved_through: date | None = None
    contiguous_open = True
    for session in sessions:
        row = rows_by_session.get(session)
        coverage = coverage_by_session.get(session)
        request_closed = (
            isinstance(coverage, Mapping)
            and coverage.get("coverage_status") == "COMPLETE"
            and int(coverage.get("expected_symbol_count", 0)) == 1
            and int(coverage.get("row_count", 0)) == 1
            and int(coverage.get("symbol_count", 0)) == 1
        )
        if row is not None:
            if row.get("price_coordinate") != QFQ_COORDINATE:
                raise ValueError("BaoStock后继价格行与QFQ坐标不一致")
            output.append(row)
        elif not request_closed:
            output.append(
                {
                    "source_symbol": source_symbol,
                    "session_date": session,
                    "source_capture_id": source_capture_id,
                    "price_coordinate": QFQ_COORDINATE,
                    "bar_status": "READ_FAILED",
                    "bar_reason": "QFQ_CAPTURE_REQUEST_NOT_CLOSED",
                    "qfq_price_quality": "UNKNOWN",
                    "qfq_price_reason": "QFQ_CAPTURE_REQUEST_NOT_CLOSED",
                    "qfq_open": None,
                    "qfq_high": None,
                    "qfq_low": None,
                    "qfq_close": None,
                }
            )
        else:
            output.append(
                {
                    "source_symbol": source_symbol,
                    "session_date": session,
                    "source_capture_id": source_capture_id,
                    "price_coordinate": QFQ_COORDINATE,
                    "bar_status": "TARGET_MISSING",
                    "bar_reason": "SOURCE_SYMBOL_OUTSIDE_ELIGIBLE_SESSION_WINDOW",
                    "qfq_price_quality": "UNKNOWN",
                    "qfq_price_reason": "SOURCE_SYMBOL_OUTSIDE_ELIGIBLE_SESSION_WINDOW",
                    "qfq_open": None,
                    "qfq_high": None,
                    "qfq_low": None,
                    "qfq_close": None,
                }
            )

        if contiguous_open:
            if request_closed and row is not None:
                saved_through = session
            else:
                contiguous_open = False
    return output, saved_through


def summarize_cohort_rows(rows, *, rule, due):
    """离线汇总冻结规则及端点事实，保留精确分数口径。"""
    groups = []
    for state, label in (
        ("TRUE", "MATCH"),
        ("FALSE", "NO_MATCH"),
        ("UNKNOWN", "UNKNOWN"),
    ):
        selected = [row for row in rows if row[1] == state]
        for weighting in ("stock", "event"):
            weighted = []
            maturity, path_quality, reasons = Counter(), Counter(), Counter()
            total = 0
            for row in selected:
                weight = (
                    row[3]
                    if weighting == "event"
                    and state == "TRUE"
                    and rule in {"D1_ANCHOR_WINDOW", "STRONG_PULLBACK_DIAGNOSTIC"}
                    else 1
                )
                total += weight
                maturity[
                    row[5] if row[4] else "MISSING_FACT" if due else "NOT_DUE"
                ] += weight
                path_quality[row[10] or ("MISSING_FACT" if due else "NOT_DUE")] += (
                    weight
                )
                if row[6] == "OBSERVED" and row[8] is not None and row[9] is not None:
                    weighted.extend([Fraction(int(row[8]), int(row[9]))] * weight)
                else:
                    reasons[row[7] or ("MISSING_FACT" if due else "NOT_DUE")] += weight
            positive = sum(value > 0 for value in weighted)
            groups.append(
                {
                    "group": label,
                    "weighting": weighting,
                    "count": total,
                    "evaluable": len(weighted),
                    "unknown": total - len(weighted),
                    "positive": positive,
                    "non_positive": len(weighted) - positive,
                    "median": str(median(weighted)) if weighted else None,
                    "maturity": dict(maturity),
                    "path_quality": dict(path_quality),
                    "unevaluable_reasons": dict(reasons),
                }
            )
    return groups


def read_persisted_cohort(
    config,
    *,
    target,
    research_cutoff,
    source_capture_id,
    profile_revision,
    history_session_count,
    d1_history_session_count,
    event_window_start,
    rule,
    horizon,
    offset=0,
    limit=30,
    symbol=None,
    include_risk=False,
    include_rps_pulse_groups=False,
    rps_board_kind="ALL",
    rps_scenario="ALL",
    rps_rule_state="ALL",
):
    """精确版本只读；未到期与已到期但未保存的事实分别表达。"""
    if target > research_cutoff or offset < 0 or not 1 <= limit <= 100:
        raise ValueError("目标日、截止日或分页参数无效")
    _, _, revision, calendar = _load_calendar(research_cutoff)
    if target not in calendar:
        raise ResearchEntryError("TARGET_NOT_TRADING_SESSION", "目标日不在官方日历中")
    index = calendar.index(target) + horizon
    endpoint_day = calendar[index] if index < len(calendar) else None
    due = endpoint_day is not None and endpoint_day <= research_cutoff
    common = dict(
        config=config,
        target=target,
        source_capture_id=source_capture_id,
        profile_revision=profile_revision,
        history_session_count=history_session_count,
        d1_history_session_count=d1_history_session_count,
        event_window_start=event_window_start,
        calendar_revision=revision,
        horizon=horizon,
        research_cutoff=research_cutoff,
        rps_board_kind=rps_board_kind,
        rps_scenario=rps_scenario,
        rps_rule_state=rps_rule_state,
    )
    source_pool_count, groups, rows, selected_rules, rps_pulse_groups = (
        read_cohort_page(
            **common,
            rule=rule,
            offset=offset,
            limit=limit,
            symbol=symbol,
            include_rps_pulse_groups=include_rps_pulse_groups,
        )
    )
    if include_risk and symbol is None:
        raise ValueError("风险分量需要指定来源代码")
    followup = None
    risk_readiness = None
    anchor_events = None
    rule_followup = None
    if symbol is not None:
        selected = next((row for row in rows if row[0] == symbol), None)
        if selected is None:
            raise ResearchSymbolNotFound("所选代码不在冻结来源池中")
        line = (
            {"numerator": str(selected[18]), "denominator": str(selected[19])}
            if selected[18] is not None and selected[19] is not None
            else None
        )
        days = tuple(
            day
            for day in calendar[calendar.index(target) : index + 1]
            if day <= research_cutoff
        )
        source_rows, saved_through = _read_qfq_forward_rows(
            config,
            source_symbol=symbol,
            sessions=days,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
        )
        birth = next(
            (row for row in source_rows if row["session_date"] == target), None
        )
        if birth is None:
            raise ValueError("T0 QFQ来源行缺失")
        effective_qfq_capture_id = (
            birth.get("source_capture_id")
            if type(birth.get("source_capture_id")) is int
            and birth.get("source_capture_id") > 0
            else source_capture_id
        )
        path = exploratory_raw_path(
            days,
            source_rows,
            horizon,
            target_session=endpoint_day,
            source_capture_id=effective_qfq_capture_id,
            price_coordinate=QFQ_COORDINATE,
            saved_through=saved_through,
        )
        followup = _frozen_line_followup(
            {
                "state": selected[1],
                "failure_line_candidate": {
                    "value": line,
                    "source": selected[20],
                    "price_coordinate": QFQ_COORDINATE,
                    "scope": "NON_PIT_EXPLORATORY",
                },
            },
            path,
        )
        followup.update(
            {
                "source_symbol": symbol,
                "maturity": path["maturity"],
                "endpoint": path["endpoint"],
                "path_quality": path["path_quality"],
            }
        )
        if rule in {"LAUNCH_CONFIRM", "RECOVERY_TURN"} and selected[1] == "TRUE":
            rule_followup = observe_frozen_scenario(
                config,
                symbol=symbol,
                target=target,
                cutoff=research_cutoff,
                source_capture_id=source_capture_id,
                target_capture_id=selected[21],
                calendar=calendar,
                horizon=horizon,
                scenario=rule,
            )
        from stock_a.storage.postgres.research_facts import ResearchFactNotFound

        d1_rows = None
        if rule == "D1_ANCHOR_WINDOW":
            d1_rows = rows
        else:
            try:
                d1_rows = read_cohort_page(
                    **common, rule="D1_ANCHOR_WINDOW", offset=0, limit=1, symbol=symbol
                )[2]
            except ResearchFactNotFound:
                d1_rows = None
        d1 = (
            next((row for row in d1_rows if row[0] == symbol), None)
            if d1_rows
            else None
        )
        if d1 is not None:
            events = read_source_events(
                config,
                target=target,
                source_capture_id=source_capture_id,
                symbol=symbol,
                d1_history_session_count=d1_history_session_count,
                calendar_revision=revision,
                event_window_start=event_window_start,
            )
            if (
                d1[21] != selected[21]
                or len(events) != d1[3]
                or any(event[14] != selected[21] for event in events)
            ):
                raise ValueError("逐锚事实与冻结D1事件数或来源捕获不一致")
        else:
            events = []
        anchor_events = [] if d1 is not None else None
        for event in events:
            lower = (
                {"numerator": str(event[3]), "denominator": str(event[4])}
                if event[3] is not None
                else None
            )
            if event[0].isoformat() != target.isoformat():
                lower_touch_followup = {
                    "status": "NOT_COMPUTED",
                    "reason_code": "ANCHOR_T0_DIFFERS_FROM_CURRENT_T0",
                    "role": "SOURCE_PRICE_DIAGNOSTIC_NOT_FORMAL_INVALIDATION",
                }
            elif event[2] != "GENERATED_AND_OBSERVED":
                lower_touch_followup = {
                    "status": "NOT_COMPUTED",
                    "reason_code": "D1_PLATFORM_NOT_GENERATED_AND_OBSERVED",
                    "role": "SOURCE_PRICE_DIAGNOSTIC_NOT_FORMAL_INVALIDATION",
                }
            else:
                lower_touch_followup = _frozen_line_followup(
                    {
                        "state": "TRUE",
                        "failure_line_candidate": {
                            "value": lower,
                            "source": "D1_PLATFORM_LOWER",
                            "price_coordinate": QFQ_COORDINATE,
                            "scope": "NON_PIT_EXPLORATORY",
                        },
                    },
                    path,
                )
            anchor_events.append(
                {
                    "anchor_session": event[0].isoformat(),
                    "anchor_capture_id": event[1],
                    "generation_status": event[2],
                    "lower": lower,
                    "upper": {"numerator": str(event[5]), "denominator": str(event[6])}
                    if event[5] is not None
                    else None,
                    "previous_session": event[7].isoformat(),
                    "previous_state": event[8],
                    "previous_quality": event[9],
                    "target_state": event[10],
                    "target_quality": event[11],
                    "change_classification": event[12],
                    "reason": event[13],
                    "lower_touch_followup": lower_touch_followup,
                }
            )
        if include_risk:
            if rule == "LAUNCH_CONFIRM":
                launch_rows = rows
            else:
                try:
                    launch_rows = read_cohort_page(
                        **common,
                        rule="LAUNCH_CONFIRM",
                        offset=0,
                        limit=1,
                        symbol=symbol,
                    )[2]
                except ResearchFactNotFound:
                    launch_rows = None
            launch = (
                next((row for row in launch_rows if row[0] == symbol), None)
                if launch_rows
                else None
            )
            if launch is not None:
                if launch[21] != selected[21]:
                    raise ValueError("启动规则与选中来源捕获不一致")
                diagnostic = {
                    "scenario": "LAUNCH_CONFIRM",
                    "results": [{"source_symbol": symbol, "scenario_raw": launch[1]}],
                }
                rps_unavailable = {
                    "target_session": target,
                    "research_cutoff": research_cutoff,
                    "source_capture_id": source_capture_id,
                    "facts": {},
                }
                attach_stock_risk_readiness(
                    config,
                    diagnostic,
                    target=target,
                    cutoff=research_cutoff,
                    source_capture_id=source_capture_id,
                    rps_read=rps_unavailable,
                )
                risk_readiness = diagnostic["results"][0]["risk_priority_readiness"]
            else:
                risk_readiness = {
                    "status": "NOT_COMPUTED",
                    "reason": "LAUNCH_CONFIRM_STATS_NOT_COMPUTED",
                }
    return {
        "scope": "NON_PIT_EXPLORATORY",
        "target_session": target.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "rule": rule,
        "horizon": horizon,
        "endpoint_session": endpoint_day.isoformat() if endpoint_day else None,
        "due": due,
        "source_pool_count": source_pool_count,
        "groups": groups,
        "rps_pulse_groups": rps_pulse_groups,
        "selected_rules": selected_rules,
        "frozen_candidate_followup": followup,
        "frozen_rule_followup": rule_followup,
        "d1_anchor_events": anchor_events,
        "risk_readiness": risk_readiness,
        "results": [
            {
                "source_symbol": row[0],
                "state": row[1],
                "reasons": row[2],
                "event_count": row[3],
                "maturity": row[5] if row[4] else "MISSING_FACT" if due else "NOT_DUE",
                "endpoint_quality": row[6],
                "endpoint_reason": row[7],
                "path_quality": row[10],
                "path_reasons": row[11],
                "mfe_quality": row[22] if len(row) >= 28 else None,
                "mfe_reason": row[23] if len(row) >= 28 else None,
                "mae_quality": row[24] if len(row) >= 28 else None,
                "mae_reason": row[25] if len(row) >= 28 else None,
                "mdd_quality": row[26] if len(row) >= 28 else None,
                "mdd_reason": row[27] if len(row) >= 28 else None,
            }
            for row in rows
        ],
        "page": {
            "offset": offset,
            "limit": limit,
            "next_offset": (
                offset + limit
                if symbol is None and offset + limit < source_pool_count
                else None
            ),
        },
        "database_transaction": "READ_ONLY",
        "database_write_attempted": False,
    }


def compare_persisted_cohorts(
    config,
    *,
    before,
    after,
    research_cutoff,
    source_capture_id,
    profile_revision,
    history_session_count,
    d1_history_session_count,
    before_event_window_start,
    after_event_window_start,
    rule,
    industry_snapshot_id,
    concept_snapshot_id,
    symbol=None,
    offset=0,
    limit=30,
):
    """比较两份冻结规则并关联当前成员快照，不推称历史PIT或相邻转移。"""
    if (
        before >= after
        or after > research_cutoff
        or offset < 0
        or not 1 <= limit <= 100
    ):
        raise ValueError("跨日会话、截止或分页参数无效")
    _, _, revision, calendar = _load_calendar(research_cutoff)
    if before not in calendar or after not in calendar:
        raise ValueError("跨日对照必须是官方会话")
    common = dict(
        config=config,
        source_capture_id=source_capture_id,
        profile_revision=profile_revision,
        history_session_count=history_session_count,
        d1_history_session_count=d1_history_session_count,
        calendar_revision=revision,
        rule=rule,
        horizon=1,
        research_cutoff=research_cutoff,
    )
    from stock_a.storage.postgres import research_facts, source_raw

    cursor_arguments = {
        name: value
        for name, value in common.items()
        if name not in {"config", "event_window_start"}
    }
    with source_raw._transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        left = research_facts._read_research_rows_cursor(
            cursor,
            target=before,
            event_window_start=before_event_window_start,
            **cursor_arguments,
        )
        right = research_facts._read_research_rows_cursor(
            cursor,
            target=after,
            event_window_start=after_event_window_start,
            **cursor_arguments,
        )
        snapshots, members = research_facts._read_board_members_cursor(
            cursor,
            industry_snapshot_id=industry_snapshot_id,
            concept_snapshot_id=concept_snapshot_id,
        )
    left_by_symbol = {row[0]: row for row in left}
    right_by_symbol = {row[0]: row for row in right}
    universe = set(left_by_symbol) | set(right_by_symbol)
    transitions = {}
    for code in universe:
        a, b = left_by_symbol.get(code), right_by_symbol.get(code)
        transitions[code] = {
            "source_symbol": code,
            "before_state": a[1] if a else "NOT_IN_SOURCE_POOL",
            "after_state": b[1] if b else "NOT_IN_SOURCE_POOL",
            "before_reasons": a[2] if a else [],
            "after_reasons": b[2] if b else [],
        }
    boards = {}
    mapped = {"INDUSTRY": set(), "CONCEPT": set()}
    symbol_boards = []
    for kind, board_id, name, code in members:
        board = boards.setdefault(
            (kind, board_id, name),
            {"members": 0, "mapped": 0, "transitions": Counter()},
        )
        board["members"] += 1
        if code in universe:
            mapped[kind].add(code)
            board["mapped"] += 1
            transition = transitions[code]
            board["transitions"][
                f"{transition['before_state']}→{transition['after_state']}"
            ] += 1
        if code == symbol:
            symbol_boards.append({"kind": kind, "native_id": board_id, "name": name})
    board_rows = [
        {
            "kind": key[0],
            "native_id": key[1],
            "name": key[2],
            "snapshot_members": value["members"],
            "mapped_members": value["mapped"],
            "unmapped_members": value["members"] - value["mapped"],
            "transitions": dict(value["transitions"]),
        }
        for key, value in sorted(boards.items())
    ]
    changes = Counter(
        f"{v['before_state']}→{v['after_state']}" for v in transitions.values()
    )
    selected = transitions.get(symbol) if symbol else None
    if symbol and selected is None:
        raise ValueError("所选代码在两个来源池均不存在")
    if selected is not None:
        selected = {**selected, "current_snapshot_boards": symbol_boards}
    return {
        "scope": "NON_PIT_EXPLORATORY",
        "membership_scope": "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT",
        "before": before.isoformat(),
        "after": after.isoformat(),
        "rule": rule,
        "adjacent_official_sessions": calendar.index(after)
        == calendar.index(before) + 1,
        "interpretation": "来源日状态对照；非相邻时不能称逐日状态转移",
        "source_pool_before": len(left),
        "source_pool_after": len(right),
        "market_transitions": dict(changes),
        "mapping": {
            kind: {
                "mapped_source_symbols": len(codes),
                "unmapped_source_symbols": len(universe - codes),
            }
            for kind, codes in mapped.items()
        },
        "snapshots": [
            {"id": row[0], "kind": row[1], "observed_at": row[2].isoformat()}
            for row in snapshots
        ],
        "boards": board_rows[offset : offset + limit],
        "board_count": len(board_rows),
        "page": {
            "offset": offset,
            "limit": limit,
            "next_offset": offset + limit if offset + limit < len(board_rows) else None,
        },
        "symbol": selected,
        "database_transaction": "READ_ONLY",
        "database_write_attempted": False,
    }
