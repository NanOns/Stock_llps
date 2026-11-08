"""从冻结研究事实直接计算三态分母及后验路径统计。"""

from collections import Counter
from fractions import Fraction
from statistics import median

from stock_a.application.source_cohort_forward import _frozen_line_followup
from stock_a.application.source_d0_raw import attach_stock_risk_readiness
from stock_a.application.source_single_followup import observe_frozen_scenario
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.source_forward_path import exploratory_raw_path
from stock_a.storage.postgres.research_events import read_source_events
from stock_a.storage.postgres.research_facts import (
    read_board_members,
    read_research_rows,
)
from stock_a.storage.postgres.source_raw import read_raw_daily_forward_pool


class ResearchSymbolNotFound(ValueError):
    """所选来源代码不在指定的冻结来源池中。"""


def read_persisted_cohort(
    config,
    *,
    target,
    research_cutoff,
    package_sha256,
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
):
    """精确版本只读；未到期与已到期但未保存的事实分别表达。"""
    if target > research_cutoff or offset < 0 or not 1 <= limit <= 100:
        raise ValueError("目标日、截止日或分页参数无效")
    _, _, revision, calendar = _load_calendar(research_cutoff)
    if target not in calendar:
        raise ValueError("目标日不在官方日历中")
    index = calendar.index(target) + horizon
    endpoint_day = calendar[index] if index < len(calendar) else None
    due = endpoint_day is not None and endpoint_day <= research_cutoff
    rows = read_research_rows(
        config,
        target=target,
        package_sha256=package_sha256,
        profile_revision=profile_revision,
        history_session_count=history_session_count,
        d1_history_session_count=d1_history_session_count,
        event_window_start=event_window_start,
        calendar_revision=revision,
        rule=rule,
        horizon=horizon,
        research_cutoff=research_cutoff,
    )
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
            if selected[18] is not None
            else None
        )
        days = tuple(
            day
            for day in calendar[calendar.index(target) : index + 1]
            if day <= research_cutoff
        )
        raw = read_raw_daily_forward_pool(
            config,
            source_symbols=(symbol,),
            sessions=days,
            research_cutoff=research_cutoff,
            package_sha256=package_sha256,
            expected_namespace_id=selected[21],
        )
        source_rows = raw["rows_by_symbol"][symbol]
        birth = next(
            (row for row in source_rows if row["session_date"] == target), None
        )
        if birth is None or str(birth["source_capture_id"]) != selected[22]:
            raise ValueError("后继Raw与冻结来源捕获不一致")
        path = exploratory_raw_path(
            days,
            source_rows,
            horizon,
            target_session=endpoint_day,
            saved_through=raw["saved_through"],
        )
        followup = _frozen_line_followup(
            {"failure_line_candidate": {"value": line, "source": selected[20]}}, path
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
                package_sha256=package_sha256,
                namespace_id=selected[21],
                target_capture_id=selected[22],
                calendar=calendar,
                horizon=horizon,
                scenario=rule,
            )
        d1_rows = (
            rows
            if rule == "D1_ANCHOR_WINDOW"
            else read_research_rows(
                config,
                target=target,
                package_sha256=package_sha256,
                profile_revision=profile_revision,
                history_session_count=history_session_count,
                d1_history_session_count=d1_history_session_count,
                event_window_start=event_window_start,
                calendar_revision=revision,
                rule="D1_ANCHOR_WINDOW",
                horizon=horizon,
                research_cutoff=research_cutoff,
            )
        )
        d1 = next((row for row in d1_rows if row[0] == symbol), None)
        events = read_source_events(
            config,
            target=target,
            package_sha256=package_sha256,
            symbol=symbol,
            d1_history_session_count=d1_history_session_count,
            calendar_revision=revision,
            event_window_start=event_window_start,
        )
        if (
            d1 is None
            or d1[21:23] != selected[21:23]
            or len(events) != d1[3]
            or any(event[15] != selected[22] for event in events)
        ):
            raise ValueError("逐锚事实与冻结D1事件数或来源捕获不一致")
        anchor_events = []
        for event in events:
            lower = (
                {"numerator": str(event[4]), "denominator": str(event[5])}
                if event[4] is not None
                else None
            )
            anchor_events.append(
                {
                    "anchor_session": event[0].isoformat(),
                    "anchor_capture_id": event[1],
                    "anchor_entry_sha256": event[2],
                    "generation_status": event[3],
                    "lower": lower,
                    "upper": {"numerator": str(event[6]), "denominator": str(event[7])}
                    if event[6] is not None
                    else None,
                    "previous_session": event[8].isoformat(),
                    "previous_state": event[9],
                    "previous_quality": event[10],
                    "target_state": event[11],
                    "target_quality": event[12],
                    "change_classification": event[13],
                    "reason": event[14],
                    "lower_touch_followup": _frozen_line_followup(
                        {
                            "failure_line_candidate": {
                                "value": lower,
                                "source": "D1_PLATFORM_LOWER",
                            }
                        },
                        path,
                    ),
                }
            )
        if include_risk:
            launch_rows = (
                rows
                if rule == "LAUNCH_CONFIRM"
                else read_research_rows(
                    config,
                    target=target,
                    package_sha256=package_sha256,
                    profile_revision=profile_revision,
                    history_session_count=history_session_count,
                    d1_history_session_count=d1_history_session_count,
                    event_window_start=event_window_start,
                    calendar_revision=revision,
                    rule="LAUNCH_CONFIRM",
                    horizon=horizon,
                    research_cutoff=research_cutoff,
                )
            )
            launch = next((row for row in launch_rows if row[0] == symbol), None)
            if launch is None or launch[21:23] != selected[21:23]:
                raise ValueError("启动规则与选中来源捕获不一致")
            diagnostic = {
                "namespace_id": selected[21],
                "scenario": "LAUNCH_CONFIRM",
                "results": [{"source_symbol": symbol, "scenario_raw": launch[1]}],
            }
            rps_unavailable = {
                "namespace_id": selected[21],
                "target_session": target,
                "research_cutoff": research_cutoff,
                "package_sha256": package_sha256,
                "facts": {},
            }
            attach_stock_risk_readiness(
                config,
                diagnostic,
                target=target,
                cutoff=research_cutoff,
                package_sha256=package_sha256,
                rps_read=rps_unavailable,
            )
            risk_readiness = diagnostic["results"][0]["risk_priority_readiness"]
    return {
        "scope": "NON_PIT_EXPLORATORY",
        "target_session": target.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "rule": rule,
        "horizon": horizon,
        "endpoint_session": endpoint_day.isoformat() if endpoint_day else None,
        "due": due,
        "source_pool_count": len(rows),
        "groups": groups,
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
            }
            for row in rows[offset : offset + limit]
        ],
        "page": {
            "offset": offset,
            "limit": limit,
            "next_offset": offset + limit if offset + limit < len(rows) else None,
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
    package_sha256,
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
        package_sha256=package_sha256,
        profile_revision=profile_revision,
        history_session_count=history_session_count,
        d1_history_session_count=d1_history_session_count,
        calendar_revision=revision,
        rule=rule,
        horizon=1,
        research_cutoff=research_cutoff,
    )
    left = read_research_rows(
        target=before, event_window_start=before_event_window_start, **common
    )
    right = read_research_rows(
        target=after, event_window_start=after_event_window_start, **common
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
    snapshots, members = read_board_members(
        config,
        industry_snapshot_id=industry_snapshot_id,
        concept_snapshot_id=concept_snapshot_id,
    )
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
