"""来源D1多锚研究事件的typed存取，不创建正式episode。"""

from datetime import date

from stock_a.storage.postgres.research_facts import _fraction_pair, _insert_and_compare
from stock_a.storage.postgres.source_raw import _transaction

EVENT_VERSION = "SOURCE_D1_ANCHOR_R1"
EVENT_COLUMNS = (
    "target_session",
    "source_symbol",
    "source_package_id",
    "target_capture_id",
    "event_type",
    "anchor_session",
    "event_version",
    "d1_history_session_count",
    "calendar_revision",
    "anchor_capture_id",
    "price_coordinate",
    "generation_status",
    "lower_numerator",
    "lower_denominator",
    "upper_numerator",
    "upper_denominator",
    "previous_session",
    "previous_state",
    "previous_quality",
    "target_state",
    "target_quality",
    "change_classification",
    "reason_code",
    "scope",
)


def prepare_source_events(
    *,
    target,
    source,
    calendar_revision,
    frozen,
    source_package_id,
    d1_history_session_count,
):
    """使用生产者已验证的数字捕获父键，不复制Raw条目摘要。"""
    target_capture = source["source_capture_id"]
    if type(target_capture) is not int or target_capture <= 0:
        raise ValueError("D1目标来源捕获数字ID无效")
    rows = []
    for symbol, item in sorted(frozen.items()):
        for anchor in item["anchors"]:
            origin = anchor["source"]
            anchor_capture = origin.get("anchor_capture_id")
            if (
                origin.get("source_package_id") != source_package_id
                or origin.get("target_capture_id") != target_capture
                or type(anchor_capture) is not int
                or anchor_capture <= 0
                or origin.get("source_symbol") != symbol
            ):
                raise ValueError("锚事件与冻结T0来源数字ID或证券不一致")
            platform = anchor.get("platform_generation") or {}
            lower = _fraction_pair(platform.get("lower"))
            upper = _fraction_pair(platform.get("upper"))
            before = (anchor.get("support_observation_before") or {}).get(
                "support_observation"
            ) or {}
            after = (anchor.get("support_observation_after") or {}).get(
                "support_observation"
            ) or {}
            rows.append(
                (
                    target,
                    symbol,
                    source_package_id,
                    target_capture,
                    "SOURCE_D1_BREAKOUT",
                    date.fromisoformat(anchor["support_anchor_session"]),
                    EVENT_VERSION,
                    d1_history_session_count,
                    calendar_revision,
                    anchor_capture,
                    "RAW_UNADJUSTED",
                    anchor.get("support_followup_status") or "NOT_GENERATED",
                    *lower,
                    *upper,
                    date.fromisoformat(anchor["previous_official_session"]),
                    before.get("state"),
                    before.get("quality"),
                    after.get("state"),
                    after.get("quality"),
                    anchor["change_classification"],
                    anchor.get("reason_code"),
                    "NON_PIT_EXPLORATORY",
                )
            )
    event_keys = {
        (row[3], row[1], row[4], row[9], row[6], row[7], row[8]) for row in rows
    }
    if len(event_keys) != len(rows):
        raise ValueError("来源D1锚业务键重复")
    return rows


def insert_source_events(
    cursor,
    *,
    rows,
    target,
    source_package_id,
    target_capture_id,
    profile_revision,
    history_session_count,
    d1_history_session_count,
    event_window_start,
    calendar_revision,
):
    """在调用者的T0事务内对照逐股事件数，再按数字父键幂等写锚。"""
    expected_prefix = (target, source_package_id, target_capture_id)
    if any((r[0], r[2], r[3]) != expected_prefix for r in rows):
        raise ValueError("事件批次混入其他T0或来源捕获")
    cursor.execute(
        """SELECT source_symbol,event_count FROM source_rule_evaluations
               WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s
                 AND rule_id='D1_ANCHOR_WINDOW'
                 AND rule_version='SOURCE_COHORT_RULES_R1' AND profile_revision=%s
                 AND history_session_count=%s AND d1_history_session_count=%s
                 AND event_window_start=%s AND calendar_revision=%s""",
        (
            *expected_prefix,
            profile_revision,
            history_session_count,
            d1_history_session_count,
            event_window_start,
            calendar_revision,
        ),
    )
    expected = dict(cursor.fetchall())
    counts = {symbol: 0 for symbol in expected}
    for row in rows:
        if row[1] not in counts:
            raise ValueError("锚事件股票不在冻结规则来源池")
        counts[row[1]] += 1
    if not expected or counts != expected:
        raise ValueError("锚事件与逐股冻结事件数不一致")
    inserted = _insert_and_compare(
        cursor,
        "source_research_events",
        EVENT_COLUMNS,
        rows,
        (
            "target_capture_id",
            "source_symbol",
            "event_type",
            "anchor_capture_id",
            "event_version",
            "d1_history_session_count",
            "calendar_revision",
        ),
        "target_session=%s AND source_package_id=%s AND target_capture_id=%s",
        expected_prefix,
    )
    return {"inserted_events": inserted, "event_facts": len(rows)}


def read_source_events(
    config,
    *,
    target,
    source_package_id,
    symbol,
    d1_history_session_count,
    calendar_revision,
    event_window_start,
):
    """按股票独立读回每个来源锚及其冻结状态。"""
    with _transaction(config, write=False) as (_, cursor, _):
        cursor.execute(
            """SELECT anchor_session,anchor_capture_id,generation_status,
                      lower_numerator,lower_denominator,
                      upper_numerator,upper_denominator,previous_session,
                      previous_state,previous_quality,target_state,target_quality,
                      change_classification,reason_code,target_capture_id
               FROM source_research_events
               WHERE target_session=%s AND source_package_id=%s
                 AND source_symbol=%s AND event_type='SOURCE_D1_BREAKOUT'
                 AND event_version=%s AND d1_history_session_count=%s AND calendar_revision=%s
                 AND anchor_session >= %s
               ORDER BY anchor_session""",
            (
                target,
                source_package_id,
                symbol,
                EVENT_VERSION,
                d1_history_session_count,
                calendar_revision,
                event_window_start,
            ),
        )
        rows = cursor.fetchall()
    if len({row[0] for row in rows}) != len(rows):
        raise ValueError("来源锚含多个捕获版本，必须明确消歧")
    return rows
