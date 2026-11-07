"""来源D1多锚研究事件的typed存取，不创建正式episode。"""

from datetime import date

from stock_a.storage.postgres.research_facts import _fraction_pair, _insert_and_compare
from stock_a.storage.postgres.source_raw import _transaction

EVENT_VERSION = "SOURCE_D1_ANCHOR_R1"
EVENT_COLUMNS = (
    "namespace_id",
    "target_session",
    "source_symbol",
    "package_sha256",
    "target_capture_id",
    "event_type",
    "anchor_session",
    "event_version",
    "d1_history_session_count",
    "calendar_revision",
    "anchor_capture_id",
    "anchor_entry_sha256",
    "source_content_id",
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
    namespace_id,
    calendar_revision,
    frozen,
    package_sha256,
    d1_history_session_count,
):
    """使用生产者已验证身份的锚，不把事件数展开为假锚。"""
    target_capture = str(source["source_capture_id"])
    rows = []
    for symbol, item in sorted(frozen.items()):
        for anchor in item["anchors"]:
            origin = anchor["source"]
            if (
                origin.get("source_content_id") != source["source_content_id"]
                or origin.get("package_sha256") != package_sha256
                or str(origin.get("target_capture_id")) != target_capture
                or not origin.get("anchor_entry_sha256")
                or origin["anchor_entry_sha256"] != origin.get("target_entry_sha256")
            ):
                raise ValueError("锚事件与冻结T0来源身份不一致")
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
                    str(namespace_id),
                    target,
                    symbol,
                    package_sha256,
                    target_capture,
                    "SOURCE_D1_BREAKOUT",
                    date.fromisoformat(anchor["support_anchor_session"]),
                    EVENT_VERSION,
                    d1_history_session_count,
                    calendar_revision,
                    str(origin["anchor_capture_id"]),
                    origin["anchor_entry_sha256"],
                    origin["source_content_id"],
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
    if len({row[:10] for row in rows}) != len(rows):
        raise ValueError("来源D1锚业务键重复")
    return rows


def insert_source_events(
    cursor,
    *,
    rows,
    namespace_id,
    target,
    package_sha256,
    target_capture_id,
    profile_revision,
    history_session_count,
    d1_history_session_count,
    event_window_start,
    calendar_revision,
):
    """在调用者的T0事务内对照逐股事件数，再幂等写锚。"""
    expected_prefix = (
        str(namespace_id),
        target,
        package_sha256,
        str(target_capture_id),
    )
    if any((r[0], r[1], r[3], r[4]) != expected_prefix for r in rows):
        raise ValueError("事件批次混入其他T0或来源捕获")
    cursor.execute(
        """SELECT source_symbol,event_count FROM stock.source_rule_evaluations
               WHERE namespace_id=%s AND target_session=%s AND package_sha256=%s
                 AND source_capture_id=%s AND rule_id='D1_ANCHOR_WINDOW'
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
        if row[2] not in counts:
            raise ValueError("锚事件股票不在冻结规则来源池")
        counts[row[2]] += 1
    if not expected or counts != expected:
        raise ValueError("锚事件与逐股冻结事件数不一致")
    inserted = _insert_and_compare(
        cursor,
        "source_research_events",
        EVENT_COLUMNS,
        rows,
        10,
        "namespace_id=%s AND target_session=%s AND package_sha256=%s AND target_capture_id=%s",
        expected_prefix,
    )
    return {"inserted_events": inserted, "event_facts": len(rows)}


def read_source_events(
    config,
    *,
    target,
    package_sha256,
    symbol,
    d1_history_session_count,
    calendar_revision,
    event_window_start,
):
    """按股票独立读回每个来源锚及其冻结状态。"""
    with _transaction(config, write=False) as (_, cursor, namespace):
        cursor.execute(
            """SELECT anchor_session,anchor_capture_id::text,anchor_entry_sha256,
                      generation_status,lower_numerator,lower_denominator,
                      upper_numerator,upper_denominator,previous_session,
                      previous_state,previous_quality,target_state,target_quality,
                      change_classification,reason_code,target_capture_id::text
               FROM stock.source_research_events
               WHERE namespace_id=%s AND target_session=%s AND package_sha256=%s
                 AND source_symbol=%s AND event_type='SOURCE_D1_BREAKOUT'
                 AND event_version=%s AND d1_history_session_count=%s AND calendar_revision=%s
                 AND anchor_session >= %s
               ORDER BY anchor_session""",
            (
                namespace,
                target,
                package_sha256,
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
