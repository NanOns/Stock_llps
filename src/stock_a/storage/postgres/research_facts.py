"""来源级规则与到期路径事实的原子写入及独立读取。"""

from __future__ import annotations

from datetime import date
from fractions import Fraction
from typing import Any, Mapping

from psycopg import sql

from stock_a.storage.postgres.source_raw import _transaction

RULE_VERSION = "SOURCE_COHORT_RULES_R1"
PATH_VERSION = "SOURCE_RAW_FORWARD_PATH_R1"
SCOPE = "NON_PIT_EXPLORATORY"


class ResearchFactConflict(ValueError):
    """同一业务键对应不同冻结事实，事务必须完整回滚。"""

    preserve_transaction_error = True


RULES = frozenset(
    {
        "LAUNCH_CONFIRM",
        "RECOVERY_TURN",
        "s1",
        "s2",
        "seed",
        "D1_T0",
        "D1_ANCHOR_WINDOW",
        "STRONG_PULLBACK_DIAGNOSTIC",
        "TREND_CONTINUE_DIAGNOSTIC",
    }
)

RULE_COLUMNS = (
    "namespace_id",
    "target_session",
    "source_symbol",
    "package_sha256",
    "source_capture_id",
    "rule_id",
    "rule_version",
    "profile_revision",
    "history_session_count",
    "d1_history_session_count",
    "event_window_start",
    "calendar_revision",
    "state",
    "reasons",
    "event_count",
    "frozen_line_numerator",
    "frozen_line_denominator",
    "frozen_line_source",
    "scope",
)
PATH_COLUMNS = (
    "namespace_id",
    "target_session",
    "source_symbol",
    "package_sha256",
    "source_capture_id",
    "horizon",
    "path_rule_version",
    "calendar_revision",
    "endpoint_session",
    "maturity",
    "endpoint_quality",
    "endpoint_reason",
    "endpoint_numerator",
    "endpoint_denominator",
    "path_quality",
    "path_reasons",
    "mfe_numerator",
    "mfe_denominator",
    "mae_numerator",
    "mae_denominator",
    "mdd_numerator",
    "mdd_denominator",
    "price_coordinate",
    "scope",
)


def _fraction_pair(value: Any) -> tuple[int | None, int | None]:
    if value is None:
        return None, None
    if isinstance(value, Mapping):
        value = value["exact"]
    fraction = Fraction(value)
    return fraction.numerator, fraction.denominator


def prepare_research_rows(
    *,
    target,
    source,
    namespace_id,
    calendar_revision,
    session_versions,
    frozen,
    outcomes,
    package_sha256,
    profile_revision,
    history_session_count,
    d1_history_session_count,
    event_window_start,
):
    """只转换实际生产者结果；未到期路径不构成持久结算事实。"""
    if not frozen or set(frozen) != set(outcomes):
        raise ValueError("冻结规则与后继路径的来源池不一致")
    capture = str(source["source_capture_id"])
    common = (str(namespace_id), target, None, package_sha256, capture)
    rules, paths = [], []
    expected_rules = None
    for symbol in sorted(frozen):
        item = frozen[symbol]
        names = set(item["rules"])
        if expected_rules is None:
            expected_rules = names
        if names != expected_rules or names != RULES:
            raise ValueError("每个来源代码必须具有一致的九个规则")
        prefix = common[:2] + (symbol,) + common[3:]
        for rule, fact in sorted(item["rules"].items()):
            state = fact["state"]
            reasons = fact.get("reasons") or []
            if state not in {"TRUE", "FALSE", "UNKNOWN"} or any(
                not isinstance(reason, str) for reason in reasons
            ):
                raise ValueError("规则状态或原因无效")
            count = 0
            if rule == "D1_ANCHOR_WINDOW" and state == "TRUE":
                count = len(item["anchors"])
            elif rule == "STRONG_PULLBACK_DIAGNOSTIC" and state == "TRUE":
                count = item["diagnostic_shape_summary"]["matched_parent_count"]
            line = fact.get("frozen_line") or {}
            line_value = line.get("value")
            line_pair = (
                (int(line_value["numerator"]), int(line_value["denominator"]))
                if line_value is not None
                else (None, None)
            )
            rules.append(
                prefix
                + (
                    rule,
                    RULE_VERSION,
                    profile_revision,
                    history_session_count,
                    d1_history_session_count,
                    event_window_start,
                    calendar_revision,
                    state,
                    list(reasons),
                    count,
                    *line_pair,
                    line.get("source"),
                    SCOPE,
                )
            )
        for horizon, fact in sorted(outcomes[symbol].items()):
            if not fact["due"]:
                continue
            endpoint_day = date.fromisoformat(fact["target_session"])
            if endpoint_day.isoformat() not in session_versions:
                raise ValueError("到期路径缺少来源会话版本")
            endpoint = fact["endpoint"]["raw_close_relative_change"]
            ep_num, ep_den = _fraction_pair(endpoint.get("value"))
            values = fact.get("path_values") or {}
            mfe = _fraction_pair(values.get("mfe"))
            mae = _fraction_pair(values.get("mae"))
            mdd = _fraction_pair(values.get("mdd"))
            paths.append(
                prefix
                + (
                    horizon,
                    PATH_VERSION,
                    calendar_revision,
                    endpoint_day,
                    fact["maturity"],
                    endpoint["quality"],
                    endpoint.get("reason"),
                    ep_num,
                    ep_den,
                    fact["path_quality"],
                    list(fact["path_reasons"]),
                    *mfe,
                    *mae,
                    *mdd,
                    "RAW_UNADJUSTED",
                    SCOPE,
                )
            )
    return rules, paths


def _insert_and_compare(cursor, table, columns, rows, key_count, selection, args):
    if not rows:
        return 0
    statement = sql.SQL(
        "INSERT INTO stock.{} ({}) VALUES ({}) ON CONFLICT DO NOTHING"
    ).format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    cursor.executemany(statement, rows)
    inserted = cursor.rowcount
    select_columns = [
        sql.SQL("{}::text").format(sql.Identifier(column))
        if column
        in {
            "namespace_id",
            "source_capture_id",
            "target_capture_id",
            "anchor_capture_id",
        }
        else sql.Identifier(column)
        for column in columns
    ]
    cursor.execute(
        sql.SQL("SELECT {} FROM stock.{} WHERE ").format(
            sql.SQL(", ").join(select_columns), sql.Identifier(table)
        )
        + sql.SQL(selection),
        args,
    )
    stored = {tuple(row[:key_count]): tuple(row) for row in cursor.fetchall()}
    for row in rows:
        actual = stored.get(tuple(row[:key_count]))
        if actual != row:
            raise ResearchFactConflict(f"{table}业务键已存在不同事实，整事务回滚")
    return inserted


def write_research_rows(config, *, rules, paths, events=None):
    """单T0整体事务；同键相同内容幂等，异值拒绝并回滚。"""
    if not rules:
        raise ValueError("不允许写入空规则来源池")
    first = rules[0]
    namespace, target, _, package, capture = first[:5]
    if any(
        (r[0], r[1], r[3], r[4]) != (namespace, target, package, capture)
        for r in rules + paths
    ):
        raise ValueError("一个事务只能写入一个T0和来源捕获")
    with _transaction(config, write=True) as (_, cursor, actual_namespace):
        if actual_namespace != namespace:
            raise ValueError("事实命名空间与连接身份不一致")
        cursor.execute(
            """SELECT source_symbol,source_capture_id::text FROM stock.source_raw_daily
               WHERE namespace_id=%s AND session_date=%s AND package_sha256=%s""",
            (namespace, target, package),
        )
        raw = cursor.fetchall()
        expected = {row[2] for row in rules}
        if (
            len(raw) != len(expected)
            or {row[0] for row in raw} != expected
            or any(row[1] != capture for row in raw)
            or len(rules) != len(expected) * len(RULES)
            or {row[5] for row in rules} != RULES
            or len({(row[2], row[5]) for row in rules}) != len(rules)
            or not {row[2] for row in paths}.issubset(expected)
            or len({tuple(row[:12]) for row in rules}) != len(rules)
            or len({tuple(row[:8]) for row in paths}) != len(paths)
        ):
            raise ValueError("规则或路径与T0完整来源池和捕获身份不一致")
        count_rules = _insert_and_compare(
            cursor,
            "source_rule_evaluations",
            RULE_COLUMNS,
            rules,
            12,
            "namespace_id=%s AND target_session=%s AND package_sha256=%s AND source_capture_id=%s",
            (namespace, target, package, capture),
        )
        count_paths = _insert_and_compare(
            cursor,
            "source_forward_path_facts",
            PATH_COLUMNS,
            paths,
            8,
            "namespace_id=%s AND target_session=%s AND package_sha256=%s AND source_capture_id=%s",
            (namespace, target, package, capture),
        )
        event_result = None
        if events is not None:
            from stock_a.storage.postgres.research_events import insert_source_events

            event_result = insert_source_events(
                cursor,
                rows=events,
                namespace_id=namespace,
                target=target,
                package_sha256=package,
                target_capture_id=capture,
                profile_revision=first[7],
                history_session_count=first[8],
                d1_history_session_count=first[9],
                event_window_start=first[10],
                calendar_revision=first[11],
            )
    return {
        "inserted_rules": count_rules,
        "inserted_paths": count_paths,
        "events": event_result,
    }


def read_research_rows(
    config,
    *,
    target,
    package_sha256,
    profile_revision,
    history_session_count,
    d1_history_session_count,
    event_window_start,
    calendar_revision,
    rule,
    horizon,
    research_cutoff,
):
    """从已保存事实读取完整三态分母及共享路径，不扫描Raw。"""
    if horizon not in {1, 3, 5, 10, 20}:
        raise ValueError("后继期限必须为1、3、5、10或20")
    with _transaction(config, write=False) as (_, cursor, namespace):
        cursor.execute(
            """SELECT r.source_symbol, r.state, r.reasons, r.event_count,
                      p.endpoint_session, p.maturity, p.endpoint_quality,
                      p.endpoint_reason, p.endpoint_numerator, p.endpoint_denominator,
                      p.path_quality, p.path_reasons,
                      p.mfe_numerator, p.mfe_denominator, p.mae_numerator,
                      p.mae_denominator, p.mdd_numerator, p.mdd_denominator,
                      r.frozen_line_numerator, r.frozen_line_denominator,
                      r.frozen_line_source, r.namespace_id::text, r.source_capture_id::text
               FROM stock.source_rule_evaluations r
               LEFT JOIN stock.source_forward_path_facts p
                 ON p.namespace_id=r.namespace_id AND p.target_session=r.target_session
                AND p.source_symbol=r.source_symbol AND p.package_sha256=r.package_sha256
                AND p.source_capture_id=r.source_capture_id AND p.horizon=%s
                AND p.path_rule_version=%s AND p.calendar_revision=r.calendar_revision
                AND p.endpoint_session <= %s
               WHERE r.namespace_id=%s AND r.target_session=%s AND r.package_sha256=%s
                 AND r.rule_id=%s AND r.rule_version=%s AND r.profile_revision=%s
                 AND r.history_session_count=%s AND r.d1_history_session_count=%s
                 AND r.event_window_start=%s AND r.calendar_revision=%s
               ORDER BY r.source_symbol""",
            (
                horizon,
                PATH_VERSION,
                research_cutoff,
                namespace,
                target,
                package_sha256,
                rule,
                RULE_VERSION,
                profile_revision,
                history_session_count,
                d1_history_session_count,
                event_window_start,
                calendar_revision,
            ),
        )
        rows = cursor.fetchall()
    if not rows:
        raise ValueError("未找到精确输入版本的持久规则事实")
    if len({row[0] for row in rows}) != len(rows):
        raise ValueError("精确规则版本存在多个来源捕获，必须先消除歧义")
    return rows


def read_board_members(config, *, industry_snapshot_id, concept_snapshot_id):
    """读取明确指定的后验行业及概念成员，不推断历史有效期。"""
    with _transaction(config, write=False) as (_, cursor, _):
        cursor.execute(
            """SELECT snapshot_id::text, source_kind, observed_at, research_mode
               FROM stock.tdx_sector_source_snapshots WHERE snapshot_id IN (%s,%s)""",
            (industry_snapshot_id, concept_snapshot_id),
        )
        snapshots = cursor.fetchall()
        kinds = {row[0]: row[1] for row in snapshots}
        if kinds != {
            str(industry_snapshot_id): "TDX_TDXHY_CFG",
            str(concept_snapshot_id): "TDX_INFOHARBOR_BLOCK_DAT",
        } or any(
            row[3] != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT" for row in snapshots
        ):
            raise ValueError("行业或概念快照身份和后验模式不匹配")
        cursor.execute(
            """SELECT m.board_kind,m.board_native_id,c.board_name,m.source_symbol
               FROM stock.tdx_sector_members m JOIN stock.tdx_sector_catalog c
                 USING (snapshot_id,board_kind,board_native_id)
               WHERE m.snapshot_id IN (%s,%s)
               ORDER BY m.board_kind,m.board_native_id,m.source_symbol""",
            (industry_snapshot_id, concept_snapshot_id),
        )
        members = cursor.fetchall()
    return snapshots, members
