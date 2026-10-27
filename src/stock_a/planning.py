"""同一DAG计划使用的纯增量模式判定。"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

from stock_a.domain import ContractError


@dataclass(frozen=True)
class AcceptedRunIdentity:
    """前一已接受研究的最小计划身份，不包含业务payload。"""

    target_session: date
    head_generation: int
    calendar_id: str
    calendar_revision: str
    semantic_revisions: Mapping[str, str]


@dataclass(frozen=True)
class IncrementalModeDecision:
    """纯分类结果；修订路径只标记，不在此执行。"""

    mode: str
    target_session: date
    expected_parent_generation: int | None
    sessions_to_append: tuple[date, ...]
    reason_codes: tuple[str, ...]


def _revision_map(value: Mapping[str, str], name: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ContractError(name + "必须是revision映射")
    result = {}
    for key, revision in value.items():
        if not isinstance(key, str) or not key.strip():
            raise ContractError(name + "含空白或非法revision键")
        if not isinstance(revision, str):
            raise ContractError(name + "的revision值必须是字符串")
        result[key] = revision
    return result


def classify_incremental_mode(
    *,
    target_session: date,
    official_sessions: tuple[date, ...],
    calendar_id: str,
    calendar_revision: str,
    semantic_revisions: Mapping[str, str],
    previous: AcceptedRunIdentity | None,
    historical_repair_required: bool = False,
) -> IncrementalModeDecision:
    """按显式会话和revision身份区分首次运行、APPEND与修订需求。

    此纯函数不认证调用者提供的来源，不读取配置/数据库，也不执行RANGE_REPAIR。
    """
    if type(target_session) is not date:
        raise ContractError("计划目标会话必须是date")
    if (not isinstance(calendar_id, str) or not calendar_id.strip()
            or not isinstance(calendar_revision, str) or not calendar_revision.strip()):
        raise ContractError("计划缺少官方日历身份")
    if (not isinstance(official_sessions, tuple) or not official_sessions
            or any(type(session) is not date for session in official_sessions)
            or tuple(sorted(set(official_sessions))) != official_sessions):
        raise ContractError("官方会话必须是非空、无重复且递增的date元组")
    if target_session not in official_sessions:
        raise ContractError("目标会话不在显式官方会话集合中")
    if type(historical_repair_required) is not bool:
        raise ContractError("historical_repair_required必须为bool")
    current_revisions = _revision_map(semantic_revisions, "当前语义revision")

    if previous is None:
        if not current_revisions or any(not revision.strip() for revision in current_revisions.values()):
            raise ContractError("首次计划缺少完整语义revision")
        return IncrementalModeDecision("INITIAL", target_session, None, (), ())
    if not isinstance(previous, AcceptedRunIdentity):
        raise ContractError("previous必须是AcceptedRunIdentity或None")
    if type(previous.target_session) is not date:
        raise ContractError("前一接受目标会话必须是date")
    if (type(previous.head_generation) is not int or previous.head_generation < 0):
        raise ContractError("前一Head generation必须是非负整数且不能是bool")
    if (not isinstance(previous.calendar_id, str) or not previous.calendar_id.strip()
            or not isinstance(previous.calendar_revision, str) or not previous.calendar_revision.strip()):
        raise ContractError("前一接受状态缺少官方日历身份")
    prior_revisions = _revision_map(previous.semantic_revisions, "前一语义revision")

    reasons = []
    if historical_repair_required:
        reasons.append("HISTORICAL_DEPENDENCY_REPAIR_REQUIRED")
    if (previous.calendar_id != calendar_id
            or previous.calendar_revision != calendar_revision):
        reasons.append("CALENDAR_IDENTITY_CHANGED")
    if target_session <= previous.target_session:
        reasons.append("TARGET_NOT_AFTER_ACCEPTED_SESSION")
    if (not prior_revisions or not current_revisions
            or any(not revision.strip() for revision in (*prior_revisions.values(), *current_revisions.values()))):
        reasons.append("SEMANTIC_REVISION_MISSING")
    elif set(prior_revisions) != set(current_revisions):
        reasons.append("SEMANTIC_REVISION_SET_CHANGED")
    elif prior_revisions != current_revisions:
        reasons.append("SEMANTIC_REVISION_CHANGED")

    if reasons:
        return IncrementalModeDecision(
            "RANGE_REPAIR_REQUIRED", target_session, previous.head_generation, (), tuple(reasons)
        )
    if previous.target_session not in official_sessions:
        return IncrementalModeDecision(
            "RANGE_REPAIR_REQUIRED", target_session, previous.head_generation, (),
            ("ACCEPTED_SESSION_NOT_IN_CURRENT_CALENDAR",),
        )
    prior_index = official_sessions.index(previous.target_session)
    target_index = official_sessions.index(target_session)
    sessions_to_append = official_sessions[prior_index + 1:target_index + 1]
    if not sessions_to_append:
        return IncrementalModeDecision(
            "RANGE_REPAIR_REQUIRED", target_session, previous.head_generation, (),
            ("APPEND_SESSION_RANGE_EMPTY",),
        )
    return IncrementalModeDecision(
        "APPEND", target_session, previous.head_generation, sessions_to_append, ()
    )
