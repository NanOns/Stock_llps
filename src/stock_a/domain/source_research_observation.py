"""来源级研究登记与逐会话观察的纯状态规则。

这些状态只描述已经保存、可比较的来源研究事实，不表示正式 Focus 资格。
调用方负责按官方日历提供相邻会话及精确来源、规则和价格坐标。
"""

from dataclasses import dataclass
from datetime import date
from typing import Literal


TriState = Literal["TRUE", "FALSE", "UNKNOWN"]
Lifecycle = Literal["ACTIVE", "PAUSED", "EXITED", "EXITED_UNKNOWN"]


@dataclass(frozen=True)
class Observation:
    """一个已计算的三态事实；缺少事实不得构造为 FALSE。"""

    session: date
    state: TriState
    evidence_id: int

    def __post_init__(self) -> None:
        if type(self.session) is not date or self.state not in {
            "TRUE",
            "FALSE",
            "UNKNOWN",
        }:
            raise ValueError("观察日期或三态无效")
        if type(self.evidence_id) is not int or self.evidence_id <= 0:
            raise ValueError("观察必须引用已保存的证据")


@dataclass(frozen=True)
class Decision:
    """研究观察决定及其生命周期；None表示没有可登记的匹配。"""

    event: str
    lifecycle: Lifecycle | None
    reason: str


def initial_observation(
    current: Observation,
    *,
    previous: Observation | None,
    previous_official_session: date,
) -> Decision:
    """区分经相邻前态证明的转入与左截断首次观察。"""
    if (
        type(previous_official_session) is not date
        or previous_official_session >= current.session
    ):
        raise ValueError("上一官方会话无效")
    if previous is not None and previous.session != previous_official_session:
        raise ValueError("前态不是相邻官方会话")
    if current.state == "UNKNOWN":
        return Decision("UNKNOWN", None, "CURRENT_STATE_UNKNOWN")
    if current.state == "FALSE":
        return Decision("KNOWN_NO_SIGNAL", None, "CURRENT_STATE_FALSE")
    if previous is not None and previous.state == "FALSE":
        return Decision("ENTRY", "ACTIVE", "ADJACENT_FALSE_TO_TRUE")
    if previous is not None and previous.state == "TRUE":
        return Decision("DAILY_LANDMARK", "ACTIVE", "ADJACENT_TRUE_TO_TRUE")
    return Decision(
        "FIRST_OBSERVED_LEFT_CENSORED", "ACTIVE", "NO_PROVED_ADJACENT_ENTRY"
    )


def advance_observation(
    *,
    lifecycle: Lifecycle,
    previous_session: date,
    expected_session: date,
    current: Observation | None,
    research_cutoff: date,
) -> Decision:
    """按官方相邻会话推进；未知暂停，缺证据不产生退出或重入。"""
    if lifecycle not in {"ACTIVE", "PAUSED", "EXITED", "EXITED_UNKNOWN"}:
        raise ValueError("观察生命周期无效")
    if any(
        type(value) is not date
        for value in (previous_session, expected_session, research_cutoff)
    ):
        raise ValueError("观察会话和截止日期必须是日期")
    if expected_session <= previous_session:
        raise ValueError("后继会话必须晚于上一观察会话")
    if current is not None and current.session != expected_session:
        raise ValueError("后继事实不是预期官方会话")
    if expected_session > research_cutoff:
        if current is not None:
            raise ValueError("不能使用截止之后的事实")
        return Decision("NOT_DUE", lifecycle, "WAITING_FOR_FUTURE_SESSION")
    if current is None or current.state == "UNKNOWN":
        return Decision(
            "PAUSED_UNKNOWN" if lifecycle in {"ACTIVE", "PAUSED"} else "UNKNOWN",
            "PAUSED" if lifecycle in {"ACTIVE", "PAUSED"} else "EXITED_UNKNOWN",
            "NO_STORED_FACT" if current is None else "CURRENT_STATE_UNKNOWN",
        )
    if lifecycle == "EXITED_UNKNOWN":
        if current.state == "TRUE":
            return Decision(
                "FIRST_OBSERVED_LEFT_CENSORED",
                "ACTIVE",
                "REENTRY_NOT_PROVED_AFTER_UNKNOWN",
            )
        return Decision("NO_MATCH", "EXITED", "CURRENT_STATE_FALSE")
    if lifecycle == "EXITED":
        if current.state == "TRUE":
            return Decision("REENTRY", "ACTIVE", "MATCH_AFTER_PROVED_EXIT")
        return Decision("NO_MATCH", "EXITED", "CURRENT_STATE_FALSE")
    if current.state == "FALSE":
        return Decision(
            "EXIT_PROVED",
            "EXITED",
            "OBSERVED_FALSE_AFTER_UNKNOWN"
            if lifecycle == "PAUSED"
            else "COMPARABLE_FALSE",
        )
    if lifecycle == "PAUSED":
        return Decision("LEFT_CENSORED", "ACTIVE", "MATCH_AFTER_UNKNOWN_INTERVAL")
    return Decision("CONTINUES", "ACTIVE", "COMPARABLE_TRUE")
