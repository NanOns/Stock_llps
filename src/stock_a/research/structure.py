"""P15十种结构的唯一生成边界；原锚不变，观察和D2失效线分别消费。"""
from dataclasses import dataclass
from fractions import Fraction

from stock_a.canonical.identity import semantic_digest
from stock_a.domain import ContractError, FieldValue, unknown
from stock_a.research.admission import Event
from stock_a.research.facts import boolean, number, capability

CONTRACT = "STOCK_A_D1_STRUCTURE_TYPE_REGISTRY_R1"
RELATIONS = {
    "IMPULSE_BODY": "SUPPORT", "IMPULSE_LOW": "SUPPORT", "BREAKOUT_PLATFORM": "SUPPORT",
    "PIVOT_LOW": "SUPPORT", "PIVOT_HIGH": "RESISTANCE", "RANGE_FLOOR": "SUPPORT",
    "RANGE_CEILING": "RESISTANCE", "GAP_ZONE": "SUPPORT", "DYNAMIC_MA5": "SUPPORT", "DYNAMIC_MA20": "SUPPORT",
}


@dataclass(frozen=True)
class Bar:
    ordinal: int
    ohlc: FieldValue


@dataclass(frozen=True)
class Anchor:
    anchor_id: str
    entity_id: str
    kind: str
    relation: str
    created_ordinal: int
    available_from: int
    test_from: int
    coordinate_id: str
    lower: object
    upper: object
    source_event_id: str
    source_ref: str


def _prices(bar):
    from stock_a.domain import exact_number
    if not isinstance(bar, Bar) or type(bar.ordinal) is not int or bar.ordinal < 0:
        raise ValueError("行情会话身份非法")
    if bar.ohlc.quality != "OBSERVED":
        return None
    o, h, l, c = (exact_number(x) for x in bar.ohlc.value)
    if min(o, h, l, c) <= 0 or not l <= min(o, c) <= max(o, c) <= h:
        raise ValueError("结构输入OHLC非法")
    return o, h, l, c


def _window(bars, statuses, count, endpoint):
    rows = tuple(bars)
    if len(rows) != count or rows[-1].ordinal > endpoint:
        return None
    ordinals = [b.ordinal for b in rows]
    if ordinals != sorted(set(ordinals)):
        raise ValueError("结构窗口重复或逆序")
    pairs = tuple(statuses)
    status = dict(pairs)
    if len(status) != len(pairs):
        raise ValueError("窗口状态重复")
    if any(status.get(i) not in {"ACTUAL", "CONFIRMED_SUSPENSION"} for i in range(ordinals[0], endpoint + 1)):
        return None
    actual = [i for i in range(ordinals[0], endpoint + 1) if status[i] == "ACTUAL"]
    if actual != ordinals:
        return None
    values = [_prices(b) for b in rows]
    return tuple(values) if all(v is not None for v in values) else None


def generate(kind, *, entity_id, ordinal, coordinate_id, source_ref, window_ref,
             actual: FieldValue, today: Bar, inputs, prior20=(), pivot5=(), statuses=(),
             source_event: Event | None = None, previous_anchor: Anchor | None = None,
             prior_qualified: FieldValue | None = None, capability_state):
    capability(capability_state)
    if kind not in RELATIONS or not all((entity_id, coordinate_id, source_ref, window_ref)) or today.ordinal != ordinal:
        raise ContractError("结构类型或精确上下文未绑定")
    current_actual = boolean(actual)
    if current_actual is False:
        return FieldValue(None, "NOT_APPLICABLE", "NO_ACTUAL_BAR")
    if current_actual is None:
        return unknown("ACTUAL_UNKNOWN")
    price = _prices(today)
    if price is None:
        return unknown("PRICE_UNKNOWN")
    o, h, l, c = price
    required = {
        "IMPULSE_BODY": ("atr_previous", "clv", "amount_ratio20", "rel_market_1"),
        "IMPULSE_LOW": ("atr_previous", "clv", "amount_ratio20", "rel_market_1"),
        "BREAKOUT_PLATFORM": ("prior_high20", "atr_today", "clv"),
        "RANGE_FLOOR": ("atr_previous", "slope_previous"),
        "RANGE_CEILING": ("atr_previous", "slope_previous"),
        "DYNAMIC_MA5": ("ma5",), "DYNAMIC_MA20": ("ma20",),
    }.get(kind, ())
    if any(key not in inputs for key in required):
        raise ContractError("结构缺少声明字段绑定")
    vals = {key: number(inputs[key], positive=key in {"atr_previous", "atr_today", "prior_high20", "ma5", "ma20"}) for key in required}
    if any(v is None for v in vals.values()):
        return unknown("STRUCTURE_INPUT_UNKNOWN")
    if "clv" in vals and not 0 <= vals["clv"] <= 1:
        raise ValueError("CLV非法")
    if "amount_ratio20" in vals and vals["amount_ratio20"] < 0:
        raise ValueError("量额比非法")
    bounds = None
    if kind.startswith("IMPULSE_"):
        a = vals["atr_previous"]
        if (c - o >= a and h - l >= a and vals["clv"] >= Fraction(7, 10)
                and vals["amount_ratio20"] >= Fraction(6, 5) and vals["rel_market_1"] > 0):
            bounds = (o, (o + c) / 2) if kind == "IMPULSE_BODY" else (l, l)
    elif kind == "BREAKOUT_PLATFORM":
        level = vals["prior_high20"]
        if c > level + vals["atr_today"] / 10 and vals["clv"] >= Fraction(7, 10):
            bounds = level, level  # 缓冲用于突破谓词，不改成D0的PHC20或扩大锚价带
    elif kind.startswith("PIVOT_"):
        rows = _window(pivot5, statuses, 5, ordinal)
        if rows is None or pivot5[-1].ordinal != ordinal:
            return unknown("PIVOT_RIGHT_CONFIRMATION_OR_WINDOW_UNKNOWN")
        component = 2 if kind == "PIVOT_LOW" else 1
        center = rows[2][component]
        others = [rows[i][component] for i in (0, 1, 3, 4)]
        if (center < min(others)) if kind == "PIVOT_LOW" else (center > max(others)):
            bounds = center, center  # 并列极值不生成pivot，右侧第二根真实可见日创建
    elif kind.startswith("RANGE_"):
        rows = _window(prior20, statuses, 20, ordinal - 1)
        if rows is None:
            return unknown("RANGE_PRIOR_WINDOW_UNKNOWN")
        low, high = min(v[2] for v in rows), max(v[1] for v in rows)
        if high - low <= 4 * vals["atr_previous"] and abs(vals["slope_previous"]) <= Fraction(1, 10):
            level = low if kind == "RANGE_FLOOR" else high
            bounds = level, level
    elif kind == "GAP_ZONE":
        if len(prior20) != 1:
            raise ContractError("GAP必须绑定一个前actual端点")
        rows = _window((*prior20, today), statuses, 2, ordinal)
        if rows is None:
            return unknown("GAP_STATUS_OR_ENDPOINT_UNKNOWN")
        prev_high = rows[0][1]
        if l > prev_high:
            bounds = prev_high, l
    else:
        n = "ma5" if kind == "DYNAMIC_MA5" else "ma20"
        allowed = {"DYNAMIC_MA5": {"T0_MA5"}, "DYNAMIC_MA20": {"T0_MA20", "T0_MA20_FALLBACK"}}
        if (source_event is None or source_event.entity_id != entity_id or source_event.session_ordinal != ordinal
                or source_event.failure_line.source_ref not in allowed[kind]
                or source_event.failure_line.coordinate_id != coordinate_id):
            return unknown("REGISTERED_MA_SOURCE_EVENT_REQUIRED")
        if vals[n] != source_event.failure_line.value:
            raise ValueError("动态MA出生值与来源事件不符")
        bounds = vals[n], vals[n]
    if bounds is None:
        return FieldValue(None, "NOT_APPLICABLE", "STRUCTURE_PREDICATE_FALSE")
    if previous_anchor is not None:
        if (previous_anchor.entity_id, previous_anchor.kind, previous_anchor.coordinate_id) != (entity_id, kind, coordinate_id):
            raise ValueError("旧锚上下文不匹配")
    if kind.startswith("RANGE_"):
        if prior_qualified is None:
            raise ContractError("RANGE必须绑定前态资格以去重")
        previous_truth = boolean(prior_qualified)
        if previous_anchor and previous_truth is True and (previous_anchor.lower, previous_anchor.upper) == bounds:
            return FieldValue(previous_anchor)
    event_id = source_event.event_id if source_event else semantic_digest((CONTRACT, kind, entity_id, ordinal, window_ref, bounds))
    anchor_id = semantic_digest((CONTRACT, kind, entity_id, event_id, coordinate_id, bounds))
    return FieldValue(Anchor(anchor_id, entity_id, kind, RELATIONS[kind], ordinal, ordinal, ordinal + 1,
                             coordinate_id, *bounds, event_id, source_ref))


def gap_fill(prior_fill, *, low: FieldValue, lower, upper):
    """日线只判断触及程度，不猜盘中顺序；填补不自动等于收盘硬破坏。"""
    from stock_a.domain import exact_number
    if prior_fill not in {"UNTOUCHED", "PARTIAL", "FILLED"}:
        raise ValueError("缺口前态非法")
    lo, hi = exact_number(lower), exact_number(upper)
    if not 0 < lo < hi:
        raise ValueError("缺口区间非法")
    p = number(low, positive=True)
    if prior_fill == "FILLED":
        return FieldValue("FILLED")
    if p is None:
        return unknown("GAP_LOW_UNKNOWN")
    return FieldValue("FILLED" if p <= lo else "PARTIAL" if p < hi or prior_fill == "PARTIAL" else "UNTOUCHED")


@dataclass(frozen=True)
class SupportObservation:
    anchor_id: str
    ordinal: int
    state: str
    acceptance: str
    freshness: str
    breach_streak: int
    acceptance_streak: int
    test_count: int
    separated_sessions: int
    retest_qualified: bool


def observe_support(anchor: Anchor, *, today: Bar, prior: SupportObservation | None,
                    lower: FieldValue, upper: FieldValue, atr_previous: FieldValue,
                    clv: FieldValue, actual: FieldValue, capability_state):
    capability(capability_state)
    if anchor.relation != "SUPPORT":
        raise ValueError("阻力锚不能冒充支撑/股票结构风险")
    t = today.ordinal
    if prior and (prior.anchor_id != anchor.anchor_id or prior.ordinal != t - 1):
        raise ValueError("锚观察必须从同锚上一会话重评")
    if prior is None and t > anchor.test_from:
        raise ContractError("较旧锚缺少观察前态，不能重置历史计数")
    if prior and prior.state == "BROKEN":
        return SupportObservation(anchor.anchor_id, t, "BROKEN", "BROKEN", "FRESH", 0, 0,
                                  prior.test_count, 0, False)
    tests = prior.test_count if prior else 0
    if t < anchor.test_from:
        return SupportObservation(anchor.anchor_id, t, "IDLE", "PENDING", "FRESH", 0, 0, tests, 0, False)
    vals = number(lower, positive=True), number(upper, positive=True), number(atr_previous, positive=True), number(clv)
    price = _prices(today)
    actual_value = boolean(actual)
    if price is None or actual_value is not True or any(v is None for v in vals):
        return SupportObservation(anchor.anchor_id, t, "UNKNOWN", "UNKNOWN", "STALE", 0, 0, tests, 0, False)
    lo, hi, a, close_location = vals
    if lo > hi or not 0 <= close_location <= 1:
        raise ValueError("支撑区间或CLV非法")
    _, h, l, c = price
    touch = h >= lo - a / 4 and l <= hi + a / 4
    breach = c < lo - a / 2
    deep = c < lo - 3 * a / 2
    old_fresh = prior is not None and prior.freshness == "FRESH"
    breach_count = (prior.breach_streak + 1 if old_fresh else 1) if breach else 0
    acceptance_count = (prior.acceptance_streak + 1 if old_fresh else 1) if c >= hi else 0
    reclaim = touch and c >= hi and close_location >= Fraction(1, 2)
    separated = prior.separated_sessions if old_fresh else 0
    qualified = old_fresh and (separated > 0 or prior.retest_qualified)
    if touch and (tests == 0 or separated > 0):
        tests += 1
    old_state = prior.state if prior else "IDLE"
    if deep or breach_count >= 2:
        state = "BROKEN"
    elif breach:
        state = "BREACHED_SHALLOW"
    elif qualified and reclaim:
        state = "HELD_CONFIRMED"
    elif reclaim and old_state in {"IDLE", "APPROACHING", "TESTING", "BREACHED_SHALLOW", "UNKNOWN"}:
        state = "RECLAIMED"
    elif old_state == "RECLAIMED" and c >= hi:
        state = "HELD_TENTATIVE"
    elif touch and not reclaim and old_state in {"HELD_TENTATIVE", "HELD_CONFIRMED", "RETESTING"}:
        state = "RETESTING"
    elif touch:
        state = "TESTING"
    elif min(abs(c - lo), abs(c - hi)) <= a:
        state = "APPROACHING"
    else:
        state = old_state if old_state != "UNKNOWN" else "IDLE"
    acceptance = "BROKEN" if state == "BROKEN" else "ACCEPTED" if acceptance_count >= 2 else "PENDING" if t < anchor.test_from + 1 else "NOT_ACCEPTED"
    if touch:
        next_separated = 0
        next_qualified = qualified and not reclaim and not breach
    else:
        next_separated = separated + 1 if tests > 0 else 0
        next_qualified = False
    return SupportObservation(anchor.anchor_id, t, state, acceptance, "FRESH", breach_count, acceptance_count,
                              tests, next_separated, next_qualified)
