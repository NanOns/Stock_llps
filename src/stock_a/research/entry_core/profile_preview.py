"""Stock A研究profile_preview职责模块。"""

from __future__ import annotations

from datetime import date, timedelta
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from stock_a.domain import ContractError, FieldValue, exact_number, unknown
from stock_a.research.core import (
    CORE_MAX_ACTUAL_LOOKBACK,
    CoreObservation,
    MarketSessionCalendar,
    calculate_core,
)
from stock_a.research.profile import (
    ProfileHandoff,
    ProfileInput,
    RuntimeProducerBinding,
    WindowIdentity,
    evaluate_closed_period_trend,
    evaluate_profile_kernel,
)
from stock_a.sources.tdx.day import DAY_STRUCT, DecodedDay, UnitContract

from stock_a.research.entry_core.fields import _RAW_FIELDS, _render_field
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.source_discovery import _normalize_symbol

_PROFILE_CORE_WINDOW_SPECS = {
    "core.ma5": (5, False),
    "core.ma10": (10, False),
    "core.ma20": (20, False),
    "core.ma60": (60, False),
    "core.hhv60": (60, False),
    "core.prior_high20": (20, True),
    "core.atr20": (21, False),
    "core.ma_slope20_atr": (25, False),
    "core.ma_slope60_atr": (70, False),
    "core.hh_progress": (10, False),
    "core.ll_progress": (10, False),
    "core.price_damage": (21, False),
    "core.bias20_atr": (21, False),
    "core.pos60": (60, False),
    "core.range_ratio": (20, False),
    "core.atr_ratio": (21, False),
    "core.vol_ratio": (21, False),
    "core.amount_ratio20": (21, False),
    "core.liquidity_mean20": (20, True),
    "core.ret1": (2, False),
    "core.ret5": (6, False),
    "core.clv": (1, False),
}
_PROFILE_PERIOD_INPUTS = {
    "profile.closed_week_trend": {
        "status": "UNKNOWN",
        "quality": "UNKNOWN_DATA",
        "reason": "CLOSED_PERIOD_INPUT_UNAVAILABLE：缺少官方周历覆盖证明、目标坐标规范ACTUAL日线、逐会话状态及公司行为选集闭包；保持UNKNOWN。",
        "missing_inputs": ("period.closed_week",),
    },
    "profile.closed_month_trend": {
        "status": "UNKNOWN",
        "quality": "UNKNOWN_DATA",
        "reason": "CLOSED_PERIOD_INPUT_UNAVAILABLE：缺少官方月历覆盖证明、目标坐标规范ACTUAL日线、逐会话状态及公司行为选集闭包；保持UNKNOWN。",
        "missing_inputs": ("period.closed_month",),
    },
    "profile.relative": {
        "status": "CONTRACT_UNBOUND",
        "reason": "跨市场RPS需要每个端点日完整、合法且有实体身份的Universe分母；相对市场比较需要同端点指数序列。候选分页不是Universe分母，本入口没有这些输入。",
        "missing_inputs": (
            "cross.rps5_delta3_pp",
            "cross.rps20",
            "cross.rel_market_1",
        ),
    },
}
_COMPARE_FIELDS = (
    "core.ma5",
    "core.ma10",
    "core.ma20",
    "core.ma60",
    "core.atr5",
    "core.atr20",
    "core.ret1",
    "core.ret5",
    "core.ret20",
    "core.amount_ratio5",
    "core.amount_ratio20",
    "core.volume_ratio5",
    "core.volume_ratio20",
)


def _source_path(roots: Iterable[str], directory: str, code: str) -> tuple[Path, Path]:
    resolved_roots: list[Path] = []
    candidates: list[tuple[Path, Path]] = []
    for value in roots:
        try:
            root = Path(value).resolve(strict=True)
        except FileNotFoundError as error:
            raise ResearchEntryError(
                "SOURCE_ROOT_INVALID", "配置的TDX来源根不存在"
            ) from error
        except PermissionError as error:
            raise ResearchEntryError(
                "SOURCE_ACCESS_DENIED", "操作系统拒绝访问配置的TDX来源根"
            ) from error
        except OSError as error:
            raise ResearchEntryError(
                "SOURCE_ROOT_INVALID", "配置的TDX来源根无法核验"
            ) from error
        if not root.is_dir():
            raise ResearchEntryError("SOURCE_ROOT_INVALID", "配置的TDX来源根不是目录")
        resolved_roots.append(root)
        candidate = root / "vipdoc" / directory / "lday" / f"{directory}{code}.day"
        try:
            source = candidate.resolve(strict=True)
        except FileNotFoundError:
            continue
        except PermissionError as error:
            raise ResearchEntryError(
                "SOURCE_ACCESS_DENIED", "操作系统拒绝核验TDX日线最终路径"
            ) from error
        except OSError as error:
            raise ResearchEntryError(
                "SOURCE_IO_ERROR", "TDX日线最终路径核验发生I/O错误"
            ) from error
        try:
            source.relative_to(root)
        except ValueError as error:
            raise ResearchEntryError(
                "SOURCE_PATH_OUTSIDE_ROOT", "TDX日线最终路径离开配置保护根"
            ) from error
        candidates.append((root, source))
    if not resolved_roots:
        raise ResearchEntryError("SOURCE_NOT_CONFIGURED", "没有配置只读TDX保护根")
    if not candidates:
        raise ResearchEntryError(
            "SOURCE_NOT_FOUND", "配置保护根中没有该代码的TDX日线文件"
        )
    if len(candidates) != 1:
        raise ResearchEntryError(
            "SOURCE_ROOT_AMBIGUOUS", "多个配置来源根均含同一代码文件，拒绝自行选择"
        )
    return candidates[0]


def _unknown_observation(session: date, reason: str) -> CoreObservation:
    values = {name: unknown(reason) for name in _RAW_FIELDS}
    return CoreObservation(
        session=session,
        state="UNKNOWN",
        coordinate_id=None,
        open=values["open"],
        high=values["high"],
        low=values["low"],
        close=values["close"],
        amount=values["amount"],
        volume=values["volume"],
    )


def _observation(
    session: date, symbol: str, decoded: DecodedDay | None
) -> CoreObservation:
    if decoded is None:
        return _unknown_observation(session, "NO_TDX_BAR_FOR_OFFICIAL_SESSION")
    return CoreObservation(
        session=session,
        state="ACTUAL",
        coordinate_id=f"RAW:TDX_SOURCE_SYMBOL:{symbol}",
        open=decoded.raw_open,
        high=decoded.raw_high,
        low=decoded.raw_low,
        close=decoded.raw_close,
        amount=decoded.amount,
        volume=decoded.volume,
    )


def _profile_row_payload(row: CoreObservation) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for name in _RAW_FIELDS:
        value = getattr(row, name)
        values[name] = _render_field(value) if isinstance(value, FieldValue) else None
    return {
        "session": row.session.isoformat(),
        "state": row.state,
        "coordinate_id": row.coordinate_id,
        "fields": values,
    }


def _profile_window(
    field_id: str,
    history: tuple[CoreObservation, ...],
    *,
    window_contract: str,
    calendar_revision: str,
    quality: str,
) -> tuple[WindowIdentity, dict[str, Any]]:
    spec = _PROFILE_CORE_WINDOW_SPECS.get(field_id)
    if spec is None:
        raise ContractError("Profile预览未登记Core窗口：" + field_id)
    count, exclude_current = spec
    end = len(history) - (1 if exclude_current else 0)
    selected = history[max(0, end - count) : end] if end > 0 else ()
    payload = [_profile_row_payload(row) for row in selected]
    digest = sha256(
        json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    if selected:
        first_endpoint: date | None = selected[0].session
        last_endpoint: date | None = selected[-1].session
    else:
        first_endpoint = last_endpoint = None
    identity = WindowIdentity(
        contract=window_contract,
        first_endpoint=first_endpoint,
        last_endpoint=last_endpoint,
        selected_rows_digest=digest,
        calendar_revision=calendar_revision,
        status_revision="TDX_NO_BAR_UNKNOWN_STATUS_R1",
    )
    return identity, {
        "contract": window_contract,
        "first_endpoint": first_endpoint.isoformat() if first_endpoint else None,
        "last_endpoint": last_endpoint.isoformat() if last_endpoint else None,
        "requested_sessions": count,
        "selected_sessions": len(selected),
        "window_complete": len(selected) == count
        and all(row.state == "ACTUAL" for row in selected),
        "unknown_session_dates": [
            row.session.isoformat() for row in selected if row.state == "UNKNOWN"
        ],
        "selected_rows_digest": digest,
        "calendar_revision": calendar_revision,
        "status_revision": "TDX_NO_BAR_UNKNOWN_STATUS_R1",
        "input_quality": quality,
    }


def _profile_preview_source_digest(path: Path, label: str) -> str:
    try:
        return sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise ResearchEntryError(
            "PROFILE_PREVIEW_PROVENANCE_UNAVAILABLE",
            f"无法读取Profile预览生产者源码身份：{label}",
        ) from error


def _build_profile_preview(
    *,
    symbol: str,
    target_session: date,
    source_last_session: date,
    source_sha256: str,
    target_bar: DecodedDay | None,
    history: tuple[CoreObservation, ...],
    core: dict[str, FieldValue],
    calendar_revision: str,
    contract: dict[str, Any],
    closed_period_bars: Mapping[str, Sequence[object]] | None = None,
    source_snapshot_id: str | None = None,
    source_snapshot_role: str = "LOCAL_FILE_SNAPSHOT_ID_NOT_CAPTURED_AVAILABLE_AT",
) -> dict[str, Any]:
    """把本次真实Raw/Core读取接到既有规则；显式限于未复权NON_PIT预览。"""
    if not isinstance(contract, dict):
        raise ResearchEntryError(
            "PROFILE_PREVIEW_CONTRACT_REQUIRED",
            "Profile预览必须使用固定配置槽位装配的day-chain合同",
        )
    if len(source_sha256) != 64 or any(
        ch not in "0123456789abcdef" for ch in source_sha256
    ):
        raise ResearchEntryError(
            "PROFILE_PREVIEW_SOURCE_ID_INVALID", "Profile预览缺少本次完整TDX字节摘要"
        )
    try:
        rules = contract["profile_expression_registry"]
        canonical_registry = contract["field_registry"]
    except (KeyError, TypeError) as error:
        raise ResearchEntryError(
            "PROFILE_PREVIEW_CONTRACT_INVALID",
            "Profile预览合同缺少FOUNDATION规则或字段登记",
        ) from error
    if (
        not isinstance(rules, dict)
        or len(rules) != 11
        or not isinstance(canonical_registry, list)
    ):
        raise ResearchEntryError(
            "PROFILE_PREVIEW_CONTRACT_INVALID",
            "Profile预览必须保留唯一11条规则和字段登记",
        )

    registry_by_id = {
        item["field_id"]: item
        for item in canonical_registry
        if isinstance(item, dict) and isinstance(item.get("field_id"), str)
    }
    if len(registry_by_id) != len(canonical_registry):
        raise ResearchEntryError(
            "PROFILE_PREVIEW_CONTRACT_INVALID", "Profile字段登记存在重复或非法field_id"
        )

    research_root = Path(__file__).resolve().parents[1]
    package_root = Path(__file__).resolve().parents[2]
    core_sha = _profile_preview_source_digest(
        research_root / "core.py", "calculate_core"
    )
    decoder_sha = _profile_preview_source_digest(
        package_root / "sources" / "tdx" / "day.py", "decode_day_record"
    )
    entry_sha = _profile_preview_source_digest(Path(__file__), "_build_profile_preview")
    normalized = _normalize_symbol(symbol)[0]
    coordinate_id = f"RAW:TDX_SOURCE_SYMBOL:{normalized}"
    snapshot_id = (
        source_snapshot_id or "TDX_LOCAL_FILE_SNAPSHOT_SHA256:" + source_sha256
    )
    target_close = (
        target_bar.raw_close
        if target_bar is not None
        else unknown("TARGET_BAR_MISSING")
    )

    components: list[dict[str, Any]] = []
    for rule_id, rule in rules.items():
        field_id = rule.get("field_id") if isinstance(rule, dict) else None
        if not isinstance(field_id, str):
            raise ResearchEntryError(
                "PROFILE_PREVIEW_CONTRACT_INVALID", "Profile规则缺少field_id"
            )
        unavailable = _PROFILE_PERIOD_INPUTS.get(field_id)
        if unavailable is not None:
            period_field_id = unavailable["missing_inputs"][0]
            supplied_period_bars = (
                closed_period_bars.get(period_field_id)
                if isinstance(closed_period_bars, Mapping)
                else None
            )
            if supplied_period_bars:
                try:
                    period_source_sha = _profile_preview_source_digest(
                        package_root / "canonical" / "adjustment_periods.py",
                        "build_closed_period_bars",
                    )
                    period_result = evaluate_closed_period_trend(
                        field_id,
                        supplied_period_bars,
                        target_session=target_session,
                        coordinate_id=coordinate_id,
                        coordinate_contract_version="RAW_UNADJUSTED",
                        producer_source_sha256=period_source_sha,
                        rules=rules,
                        field_registry=canonical_registry,
                    )
                except ContractError as error:
                    raise ResearchEntryError(
                        "PROFILE_PERIOD_INPUT_INVALID",
                        f"{field_id}闭合周期输入不符合精确Profile交接合同：{error}",
                    ) from error
                components.append(
                    {
                        "field_id": field_id,
                        "rule_id": rule.get("expression_ref"),
                        "expression_ref": rule.get("expression_ref"),
                        "status": "EVALUATED"
                        if period_result.quality == "OBSERVED"
                        else "UNKNOWN",
                        "quality": period_result.quality,
                        "value": _render_field(period_result)["value"],
                        "reason": period_result.reason,
                        "production_eligibility": "PREVIEW_ONLY_RAW_NON_PIT",
                        "input_fields": [period_field_id],
                    }
                )
                continue
            missing_inputs = []
            for missing_id in unavailable["missing_inputs"]:
                field_spec = registry_by_id.get(missing_id, {})
                missing_inputs.append(
                    {
                        "field_id": missing_id,
                        "producer_symbol": field_spec.get("producer_symbol"),
                        "reason": unavailable["reason"],
                    }
                )
            components.append(
                {
                    "field_id": field_id,
                    "rule_id": rule_id,
                    "expression_ref": rule.get("expression_ref"),
                    "status": unavailable["status"],
                    "quality": unavailable.get("quality", unavailable["status"]),
                    "production_eligibility": "NOT_ELIGIBLE",
                    "reason": unavailable["reason"],
                    "missing_inputs": missing_inputs,
                    "input_fields": list(rule.get("input_fields", [])),
                }
            )
            continue

        input_fields = rule.get("input_fields")
        if not isinstance(input_fields, list) or not input_fields:
            raise ResearchEntryError(
                "PROFILE_PREVIEW_CONTRACT_INVALID",
                "Profile规则没有明确输入字段：" + field_id,
            )
        inputs: dict[str, ProfileInput] = {}
        expected_context: dict[str, dict[str, Any]] = {}
        expected_windows: dict[str, WindowIdentity] = {}
        runtime_producers: dict[str, RuntimeProducerBinding] = {}
        used_registry: list[dict[str, Any]] = []
        input_provenance: list[dict[str, Any]] = []

        for input_id in input_fields:
            registered = registry_by_id.get(input_id)
            if registered is None:
                raise ResearchEntryError(
                    "PROFILE_PREVIEW_CONTRACT_INVALID",
                    "Profile输入未登记：" + str(input_id),
                )
            field_registry = dict(registered)
            if input_id == "adjusted.close":
                # 仅本预览局部把同目标、同Raw坐标的source close映射到规则输入名。
                # 不改正式合同，也不宣称执行了P06复权生产者。
                field_registry.update(
                    {
                        "unit": "CNY",
                        "producer_symbol": "stock_a.research.entry:_build_profile_preview",
                        "producer_source_sha256": entry_sha,
                        "expression_ref": "TDX_RAW_CLOSE_PREVIEW_ALIAS_R1",
                        "algorithm_version": None,
                        "parameter_version": "TDX_RAW_UNADJUSTED_PREVIEW_R1",
                        "context_dependencies": [
                            "context.target_session",
                            "context.source_cutoff",
                        ],
                        "window_contract": "NONE",
                        "input_fields": ["raw.close"],
                    }
                )
                source_field_id = "raw.close"
                source_producer = "stock_a.sources.tdx.day:decode_day_record"
                source_code_sha = decoder_sha
                input_value = target_close
                input_window = {
                    "contract": "TARGET_SESSION_R1",
                    "first_endpoint": target_session.isoformat()
                    if target_bar is not None
                    else None,
                    "last_endpoint": target_session.isoformat()
                    if target_bar is not None
                    else None,
                    "requested_sessions": 1,
                    "selected_sessions": 1 if target_bar is not None else 0,
                    "selected_rows_digest": sha256(
                        json.dumps(
                            [_profile_row_payload(history[-1])]
                            if target_bar is not None
                            else [],
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                    ).hexdigest(),
                }
            else:
                source_field_id = input_id
                source_producer = "stock_a.research.core:calculate_core"
                source_code_sha = core_sha
                if input_id not in core:
                    raise ResearchEntryError(
                        "PROFILE_PREVIEW_CORE_FIELD_MISSING",
                        "本次Core结果缺少画像输入：" + input_id,
                    )
                input_value = core[input_id]
                window_contract = field_registry.get("window_contract", "NONE")
                if window_contract != "NONE":
                    window_identity, input_window = _profile_window(
                        input_id,
                        history,
                        window_contract=window_contract,
                        calendar_revision=calendar_revision,
                        quality=input_value.quality,
                    )
                    expected_windows[input_id] = window_identity
                else:
                    input_window = None

            producer_symbol = field_registry.get("producer_symbol")
            if not isinstance(producer_symbol, str) or not producer_symbol:
                raise ResearchEntryError(
                    "PROFILE_PREVIEW_PRODUCER_UNBOUND",
                    "画像输入缺少实际生产者：" + input_id,
                )
            required_contract_text: dict[str, str] = {}
            for key in (
                "unit",
                "expression_ref",
                "parameter_version",
                "coordinate_role",
            ):
                value = field_registry.get(key)
                if not isinstance(value, str) or not value:
                    raise ResearchEntryError(
                        "PROFILE_PREVIEW_FIELD_CONTRACT_INVALID",
                        f"画像输入缺少有效的{key}合同：{input_id}",
                    )
                required_contract_text[key] = value
            field_registry["producer_source_sha256"] = source_code_sha
            used_registry.append(field_registry)

            contexts: dict[str, Any] = {}
            for dependency in field_registry.get("context_dependencies", []):
                if dependency == "context.target_session":
                    contexts[dependency] = target_session
                elif dependency == "context.source_cutoff":
                    contexts[dependency] = snapshot_id
                elif dependency == "context.calendar_ref":
                    contexts[dependency] = calendar_revision
                else:
                    raise ResearchEntryError(
                        "PROFILE_PREVIEW_CONTEXT_UNBOUND",
                        f"当前只读预览没有该输入上下文生产者：{input_id}/{dependency}",
                    )
            expected_context[input_id] = dict(contexts)
            producer = RuntimeProducerBinding(
                symbol=producer_symbol,
                source_sha256=source_code_sha,
                algorithm_version=field_registry.get("algorithm_version"),
            )
            runtime_producers[producer_symbol] = producer
            window_identity = expected_windows.get(input_id)
            inputs[input_id] = ProfileInput(
                field_id=input_id,
                field=input_value,
                unit=required_contract_text["unit"],
                producer_symbol=producer_symbol,
                producer_source_sha256=source_code_sha,
                expression_ref=required_contract_text["expression_ref"],
                algorithm_version=field_registry.get("algorithm_version"),
                parameter_version=required_contract_text["parameter_version"],
                window=window_identity,
                coordinate_role=required_contract_text["coordinate_role"],
                coordinate_id=coordinate_id,
                context=contexts,
            )
            input_provenance.append(
                {
                    "rule_input_field": input_id,
                    "source_field_id": source_field_id,
                    "value": _render_field(input_value),
                    "unit": field_registry.get("unit"),
                    "producer_symbol": producer_symbol,
                    "source_producer": source_producer,
                    "producer_source_sha256": source_code_sha,
                    "expression_ref": field_registry.get("expression_ref"),
                    "parameter_version": field_registry.get("parameter_version"),
                    "window": input_window,
                    "coordinate_role": field_registry.get("coordinate_role"),
                    "coordinate_id": coordinate_id,
                    "context": {
                        key: value.isoformat() if isinstance(value, date) else value
                        for key, value in contexts.items()
                    },
                    "unit_trust": "CALLER_DECLARED_ASSUMPTION"
                    if input_id in {"core.amount_ratio20", "core.liquidity_mean20"}
                    else None,
                }
            )

        handoff = ProfileHandoff(
            inputs=inputs,
            target_session=target_session,
            source_cutoff=snapshot_id,
            target_coordinate_id=coordinate_id,
            expected_windows=expected_windows,
            expected_context=expected_context,
            runtime_producers=runtime_producers,
        )
        result = evaluate_profile_kernel(field_id, handoff, rules, used_registry)
        components.append(
            {
                "field_id": field_id,
                "rule_id": rule_id,
                "expression_ref": rule.get("expression_ref"),
                "status": "EVALUATED",
                "quality": result.quality,
                "value": _render_field(result)["value"],
                "reason": result.reason,
                "production_eligibility": "PREVIEW_ONLY_RAW_UNADJUSTED_NON_PIT",
                "calculation_basis": {
                    "input_fields": list(rule["input_fields"]),
                    "operands": rule.get("operands", {}),
                    "parameters": rule.get("parameters", {}),
                    "ordered_branches": rule.get("ordered_branches", []),
                },
                "inputs": input_provenance,
            }
        )

    return {
        "contract": "STOCK-A-PROFILE-COMPONENT-PREVIEW/R1",
        "scope": "REAL_TDX_RAW_CORE_READONLY_COMPONENT_PREVIEW",
        "target_session": target_session.isoformat(),
        "source_last_session": source_last_session.isoformat(),
        "identity": {
            "status": "SOURCE_SYMBOL_ONLY",
            "symbol": normalized,
            "security_id": None,
        },
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "source": {
            "raw_byte_sha256": source_sha256,
            "source_cutoff_id": snapshot_id,
            "source_cutoff_role": source_snapshot_role,
            "local_read_clock_is_historical_availability": False,
        },
        "components": components,
        "counts": {
            "total": len(components),
            "evaluated": sum(item["status"] == "EVALUATED" for item in components),
            "observed": sum(item.get("quality") == "OBSERVED" for item in components),
            "unknown_data": sum(
                item.get("quality") == "UNKNOWN_DATA" for item in components
            ),
            "not_implemented": sum(
                item["status"] == "NOT_IMPLEMENTED" for item in components
            ),
            "contract_unbound": sum(
                item["status"] == "CONTRACT_UNBOUND" for item in components
            ),
        },
    }
