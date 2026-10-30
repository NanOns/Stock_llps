"""P03来源集合正式写入、提交回执、独立读回和单次封口。"""

from __future__ import annotations

from datetime import date, datetime, timezone
from hashlib import sha256
import json
import re
from typing import Any, Mapping
import uuid

from stock_a.domain.source_set_candidate import source_set_candidate_digest
from stock_a.domain.source_set_seal import (
    SourceSetSealError,
    source_set_attempt_id,
    source_set_final_seal_digest,
    source_set_identity_digest,
    source_set_id_from_digest,
)

from . import source_raw
from .source_set_candidate import (
    _CANDIDATE_COLUMNS,
    _INPUT_COLUMNS,
    _read_candidate_facts,
)


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_FACT_KEYS = (
    "namespace_id",
    "target_session",
    "package_sha256",
    "source_set_id",
    "revision_id",
    "source_content_id",
    "source_capture_id",
    "source_digest",
    "source_raw_digest",
    "source_dataset_id",
    "source_byte_count",
    "parser_contract",
    "capture_expected_row_count",
    "raw_row_count",
    "target_present_count",
    "target_missing_count",
    "read_failed_count",
    "capture_read_started_at",
    "capture_validated_at",
    "source_available_at",
    "source_time_quality",
    "source_clock_role",
    "price_unit_status",
    "amount_unit_status",
    "volume_unit_status",
    "calendar_id",
    "calendar_revision",
    "calendar_sha256",
    "scope_id",
    "scope_contract_version",
    "scope_contract_digest",
    "scope_catalog_digest",
    "source_semantic_revision",
    "business_input_digest",
    "plan_sha256",
    "contract_sha256",
    "head_generation",
)
_SOURCE_SELECT = (
    "namespace_id::text AS namespace_id",
    "ingress_attempt_id::text AS ingress_attempt_id",
    "target_session AS target_session",
    "package_sha256 AS package_sha256",
    "source_set_id::text AS source_set_id",
    "revision_id AS revision_id",
    "source_content_id AS source_content_id",
    "source_capture_id::text AS source_capture_id",
    "source_digest AS source_digest",
    "source_raw_digest AS source_raw_digest",
    "source_dataset_id AS source_dataset_id",
    "source_byte_count AS source_byte_count",
    "parser_contract AS parser_contract",
    "capture_expected_row_count AS capture_expected_row_count",
    "raw_row_count AS raw_row_count",
    "target_present_count AS target_present_count",
    "target_missing_count AS target_missing_count",
    "read_failed_count AS read_failed_count",
    "capture_read_started_at AS capture_read_started_at",
    "capture_validated_at AS capture_validated_at",
    "source_available_at AS source_available_at",
    "source_time_quality AS source_time_quality",
    "source_clock_role AS source_clock_role",
    "price_unit_status AS price_unit_status",
    "amount_unit_status AS amount_unit_status",
    "volume_unit_status AS volume_unit_status",
    "calendar_id AS calendar_id",
    "calendar_revision AS calendar_revision",
    "calendar_sha256 AS calendar_sha256",
    "scope_id AS scope_id",
    "scope_contract_version AS scope_contract_version",
    "scope_contract_digest AS scope_contract_digest",
    "scope_catalog_digest AS scope_catalog_digest",
    "source_semantic_revision AS source_semantic_revision",
    "business_input_digest AS business_input_digest",
    "plan_sha256 AS plan_sha256",
    "contract_sha256 AS contract_sha256",
    "head_generation AS head_generation",
    "durability_status AS durability_status",
    "commit_ack_observed_at AS commit_ack_observed_at",
    "durable_at AS durable_at",
    "durability_source_capture_id::text AS durability_source_capture_id",
    "durability_source_digest AS durability_source_digest",
    "seal_digest_sha256 AS seal_digest_sha256",
    "seal_status AS seal_status",
    "seal_writer_transaction_id AS seal_writer_transaction_id",
    "sealed_at AS sealed_at",
)
_SOURCE_COLUMNS = tuple(item.split(" AS ", 1)[1] for item in _SOURCE_SELECT)
_P02_COLUMNS = (
    "namespace_id",
    "ingress_attempt_id",
    "capability_id",
    "status",
    "plan_sha256",
    "policy_sha256",
    "binding_sha256",
    "policy_contract",
    "max_logins",
    "max_queries",
    "query_timeout_seconds",
    "reason_code",
    "data_quality",
    "actual_logins",
    "actual_queries",
    "network_accessed",
    "writer_transaction_id",
    "observer_transaction_id",
    "pending_row_xmin",
    "pending_creation_transaction_id",
    "commit_ack_observed_at",
    "durable_at",
    "source_capture_id",
    "source_digest",
    "capability_digest",
)
_CAPABILITY_SELECT = (
    "namespace_id::text AS namespace_id",
    "ingress_attempt_id::text AS ingress_attempt_id",
    "capability_id AS capability_id",
    "status AS status",
    "plan_sha256 AS plan_sha256",
    "policy_sha256 AS policy_sha256",
    "binding_sha256 AS binding_sha256",
    "policy_contract AS policy_contract",
    "max_logins AS max_logins",
    "max_queries AS max_queries",
    "query_timeout_seconds AS query_timeout_seconds",
    "reason_code AS reason_code",
    "data_quality AS data_quality",
    "actual_logins AS actual_logins",
    "actual_queries AS actual_queries",
    "network_accessed AS network_accessed",
    "writer_transaction_id AS writer_transaction_id",
    "observer_transaction_id AS observer_transaction_id",
    "pending_row_xmin AS pending_row_xmin",
    "pending_creation_transaction_id AS pending_creation_transaction_id",
    "commit_ack_observed_at AS commit_ack_observed_at",
    "durable_at AS durable_at",
    "source_capture_id::text AS source_capture_id",
    "source_digest AS source_digest",
    "capability_digest AS capability_digest",
)
_CAPABILITY_COLUMNS = tuple(item.split(" AS ", 1)[1] for item in _CAPABILITY_SELECT)


class SourceSetWriteError(RuntimeError):
    """正式P03来源事实、事务回执或幂等恢复不符合合同。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _canonical_digest(value: Mapping[str, Any]) -> str:
    def canonical_item(item: Any) -> Any:
        if isinstance(item, datetime):
            if item.tzinfo is None or item.utcoffset() is None:
                raise SourceSetWriteError(
                    "SOURCE_SET_CLOCK_INVALID", "能力摘要时钟必须包含时区"
                )
            return item.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if isinstance(item, Mapping):
            return {str(key): canonical_item(child) for key, child in item.items()}
        if isinstance(item, (tuple, list)):
            return [canonical_item(child) for child in item]
        return item

    return sha256(
        json.dumps(
            canonical_item(dict(value)),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _record(cursor: Any, row: Any, columns: tuple[str, ...]) -> dict[str, Any]:
    names = tuple(column.name for column in cursor.description)
    if (
        names != columns
        or not isinstance(row, (tuple, list))
        or len(row) != len(columns)
    ):
        raise SourceSetWriteError(
            "SOURCE_SET_ROW_SHAPE_INVALID", "PG返回列名、顺序或数量不符合正式P03合同"
        )
    return dict(zip(columns, row, strict=True))


def _p02_identity(
    *, target_session: date, package_sha256: str, p02: Mapping[str, Any]
) -> dict[str, Any]:
    policy = p02.get("budget")
    requests = p02.get("actual_requests")
    if (
        p02.get("status") != "NOT_REQUESTED"
        or p02.get("reason_code") != "OPTIONAL_SOURCE_NOT_REQUESTED"
        or p02.get("target_session") != target_session.isoformat()
        or p02.get("package_sha256") != package_sha256
        or p02.get("policy_contract") != "STOCK-A-OPTIONAL-BAOSTOCK-SUPPLEMENT/R1"
        or p02.get("capability") != "UNKNOWN_NOT_MEASURED"
        or p02.get("data_quality") != "NOT_MEASURED_NO_RESPONSE"
        or p02.get("source_available_at") is not None
        or p02.get("network_accessed") is not False
        or requests != {"logins": 0, "queries": 0}
        or policy
        != {
            "max_logins": 1,
            "max_queries_per_target_session": 10,
            "query_timeout_seconds": 30,
        }
        or not isinstance(p02.get("plan_sha256"), str)
        or _SHA256.fullmatch(p02["plan_sha256"]) is None
        or not isinstance(p02.get("policy_sha256"), str)
        or _SHA256.fullmatch(p02["policy_sha256"]) is None
        or not isinstance(p02.get("binding_sha256"), str)
        or _SHA256.fullmatch(p02["binding_sha256"]) is None
        or p02.get("binding_status") != "PREVIEW_COMPUTED_NOT_PERSISTED"
    ):
        raise SourceSetWriteError(
            "P02_EVIDENCE_INVALID",
            "P02必须是本次目标日/SHA/P00计划绑定的真实NOT_REQUESTED零请求记录",
        )
    return {
        "status": "NOT_REQUESTED",
        "plan_sha256": p02["plan_sha256"],
        "policy_sha256": p02["policy_sha256"],
        "binding_sha256": p02["binding_sha256"],
        "policy_contract": p02["policy_contract"],
        "max_logins": 1,
        "max_queries": 10,
        "query_timeout_seconds": 30,
        "reason_code": "OPTIONAL_SOURCE_NOT_REQUESTED",
        "data_quality": "NOT_MEASURED_NO_RESPONSE",
        "actual_logins": 0,
        "actual_queries": 0,
        "network_accessed": False,
    }


def _p02_binding_digest(
    *, target_session: date, package_sha256: str, p02: Mapping[str, Any]
) -> str:
    return _canonical_digest(
        {
            key: value
            for key, value in {
                "target_session": target_session.isoformat(),
                "package_sha256": package_sha256,
                "plan_sha256": p02.get("plan_sha256"),
                "policy_contract": p02.get("policy_contract"),
                "policy_sha256": p02.get("policy_sha256"),
                "status": p02.get("status"),
                "reason_code": p02.get("reason_code"),
                "actual_requests": p02.get("actual_requests"),
                "network_accessed": p02.get("network_accessed"),
                "data_quality": p02.get("data_quality"),
            }.items()
        }
    )


def _p02_row_digest(
    *, target_session: date, package_sha256: str, row: Mapping[str, Any]
) -> str:
    payload = {
        "capability_id": row["capability_id"],
        "target_session": target_session.isoformat(),
        "package_sha256": package_sha256,
        **{
            key: row[key]
            for key in (
                "status",
                "plan_sha256",
                "policy_sha256",
                "binding_sha256",
                "policy_contract",
                "max_logins",
                "max_queries",
                "query_timeout_seconds",
                "reason_code",
                "data_quality",
                "actual_logins",
                "actual_queries",
                "network_accessed",
            )
        },
    }
    return _canonical_digest(payload)


def _stored_p02_row(
    *,
    namespace_id: str,
    attempt_id: str,
    source_facts: Mapping[str, Any],
    p02: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "namespace_id": namespace_id,
        "ingress_attempt_id": attempt_id,
        "capability_id": "BAOSTOCK_SUPPLEMENT",
        **_p02_identity(
            target_session=source_facts["target_session"],
            package_sha256=source_facts["package_sha256"],
            p02=p02,
        ),
        "writer_transaction_id": None,
        "observer_transaction_id": None,
        "pending_row_xmin": None,
        "pending_creation_transaction_id": None,
        "commit_ack_observed_at": None,
        "durable_at": None,
        "source_capture_id": None,
        "source_digest": None,
        "capability_digest": "",
    }


def _stored_ack_row(
    *,
    namespace_id: str,
    attempt_id: str,
    source_facts: Mapping[str, Any],
    p02_row: Mapping[str, Any],
    writer_transaction_id: str,
    observer_transaction_id: str,
    pending_row_xmin: str,
    pending_creation_transaction_id: str,
    commit_ack_observed_at: datetime,
    durable_at: datetime,
) -> dict[str, Any]:
    row = {
        "namespace_id": namespace_id,
        "ingress_attempt_id": attempt_id,
        "capability_id": "P03_COMMIT_ACK",
        "status": "VERIFIED_POST_COMMIT_READBACK",
        "plan_sha256": source_facts["plan_sha256"],
        "policy_sha256": p02_row["policy_sha256"],
        "binding_sha256": p02_row["binding_sha256"],
        "policy_contract": None,
        "max_logins": None,
        "max_queries": None,
        "query_timeout_seconds": None,
        "reason_code": "PENDING_ROW_INDEPENDENTLY_READ",
        "data_quality": None,
        "actual_logins": 0,
        "actual_queries": 0,
        "network_accessed": False,
        "writer_transaction_id": writer_transaction_id,
        "observer_transaction_id": observer_transaction_id,
        "pending_row_xmin": pending_row_xmin,
        "pending_creation_transaction_id": pending_creation_transaction_id,
        "commit_ack_observed_at": commit_ack_observed_at,
        "durable_at": durable_at,
        "source_capture_id": source_facts["source_capture_id"],
        "source_digest": source_facts["source_digest"],
    }
    row["capability_digest"] = _ack_capability_digest(row)
    return row


def _source_payload(
    *,
    header: Mapping[str, Any],
    inputs: list[Mapping[str, Any]],
    target_session: date,
    package_sha256: str,
    ingress: Mapping[str, Any],
) -> dict[str, Any]:
    plan = ingress.get("plan")
    if not isinstance(plan, Mapping):
        raise SourceSetWriteError("P00_PLAN_MISSING", "缺少本次P00 SOURCE_INGRESS计划")
    expected_generation = plan.get("expected_parent_generation")
    if expected_generation is not None and (
        type(expected_generation) is not int or expected_generation < 0
    ):
        raise SourceSetWriteError("P00_GENERATION_INVALID", "P00前态generation非法")
    family_units = {
        item.get("field_family"): item.get("unit_status")
        for item in inputs
        if item.get("input_role") == "FIELD_QUALITY"
    }
    facts = {
        "namespace_id": str(header["namespace_id"]),
        "target_session": target_session,
        "package_sha256": package_sha256,
        "source_set_id": None,
        "revision_id": 1,
        "source_content_id": str(header["source_content_id"]),
        "source_capture_id": str(header["source_capture_id"]),
        "source_digest": header["candidate_sha256"],
        "source_raw_digest": header["raw_source_digest"],
        "source_dataset_id": header["source_dataset_id"],
        "source_byte_count": int(header["source_byte_count"]),
        "parser_contract": header["parser_contract"],
        "capture_expected_row_count": int(header["capture_expected_row_count"]),
        "raw_row_count": int(header["raw_row_count"]),
        "target_present_count": int(header["target_present_count"]),
        "target_missing_count": int(header["target_missing_count"]),
        "read_failed_count": int(header["read_failed_count"]),
        "capture_read_started_at": header["capture_read_started_at"],
        "capture_validated_at": header["capture_validated_at"],
        "source_available_at": header["source_available_at"],
        "source_time_quality": header["source_time_quality"],
        "source_clock_role": header["source_clock_role"],
        "price_unit_status": family_units.get("PRICE"),
        "amount_unit_status": family_units.get("AMOUNT"),
        "volume_unit_status": family_units.get("VOLUME"),
        "calendar_id": header["calendar_id"],
        "calendar_revision": header["calendar_revision"],
        "calendar_sha256": header["calendar_sha256"],
        "scope_id": "SOURCE_INGRESS",
        "scope_contract_version": ingress.get("scope_contract_version"),
        "scope_contract_digest": ingress.get("scope_contract_digest"),
        "scope_catalog_digest": ingress.get("scope_catalog_digest"),
        "source_semantic_revision": header["parser_contract"],
        "business_input_digest": plan.get("input_digest"),
        "plan_sha256": plan.get("plan_sha256"),
        "contract_sha256": plan.get("contract_sha256"),
        "head_generation": (
            expected_generation if expected_generation is not None else 0
        )
        + 1,
    }
    if (
        facts["source_dataset_id"] != "TDX_VIPDATA_PACKAGE"
        or facts["source_time_quality"] not in {"UNKNOWN", "NON_PIT_OBSERVED"}
        or facts["source_available_at"] is not None
        or facts["read_failed_count"] != 0
        or facts["raw_row_count"] != facts["capture_expected_row_count"]
        or facts["raw_row_count"]
        != facts["target_present_count"] + facts["target_missing_count"]
        or any(
            not isinstance(facts[key], str) or _SHA256.fullmatch(facts[key]) is None
            for key in (
                "source_digest",
                "source_raw_digest",
                "calendar_sha256",
                "scope_contract_digest",
                "scope_catalog_digest",
                "business_input_digest",
                "plan_sha256",
                "contract_sha256",
            )
        )
        or not all(
            isinstance(facts[key], str) and facts[key]
            for key in (
                "parser_contract",
                "source_clock_role",
                "scope_contract_version",
                "calendar_id",
                "calendar_revision",
            )
        )
        or any(
            facts[key] not in {"RAW_UNADJUSTED", "UNIT_UNVERIFIED"}
            for key in (
                "price_unit_status",
                "amount_unit_status",
                "volume_unit_status",
            )
        )
    ):
        raise SourceSetWriteError(
            "SOURCE_SET_FACTS_INVALID",
            "候选来源、质量分母、时钟或P00绑定不满足正式P03合同",
        )
    return facts


def _source_values(facts: Mapping[str, Any]) -> list[Any]:
    return [facts[key] for key in _SOURCE_FACT_KEYS]


def _assert_source_match(
    actual: Mapping[str, Any], expected: Mapping[str, Any]
) -> None:
    for key in _SOURCE_FACT_KEYS:
        left = actual[key]
        right = expected[key]
        if isinstance(left, datetime) and isinstance(right, datetime):
            equal = left.astimezone(timezone.utc) == right.astimezone(timezone.utc)
        elif isinstance(left, date) and isinstance(right, date):
            equal = left == right
        else:
            equal = left == right
        if not equal:
            raise SourceSetWriteError(
                "SOURCE_SET_ATTEMPT_CONFLICT",
                f"同一PENDING尝试的{key}与本次精确来源事实不一致",
            )


def _validate_current_policy(config: Mapping[str, Any], p02: Mapping[str, Any]) -> str:
    source_policy = config.get("source_policy")
    policy = (
        source_policy.get("baostock_supplement")
        if isinstance(source_policy, Mapping)
        else None
    )
    expected = {
        "contract": "STOCK-A-OPTIONAL-BAOSTOCK-SUPPLEMENT/R1",
        "request_policy": "NOT_REQUESTED",
        "reason_code": "OPTIONAL_SOURCE_NOT_REQUESTED",
        "required_for_core": False,
        "request_budget": {
            "max_logins": 1,
            "max_queries_per_target_session": 10,
            "query_timeout_seconds": 30,
        },
    }
    if not isinstance(policy, Mapping) or dict(policy) != expected:
        raise SourceSetWriteError(
            "P02_POLICY_INVALID", "唯一正式配置中的Bao可选策略与NOT_REQUESTED合同不一致"
        )
    policy_sha = _canonical_digest(policy)
    if p02.get("policy_sha256") != policy_sha:
        raise SourceSetWriteError(
            "P02_POLICY_SHA_MISMATCH", "本次P02策略SHA与唯一正式配置不一致"
        )
    return policy_sha


def _verify_stored_candidate(
    cursor: Any,
    *,
    namespace_id: str,
    facts: Mapping[str, Any],
    expected_header: Mapping[str, Any],
    expected_candidate_digest: str,
    expected_inputs: list[Mapping[str, Any]],
) -> None:
    cursor.execute(
        f"SELECT {', '.join(_CANDIDATE_COLUMNS[:-1])} "
        "FROM stock.source_set_candidates WHERE namespace_id=%s::uuid AND target_session=%s AND package_sha256=%s",
        (namespace_id, facts["target_session"], facts["package_sha256"]),
    )
    row = cursor.fetchone()
    expected = tuple(expected_header[column] for column in _CANDIDATE_COLUMNS[:-1])
    normalize = lambda value: str(value) if isinstance(value, uuid.UUID) else value
    if tuple(map(normalize, row or ())) != tuple(map(normalize, expected)):
        raise SourceSetWriteError(
            "SOURCE_SET_CANDIDATE_BINDING_MISMATCH",
            "正式P03输入与PG已保存候选、TDX内容、捕获时间、Raw摘要或质量分母不一致",
        )
    cursor.execute(
        f"SELECT {', '.join(_INPUT_COLUMNS)} "
        "FROM stock.source_set_candidate_inputs WHERE namespace_id=%s::uuid AND target_session=%s "
        "AND package_sha256=%s ORDER BY input_ordinal",
        (namespace_id, facts["target_session"], facts["package_sha256"]),
    )
    stored = cursor.fetchall()
    if len(stored) != len(expected_inputs):
        raise SourceSetWriteError(
            "SOURCE_SET_CANDIDATE_INPUTS_INCOMPLETE",
            "PG已保存候选输入行数不符合七项合同",
        )
    expected_rows = tuple(
        tuple(item[column] for column in _INPUT_COLUMNS) for item in expected_inputs
    )
    if tuple(tuple(map(normalize, row)) for row in stored) != tuple(
        tuple(map(normalize, row)) for row in expected_rows
    ):
        raise SourceSetWriteError(
            "SOURCE_SET_CANDIDATE_INPUTS_MISMATCH",
            "PG已保存候选的全部输入身份、质量向量、分母或单位与实时Raw核验不一致",
        )


def _pending_values(
    facts: Mapping[str, Any], attempt_id: str, p02: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        **facts,
        "ingress_attempt_id": attempt_id,
        "source_set_id": None,
        "durability_status": "PENDING",
        "commit_ack_observed_at": None,
        "durable_at": None,
        "durability_source_capture_id": None,
        "durability_source_digest": None,
        "seal_digest_sha256": None,
        "seal_status": "PENDING",
        "seal_writer_transaction_id": None,
        "sealed_at": None,
    }


def _insert_or_read_pending(
    config: Mapping[str, Any],
    *,
    facts: Mapping[str, Any],
    attempt_id: str,
    p02_row: Mapping[str, Any],
    p02_sha: str,
) -> tuple[str, datetime, bool]:
    inserted = False
    writer_transaction_id = ""
    with source_raw._transaction(config, write=True) as (
        _connection,
        cursor,
        namespace_id,
    ):
        if namespace_id != facts["namespace_id"]:
            raise SourceSetWriteError(
                "SOURCE_SET_NAMESPACE_CHANGED",
                "候选读取与P03写入间DEV namespace发生变化",
            )
        cursor.execute("SELECT txid_current()::text")
        writer_transaction_id = str(cursor.fetchone()[0])
        columns = (
            *_SOURCE_FACT_KEYS,
            "ingress_attempt_id",
            "seal_status",
            "durability_status",
        )
        values = [
            *[facts[key] for key in _SOURCE_FACT_KEYS],
            attempt_id,
            "PENDING",
            "PENDING",
        ]
        cursor.execute(
            f"INSERT INTO stock.source_sets ({', '.join(columns)}) "
            f"VALUES ({', '.join(['%s'] * len(columns))}) "
            "ON CONFLICT (namespace_id, ingress_attempt_id) DO NOTHING",
            values,
        )
        inserted = cursor.rowcount == 1
        if cursor.rowcount not in (0, 1):
            raise SourceSetWriteError(
                "SOURCE_SET_PARENT_INSERT_BOUND", "PENDING主行插入超过一行"
            )
        if inserted:
            p02_values = dict(p02_row)
            p02_values["capability_digest"] = _p02_row_digest(
                target_session=facts["target_session"],
                package_sha256=facts["package_sha256"],
                row=p02_values,
            )
            cursor.execute(
                f"INSERT INTO stock.source_set_capabilities ({', '.join(_P02_COLUMNS)}) "
                f"VALUES ({', '.join(['%s'] * len(_P02_COLUMNS))})",
                [p02_values[key] for key in _P02_COLUMNS],
            )
            if cursor.rowcount != 1:
                raise SourceSetWriteError(
                    "SOURCE_SET_P02_INSERT_FAILED",
                    "本次PENDING没有恰好插入一条P02能力记录",
                )
        cursor.execute(
            "SELECT source_content_id, source_capture_id::text, source_digest, source_raw_digest, "
            "plan_sha256, seal_status, durability_status FROM stock.source_sets "
            "WHERE namespace_id=%s::uuid AND ingress_attempt_id=%s::uuid",
            (namespace_id, attempt_id),
        )
        stored_parent = cursor.fetchone()
        expected_parent = (
            facts["source_content_id"],
            facts["source_capture_id"],
            facts["source_digest"],
            facts["source_raw_digest"],
            facts["plan_sha256"],
            "PENDING",
            "PENDING",
        )
        if stored_parent != expected_parent:
            raise SourceSetWriteError(
                "SOURCE_SET_ATTEMPT_CONFLICT",
                "同一PENDING尝试已经绑定不同来源摘要或状态",
            )
        cursor.execute(
            f"SELECT {', '.join(_CAPABILITY_SELECT)} FROM stock.source_set_capabilities "
            "WHERE namespace_id=%s::uuid AND ingress_attempt_id=%s::uuid "
            "AND capability_id='BAOSTOCK_SUPPLEMENT'",
            (namespace_id, attempt_id),
        )
        actual_row = _record(cursor, cursor.fetchone(), _CAPABILITY_COLUMNS)
        expected_p02 = dict(p02_row)
        expected_p02["capability_digest"] = _p02_row_digest(
            target_session=facts["target_session"],
            package_sha256=facts["package_sha256"],
            row=expected_p02,
        )
        if any(actual_row[key] != expected_p02[key] for key in _P02_COLUMNS):
            raise SourceSetWriteError(
                "SOURCE_SET_P02_CONFLICT",
                "同一PENDING尝试的P02计划或策略证据与本次不一致",
            )
    return writer_transaction_id, datetime.now(timezone.utc), inserted


def _read_pending_observation(
    config: Mapping[str, Any],
    *,
    attempt_id: str,
    expected_facts: Mapping[str, Any],
    expected_p02: Mapping[str, Any],
) -> tuple[dict[str, Any], str, str, datetime]:
    with source_raw._transaction(config, write=False) as (
        _connection,
        cursor,
        namespace_id,
    ):
        cursor.execute(
            f"SELECT {', '.join('s.' + item.split(' AS ')[0] + ' AS ' + item.split(' AS ')[1] for item in _SOURCE_SELECT)}, "
            "s.xmin::text AS pending_xmin, age(s.xmin)::text AS pending_xmin_age, "
            "txid_current()::text AS observer_transaction_id "
            "FROM stock.source_sets s WHERE s.namespace_id=%s::uuid AND s.ingress_attempt_id=%s::uuid",
            (namespace_id, attempt_id),
        )
        columns = (
            *_SOURCE_COLUMNS,
            "pending_xmin",
            "pending_xmin_age",
            "observer_transaction_id",
        )
        record = _record(cursor, cursor.fetchone(), columns)
        if record["seal_status"] != "PENDING" or record["source_set_id"] is not None:
            raise SourceSetWriteError(
                "SOURCE_SET_PENDING_STATE_INVALID", "独立读回不是未封口PENDING尝试"
            )
        _assert_source_match(record, expected_facts)
        cursor.execute(
            f"SELECT {', '.join(_CAPABILITY_SELECT)} FROM stock.source_set_capabilities "
            "WHERE namespace_id=%s::uuid AND ingress_attempt_id=%s::uuid "
            "AND capability_id='BAOSTOCK_SUPPLEMENT'",
            (namespace_id, attempt_id),
        )
        p02_row = _record(cursor, cursor.fetchone(), _CAPABILITY_COLUMNS)
        if any(p02_row[key] != expected_p02[key] for key in _P02_COLUMNS):
            raise SourceSetWriteError(
                "SOURCE_SET_P02_CONFLICT", "独立读回的P02能力与本次P00/P02记录不一致"
            )
        writer_xid = ""
        cursor.execute(
            "SELECT txid_current()::text, age(%s::xid)::text",
            (record["pending_xmin"],),
        )
        observer_txid, xmin_age = (int(item) for item in cursor.fetchone())
        pending_xmin = int(record["pending_xmin"])
        if (
            int(record["pending_xmin_age"]) != xmin_age
            or xmin_age < 0
            or xmin_age >= 2**31
        ):
            raise SourceSetWriteError(
                "SOURCE_SET_XID_AGE_INVALID",
                "xmin age回执无法证明精确PENDING行仍可验证",
            )
        writer_xid = observer_txid - xmin_age
        if (
            writer_xid <= 0
            or observer_txid == writer_xid
            or pending_xmin != writer_xid % (2**32)
        ):
            raise SourceSetWriteError(
                "SOURCE_SET_XID_BINDING_INVALID", "xmin、写事务和独立读取事务不一致"
            )
        durable_at = datetime.now(timezone.utc)
        ack_time = expected_p02.get("_commit_ack_observed_at")
        if not isinstance(ack_time, datetime) or durable_at < ack_time:
            raise SourceSetWriteError(
                "SOURCE_SET_ACK_CLOCK_INVALID", "独立读回时钟早于实际提交ACK观察"
            )
        record["_pending_row_xmin"] = str(pending_xmin)
        record["_pending_creation_transaction_id"] = str(writer_xid)
        record["_observer_transaction_id"] = str(observer_txid)
    return record, str(writer_xid), str(observer_txid), durable_at


def _ack_capability_digest(row: Mapping[str, Any]) -> str:
    return _canonical_digest(
        {key: value for key, value in row.items() if key != "capability_digest"}
    )


def _persist_ack(
    config: Mapping[str, Any],
    *,
    namespace_id: str,
    attempt_id: str,
    ack_row: Mapping[str, Any],
) -> tuple[dict[str, Any], bool]:
    inserted = False
    with source_raw._transaction(config, write=True) as (
        _connection,
        cursor,
        actual_namespace,
    ):
        if actual_namespace != namespace_id:
            raise SourceSetWriteError(
                "SOURCE_SET_NAMESPACE_CHANGED", "P03确认事务namespace发生变化"
            )
        values = dict(ack_row)
        values["capability_digest"] = _ack_capability_digest(values)
        cursor.execute(
            f"INSERT INTO stock.source_set_capabilities ({', '.join(_P02_COLUMNS)}) "
            f"VALUES ({', '.join(['%s'] * len(_P02_COLUMNS))}) "
            "ON CONFLICT (namespace_id, ingress_attempt_id, capability_id) DO NOTHING",
            [values[key] for key in _P02_COLUMNS],
        )
        inserted = cursor.rowcount == 1
        if cursor.rowcount not in (0, 1):
            raise SourceSetWriteError(
                "SOURCE_SET_ACK_INSERT_BOUND", "提交确认关系插入超过一行"
            )
        cursor.execute(
            f"SELECT {', '.join(_CAPABILITY_SELECT)} FROM stock.source_set_capabilities "
            "WHERE namespace_id=%s::uuid AND ingress_attempt_id=%s::uuid AND capability_id='P03_COMMIT_ACK'",
            (namespace_id, attempt_id),
        )
        stored = _record(cursor, cursor.fetchone(), _CAPABILITY_COLUMNS)
        if inserted and any(stored[key] != values[key] for key in _P02_COLUMNS):
            raise SourceSetWriteError(
                "SOURCE_SET_ACK_CONFLICT", "本次提交确认记录与事务内刚插入值不一致"
            )
        values = {key: stored[key] for key in _P02_COLUMNS}
    return values, inserted


def _read_and_validate_ack(
    config: Mapping[str, Any],
    *,
    namespace_id: str,
    attempt_id: str,
    source_facts: Mapping[str, Any],
    p02_row: Mapping[str, Any],
) -> dict[str, Any]:
    with source_raw._transaction(config, write=False) as (
        _connection,
        cursor,
        actual_namespace,
    ):
        if actual_namespace != namespace_id:
            raise SourceSetWriteError(
                "SOURCE_SET_NAMESPACE_CHANGED", "提交确认读回namespace发生变化"
            )
        cursor.execute(
            f"SELECT {', '.join(_CAPABILITY_SELECT)}, xmin::text AS ack_row_xmin, "
            "age(xmin)::text AS ack_row_xmin_age, txid_current()::text AS ack_observer_transaction_id "
            "FROM stock.source_set_capabilities WHERE namespace_id=%s::uuid "
            "AND ingress_attempt_id=%s::uuid ORDER BY capability_id",
            (namespace_id, attempt_id),
        )
        rows = cursor.fetchall()
        expected_columns = (
            *_CAPABILITY_COLUMNS,
            "ack_row_xmin",
            "ack_row_xmin_age",
            "ack_observer_transaction_id",
        )
        if len(rows) != 2:
            raise SourceSetWriteError(
                "SOURCE_SET_CAPABILITY_SET_INVALID",
                "正式P03必须恰有P02与COMMIT_ACK两条typed能力记录",
            )
        records = [_record(cursor, row, expected_columns) for row in rows]
        p02_actual = next(
            (row for row in records if row["capability_id"] == "BAOSTOCK_SUPPLEMENT"),
            None,
        )
        ack_actual = next(
            (row for row in records if row["capability_id"] == "P03_COMMIT_ACK"), None
        )
        if p02_actual is None or ack_actual is None:
            raise SourceSetWriteError(
                "SOURCE_SET_CAPABILITY_SET_INVALID", "正式P03能力角色缺失或重复"
            )
        for key in _P02_COLUMNS:
            if key in p02_row and p02_actual[key] != p02_row[key]:
                raise SourceSetWriteError(
                    "SOURCE_SET_P02_BINDING_MISMATCH", f"P02能力{key}与预期不一致"
                )
        if (
            ack_actual["status"] != "VERIFIED_POST_COMMIT_READBACK"
            or ack_actual["plan_sha256"] != source_facts["plan_sha256"]
            or ack_actual["policy_sha256"] != p02_row["policy_sha256"]
            or ack_actual["binding_sha256"] != p02_row["binding_sha256"]
            or ack_actual["source_capture_id"] != source_facts["source_capture_id"]
            or ack_actual["source_digest"] != source_facts["source_digest"]
            or ack_actual["network_accessed"] is not False
            or ack_actual["actual_logins"] != 0
            or ack_actual["actual_queries"] != 0
            or ack_actual["durable_at"] < ack_actual["commit_ack_observed_at"]
        ):
            raise SourceSetWriteError(
                "SOURCE_SET_ACK_BINDING_INVALID",
                "COMMIT_ACK与精确PENDING来源、P00/P02或真实时钟不一致",
            )
        if ack_actual["capability_digest"] != _ack_capability_digest(
            {key: ack_actual[key] for key in _P02_COLUMNS if key != "capability_digest"}
        ):
            raise SourceSetWriteError(
                "SOURCE_SET_ACK_DIGEST_INVALID", "COMMIT_ACK typed行摘要复算不一致"
            )
        try:
            ack_row_xmin = int(ack_actual["ack_row_xmin"])
            age = int(ack_actual["ack_row_xmin_age"])
            ack_reader_xid = int(ack_actual["ack_observer_transaction_id"])
        except (TypeError, ValueError) as error:
            raise SourceSetWriteError(
                "SOURCE_SET_ACK_XID_INVALID", "ACK行独立读回缺少有效PG XID"
            ) from error
        if (
            age < 0
            or age >= 2**31
            or ack_reader_xid - age <= 0
            or ack_row_xmin != (ack_reader_xid - age) % (2**32)
        ):
            raise SourceSetWriteError(
                "SOURCE_SET_ACK_XID_INVALID", "ACK行xmin与独立读回事务无法闭合"
            )
        cursor.execute(
            f"SELECT {', '.join(item.split(' AS ')[0] + ' AS ' + item.split(' AS ')[1] for item in _SOURCE_SELECT)}, "
            "xmin::text AS pending_row_xmin, age(xmin)::text AS pending_row_xmin_age, "
            "txid_current()::text AS pending_reader_transaction_id "
            "FROM stock.source_sets WHERE namespace_id=%s::uuid AND ingress_attempt_id=%s::uuid",
            (namespace_id, attempt_id),
        )
        parent_columns = (
            *_SOURCE_COLUMNS,
            "pending_row_xmin",
            "pending_row_xmin_age",
            "pending_reader_transaction_id",
        )
        parent = _record(cursor, cursor.fetchone(), parent_columns)
        if parent["seal_status"] not in {"PENDING", "SEALED"}:
            raise SourceSetWriteError(
                "SOURCE_SET_STATE_INVALID", "正式来源集合尝试状态非法"
            )
        _assert_source_match(parent, source_facts)
        try:
            pending_xmin = int(parent["pending_row_xmin"])
            pending_age = int(parent["pending_row_xmin_age"])
            pending_reader_xid = int(parent["pending_reader_transaction_id"])
            ack_writer_xid = int(ack_actual["writer_transaction_id"])
            ack_observer_xid = int(ack_actual["observer_transaction_id"])
            pending_creation_xid = int(ack_actual["pending_creation_transaction_id"])
        except (TypeError, ValueError) as error:
            raise SourceSetWriteError(
                "SOURCE_SET_PENDING_XID_INVALID", "独立读回未提供有效PENDING事务身份"
            ) from error
        if (
            parent["seal_status"] != "PENDING"
            or pending_age < 0
            or pending_age >= 2**31
            or pending_xmin != (pending_reader_xid - pending_age) % (2**32)
            or str(pending_xmin) != ack_actual["pending_row_xmin"]
            or ack_writer_xid < pending_reader_xid - pending_age
            or pending_creation_xid != pending_reader_xid - pending_age
            or ack_observer_xid <= ack_writer_xid
            or pending_reader_xid <= ack_observer_xid
            or ack_actual["pending_row_xmin"] != str(pending_xmin)
        ):
            raise SourceSetWriteError(
                "SOURCE_SET_PENDING_XID_MISMATCH",
                "ACK未对应仍处于PENDING的真实创建事务及独立读回",
            )
        if parent["seal_status"] == "SEALED" and parent["source_set_id"] is None:
            raise SourceSetWriteError(
                "SOURCE_SET_SEAL_ID_MISSING", "SEALED行缺少正式source_set_id"
            )
        return {key: ack_actual[key] for key in _P02_COLUMNS}


def _seal_transaction(
    config: Mapping[str, Any],
    *,
    namespace_id: str,
    attempt_id: str,
    source_facts: Mapping[str, Any],
    p02_row: Mapping[str, Any],
    ack_row: Mapping[str, Any],
) -> tuple[dict[str, Any], str, bool]:
    from stock_a.storage.postgres.source_set_ingress import read_p04_source_input

    sealed_at = None
    seal_xid = ""
    identity_digest = source_set_identity_digest(
        source_facts=source_facts,
        p02_facts=p02_row,
        durability_facts=ack_row,
    )
    source_set_id = source_set_id_from_digest(identity_digest)
    with source_raw._transaction(config, write=True) as (
        _connection,
        cursor,
        actual_namespace,
    ):
        if actual_namespace != namespace_id:
            raise SourceSetWriteError(
                "SOURCE_SET_NAMESPACE_CHANGED", "封口事务namespace发生变化"
            )
        cursor.execute(
            "SELECT seal_status, source_set_id::text AS source_set_id, plan_sha256, source_digest, "
            "durability_status, durable_at FROM stock.source_sets "
            "WHERE namespace_id=%s::uuid AND ingress_attempt_id=%s::uuid FOR UPDATE",
            (namespace_id, attempt_id),
        )
        current = cursor.fetchone()
        if current is None:
            raise SourceSetWriteError(
                "SOURCE_SET_PENDING_MISSING", "最终封口前未读回精确PENDING尝试"
            )
        if current[0] == "SEALED":
            if current[1] != source_set_id:
                raise SourceSetWriteError(
                    "SOURCE_SET_FINAL_ID_CONFLICT",
                    "相同PENDING尝试已封口为不同source_set_id",
                )
            existing = read_p04_source_input(
                config,
                target_session=source_facts["target_session"],
                package_sha256=source_facts["package_sha256"],
            )
            if (
                existing.get("status") != "AVAILABLE"
                or existing.get("source_set_id") != source_set_id
            ):
                raise SourceSetWriteError(
                    "SOURCE_SET_SEALED_READBACK_INVALID", "既有SEALED尝试精确读回失败"
                )
            return existing, source_set_id, False
        if (
            current[0] != "PENDING"
            or current[1] is not None
            or current[2] != source_facts["plan_sha256"]
            or current[3] != source_facts["source_digest"]
        ):
            raise SourceSetWriteError(
                "SOURCE_SET_PENDING_CONFLICT", "封口时发现PENDING主行被更改，拒绝覆盖"
            )
        cursor.execute("SELECT txid_current()::text, clock_timestamp()")
        seal_xid, sealed_at = cursor.fetchone()
        seal_xid = str(seal_xid)
        if (
            not isinstance(sealed_at, datetime)
            or sealed_at.tzinfo is None
            or sealed_at < ack_row["durable_at"]
        ):
            raise SourceSetWriteError(
                "SOURCE_SET_SEAL_CLOCK_INVALID",
                "封口观察时钟缺失或早于PENDING durable_at",
            )
        seal_digest = source_set_final_seal_digest(
            source_set_id=source_set_id,
            identity_digest=identity_digest,
            seal_writer_transaction_id=seal_xid,
            sealed_at=sealed_at,
        )
        cursor.execute(
            "UPDATE stock.source_sets SET source_set_id=%s::uuid, durability_status='VERIFIED_POST_COMMIT_READBACK', "
            "commit_ack_observed_at=%s, durable_at=%s, durability_source_capture_id=%s::uuid, "
            "durability_source_digest=%s, seal_digest_sha256=%s, seal_status='SEALED', "
            "seal_writer_transaction_id=%s, sealed_at=%s "
            "WHERE namespace_id=%s::uuid AND ingress_attempt_id=%s::uuid AND target_session=%s "
            "AND package_sha256=%s AND source_digest=%s AND plan_sha256=%s "
            "AND seal_status='PENDING' AND source_set_id IS NULL RETURNING source_set_id::text, seal_status, seal_digest_sha256",
            (
                source_set_id,
                ack_row["commit_ack_observed_at"],
                ack_row["durable_at"],
                ack_row["source_capture_id"],
                ack_row["source_digest"],
                seal_digest,
                seal_xid,
                sealed_at,
                namespace_id,
                attempt_id,
                source_facts["target_session"],
                source_facts["package_sha256"],
                source_facts["source_digest"],
                source_facts["plan_sha256"],
            ),
        )
        returned = cursor.fetchone()
        if cursor.rowcount != 1 or returned != (source_set_id, "SEALED", seal_digest):
            raise SourceSetWriteError(
                "SOURCE_SET_SEAL_UPDATE_NOT_EXACT",
                "唯一封口UPDATE未精确返回正式身份、状态和摘要",
            )
    readback = read_p04_source_input(
        config,
        target_session=source_facts["target_session"],
        package_sha256=source_facts["package_sha256"],
    )
    if (
        readback.get("status") != "AVAILABLE"
        or readback.get("source_set_id") != source_set_id
    ):
        raise SourceSetWriteError(
            "SOURCE_SET_COMMITTED_READBACK_INVALID",
            "SEALED提交后独立P04精确读回没有返回同一source_set_id",
        )
    if readback.get("seal_writer_transaction_id") != seal_xid:
        raise SourceSetWriteError(
            "SOURCE_SET_SEAL_XID_MISMATCH", "P04读回的SEALED行xmin未绑定封口事务XID"
        )
    return readback, source_set_id, True


def persist_formal_source_set(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    calendar: Mapping[str, str],
    ingress: Mapping[str, Any],
) -> dict[str, Any]:
    """验证PG真实来源和P00/P02，再按PENDING→ACK→读回→SEALED提交正式P03。"""
    if (
        type(target_session) is not date
        or not isinstance(package_sha256, str)
        or _SHA256.fullmatch(package_sha256) is None
    ):
        raise SourceSetWriteError(
            "SOURCE_SET_KEY_INVALID", "正式P03必须绑定显式日期和完整小写包SHA"
        )
    if (
        ingress.get("scope") != "SOURCE_INGRESS"
        or ingress.get("target_session") != target_session.isoformat()
        or ingress.get("package_sha256") != package_sha256
    ):
        raise SourceSetWriteError(
            "P00_SCOPE_BINDING_INVALID", "P00计划未绑定精确SOURCE_INGRESS日期和来源版本"
        )
    if (
        ingress.get("source_scope_status") != "SOURCE_INGRESS_READY"
        or ingress.get("environment_gate", {}).get("status") != "READY"
    ):
        raise SourceSetWriteError(
            "P00_SOURCE_INGRESS_BLOCKED",
            "本次有界真实Phase0/P00没有证明SOURCE_INGRESS准入",
        )
    p02 = ingress.get("p02_baostock")
    plan = ingress.get("plan")
    if (
        not isinstance(p02, Mapping)
        or not isinstance(plan, Mapping)
        or p02.get("plan_sha256") != plan.get("plan_sha256")
    ):
        raise SourceSetWriteError(
            "P02_PLAN_BINDING_INVALID", "P02策略或请求账没有绑定本次P00 plan_sha256"
        )
    if (
        plan.get("mode") not in {"INITIAL", "APPEND"}
        or plan.get("status") != "PLANNED"
        or plan.get("candidate_ready") is not True
    ):
        raise SourceSetWriteError(
            "P00_INCREMENTAL_MODE_BLOCKED", "正式来源写入只接受明确INITIAL或APPEND计划"
        )
    p02_identity = _p02_identity(
        target_session=target_session, package_sha256=package_sha256, p02=p02
    )
    expected_policy_sha = _validate_current_policy(config, p02)
    if (
        p02_identity["policy_sha256"] != expected_policy_sha
        or _p02_binding_digest(
            target_session=target_session, package_sha256=package_sha256, p02=p02
        )
        != p02["binding_sha256"]
    ):
        raise SourceSetWriteError(
            "P02_BINDING_DIGEST_INVALID",
            "P02策略、预算、零请求状态或绑定摘要复算不匹配",
        )
    header, inputs = _read_candidate_facts(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
        calendar=calendar,
    )
    try:
        candidate_digest = source_set_candidate_digest(header, inputs)
    except ValueError as error:
        raise SourceSetWriteError(
            "SOURCE_SET_CANDIDATE_DIGEST_INVALID", "实时Raw候选无法独立重算摘要"
        ) from error
    if candidate_digest != header.get("candidate_sha256"):
        raise SourceSetWriteError(
            "SOURCE_SET_CANDIDATE_DIGEST_INVALID", "实时Raw候选摘要复算不匹配"
        )
    facts = _source_payload(
        header=header,
        inputs=inputs,
        target_session=target_session,
        package_sha256=package_sha256,
        ingress=ingress,
    )
    p02_row = _stored_p02_row(
        namespace_id=facts["namespace_id"],
        attempt_id="",
        source_facts=facts,
        p02=p02,
    )
    attempt_id = source_set_attempt_id(
        namespace_id=facts["namespace_id"],
        target_session=target_session.isoformat(),
        package_sha256=package_sha256,
        revision_id=facts["revision_id"],
        source_digest=facts["source_digest"],
        plan_sha256=facts["plan_sha256"],
        p02_binding_sha256=p02["binding_sha256"],
    )
    p02_row["ingress_attempt_id"] = attempt_id
    p02_row["capability_digest"] = _p02_row_digest(
        target_session=target_session, package_sha256=package_sha256, row=p02_row
    )
    with source_raw._transaction(config, write=False) as (
        _connection,
        cursor,
        namespace_id,
    ):
        if namespace_id != facts["namespace_id"]:
            raise SourceSetWriteError(
                "SOURCE_SET_NAMESPACE_CHANGED", "Raw实时核验与当前P03 namespace不一致"
            )
        _verify_stored_candidate(
            cursor,
            namespace_id=namespace_id,
            facts=facts,
            expected_header=header,
            expected_candidate_digest=candidate_digest,
            expected_inputs=inputs,
        )
    commit_writer_xid, commit_ack_at, inserted_pending = _insert_or_read_pending(
        config,
        facts=facts,
        attempt_id=attempt_id,
        p02_row=p02_row,
        p02_sha=expected_policy_sha,
    )
    ack_p02 = {**p02_row, "_commit_ack_observed_at": commit_ack_at}
    pending_row, pending_writer_xid, observer_xid, durable_at = (
        _read_pending_observation(
            config,
            attempt_id=attempt_id,
            expected_facts=facts,
            expected_p02=ack_p02,
        )
    )
    if inserted_pending and commit_writer_xid != pending_writer_xid:
        raise SourceSetWriteError(
            "SOURCE_SET_CREATE_XID_MISMATCH", "新PENDING行xmin与首次INSERT事务XID不一致"
        )
    ack_row = _stored_ack_row(
        namespace_id=facts["namespace_id"],
        attempt_id=attempt_id,
        source_facts=facts,
        p02_row=p02_row,
        writer_transaction_id=commit_writer_xid,
        observer_transaction_id=observer_xid,
        pending_row_xmin=pending_row["_pending_row_xmin"],
        pending_creation_transaction_id=pending_writer_xid,
        commit_ack_observed_at=commit_ack_at,
        durable_at=durable_at,
    )
    persisted_ack, inserted_ack = _persist_ack(
        config,
        namespace_id=facts["namespace_id"],
        attempt_id=attempt_id,
        ack_row=ack_row,
    )
    validated_ack = _read_and_validate_ack(
        config,
        namespace_id=facts["namespace_id"],
        attempt_id=attempt_id,
        source_facts=facts,
        p02_row=p02_row,
    )
    readback, source_set_id, sealed_now = _seal_transaction(
        config,
        namespace_id=facts["namespace_id"],
        attempt_id=attempt_id,
        source_facts=facts,
        p02_row=p02_row,
        ack_row=validated_ack,
    )
    return {
        "status": "SEALED" if sealed_now else "ALREADY_SEALED",
        "source_set_id": source_set_id,
        "ingress_attempt_id": attempt_id,
        "source_digest": facts["source_digest"],
        "source_capture_id": facts["source_capture_id"],
        "source_content_id": facts["source_content_id"],
        "target_session": target_session.isoformat(),
        "package_sha256": package_sha256,
        "seal_status": "SEALED",
        "durability_status": "VERIFIED_POST_COMMIT_READBACK",
        "commit_ack_observed_at": readback.get("commit_ack_observed_at"),
        "durable_at": readback.get("durable_at"),
        "p02_capability": readback.get("p02_capability"),
        "p00_plan_sha256": facts["plan_sha256"],
        "inserted_parent_rows": int(inserted_pending),
        "inserted_capability_rows": int(inserted_pending) + int(inserted_ack),
        "maximum_parent_rows": 1,
        "maximum_capability_rows": 2,
        "candidate_parent_inserted": False,
        "candidate_input_rows_inserted": 0,
        "candidate_digest": candidate_digest,
        "p02_binding_sha256": p02_row["binding_sha256"],
        "p02_policy_sha256": p02_row["policy_sha256"],
        "pending_commit_ack_and_independent_readback": True,
        "durable_at_role": "PENDING_ROW_POST_COMMIT_INDEPENDENT_READBACK_OBSERVED_AT",
        "seal_identity_formula": "UUID5(namespace_id, SHA256(SA_SEAL_CANONICAL_JSON_V1(identity_payload)))",
        "seal_identity_includes": [
            "content_sha256",
            "canonical_digest",
            "durable_at",
            "P02 binding and policy",
            "dataset",
            "parser",
            "unit",
            "capture",
            "P00 plan",
            "scope",
            "calendar",
        ],
        "p04_readback": readback,
        "database_write": True,
        "ddl_executed": False,
        "delete_executed": False,
        "history_update_executed": False,
        "head_changed": False,
        "production_switched": False,
    }
