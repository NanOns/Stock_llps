"""正式来源集合的精确读者与提交后确认合同。"""

from __future__ import annotations

from datetime import date, datetime, timezone
import re
from typing import Any, Mapping
import uuid

from . import source_raw
from stock_a.domain.source_set_seal import (
    SourceSetSealError,
    source_set_final_seal_digest,
    source_set_identity_digest,
    source_set_id_from_digest,
)


_SHA256 = re.compile(r"^[0-9a-f]{64}$")

# 这是R01正式source_set关系的最小读取投影。没有这些typed列时失败关闭；
# 正式表尚未由获准迁移创建时，只能报告首次观察或读者不可用。
_SOURCE_SET_SELECT = (
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
    "xmin::text AS seal_row_xmin",
    "age(xmin)::text AS seal_row_xmin_age",
    "txid_current()::text AS seal_reader_transaction_id",
)
_SOURCE_SET_COLUMNS = tuple(item.split(" AS ", 1)[1] for item in _SOURCE_SET_SELECT)
_CAPABILITY_COLUMNS = (
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
_CAPABILITY_ID = "BAOSTOCK_SUPPLEMENT"
_ACK_CAPABILITY_ID = "P03_COMMIT_ACK"


class SourceSetIngressError(ValueError):
    """正式来源集合读回或提交证据不符合typed合同。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _as_record(row: Any, columns: tuple[str, ...]) -> dict[str, Any]:
    if isinstance(row, Mapping):
        if set(row) != set(columns):
            raise SourceSetIngressError(
                "SOURCE_SET_ROW_SHAPE_INVALID", "正式来源集合映射列名不符合合同"
            )
        return {column: row[column] for column in columns}
    try:
        return dict(zip(columns, row, strict=True))
    except (TypeError, ValueError) as error:
        raise SourceSetIngressError(
            "SOURCE_SET_ROW_SHAPE_INVALID", "正式来源集合读回列数不符合合同"
        ) from error


def _cursor_column_names(cursor: Any) -> tuple[str, ...]:
    description = getattr(cursor, "description", None)
    if not isinstance(description, (tuple, list)):
        raise SourceSetIngressError(
            "SOURCE_SET_CURSOR_DESCRIPTION_INVALID", "正式来源集合游标缺少列描述"
        )
    names = []
    for column in description:
        name = getattr(column, "name", None)
        if name is None:
            try:
                name = column[0]
            except (IndexError, TypeError):
                name = None
        if not isinstance(name, str) or not name:
            raise SourceSetIngressError(
                "SOURCE_SET_CURSOR_DESCRIPTION_INVALID",
                "正式来源集合游标列名无效",
            )
        names.append(name)
    return tuple(names)


def _as_cursor_record(
    cursor: Any, row: Any, columns: tuple[str, ...]
) -> dict[str, Any]:
    actual_columns = _cursor_column_names(cursor)
    if actual_columns != columns:
        raise SourceSetIngressError(
            "SOURCE_SET_CURSOR_COLUMNS_MISMATCH",
            "正式来源集合游标列名或顺序不符合合同",
        )
    return _as_record(row, columns)


def _aware(value: Any, label: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_CLOCK_INVALID", f"来源集合{label}必须是带时区的时间"
        )
    return value


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SourceSetIngressError(
            "SOURCE_SET_IDENTITY_INCOMPLETE", f"来源集合{label}缺失"
        )
    return value


def _required_sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise SourceSetIngressError(
            "SOURCE_SET_DIGEST_INVALID", f"来源集合{label}必须是小写SHA-256"
        )
    return value


def _validate_source_set(
    raw: Mapping[str, Any], *, namespace_id: str
) -> dict[str, Any]:
    row = dict(raw)
    if str(row.get("namespace_id")) != str(namespace_id):
        raise SourceSetIngressError(
            "SOURCE_SET_NAMESPACE_MISMATCH", "来源集合跨DEV namespace"
        )
    if type(row.get("target_session")) is not date:
        raise SourceSetIngressError(
            "SOURCE_SET_SESSION_INVALID", "来源集合目标会话必须是PG date"
        )
    for key in (
        "package_sha256",
        "source_digest",
        "source_raw_digest",
        "calendar_sha256",
        "durability_source_digest",
        "seal_digest_sha256",
        "business_input_digest",
        "plan_sha256",
        "contract_sha256",
        "scope_contract_digest",
        "scope_catalog_digest",
    ):
        _required_sha(row.get(key), key)
    for key in (
        "ingress_attempt_id",
        "source_set_id",
        "source_content_id",
        "source_capture_id",
        "durability_source_capture_id",
        "calendar_id",
        "calendar_revision",
        "scope_id",
        "scope_contract_version",
        "source_semantic_revision",
    ):
        _required_text(row.get(key), key)
    if type(row.get("revision_id")) is not int or row["revision_id"] < 1:
        raise SourceSetIngressError(
            "SOURCE_SET_REVISION_INVALID", "来源集合revision_id必须是非负整数"
        )
    if type(row.get("head_generation")) is not int or row["head_generation"] < 0:
        raise SourceSetIngressError(
            "SOURCE_SET_GENERATION_INVALID", "来源集合generation必须是非负整数"
        )
    if row.get("seal_status") != "SEALED":
        raise SourceSetIngressError(
            "SOURCE_SET_NOT_SEALED", "非SEALED来源集合不能成为accepted-state"
        )
    if row.get("durability_status") != "VERIFIED_POST_COMMIT_READBACK":
        raise SourceSetIngressError(
            "SOURCE_SET_DURABILITY_UNVERIFIED", "来源集合缺少提交后精确读回证据"
        )
    ack_at = _aware(row.get("commit_ack_observed_at"), "提交确认观察时钟")
    durable_at = _aware(row.get("durable_at"), "提交后确认读回时钟")
    sealed_at = _aware(row.get("sealed_at"), "封口时钟")
    if ack_at > durable_at or durable_at > sealed_at:
        raise SourceSetIngressError(
            "SOURCE_SET_DURABILITY_ORDER_INVALID",
            "提交确认、PENDING独立读回和最终封口时钟顺序不成立",
        )
    if (
        row["durability_source_capture_id"] != row["source_capture_id"]
        or row["durability_source_digest"] != row["source_digest"]
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_DURABILITY_BINDING_MISMATCH",
            "提交后读回证据没有绑定本来源集合的捕获和内容摘要",
        )
    if row["scope_id"] != "SOURCE_INGRESS":
        raise SourceSetIngressError(
            "SOURCE_SET_SCOPE_MISMATCH", "accepted-state不属于SOURCE_INGRESS scope"
        )
    if row.get("seal_status") != "SEALED":
        raise SourceSetIngressError(
            "SOURCE_SET_NOT_SEALED", "非SEALED来源集合不能成为accepted-state"
        )
    for key in ("capture_read_started_at", "capture_validated_at"):
        _aware(row.get(key), key)
    if row["capture_validated_at"] < row["capture_read_started_at"]:
        raise SourceSetIngressError(
            "SOURCE_SET_CAPTURE_CLOCK_INVALID", "捕获校验时间早于捕获读取时间"
        )
    if (
        row.get("source_available_at") is not None
        and row.get("source_time_quality") != "VERIFIED_SOURCE_TIME"
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_SOURCE_CLOCK_INVALID", "未验证来源时钟却填入available_at"
        )
    if (
        row.get("source_dataset_id") != "TDX_VIPDATA_PACKAGE"
        or row.get("price_unit_status") not in {"RAW_UNADJUSTED", "UNIT_UNVERIFIED"}
        or row.get("amount_unit_status") not in {"RAW_UNADJUSTED", "UNIT_UNVERIFIED"}
        or row.get("volume_unit_status") not in {"RAW_UNADJUSTED", "UNIT_UNVERIFIED"}
        or row.get("raw_row_count") != row.get("capture_expected_row_count")
        or row.get("raw_row_count")
        != row.get("target_present_count")
        + row.get("target_missing_count")
        + row.get("read_failed_count")
        or row.get("read_failed_count") != 0
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_RAW_PARTITION_INVALID",
            "Raw分母、状态分区、来源或单位字段不符合候选合同",
        )
    try:
        uuid.UUID(row["source_set_id"])
        uuid.UUID(row["ingress_attempt_id"])
    except (ValueError, TypeError, AttributeError) as error:
        raise SourceSetIngressError(
            "SOURCE_SET_UUID_INVALID", "source_set_id或ingress_attempt_id不是规范UUID"
        ) from error
    try:
        seal_xid = int(row.get("seal_writer_transaction_id"))
        row_xmin = int(row.get("seal_row_xmin"))
        row_xmin_age = int(row.get("seal_row_xmin_age"))
        reader_xid = int(row.get("seal_reader_transaction_id"))
    except (TypeError, ValueError) as error:
        raise SourceSetIngressError(
            "SOURCE_SET_SEAL_XID_INVALID", "SEALED独立读回缺少有效PG事务证据"
        ) from error
    if (
        seal_xid <= 0
        or row_xmin_age < 0
        or row_xmin_age >= 2**31
        or row_xmin != seal_xid % (2**32)
        or reader_xid - seal_xid != row_xmin_age
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_SEAL_XID_MISMATCH", "SEALED行xmin与独立读回事务无法闭合"
        )
    row["seal_writer_transaction_id"] = str(seal_xid)
    return row


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    import json
    from hashlib import sha256

    def canonical_item(item: Any) -> Any:
        if isinstance(item, datetime):
            if item.tzinfo is None or item.utcoffset() is None:
                raise SourceSetIngressError(
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


def _capability_digest(row: Mapping[str, Any]) -> str:
    return _canonical_sha256(
        {key: value for key, value in row.items() if key != "capability_digest"}
    )


def _read_capability(
    cursor: Any,
    row: Mapping[str, Any],
    *,
    namespace_id: str,
    config: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    cursor.execute(
        f"SELECT {', '.join(_CAPABILITY_SELECT)} "
        "FROM stock.source_set_capabilities "
        "WHERE namespace_id=%s::uuid AND ingress_attempt_id=%s::uuid "
        "ORDER BY capability_id",
        (namespace_id, row["ingress_attempt_id"]),
    )
    raw_rows = cursor.fetchall()
    if len(raw_rows) != 2:
        raise SourceSetIngressError(
            "SOURCE_SET_CAPABILITY_SET_INVALID",
            "正式来源集合必须有且只有P02与提交后确认两条能力关系",
        )
    records = [
        _as_cursor_record(cursor, item, _CAPABILITY_COLUMNS) for item in raw_rows
    ]
    by_id = {item["capability_id"]: item for item in records}
    if set(by_id) != {_CAPABILITY_ID, _ACK_CAPABILITY_ID}:
        raise SourceSetIngressError(
            "SOURCE_SET_CAPABILITY_SET_INVALID", "正式来源能力角色缺失、重复或未知"
        )
    capability = by_id[_CAPABILITY_ID]
    ack = by_id[_ACK_CAPABILITY_ID]
    p02_policy = config.get("source_policy", {}).get("baostock_supplement")
    if not isinstance(p02_policy, Mapping):
        raise SourceSetIngressError(
            "SOURCE_SET_P02_POLICY_MISSING", "唯一正式配置缺少BaoStock策略合同"
        )
    expected_policy = {
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
    if dict(p02_policy) != expected_policy:
        raise SourceSetIngressError(
            "SOURCE_SET_P02_POLICY_INVALID", "唯一正式配置中的P02策略不符合登记合同"
        )
    policy_sha = _canonical_sha256(p02_policy)
    budget = p02_policy.get("request_budget")
    expected_binding = _canonical_sha256(
        {
            "target_session": row["target_session"].isoformat(),
            "package_sha256": row["package_sha256"],
            "plan_sha256": row["plan_sha256"],
            "policy_contract": capability["policy_contract"],
            "policy_sha256": capability["policy_sha256"],
            "status": capability["status"],
            "reason_code": capability["reason_code"],
            "actual_requests": {
                "logins": capability["actual_logins"],
                "queries": capability["actual_queries"],
            },
            "network_accessed": capability["network_accessed"],
            "data_quality": capability["data_quality"],
        }
    )
    for item in (capability, ack):
        if (
            item["plan_sha256"] != row["plan_sha256"]
            or item["policy_sha256"] != policy_sha
        ):
            raise SourceSetIngressError(
                "SOURCE_SET_P02_BINDING_INVALID",
                "P00/P02计划或策略摘要与当前正式配置不一致",
            )
    if (
        capability["status"] != "NOT_REQUESTED"
        or capability["policy_contract"] != p02_policy.get("contract")
        or budget
        != {
            "max_logins": 1,
            "max_queries_per_target_session": 10,
            "query_timeout_seconds": 30,
        }
        or capability["max_logins"] != 1
        or capability["max_queries"] != 10
        or capability["query_timeout_seconds"] != 30
        or capability["reason_code"] != "OPTIONAL_SOURCE_NOT_REQUESTED"
        or capability["data_quality"] != "NOT_MEASURED_NO_RESPONSE"
        or capability["actual_logins"] != 0
        or capability["actual_queries"] != 0
        or capability["network_accessed"] is not False
        or capability["binding_sha256"] != expected_binding
        or capability["capability_digest"]
        != _canonical_sha256(
            {
                "capability_id": capability["capability_id"],
                "target_session": row["target_session"].isoformat(),
                "package_sha256": row["package_sha256"],
                **{
                    key: capability[key]
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
        )
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_P02_BINDING_INVALID",
            "P02状态、零请求分母、配置策略或typed摘要复算不匹配",
        )
    if (
        ack["status"] != "VERIFIED_POST_COMMIT_READBACK"
        or ack["capability_digest"] != _capability_digest(ack)
        or ack["reason_code"] != "PENDING_ROW_INDEPENDENTLY_READ"
        or ack["source_capture_id"] != row["source_capture_id"]
        or ack["source_digest"] != row["source_digest"]
        or ack["binding_sha256"] != capability["binding_sha256"]
        or ack["actual_logins"] != 0
        or ack["actual_queries"] != 0
        or ack["network_accessed"] is not False
        or ack.get("pending_creation_transaction_id") is None
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_COMMIT_ACK_INVALID",
            "PENDING提交确认没有绑定精确来源、P02和回执摘要",
        )
    ack_at = _aware(ack["commit_ack_observed_at"], "提交确认观察时钟")
    ack_durable = _aware(ack["durable_at"], "PENDING独立读回时钟")
    if (
        ack["writer_transaction_id"] == ack["observer_transaction_id"]
        or ack_durable < ack_at
        or ack_at != row["commit_ack_observed_at"]
        or ack_durable != row["durable_at"]
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_COMMIT_ACK_CLOCK_INVALID",
            "提交回执XID或时钟未与封口主行精确对应",
        )
    for key in ("writer_transaction_id", "observer_transaction_id", "pending_row_xmin"):
        value = ack.get(key)
        if not isinstance(value, str) or not value.isdecimal() or int(value) <= 0:
            raise SourceSetIngressError(
                "SOURCE_SET_COMMIT_ACK_XID_INVALID",
                "PENDING提交回执缺少合法十进制PG XID",
            )
    try:
        pending_creator_xid = int(ack["pending_creation_transaction_id"])
        ack_writer_xid = int(ack["writer_transaction_id"])
        ack_observer_xid = int(ack["observer_transaction_id"])
        pending_xmin = int(ack["pending_row_xmin"])
    except (TypeError, ValueError) as error:
        raise SourceSetIngressError(
            "SOURCE_SET_COMMIT_ACK_XID_INVALID", "PENDING创建事务与回执XID格式无效"
        ) from error
    if (
        pending_creator_xid <= 0
        or pending_xmin != pending_creator_xid % (2**32)
        or ack_writer_xid < pending_creator_xid
        or ack_observer_xid <= ack_writer_xid
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_COMMIT_ACK_XID_INVALID",
            "PENDING创建、COMMIT ACK与独立观察XID顺序无法闭合",
        )
    cursor.execute(
        "SELECT xmin::text, age(xmin)::text, txid_current()::text "
        "FROM stock.source_set_capabilities WHERE namespace_id=%s::uuid "
        "AND ingress_attempt_id=%s::uuid AND capability_id=%s",
        (namespace_id, row["ingress_attempt_id"], _ACK_CAPABILITY_ID),
    )
    ack_xmin, ack_age, ack_reader_xid = cursor.fetchone()
    try:
        ack_xmin_value, ack_age_value, ack_reader_xid_value = (
            int(ack_xmin),
            int(ack_age),
            int(ack_reader_xid),
        )
    except (TypeError, ValueError) as error:
        raise SourceSetIngressError(
            "SOURCE_SET_COMMIT_ACK_XID_INVALID", "能力回执行独立读回缺少有效xmin/XID"
        ) from error
    if (
        ack_age_value < 0
        or ack_age_value >= 2**31
        or ack_reader_xid_value - ack_age_value <= ack_observer_xid
        or ack_xmin_value != (ack_reader_xid_value - ack_age_value) % (2**32)
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_COMMIT_ACK_XID_INVALID",
            "ACK能力记录没有在其PENDING读回之后提交并被当前事务精确读到",
        )
    try:
        identity_digest = source_set_identity_digest(
            source_facts=row,
            p02_facts=capability,
            durability_facts=ack,
        )
        expected_source_set_id = source_set_id_from_digest(identity_digest)
        expected_seal_digest = source_set_final_seal_digest(
            source_set_id=expected_source_set_id,
            identity_digest=identity_digest,
            seal_writer_transaction_id=row["seal_writer_transaction_id"],
            sealed_at=row["sealed_at"],
        )
    except (SourceSetSealError, ValueError, TypeError) as error:
        raise SourceSetIngressError(
            "SOURCE_SET_IDENTITY_INVALID", "正式source_set身份摘要无法按唯一公式复算"
        ) from error
    if (
        expected_source_set_id != row["source_set_id"]
        or expected_seal_digest != row["seal_digest_sha256"]
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_IDENTITY_DIGEST_MISMATCH",
            "source_set_id或seal摘要不包含当前durable_at、捕获和P02真实绑定",
        )
    return capability, ack


def _tables_status(cursor: Any) -> tuple[str | None, str | None]:
    cursor.execute(
        """SELECT to_regclass('stock.source_sets')::text,
                  to_regclass('stock.source_set_capabilities')::text"""
    )
    result = cursor.fetchone()
    if not isinstance(result, (tuple, list)) or len(result) != 2:
        raise SourceSetIngressError(
            "SOURCE_SET_RELATION_CHECK_INVALID", "正式来源集合表检查返回值非法"
        )
    return result[0], result[1]


def _accepted_projection(namespace_id: str, row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "target_session": row["target_session"],
        "head_generation": row["head_generation"],
        "calendar_id": row["calendar_id"],
        "calendar_revision": row["calendar_revision"],
        "scope_id": row["scope_id"],
        "scope_contract_version": row["scope_contract_version"],
        "scope_contract_digest": row["scope_contract_digest"],
        "scope_catalog_digest": row["scope_catalog_digest"],
        "semantic_revisions": {"source": row["source_semantic_revision"]},
        "business_input_digest": row["business_input_digest"],
        "namespace_id": namespace_id,
        "source_set_id": row["source_set_id"],
        "revision_id": row["revision_id"],
        "package_sha256": row["package_sha256"],
        "source_content_id": row["source_content_id"],
        "source_capture_id": row["source_capture_id"],
        "source_digest": row["source_digest"],
        "plan_sha256": row["plan_sha256"],
        "commit_ack_observed_at": row["commit_ack_observed_at"],
        "durable_at": row["durable_at"],
        "sealed_at": row["sealed_at"],
    }


def _validate_source_references(cursor: Any, row: Mapping[str, Any]) -> dict[str, Any]:
    """以candidate/capture/content FKs及Raw分母校验SEALED的真实PG上游。"""
    cursor.execute(
        """SELECT c.candidate_sha256, c.source_content_id, c.source_capture_id::text,
                  c.raw_source_digest, c.capture_canonical_digest, c.raw_row_count,
                  c.capture_expected_row_count, c.target_present_count, c.target_missing_count,
                  c.read_failed_count, c.source_dataset_id, c.source_byte_sha256,
                  c.source_byte_count, c.parser_contract,
                  cap.content_id, cap.normalized_count, cap.canonical_digest,
                  cap.read_started_at, cap.validated_at, cap.source_available_at,
                  cap.source_time_quality, cap.source_clock_role, cap.parser_contract,
                  content.dataset_id, content.byte_sha, content.byte_count
           FROM stock.source_set_candidates c
           JOIN stock.source_captures cap
             ON cap.namespace_id=c.namespace_id AND cap.capture_id=c.source_capture_id
           JOIN stock.source_contents content
             ON content.namespace_id=c.namespace_id AND content.content_id=c.source_content_id
           WHERE c.namespace_id=%s::uuid AND c.target_session=%s AND c.package_sha256=%s""",
        (row["namespace_id"], row["target_session"], row["package_sha256"]),
    )
    source_rows = cursor.fetchall()
    if len(source_rows) != 1:
        raise SourceSetIngressError(
            "P04_SOURCE_REFERENCE_MISSING",
            "正式source_set未精确引用唯一PG候选、capture和content",
        )
    candidate = _as_record(
        source_rows[0],
        (
            "candidate_sha256",
            "candidate_content_id",
            "candidate_capture_id",
            "candidate_raw_digest",
            "candidate_capture_digest",
            "candidate_raw_count",
            "candidate_expected_count",
            "candidate_present_count",
            "candidate_missing_count",
            "candidate_failed_count",
            "candidate_dataset_id",
            "candidate_byte_sha",
            "candidate_byte_count",
            "candidate_parser_contract",
            "capture_content_id",
            "capture_normalized_count",
            "capture_digest",
            "capture_read_started_at",
            "capture_validated_at",
            "capture_source_available_at",
            "capture_time_quality",
            "capture_clock_role",
            "capture_parser_contract",
            "content_dataset_id",
            "content_byte_sha",
            "content_byte_count",
        ),
    )
    checks = (
        ("candidate_sha256", row["source_digest"]),
        ("candidate_content_id", row["source_content_id"]),
        ("candidate_capture_id", row["source_capture_id"]),
        ("candidate_raw_digest", row["source_raw_digest"]),
        ("candidate_capture_digest", row["source_raw_digest"]),
        ("candidate_raw_count", row["raw_row_count"]),
        ("candidate_expected_count", row["capture_expected_row_count"]),
        ("candidate_present_count", row["target_present_count"]),
        ("candidate_missing_count", row["target_missing_count"]),
        ("candidate_failed_count", row["read_failed_count"]),
        ("candidate_dataset_id", row["source_dataset_id"]),
        ("candidate_byte_sha", row["package_sha256"]),
        ("candidate_byte_count", row["source_byte_count"]),
        ("candidate_parser_contract", row["parser_contract"]),
        ("capture_content_id", row["source_content_id"]),
        ("capture_normalized_count", row["capture_expected_row_count"]),
        ("capture_digest", row["source_raw_digest"]),
        ("capture_read_started_at", row["capture_read_started_at"]),
        ("capture_validated_at", row["capture_validated_at"]),
        ("capture_source_available_at", row["source_available_at"]),
        ("capture_time_quality", row["source_time_quality"]),
        ("capture_clock_role", row["source_clock_role"]),
        ("capture_parser_contract", row["parser_contract"]),
        ("content_dataset_id", row["source_dataset_id"]),
        ("content_byte_sha", row["package_sha256"]),
        ("content_byte_count", row["source_byte_count"]),
    )
    for key, expected in checks:
        actual = candidate[key]
        if isinstance(actual, datetime) and isinstance(expected, datetime):
            matches = actual.astimezone(timezone.utc) == expected.astimezone(
                timezone.utc
            )
        else:
            matches = actual == expected
        if not matches:
            raise SourceSetIngressError(
                "P04_SOURCE_REFERENCE_MISMATCH",
                f"PG候选/capture/content字段{key}与SEALED主行不一致",
            )
    raw_rows = source_raw._read_raw_rows(
        cursor,
        row["namespace_id"],
        row["target_session"],
        row["package_sha256"],
    )
    status_counts = {
        status: sum(item["bar_status"] == status for item in raw_rows)
        for status in ("TARGET_PRESENT", "TARGET_MISSING", "READ_FAILED")
    }
    capture_ids = {str(item["source_capture_id"]) for item in raw_rows}
    content_ids = {item["source_content_id"] for item in raw_rows}
    broken_references = sum(
        str(item["source_capture_id"]) != row["source_capture_id"]
        or item["source_content_id"] != row["source_content_id"]
        or item["session_date"] != row["target_session"]
        or item["package_sha256"] != row["package_sha256"]
        for item in raw_rows
    )
    unknown_statuses = sum(
        item["bar_status"] not in {"TARGET_PRESENT", "TARGET_MISSING", "READ_FAILED"}
        for item in raw_rows
    )
    actual_raw_digest = source_raw._row_digest(raw_rows)
    if (
        len(raw_rows) != row["raw_row_count"]
        or len(raw_rows) != row["capture_expected_row_count"]
        or status_counts["TARGET_PRESENT"] != row["target_present_count"]
        or status_counts["TARGET_MISSING"] != row["target_missing_count"]
        or status_counts["READ_FAILED"] != row["read_failed_count"]
        or capture_ids != {row["source_capture_id"]}
        or content_ids != {row["source_content_id"]}
        or broken_references != 0
        or unknown_statuses != 0
        or actual_raw_digest != row["source_raw_digest"]
        or actual_raw_digest != candidate["candidate_raw_digest"]
    ):
        raise SourceSetIngressError(
            "P04_RAW_DENOMINATOR_MISMATCH",
            "正式Raw逐行摘要、状态分母、capture或content引用与封口摘要不一致",
        )
    counts = (
        len(raw_rows),
        status_counts["TARGET_PRESENT"],
        status_counts["TARGET_MISSING"],
        status_counts["READ_FAILED"],
    )
    return {
        "candidate_sha256": candidate["candidate_sha256"],
        "source_content_id": candidate["candidate_content_id"],
        "source_capture_id": candidate["candidate_capture_id"],
        "raw_row_count": counts[0],
        "target_present_count": counts[1],
        "target_missing_count": counts[2],
        "read_failed_count": counts[3],
        "raw_digest_recomputed": True,
        "raw_digest_sha256": actual_raw_digest,
        "candidate_and_capture_exact": True,
    }


def _same_day_pending_sealed_resolution(
    *,
    pending: Mapping[str, Any],
    sealed_rows: list[Mapping[str, Any]],
    package_sha256: str | None,
    source_digest: str | None,
) -> tuple[str, Mapping[str, Any] | None]:
    """只把请求精确匹配的SEALED与同输入PENDING组合判为只读NOOP。"""
    exact_sealed = [
        row
        for row in sealed_rows
        if package_sha256 is not None
        and source_digest is not None
        and row["package_sha256"] == package_sha256
        and row["source_digest"] == source_digest
    ]
    if len(exact_sealed) != 1:
        return "NO_EXACT_SEALED", None
    if (
        pending.get("package_sha256") == package_sha256
        and pending.get("source_digest") == source_digest
    ):
        return "EXACT_NOOP", exact_sealed[0]
    return "INPUT_MISMATCH", exact_sealed[0]


def read_accepted_source_state(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str | None = None,
    source_digest: str | None = None,
) -> dict[str, Any]:
    """把目标日的SEALED、PENDING、未来接受和空前态分别分类。"""
    if type(target_session) is not date:
        raise ValueError("accepted-state查询必须绑定显式目标会话")
    if package_sha256 is not None and _SHA256.fullmatch(package_sha256) is None:
        raise ValueError("accepted-state来源包SHA必须是完整小写SHA-256")
    if source_digest is not None and _SHA256.fullmatch(source_digest) is None:
        raise ValueError("accepted-state来源摘要必须是完整小写SHA-256")
    with source_raw._transaction(config, write=False) as (
        _connection,
        cursor,
        namespace_id,
    ):
        source_sets, capabilities = _tables_status(cursor)
        if source_sets is None and capabilities is None:
            return {
                "status": "FIRST_OBSERVATION",
                "previous": None,
                "reason": "正式source_set关系尚不存在；候选和Raw行不计为accepted-state",
                "database_write": False,
            }
        if source_sets is None or capabilities is None:
            return {
                "status": "BLOCKED",
                "previous": None,
                "reason": "正式source_set主表和能力表未成对存在",
                "reason_code": "FORMAL_SOURCE_SET_RELATIONS_INCOMPLETE",
                "database_write": False,
            }
        columns = ", ".join(_SOURCE_SET_SELECT)
        cursor.execute(
            f"SELECT {columns} FROM stock.source_sets WHERE namespace_id=%s::uuid "
            "AND target_session=%s ORDER BY seal_status, revision_id, package_sha256, ingress_attempt_id",
            (namespace_id, target_session),
        )
        same_rows = [
            _as_cursor_record(cursor, raw, _SOURCE_SET_COLUMNS)
            for raw in cursor.fetchall()
        ]
        same_pending = [row for row in same_rows if row.get("seal_status") == "PENDING"]
        same_sealed = [row for row in same_rows if row.get("seal_status") == "SEALED"]
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM stock.source_sets WHERE namespace_id=%s::uuid "
            "AND target_session>%s AND seal_status='SEALED'), "
            "EXISTS (SELECT 1 FROM stock.source_sets p WHERE p.namespace_id=%s::uuid "
            "AND p.seal_status='PENDING' AND NOT EXISTS (SELECT 1 FROM stock.source_sets s "
            "WHERE s.namespace_id=p.namespace_id AND s.target_session=p.target_session "
            "AND s.package_sha256=p.package_sha256 AND s.source_digest=p.source_digest "
            "AND s.seal_status='SEALED'))",
            (namespace_id, target_session, namespace_id),
        )
        future_and_any_pending = cursor.fetchone()
        if (
            not isinstance(future_and_any_pending, (tuple, list))
            or len(future_and_any_pending) != 2
        ):
            raise SourceSetIngressError(
                "ACCEPTED_SOURCE_STATE_SHAPE_INVALID",
                "accepted-state未来与PENDING检查返回值非法",
            )
        has_future = bool(future_and_any_pending[0])
        has_active_pending = bool(future_and_any_pending[1])
        cursor.execute(
            f"SELECT {columns} FROM stock.source_sets WHERE namespace_id=%s::uuid "
            "AND target_session<%s AND seal_status='SEALED' "
            "ORDER BY target_session DESC, revision_id DESC, source_set_id DESC LIMIT 2",
            (namespace_id, target_session),
        )
        prior_rows = cursor.fetchall()
        prior_records = [
            _as_cursor_record(cursor, raw, _SOURCE_SET_COLUMNS) for raw in prior_rows
        ]

        def validate_accepted(accepted_row: Mapping[str, Any]) -> dict[str, Any]:
            accepted_row = _validate_source_set(
                accepted_row,
                namespace_id=str(namespace_id),
            )
            p02, _ack = _read_capability(
                cursor, accepted_row, namespace_id=str(namespace_id), config=config
            )
            projection = _accepted_projection(str(namespace_id), accepted_row)
            projection["p02_capability"] = p02
            return projection

        prior = validate_accepted(prior_records[0]) if prior_records else None
        if len(prior_records) > 1:
            earlier = prior_records[1]
            if (
                earlier["target_session"] == prior["target_session"]
                and earlier["revision_id"] == prior["revision_id"]
            ):
                raise SourceSetIngressError(
                    "ACCEPTED_SOURCE_STATE_AMBIGUOUS",
                    "前态同日同revision存在多条已封口source_set",
                )
        if same_pending:
            if len(same_pending) != 1:
                return {
                    "status": "BLOCKED",
                    "previous": prior,
                    "reason": "目标日存在多条未封口PENDING记录，无法唯一恢复",
                    "reason_code": "MULTIPLE_TARGET_PENDING_SOURCE_SETS",
                    "database_write": False,
                }
            pending = same_pending[0]
            resolution, exact_sealed = _same_day_pending_sealed_resolution(
                pending=pending,
                sealed_rows=same_sealed,
                package_sha256=package_sha256,
                source_digest=source_digest,
            )
            if resolution == "INPUT_MISMATCH":
                return {
                    "status": "BLOCKED",
                    "previous": prior,
                    "pending": {
                        "ingress_attempt_id": pending["ingress_attempt_id"],
                        "target_session": pending["target_session"],
                        "package_sha256": pending["package_sha256"],
                        "source_digest": pending["source_digest"],
                    },
                    "same_day_sealed_conflict": True,
                    "reason": "目标日精确SEALED已存在，但并存PENDING绑定不同输入；保留两者并阻止隐式幂等恢复",
                    "reason_code": "SAME_DAY_PENDING_SEALED_INPUT_MISMATCH",
                    "database_write": False,
                }
            if resolution == "EXACT_NOOP":
                if exact_sealed is None:
                    raise SourceSetIngressError(
                        "ACCEPTED_SOURCE_STATE_RESOLUTION_INVALID",
                        "SEALED与PENDING判定结果缺少精确封口行",
                    )
                accepted_row = _validate_source_set(
                    exact_sealed, namespace_id=str(namespace_id)
                )
                p02, _ack = _read_capability(
                    cursor,
                    accepted_row,
                    namespace_id=str(namespace_id),
                    config=config,
                )
                accepted = _accepted_projection(str(namespace_id), accepted_row)
                accepted["p02_capability"] = p02
                return {
                    "status": "NOOP",
                    "previous": accepted,
                    "plan_status": "NOOP",
                    "same_day_pending_superseded_by_exact_sealed": True,
                    "superseded_pending_attempt_id": pending["ingress_attempt_id"],
                    "reason": "同日存在精确SEALED且候选摘要相同；以已封口来源为准，只读恢复为NOOP并保留未改动PENDING",
                    "database_write": False,
                }
            if has_future:
                return {
                    "status": "RANGE_REPAIR_REQUIRED",
                    "previous": prior,
                    "pending": {
                        "ingress_attempt_id": pending["ingress_attempt_id"],
                        "target_session": pending["target_session"],
                        "package_sha256": pending["package_sha256"],
                        "source_digest": pending["source_digest"],
                    },
                    "reason": "目标日存在PENDING且namespace已有未来SEALED来源；必须先走显式历史修复",
                    "reason_code": "FUTURE_ACCEPTED_WITH_TARGET_PENDING",
                    "database_write": False,
                }
            same_sealed_conflict = bool(same_sealed)
            return {
                "status": "RESUME_REQUIRED",
                "previous": prior,
                "pending": {
                    "ingress_attempt_id": pending["ingress_attempt_id"],
                    "target_session": pending["target_session"],
                    "package_sha256": pending["package_sha256"],
                    "source_digest": pending["source_digest"],
                    "plan_sha256": pending["plan_sha256"],
                    "revision_id": pending["revision_id"],
                    "durability_status": pending["durability_status"],
                },
                "same_day_sealed_conflict": same_sealed_conflict,
                "plan_status": "ACCEPTED" if prior is not None else "FIRST_OBSERVATION",
                "reason": "目标日存在精确PENDING尝试；仅允许同一输入有界恢复，不允许普通APPEND或覆盖",
                "reason_code": "TARGET_PENDING_REQUIRES_EXACT_RESUME",
                "database_write": False,
            }
        if same_sealed:
            if len(same_sealed) != 1:
                return {
                    "status": "RANGE_REPAIR_REQUIRED",
                    "previous": None,
                    "reason": "目标日存在多个已封口修订，必须显式选择合法修复版本",
                    "reason_code": "SAME_DAY_SEALED_REVISION_AMBIGUOUS",
                    "database_write": False,
                }
            same = _validate_source_set(same_sealed[0], namespace_id=str(namespace_id))
            p02, _ack = _read_capability(
                cursor, same, namespace_id=str(namespace_id), config=config
            )
            accepted = _accepted_projection(str(namespace_id), same)
            accepted["p02_capability"] = p02
            exact = (
                package_sha256 is not None
                and source_digest is not None
                and same["package_sha256"] == package_sha256
                and same["source_digest"] == source_digest
            )
            if exact:
                return {
                    "status": "NOOP",
                    "previous": accepted,
                    "plan_status": "NOOP",
                    "reason": "目标日同一来源包和source_digest已精确封口",
                    "database_write": False,
                }
            return {
                "status": "RANGE_REPAIR_REQUIRED",
                "previous": accepted,
                "plan_status": "RANGE_REPAIR_REQUIRED",
                "reason": "目标日已封口，但本次包SHA或来源摘要不同；须显式追加修订",
                "reason_code": "SAME_DAY_SOURCE_REVISION_REQUIRED",
                "database_write": False,
            }
        if has_future:
            return {
                "status": "RANGE_REPAIR_REQUIRED",
                "previous": prior,
                "plan_status": "RANGE_REPAIR_REQUIRED",
                "reason": "namespace已有较晚SEALED来源；历史目标不是首次观察或普通APPEND",
                "reason_code": "FUTURE_ACCEPTED_SOURCE_SET_EXISTS",
                "database_write": False,
            }
        if has_active_pending:
            return {
                "status": "BLOCKED",
                "previous": prior,
                "reason": "当前namespace另有未封口PENDING来源；需先完成其精确恢复",
                "reason_code": "UNRELATED_PENDING_SOURCE_SET_EXISTS",
                "database_write": False,
            }
        if prior is not None:
            return {
                "status": "APPEND",
                "previous": prior,
                "plan_status": "ACCEPTED",
                "reason": "目标日之前已有最近SEALED正式来源，允许按同一前态APPEND",
                "database_write": False,
            }
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM stock.source_sets WHERE namespace_id=%s::uuid)",
            (namespace_id,),
        )
        any_rows = bool(cursor.fetchone()[0])
        if any_rows:
            return {
                "status": "BLOCKED",
                "previous": None,
                "reason": "正式来源表存在其它目标日期的PENDING或无法作为首次前态的记录",
                "reason_code": "UNRELATED_UNSEALED_SOURCE_SET_EXISTS",
                "database_write": False,
            }
        return {
            "status": "FIRST_OBSERVATION",
            "previous": None,
            "plan_status": "FIRST_OBSERVATION",
            "reason": "当前DEV namespace无任何SEALED或PENDING正式来源集合",
            "database_write": False,
        }


def read_p04_source_input(
    config: Mapping[str, Any], *, target_session: date, package_sha256: str
) -> dict[str, Any]:
    """按namespace、目标日和包SHA读取已封口P03 typed来源集合。"""
    if type(target_session) is not date:
        raise ValueError("P04来源输入必须绑定显式目标会话")
    if not isinstance(package_sha256, str) or _SHA256.fullmatch(package_sha256) is None:
        raise ValueError("P04来源输入必须绑定完整小写SHA-256")
    with source_raw._transaction(config, write=False) as (
        _connection,
        cursor,
        namespace_id,
    ):
        source_sets, capabilities = _tables_status(cursor)
        if source_sets is None or capabilities is None:
            return {
                "status": "P04_INGRESS_READER_NOT_CONNECTED",
                "target_session": target_session.isoformat(),
                "package_sha256": package_sha256,
                "namespace_id": str(namespace_id),
                "database_write": False,
                "ddl_executed": False,
                "candidate_fallback": False,
                "reason_code": "FORMAL_SOURCE_SET_RELATIONS_NOT_PRESENT",
            }
        columns = ", ".join(_SOURCE_SET_SELECT)
        cursor.execute(
            f"SELECT {columns} FROM stock.source_sets "
            "WHERE namespace_id=%s::uuid AND target_session=%s "
            "AND package_sha256=%s AND seal_status='SEALED' "
            "ORDER BY revision_id DESC, source_set_id DESC LIMIT 2",
            (namespace_id, target_session, package_sha256),
        )
        raw_rows = cursor.fetchall()
        if not raw_rows:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM stock.source_sets "
                "WHERE namespace_id=%s::uuid AND target_session=%s "
                "AND package_sha256=%s)",
                (namespace_id, target_session, package_sha256),
            )
            has_unsealed_match = bool(cursor.fetchone()[0])
            if has_unsealed_match:
                return {
                    "status": "BLOCKED",
                    "target_session": target_session.isoformat(),
                    "package_sha256": package_sha256,
                    "namespace_id": str(namespace_id),
                    "database_write": False,
                    "ddl_executed": False,
                    "candidate_fallback": False,
                    "reason_code": "EXACT_SOURCE_SET_EXISTS_BUT_IS_NOT_SEALED",
                }
            return {
                "status": "NO_MATCH",
                "target_session": target_session.isoformat(),
                "package_sha256": package_sha256,
                "namespace_id": str(namespace_id),
                "database_write": False,
                "ddl_executed": False,
                "candidate_fallback": False,
                "reason_code": "NO_EXACT_SEALED_SOURCE_SET",
            }
        row = _validate_source_set(
            _as_cursor_record(cursor, raw_rows[0], _SOURCE_SET_COLUMNS),
            namespace_id=str(namespace_id),
        )
        if (
            row["target_session"] != target_session
            or row["package_sha256"] != package_sha256
        ):
            raise SourceSetIngressError(
                "P04_SOURCE_SET_KEY_MISMATCH", "P04读回关系与显式日期或包SHA不一致"
            )
        if len(raw_rows) > 1:
            second = _as_cursor_record(cursor, raw_rows[1], _SOURCE_SET_COLUMNS)
            if second.get("revision_id") == row["revision_id"]:
                raise SourceSetIngressError(
                    "P04_SOURCE_SET_AMBIGUOUS", "精确日期和SHA存在相同revision冲突"
                )
        capability, ack = _read_capability(
            cursor, row, namespace_id=str(namespace_id), config=config
        )
        raw_revalidation = _validate_source_references(cursor, row)
    return {
        "status": "AVAILABLE",
        "target_session": target_session.isoformat(),
        "package_sha256": package_sha256,
        "namespace_id": str(namespace_id),
        "source_set_id": row["source_set_id"],
        "revision_id": row["revision_id"],
        "source_content_id": row["source_content_id"],
        "source_capture_id": row["source_capture_id"],
        "source_digest": row["source_digest"],
        "source_raw_digest": row["source_raw_digest"],
        "source_dataset_id": row["source_dataset_id"],
        "source_byte_count": row["source_byte_count"],
        "parser_contract": row["parser_contract"],
        "source_semantic_revision": row["source_semantic_revision"],
        "raw_row_count": row["raw_row_count"],
        "capture_expected_row_count": row["capture_expected_row_count"],
        "target_present_count": row["target_present_count"],
        "target_missing_count": row["target_missing_count"],
        "read_failed_count": row["read_failed_count"],
        "source_available_at": (
            row["source_available_at"].isoformat()
            if row["source_available_at"] is not None
            else None
        ),
        "source_time_quality": row["source_time_quality"],
        "source_clock_role": row["source_clock_role"],
        "capture_read_started_at": row["capture_read_started_at"].isoformat(),
        "capture_validated_at": row["capture_validated_at"].isoformat(),
        "price_unit_status": row["price_unit_status"],
        "amount_unit_status": row["amount_unit_status"],
        "volume_unit_status": row["volume_unit_status"],
        "seal_status": row["seal_status"],
        "seal_digest_sha256": row["seal_digest_sha256"],
        "seal_writer_transaction_id": row["seal_writer_transaction_id"],
        "sealed_at": row["sealed_at"].isoformat(),
        "durability_status": row["durability_status"],
        "commit_ack_observed_at": row["commit_ack_observed_at"].isoformat(),
        "durable_at": row["durable_at"].isoformat(),
        "calendar_id": row["calendar_id"],
        "calendar_revision": row["calendar_revision"],
        "calendar_sha256": row["calendar_sha256"],
        "scope_id": row["scope_id"],
        "scope_contract_version": row["scope_contract_version"],
        "scope_contract_digest": row["scope_contract_digest"],
        "scope_catalog_digest": row["scope_catalog_digest"],
        "business_input_digest": row["business_input_digest"],
        "plan_sha256": row["plan_sha256"],
        "contract_sha256": row["contract_sha256"],
        "head_generation": row["head_generation"],
        "p02_capability": capability,
        "commit_ack": {
            "status": ack["status"],
            "writer_transaction_id": ack["writer_transaction_id"],
            "observer_transaction_id": ack["observer_transaction_id"],
            "pending_row_xmin": ack["pending_row_xmin"],
            "pending_creation_transaction_id": ack["pending_creation_transaction_id"],
            "commit_ack_observed_at": ack["commit_ack_observed_at"].isoformat(),
            "durable_at": ack["durable_at"].isoformat(),
            "capability_digest": ack["capability_digest"],
        },
        "raw_revalidation": raw_revalidation,
        "identity_digest_revalidated": True,
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "stable_security_identity": "P04_IDENTITY_RESOLUTION_PENDING",
        "database_write": False,
        "ddl_executed": False,
        "candidate_fallback": False,
    }


def inspect_p04_source_input(
    config: Mapping[str, Any], *, target_session: date, package_sha256: str
) -> dict[str, Any]:
    """兼容只读准备检查入口；结果来自同一个精确P04 typed reader。"""
    return read_p04_source_input(
        config, target_session=target_session, package_sha256=package_sha256
    )
