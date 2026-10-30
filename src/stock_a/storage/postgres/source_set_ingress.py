"""正式来源集合的精确读者与提交后确认合同。"""

from __future__ import annotations

from datetime import date, datetime, timezone
import re
from dataclasses import dataclass
from typing import Any, Mapping

from . import source_raw


_SHA256 = re.compile(r"^[0-9a-f]{64}$")

# 这是R01正式source_set关系的最小读取投影。没有这些typed列时失败关闭；
# 正式表尚未由获准迁移创建时，只能报告首次观察或读者不可用。
_SOURCE_SET_COLUMNS = (
    "namespace_id::text",
    "target_session",
    "package_sha256",
    "source_set_id::text",
    "revision_id",
    "source_content_id",
    "source_capture_id::text",
    "source_digest",
    "seal_status",
    "sealed_at",
    "durability_status",
    "commit_ack_observed_at",
    "durable_at",
    "durability_source_capture_id::text",
    "durability_source_digest",
    "calendar_id",
    "calendar_revision",
    "scope_id",
    "scope_contract_version",
    "scope_contract_digest",
    "scope_catalog_digest",
    "source_semantic_revision",
    "business_input_digest",
    "head_generation",
    "plan_sha256",
)
_CAPABILITY_COLUMNS = (
    "status",
    "plan_sha256",
    "policy_sha256",
    "actual_logins",
    "actual_queries",
    "network_accessed",
)
_CAPABILITY_ID = "BAOSTOCK_SUPPLEMENT"


class SourceSetIngressError(ValueError):
    """正式来源集合读回或提交证据不符合typed合同。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


class PostCommitEvidenceError(ValueError):
    """提交后读回证据不完整、错键或时序不成立。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


_COMMIT_RECEIPT_SEAL = object()


@dataclass(frozen=True)
class _CommitAckReceipt:
    """正式P03写事务的内部回执类型；当前没有P03写入器可构造此回执。"""

    identity: Mapping[str, Any]
    writer_transaction_id: str
    ack_observed_at: datetime
    _seal: object


def _as_record(row: Any, columns: tuple[str, ...]) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    try:
        return dict(zip(columns, row, strict=True))
    except (TypeError, ValueError) as error:
        raise SourceSetIngressError(
            "SOURCE_SET_ROW_SHAPE_INVALID", "正式来源集合读回列数不符合合同"
        ) from error


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
        "durability_source_digest",
        "business_input_digest",
        "plan_sha256",
        "scope_contract_digest",
        "scope_catalog_digest",
    ):
        _required_sha(row.get(key), key)
    for key in (
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
    if type(row.get("revision_id")) is not int or row["revision_id"] < 0:
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
    if sealed_at > ack_at or ack_at > durable_at:
        raise SourceSetIngressError(
            "SOURCE_SET_DURABILITY_ORDER_INVALID",
            "封口、提交确认和提交后读回时钟顺序不成立",
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
    return row


def _read_capability(
    cursor: Any, row: Mapping[str, Any], *, namespace_id: str
) -> dict[str, Any]:
    cursor.execute(
        f"SELECT {', '.join(_CAPABILITY_COLUMNS)} "
        "FROM stock.source_set_capabilities "
        "WHERE namespace_id=%s::uuid AND target_session=%s "
        "AND source_set_id=%s::uuid AND capability_id=%s",
        (
            namespace_id,
            row["target_session"],
            row["source_set_id"],
            _CAPABILITY_ID,
        ),
    )
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise SourceSetIngressError(
            "SOURCE_SET_P02_BINDING_MISSING",
            "正式来源集合必须有且只有一条精确P02能力关系",
        )
    capability = _as_record(rows[0], _CAPABILITY_COLUMNS)
    if (
        capability.get("plan_sha256") != row["plan_sha256"]
        or _SHA256.fullmatch(str(capability.get("policy_sha256", ""))) is None
        or capability.get("status") not in {"NOT_REQUESTED", "AVAILABLE", "DEGRADED"}
        or type(capability.get("actual_logins")) is not int
        or capability["actual_logins"] < 0
        or type(capability.get("actual_queries")) is not int
        or capability["actual_queries"] < 0
        or type(capability.get("network_accessed")) is not bool
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_P02_BINDING_INVALID",
            "P02能力状态、请求计数或P00/P02摘要绑定不合法",
        )
    if capability["status"] == "NOT_REQUESTED" and (
        capability["actual_logins"] != 0
        or capability["actual_queries"] != 0
        or capability["network_accessed"]
    ):
        raise SourceSetIngressError(
            "SOURCE_SET_P02_ZERO_REQUEST_INVALID",
            "NOT_REQUESTED必须保持零登录、零查询且未访问网络",
        )
    return capability


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


def read_accepted_source_state(
    config: Mapping[str, Any], *, target_session: date
) -> dict[str, Any]:
    """读取当前DEV namespace内最新已封口P03关系；候选永不冒充accepted-state。"""
    if type(target_session) is not date:
        raise ValueError("accepted-state查询必须绑定显式目标会话")
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
        columns = ", ".join(_SOURCE_SET_COLUMNS)
        cursor.execute(
            f"SELECT {columns} FROM stock.source_sets "
            "WHERE namespace_id=%s::uuid AND seal_status='SEALED' "
            "ORDER BY target_session DESC, revision_id DESC, source_set_id DESC LIMIT 2",
            (namespace_id,),
        )
        raw_rows = cursor.fetchall()
        if not raw_rows:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM stock.source_sets "
                "WHERE namespace_id=%s::uuid)",
                (namespace_id,),
            )
            has_unsealed_rows = bool(cursor.fetchone()[0])
            if has_unsealed_rows:
                return {
                    "status": "BLOCKED",
                    "previous": None,
                    "reason": "当前namespace存在来源集合行，但没有可验证的SEALED前态",
                    "reason_code": "UNSEALED_SOURCE_SET_CANNOT_MEAN_FIRST_OBSERVATION",
                    "database_write": False,
                }
            return {
                "status": "FIRST_OBSERVATION",
                "previous": None,
                "reason": "当前DEV namespace没有已封口的正式source_set；非封口行不作前态",
                "database_write": False,
            }
        row = _validate_source_set(
            _as_record(raw_rows[0], _SOURCE_SET_COLUMNS), namespace_id=str(namespace_id)
        )
        if len(raw_rows) > 1:
            second = _as_record(raw_rows[1], _SOURCE_SET_COLUMNS)
            if (
                second.get("target_session") == row["target_session"]
                and second.get("revision_id") == row["revision_id"]
            ):
                raise SourceSetIngressError(
                    "ACCEPTED_SOURCE_STATE_AMBIGUOUS",
                    "同一交易日和revision存在多个正式source_set，拒绝猜选前态",
                )
        capability = _read_capability(cursor, row, namespace_id=str(namespace_id))
        accepted = _accepted_projection(str(namespace_id), row)
        accepted["p02_capability"] = capability
        return {
            "status": "ACCEPTED",
            "previous": accepted,
            "reason": "从当前namespace按日期、封口、revision和source_set精确读回",
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
        columns = ", ".join(_SOURCE_SET_COLUMNS)
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
            _as_record(raw_rows[0], _SOURCE_SET_COLUMNS), namespace_id=str(namespace_id)
        )
        if (
            row["target_session"] != target_session
            or row["package_sha256"] != package_sha256
        ):
            raise SourceSetIngressError(
                "P04_SOURCE_SET_KEY_MISMATCH", "P04读回关系与显式日期或包SHA不一致"
            )
        if len(raw_rows) > 1:
            second = _as_record(raw_rows[1], _SOURCE_SET_COLUMNS)
            if second.get("revision_id") == row["revision_id"]:
                raise SourceSetIngressError(
                    "P04_SOURCE_SET_AMBIGUOUS", "精确日期和SHA存在相同revision冲突"
                )
        capability = _read_capability(cursor, row, namespace_id=str(namespace_id))
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
        "seal_status": row["seal_status"],
        "durability_status": row["durability_status"],
        "commit_ack_observed_at": row["commit_ack_observed_at"].isoformat(),
        "durable_at": row["durable_at"].isoformat(),
        "calendar_id": row["calendar_id"],
        "calendar_revision": row["calendar_revision"],
        "scope_id": row["scope_id"],
        "plan_sha256": row["plan_sha256"],
        "p02_capability": capability,
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


def _identity_value(value: Any) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)


def verify_post_commit_readback(
    *,
    expected_identity: Mapping[str, Any],
    commit_ack: _CommitAckReceipt,
    observed_row: Mapping[str, Any],
    post_commit_observed_at: datetime,
    observer_transaction_id: str,
) -> dict[str, Any]:
    """仅在COMMIT返回成功且另一事务读回完全匹配后形成durable_at证据。

    durable_at是提交后独立读回的观察时钟，不是PG实际commit timestamp；普通GET查询时钟
    不参与证明。正式P03落库后需将此有界typed ACK写入0006的能力关系。
    """
    identity_keys = (
        "namespace_id",
        "target_session",
        "source_set_id",
        "revision_id",
        "package_sha256",
        "source_capture_id",
        "source_digest",
    )
    if not isinstance(expected_identity, Mapping) or any(
        key not in expected_identity for key in identity_keys
    ):
        raise PostCommitEvidenceError(
            "COMMIT_ACK_EXPECTED_IDENTITY_INCOMPLETE", "提交确认缺少精确来源集合业务键"
        )
    if (
        not isinstance(commit_ack, _CommitAckReceipt)
        or commit_ack._seal is not _COMMIT_RECEIPT_SEAL
        or not isinstance(commit_ack.writer_transaction_id, str)
        or not commit_ack.writer_transaction_id.isdecimal()
        or int(commit_ack.writer_transaction_id) <= 0
        or not isinstance(commit_ack.identity, Mapping)
    ):
        raise PostCommitEvidenceError(
            "COMMIT_ACK_NOT_CONFIRMED",
            "普通映射或未由正式写事务边界产生的对象不能作为COMMIT确认",
        )
    if (
        not isinstance(observer_transaction_id, str)
        or not observer_transaction_id
        or observer_transaction_id == commit_ack.writer_transaction_id
    ):
        raise PostCommitEvidenceError(
            "COMMIT_ACK_OBSERVER_NOT_INDEPENDENT",
            "确认读回必须来自与写事务不同的数据库事务",
        )
    for source, label in (
        (expected_identity, "预期键"),
        (commit_ack.identity, "提交确认键"),
        (observed_row, "提交后读回键"),
    ):
        if not isinstance(source, Mapping):
            raise PostCommitEvidenceError(
                "COMMIT_ACK_READBACK_INVALID", f"{label}不是typed映射"
            )
        for key in identity_keys:
            if key not in source or _identity_value(source[key]) != _identity_value(
                expected_identity[key]
            ):
                raise PostCommitEvidenceError(
                    "COMMIT_ACK_BUSINESS_KEY_MISMATCH",
                    f"{label}的{key}与精确预期业务键不一致",
                )
    ack_at = commit_ack.ack_observed_at
    if (
        not isinstance(ack_at, datetime)
        or ack_at.tzinfo is None
        or ack_at.utcoffset() is None
        or not isinstance(post_commit_observed_at, datetime)
        or post_commit_observed_at.tzinfo is None
        or post_commit_observed_at.utcoffset() is None
        or post_commit_observed_at < ack_at
    ):
        raise PostCommitEvidenceError(
            "COMMIT_ACK_CLOCK_ORDER_INVALID",
            "提交确认与提交后观察时钟缺失、无时区或顺序不成立",
        )
    return {
        "status": "VERIFIED_POST_COMMIT_READBACK",
        "method": "COMMIT_ACK_PLUS_SEPARATE_TRANSACTION_EXACT_READBACK",
        "namespace_id": str(expected_identity["namespace_id"]),
        "target_session": _identity_value(expected_identity["target_session"]),
        "source_set_id": str(expected_identity["source_set_id"]),
        "revision_id": expected_identity["revision_id"],
        "package_sha256": expected_identity["package_sha256"],
        "source_capture_id": str(expected_identity["source_capture_id"]),
        "source_digest": expected_identity["source_digest"],
        "writer_transaction_id": commit_ack.writer_transaction_id,
        "observer_transaction_id": observer_transaction_id,
        "server_commit_at": None,
        "commit_ack_observed_at": ack_at.isoformat(),
        "post_commit_observed_at": post_commit_observed_at.isoformat(),
        "durable_at": post_commit_observed_at.isoformat(),
        "ordinary_query_observed_at": None,
        "persistence_status": "REQUIRES_TYPED_0006_ACK_ROW",
    }


def confirm_source_set_post_commit(
    config: Mapping[str, Any],
    *,
    expected_identity: Mapping[str, Any],
    commit_ack: _CommitAckReceipt,
) -> dict[str, Any]:
    """用新的只读PG事务按完整source_set键确认前一写事务已提交。"""
    required = (
        "namespace_id",
        "target_session",
        "source_set_id",
        "revision_id",
        "package_sha256",
        "source_capture_id",
        "source_digest",
    )
    if not isinstance(expected_identity, Mapping) or any(
        key not in expected_identity for key in required
    ):
        raise PostCommitEvidenceError(
            "COMMIT_ACK_EXPECTED_IDENTITY_INCOMPLETE", "提交确认缺少精确来源集合业务键"
        )
    if (
        not isinstance(commit_ack, _CommitAckReceipt)
        or commit_ack._seal is not _COMMIT_RECEIPT_SEAL
    ):
        raise PostCommitEvidenceError(
            "COMMIT_ACK_NOT_CONFIRMED",
            "普通映射或未由正式写事务边界产生的对象不能作为COMMIT确认",
        )
    with source_raw._transaction(config, write=False) as (
        connection,
        cursor,
        namespace_id,
    ):
        if str(namespace_id) != str(expected_identity["namespace_id"]):
            raise PostCommitEvidenceError(
                "COMMIT_ACK_NAMESPACE_MISMATCH", "提交后读回连接落在不同namespace"
            )
        cursor.execute(
            """SELECT namespace_id::text, target_session, source_set_id::text,
                      revision_id, package_sha256, source_capture_id::text, source_digest,
                      xmin::text, age(xmin), txid_current()::text
               FROM stock.source_sets
               WHERE namespace_id=%s::uuid AND target_session=%s
                 AND source_set_id=%s::uuid AND revision_id=%s
                 AND package_sha256=%s AND source_capture_id=%s::uuid
                 AND source_digest=%s""",
            tuple(expected_identity[key] for key in required),
        )
        row = cursor.fetchone()
        if row is None:
            raise PostCommitEvidenceError(
                "SOURCE_SET_COMMITTED_ROW_NOT_FOUND",
                "新事务没有读回精确提交的来源集合主行",
            )
        try:
            writer_txid = int(commit_ack.writer_transaction_id)
            row_xmin = int(row[7])
            xmin_age = int(row[8])
            observer_txid = int(row[9])
        except (TypeError, ValueError) as error:
            raise PostCommitEvidenceError(
                "COMMIT_ACK_TRANSACTION_ID_INVALID",
                "PG事务标识或来源行xmin不是有效十进制值",
            ) from error
        if (
            row_xmin != writer_txid % (2**32)
            or xmin_age < 0
            or xmin_age >= 2**31
            or observer_txid - writer_txid != xmin_age
        ):
            raise PostCommitEvidenceError(
                "SOURCE_SET_WRITER_TRANSACTION_MISMATCH",
                "来源集合行的实际创建事务与提交确认事务不一致",
            )
        observed = _as_record(
            row,
            (
                "namespace_id",
                "target_session",
                "source_set_id",
                "revision_id",
                "package_sha256",
                "source_capture_id",
                "source_digest",
            ),
        )
        observer_transaction_id = str(observer_txid)
        observed_at = datetime.now(timezone.utc)
    return verify_post_commit_readback(
        expected_identity=expected_identity,
        commit_ack=commit_ack,
        observed_row=observed,
        post_commit_observed_at=observed_at,
        observer_transaction_id=observer_transaction_id,
    )
