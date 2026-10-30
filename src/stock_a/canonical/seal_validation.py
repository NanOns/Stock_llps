"""P20来源封口纯验证内核；不认证外部签发，也不写库或批准发布。"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from hashlib import sha256
import json
import math
import unicodedata

from stock_a.domain import ContractError


class SourceSealValidationError(ContractError):
    """来源封口预检失败，附稳定错误码。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class SealProofResult:
    """纯验证的局部结论；PASS只说明显式输入间可复算一致。"""

    status: str
    digest: str | None = None
    errors: tuple[str, ...] = ()
    replayability: str = "NOT_APPLICABLE"
    proof_digests: tuple[tuple[str, str], ...] = ()
    value: str | None = None


def _fail(code: str, message: str) -> None:
    raise SourceSealValidationError(code, message)


def _canonical_value(value: object) -> object:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            _fail("SOURCE_SEAL_CANONICAL_VALUE_INVALID", "规范摘要禁止浮点数")
        _fail("SOURCE_SEAL_CANONICAL_VALUE_INVALID", "精确数值须以十进制定点字符串表达")
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, (date, datetime)):
        _fail("SOURCE_SEAL_CANONICAL_VALUE_INVALID", "日期和时钟须先按封口合同显式编码")
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                _fail(
                    "SOURCE_SEAL_CANONICAL_VALUE_INVALID", "规范摘要对象键必须为字符串"
                )
            normalized_key = unicodedata.normalize("NFC", key)
            if normalized_key in normalized:
                _fail(
                    "SOURCE_SEAL_CANONICAL_DUPLICATE_KEY",
                    "Unicode规范化后出现重复摘要键",
                )
            normalized[normalized_key] = _canonical_value(item)
        return {key: normalized[key] for key in sorted(normalized)}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    _fail("SOURCE_SEAL_CANONICAL_VALUE_INVALID", "规范摘要含不支持的值类型")


def canonical_json_bytes(schema_id: str, payload: object) -> bytes:
    """按SA_SEAL_CANONICAL_JSON_V1编码；不负责证明调用方输入真实。"""
    if not isinstance(schema_id, str) or not schema_id or "\n" in schema_id:
        _fail("SOURCE_SEAL_SCHEMA_INVALID", "封口摘要schema_id缺失或非法")
    normalized_payload = _canonical_value(payload)
    try:
        encoded = json.dumps(
            normalized_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        _fail("SOURCE_SEAL_CANONICAL_VALUE_INVALID", "封口摘要无法按V1编码")
    return f"STOCK_A_SEAL|{schema_id}|V1\n".encode("utf-8") + encoded


def canonical_decimal(value: str) -> str:
    """将十进制定点文本规范化；输入不得经过二进制浮点。"""
    if not isinstance(value, str) or not value or "e" in value.lower():
        _fail("SOURCE_SEAL_DECIMAL_INVALID", "精确数值必须是无指数十进制定点字符串")
    try:
        number = Decimal(value)
    except InvalidOperation:
        _fail("SOURCE_SEAL_DECIMAL_INVALID", "精确数值不是有效十进制")
    if not number.is_finite():
        _fail("SOURCE_SEAL_DECIMAL_INVALID", "精确数值不得为NaN或Infinity")
    if number == 0:
        return "0"
    normalized = format(number.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized


def canonical_date(value: date) -> str:
    if isinstance(value, datetime) or not isinstance(value, date):
        _fail("SOURCE_SEAL_DATE_INVALID", "业务日期必须是date")
    return value.isoformat()


def canonical_clock(value: object) -> str:
    normalized = normalize_observed_clock(value)
    return normalized.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def semantic_digest(schema_id: str, payload: object) -> str:
    return sha256(canonical_json_bytes(schema_id, payload)).hexdigest()


def _record_key(
    row: Mapping[str, object], key_fields: tuple[str, ...]
) -> tuple[bytes, ...]:
    if any(field not in row for field in key_fields):
        _fail("SOURCE_SEAL_RECORD_KEY_MISSING", "封口记录缺少业务排序键")
    # 复合键按字段顺序逐字段采用UTF-8字节字典序；tuple比较保留字段边界，
    # 不使用长度前缀改变字典序。
    parts: list[bytes] = []
    for field in key_fields:
        value = row[field]
        if not isinstance(value, str) or not value:
            _fail(
                "SOURCE_SEAL_RECORD_KEY_INVALID", "封口记录业务排序键必须是非空字符串"
            )
        parts.append(unicodedata.normalize("NFC", value).encode("utf-8"))
    return tuple(parts)


def records_digest(
    schema_id: str,
    rows: Sequence[Mapping[str, object]],
    key_fields: tuple[str, ...],
) -> str:
    """对按显式业务复合键排序的行集求摘要，并拒绝重复键。"""
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        _fail("SOURCE_SEAL_RECORDS_INVALID", "封口记录集合类型非法")
    if not key_fields or len(set(key_fields)) != len(key_fields):
        _fail("SOURCE_SEAL_SCHEMA_INVALID", "封口记录业务键缺失或重复")
    keyed: list[tuple[tuple[bytes, ...], Mapping[str, object]]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            _fail("SOURCE_SEAL_RECORDS_INVALID", "封口记录必须是字段映射")
        keyed.append((_record_key(row, key_fields), row))
    keyed.sort(key=lambda item: item[0])
    for index in range(1, len(keyed)):
        if keyed[index - 1][0] == keyed[index][0]:
            _fail("SOURCE_SEAL_DUPLICATE_BUSINESS_KEY", "封口记录集合含重复业务键")
    return semantic_digest(schema_id, [row for _, row in keyed])


def inventory_digest(inventory: Mapping[str, object]) -> str:
    """按V1摘要绑定身份、声明范围、枚举和实际条目。"""
    entries = inventory.get("entries")
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        _fail("SOURCE_SEAL_INVENTORY_INCOMPLETE", "来源清单条目集合非法")
    entries_digest = records_digest(
        "SOURCE_INVENTORY_ENTRIES_V1", entries, ("source_key",)
    )
    identity = {
        key: inventory[key]
        for key in (
            "dataset_id",
            "dataset_version",
            "source_role",
            "source_revision",
            "query_scope",
            "parser_version",
            "unit_version",
            "capture_id",
            "source_content_sha256",
        )
    }
    return semantic_digest(
        "SOURCE_INVENTORY_V1",
        {
            "identity": identity,
            "entries_digest": entries_digest,
            "required_directories": sorted(
                inventory["required_directories"],
                key=lambda value: value.encode("utf-8"),
            ),
            "completed_directories": sorted(
                inventory["completed_directories"],
                key=lambda value: value.encode("utf-8"),
            ),
            "failures": list(inventory["failures"]),
            "enumeration_started_at": canonical_clock(
                inventory["enumeration_started_at"]
            ),
            "enumeration_completed_at": canonical_clock(
                inventory["enumeration_completed_at"]
            ),
            "budget": inventory["budget"],
        },
    )


def _require_sha256(value: object, code: str) -> str:
    if not isinstance(value, str) or len(value) != 64:
        _fail(code, "封口摘要必须为64位小写SHA-256")
    if value != value.lower():
        _fail(code, "封口摘要必须为64位小写SHA-256")
    if any(char not in "0123456789abcdef" for char in value):
        _fail(code, "封口摘要必须为64位小写SHA-256")
    return value


def validate_source_inventory(
    inventory: Mapping[str, object],
    expected_entities: Sequence[Mapping[str, object]] | None,
    actual_entities: Sequence[Mapping[str, object]],
) -> SealProofResult:
    """复算声明范围的来源清单与P04实体/会话覆盖，不推断全市场分母。"""
    required = (
        "dataset_id",
        "dataset_version",
        "source_role",
        "source_revision",
        "query_scope",
        "parser_version",
        "unit_version",
        "capture_id",
        "source_content_sha256",
        "entries",
        "required_directories",
        "completed_directories",
        "failures",
        "enumeration_complete",
        "budget",
        "enumeration_started_at",
        "enumeration_completed_at",
    )
    if not isinstance(inventory, Mapping) or any(
        key not in inventory for key in required
    ):
        _fail("SOURCE_SEAL_INVENTORY_INCOMPLETE", "来源清单缺少必需身份或枚举证据")
    if inventory.get("enumeration_complete") is not True:
        _fail("SOURCE_SEAL_INVENTORY_INCOMPLETE", "来源条目枚举未证明完整结束")
    if not isinstance(inventory.get("budget"), Mapping):
        _fail("SOURCE_SEAL_INVENTORY_INCOMPLETE", "来源枚举缺少预算记录")
    budget = inventory["budget"]
    budget_fields = (
        "max_entries",
        "max_bytes",
        "actual_entries",
        "actual_bytes",
        "max_duration_ms",
        "elapsed_ms",
    )
    if any(
        not isinstance(budget.get(key), int)
        or isinstance(budget.get(key), bool)
        or budget[key] < 0
        for key in budget_fields
    ):
        _fail(
            "SOURCE_SEAL_INVENTORY_INCOMPLETE", "来源枚举预算须显式记录限额与实际消耗"
        )
    if (
        budget["actual_entries"] > budget["max_entries"]
        or budget["actual_bytes"] > budget["max_bytes"]
        or budget["elapsed_ms"] > budget["max_duration_ms"]
    ):
        return SealProofResult(
            "PARTIAL_DECLARED_SCOPE", errors=("SOURCE_INVENTORY_BUDGET_EXCEEDED",)
        )
    identity_fields = (
        "dataset_id",
        "dataset_version",
        "source_role",
        "source_revision",
        "parser_version",
        "unit_version",
        "capture_id",
    )
    if any(
        not isinstance(inventory.get(key), str) or not inventory[key]
        for key in identity_fields
    ) or not isinstance(inventory.get("query_scope"), Mapping):
        _fail("SOURCE_SEAL_INVENTORY_INCOMPLETE", "来源清单身份/版本字段缺失")
    _require_sha256(
        inventory["source_content_sha256"], "SOURCE_SEAL_SOURCE_DIGEST_INVALID"
    )
    required_dirs = inventory["required_directories"]
    completed_dirs = inventory["completed_directories"]
    failures = inventory["failures"]
    entries = inventory["entries"]
    if not all(
        isinstance(value, Sequence) and not isinstance(value, (str, bytes))
        for value in (required_dirs, completed_dirs, failures, entries)
    ):
        _fail("SOURCE_SEAL_INVENTORY_INCOMPLETE", "来源枚举集合类型非法")
    if any(
        not isinstance(value, str) or not value
        for value in (*required_dirs, *completed_dirs)
    ):
        _fail("SOURCE_SEAL_INVENTORY_INCOMPLETE", "来源目录身份必须是非空字符串")
    if any(not isinstance(value, Mapping) for value in failures):
        _fail("SOURCE_SEAL_INVENTORY_INCOMPLETE", "来源枚举失败项必须是结构化证据")
    for entry in entries:
        if (
            not isinstance(entry, Mapping)
            or "byte_size" not in entry
            or not isinstance(entry["byte_size"], int)
            or isinstance(entry["byte_size"], bool)
            or entry["byte_size"] < 0
        ):
            _fail("SOURCE_SEAL_INVENTORY_INCOMPLETE", "来源清单条目缺少非负byte_size")
        _require_sha256(
            entry.get("content_sha256"), "SOURCE_SEAL_INVENTORY_DIGEST_INVALID"
        )
    if budget["actual_entries"] != len(entries) or budget["actual_bytes"] != sum(
        entry["byte_size"] for entry in entries
    ):
        _fail(
            "SOURCE_SEAL_INVENTORY_INCOMPLETE",
            "来源清单实际条目数/字节数与预算回执不一致",
        )
    if set(required_dirs) != set(completed_dirs) or failures:
        return SealProofResult(
            "PARTIAL_DECLARED_SCOPE", errors=("SOURCE_INVENTORY_INCOMPLETE",)
        )
    if normalize_observed_clock(
        inventory["enumeration_started_at"]
    ) > normalize_observed_clock(inventory["enumeration_completed_at"]):
        _fail("SOURCE_SEAL_CLOCK_ORDER_INVALID", "来源枚举结束早于开始")
    inventory_digest_value = inventory_digest(inventory)
    claimed_inventory_digest = inventory.get("inventory_digest")
    if (
        claimed_inventory_digest is not None
        and _require_sha256(
            claimed_inventory_digest, "SOURCE_SEAL_INVENTORY_DIGEST_INVALID"
        )
        != inventory_digest_value
    ):
        _fail("SOURCE_SEAL_INVENTORY_DIGEST_MISMATCH", "来源清单摘要与逐项清单不一致")
    if expected_entities is None:
        return SealProofResult(
            "COVERAGE_UNPROVEN",
            inventory_digest_value,
            ("P04_EXPECTED_ENTITY_SET_MISSING",),
        )
    expected_digest = records_digest(
        "ENTITY_SESSION_EXPECTED_V1", expected_entities, ("entity_id", "session")
    )
    actual_digest = records_digest(
        "ENTITY_SESSION_ACTUAL_V1", actual_entities, ("entity_id", "session")
    )
    expected_keys = {(row["entity_id"], row["session"]) for row in expected_entities}
    actual_keys = {(row["entity_id"], row["session"]) for row in actual_entities}
    declared_gaps = inventory.get("coverage_exceptions", ())
    if not isinstance(declared_gaps, Sequence) or isinstance(
        declared_gaps, (str, bytes)
    ):
        _fail("SOURCE_SEAL_COVERAGE_INVALID", "覆盖例外必须是有类型的集合")
    exception_keys: set[tuple[object, object]] = set()
    allowed_statuses = {
        "CONFIRMED_SUSPENDED",
        "PRELISTING",
        "KNOWN_ABSENT",
        "UNMAPPED",
        "UNKNOWN_GAP",
    }
    for row in declared_gaps:
        if (
            not isinstance(row, Mapping)
            or row.get("status") not in allowed_statuses
            or "entity_id" not in row
            or "session" not in row
        ):
            _fail("SOURCE_SEAL_COVERAGE_INVALID", "覆盖例外缺少允许状态或复合业务键")
        key = (row["entity_id"], row["session"])
        if key in exception_keys:
            _fail("SOURCE_SEAL_DUPLICATE_BUSINESS_KEY", "覆盖例外含重复业务键")
        exception_keys.add(key)
    if exception_keys & actual_keys or not exception_keys <= expected_keys:
        _fail("SOURCE_SEAL_COVERAGE_CONFLICT", "覆盖例外与实际行冲突或超出P04预期集合")
    accounted = actual_keys | exception_keys
    if accounted != expected_keys:
        return SealProofResult(
            "PARTIAL_DECLARED_SCOPE",
            inventory_digest_value,
            ("ENTITY_SESSION_COVERAGE_GAP",),
        )
    declared_coverage_digest = inventory.get("coverage_digest")
    coverage_payload = {
        "inventory_digest": inventory_digest_value,
        "expected_digest": expected_digest,
        "actual_digest": actual_digest,
        "exceptions": sorted(
            (row for row in declared_gaps),
            key=lambda row: _record_key(row, ("entity_id", "session")),
        ),
    }
    coverage_digest = semantic_digest("SOURCE_COVERAGE_V1", coverage_payload)
    if (
        declared_coverage_digest is not None
        and _require_sha256(
            declared_coverage_digest, "SOURCE_SEAL_COVERAGE_DIGEST_INVALID"
        )
        != coverage_digest
    ):
        _fail(
            "SOURCE_SEAL_COVERAGE_DIGEST_MISMATCH", "覆盖摘要与实际实体/会话集合不一致"
        )
    uncertain_statuses = {"UNMAPPED", "UNKNOWN_GAP"}
    if any(row["status"] in uncertain_statuses for row in declared_gaps):
        return SealProofResult(
            "PARTIAL_DECLARED_SCOPE",
            coverage_digest,
            ("ENTITY_SESSION_COVERAGE_UNRESOLVED",),
        )
    return SealProofResult(
        "COMPLETE_DECLARED_SCOPE",
        coverage_digest,
        proof_digests=(
            ("inventory_digest", inventory_digest_value),
            ("coverage_digest", coverage_digest),
        ),
    )


def validate_required_inventories(
    *,
    required_datasets: Sequence[Mapping[str, object]],
    inventories: Sequence[Mapping[str, object]],
    expected_entities_by_dataset: Mapping[str, Sequence[Mapping[str, object]] | None],
    actual_entities_by_dataset: Mapping[str, Sequence[Mapping[str, object]]],
) -> SealProofResult:
    """逐项核对scope所需数据集集合，并分别验证每个数据集的实体会话覆盖。"""
    identity_fields = (
        "dataset_id",
        "dataset_version",
        "source_role",
        "source_revision",
        "query_scope",
    )

    def identity(item: Mapping[str, object]) -> tuple[str, ...]:
        if (
            not isinstance(item, Mapping)
            or any(key not in item for key in identity_fields)
            or not isinstance(item.get("query_scope"), Mapping)
        ):
            _fail("SOURCE_SEAL_REQUIRED_DATASET_INVALID", "required dataset身份不完整")
        scalar = tuple(item[key] for key in identity_fields[:4])
        if any(not isinstance(value, str) or not value for value in scalar):
            _fail(
                "SOURCE_SEAL_REQUIRED_DATASET_INVALID",
                "required dataset身份字段必须为非空字符串",
            )
        return (*scalar, semantic_digest("DATASET_QUERY_SCOPE_V1", item["query_scope"]))

    if (
        not isinstance(required_datasets, Sequence)
        or isinstance(required_datasets, (str, bytes))
        or not required_datasets
    ):
        _fail(
            "SOURCE_SEAL_REQUIRED_DATASET_INVALID",
            "P20 scope的required dataset集合缺失",
        )
    if not isinstance(inventories, Sequence) or isinstance(inventories, (str, bytes)):
        _fail("SOURCE_SEAL_REQUIRED_DATASET_INVALID", "实际来源inventory集合非法")
    if not isinstance(expected_entities_by_dataset, Mapping) or not isinstance(
        actual_entities_by_dataset, Mapping
    ):
        _fail(
            "SOURCE_SEAL_REQUIRED_DATASET_INVALID", "required dataset覆盖映射类型非法"
        )
    required_keys = [identity(item) for item in required_datasets]
    actual_keys = [identity(item) for item in inventories]
    required_ids = [item["dataset_id"] for item in required_datasets]
    actual_ids = [item["dataset_id"] for item in inventories]
    if (
        len(set(required_keys)) != len(required_keys)
        or len(set(actual_keys)) != len(actual_keys)
        or len(set(required_ids)) != len(required_ids)
        or len(set(actual_ids)) != len(actual_ids)
    ):
        _fail(
            "SOURCE_SEAL_DUPLICATE_BUSINESS_KEY",
            "required或实际来源dataset集合含重复身份",
        )
    if set(required_keys) != set(actual_keys):
        return SealProofResult(
            "PARTIAL_DECLARED_SCOPE", errors=("REQUIRED_DATASET_SET_MISMATCH",)
        )
    required_id_set = set(required_ids)
    expected_id_set, actual_id_set = (
        set(expected_entities_by_dataset),
        set(actual_entities_by_dataset),
    )
    if not required_id_set <= expected_id_set or not required_id_set <= actual_id_set:
        return SealProofResult(
            "COVERAGE_UNPROVEN", errors=("DATASET_COVERAGE_EVIDENCE_MISSING",)
        )
    if expected_id_set != required_id_set or actual_id_set != required_id_set:
        return SealProofResult(
            "PARTIAL_DECLARED_SCOPE",
            errors=("DATASET_COVERAGE_EVIDENCE_SCOPE_MISMATCH",),
        )
    actual_by_id = {item["dataset_id"]: item for item in inventories}
    proofs: list[tuple[str, str]] = []
    unresolved = False
    for required in required_datasets:
        dataset_id = required["dataset_id"]
        inventory = actual_by_id[dataset_id]
        expected = expected_entities_by_dataset.get(dataset_id)
        actual = actual_entities_by_dataset.get(dataset_id)
        if (
            dataset_id not in expected_entities_by_dataset
            or dataset_id not in actual_entities_by_dataset
        ):
            return SealProofResult(
                "COVERAGE_UNPROVEN",
                errors=(f"DATASET_COVERAGE_EVIDENCE_MISSING:{dataset_id}",),
            )
        result = validate_source_inventory(inventory, expected, actual)
        if result.status == "COVERAGE_UNPROVEN":
            return result
        if result.status != "COMPLETE_DECLARED_SCOPE":
            unresolved = True
        proofs.extend(
            (f"{dataset_id}:{name}", digest) for name, digest in result.proof_digests
        )
    digest = semantic_digest(
        "REQUIRED_DATASET_SET_V1",
        {
            "required": sorted((list(key) for key in required_keys)),
            "proofs": sorted(proofs),
        },
    )
    proofs.append(("required_dataset_set_digest", digest))
    return SealProofResult(
        "PARTIAL_DECLARED_SCOPE" if unresolved else "COMPLETE_DECLARED_SCOPE",
        digest,
        errors=("ONE_OR_MORE_DATASETS_PARTIAL",) if unresolved else (),
        proof_digests=tuple(proofs),
    )


def _fraction_decimal_text(value: str, numerator: int, denominator: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 128
        or "e" in value.lower()
        or not isinstance(numerator, int)
        or isinstance(numerator, bool)
        or not isinstance(denominator, int)
        or isinstance(denominator, bool)
        or denominator <= 0
        or abs(numerator) > 2**63 - 1
        or denominator > 2**63 - 1
    ):
        _fail(
            "SOURCE_SEAL_CONVERSION_CONTRACT_INVALID",
            "精确单位换算需要十进制文本和整数有理倍率",
        )
    try:
        result = Fraction(Decimal(value)) * Fraction(numerator, denominator)
    except (InvalidOperation, ValueError, ZeroDivisionError):
        _fail("SOURCE_SEAL_CONVERSION_VALUE_INVALID", "解析行的精确数值或单位倍率非法")
    reduced_denominator = result.denominator
    twos = fives = 0
    while reduced_denominator % 2 == 0:
        reduced_denominator //= 2
        twos += 1
    while reduced_denominator % 5 == 0:
        reduced_denominator //= 5
        fives += 1
    if reduced_denominator != 1:
        _fail(
            "SOURCE_SEAL_CONVERSION_CONTRACT_INVALID",
            "单位倍率结果不能有限十进制定点表达",
        )
    scale = 10 ** max(twos, fives)
    scaled = result.numerator * (scale // result.denominator)
    sign = "-" if scaled < 0 else ""
    digits = str(abs(scaled)).rjust(max(1, len(str(scale)) - 1), "0")
    if scale == 1:
        return sign + digits
    split = len(digits) - (len(str(scale)) - 1)
    integer, fractional = digits[:split] or "0", digits[split:]
    fractional = fractional.rstrip("0")
    return sign + integer + ("." + fractional if fractional else "")


def _source_mapping_disposition_digest(
    parsed_rows: Sequence[Mapping[str, object]],
    mapped_rows: Sequence[Mapping[str, object]],
    mapping_dispositions: Sequence[Mapping[str, object]],
) -> tuple[str, tuple[str, ...]]:
    if not isinstance(mapping_dispositions, Sequence) or isinstance(
        mapping_dispositions, (str, bytes)
    ):
        _fail("SOURCE_SEAL_MAPPING_DISPOSITION_INVALID", "来源映射处置集合类型非法")
    parsed_keys = [
        row.get("source_record_key") if isinstance(row, Mapping) else None
        for row in parsed_rows
    ]
    if any(not isinstance(key, str) or not key for key in parsed_keys) or len(
        set(parsed_keys)
    ) != len(parsed_keys):
        _fail("SOURCE_SEAL_RECORD_KEY_INVALID", "解析来源记录键缺失或重复")
    disposition_by_key: dict[str, Mapping[str, object]] = {}
    allowed = {"MAPPED", "UNMAPPED", "IDENTITY_CONFLICT", "REJECTED_BY_SCOPE"}
    for item in mapping_dispositions:
        if not isinstance(item, Mapping):
            _fail(
                "SOURCE_SEAL_MAPPING_DISPOSITION_INVALID",
                "来源映射处置必须是结构化记录",
            )
        key, status = item.get("source_record_key"), item.get("status")
        if (
            not isinstance(key, str)
            or not key
            or status not in allowed
            or "reason" not in item
        ):
            _fail(
                "SOURCE_SEAL_MAPPING_DISPOSITION_INVALID",
                "来源映射处置缺少记录键、允许状态或显式原因",
            )
        reason = item["reason"]
        if status == "MAPPED":
            if reason is not None and (not isinstance(reason, str) or not reason):
                _fail(
                    "SOURCE_SEAL_MAPPING_DISPOSITION_INVALID",
                    "MAPPED处置原因仅可为空或非空字符串",
                )
        elif not isinstance(reason, str) or not reason.strip():
            _fail(
                "SOURCE_SEAL_MAPPING_DISPOSITION_INVALID",
                "未映射、身份冲突或范围拒绝必须提供非空原因",
            )
        if key in disposition_by_key:
            _fail("SOURCE_SEAL_DUPLICATE_BUSINESS_KEY", "解析来源记录存在重复映射处置")
        disposition_by_key[key] = item
    if set(disposition_by_key) != set(parsed_keys):
        _fail(
            "SOURCE_SEAL_MAPPING_DISPOSITION_INCOMPLETE",
            "每条解析来源记录必须恰有一条映射或具名例外处置",
        )
    mapped_keys: list[str] = []
    for row in mapped_rows:
        key = row.get("source_record_key") if isinstance(row, Mapping) else None
        if not isinstance(key, str) or not key:
            _fail("SOURCE_SEAL_RECORD_KEY_INVALID", "P04映射来源记录键缺失")
        mapped_keys.append(key)
    if len(set(mapped_keys)) != len(mapped_keys):
        _fail("SOURCE_SEAL_DUPLICATE_BUSINESS_KEY", "同一解析来源记录存在重复P04映射")
    mapped_set = set(mapped_keys)
    declared_mapped = {
        key for key, item in disposition_by_key.items() if item["status"] == "MAPPED"
    }
    if mapped_set != declared_mapped:
        _fail(
            "SOURCE_SEAL_MAPPING_DISPOSITION_MISMATCH",
            "P04实际映射集合与逐记录处置不一致",
        )
    digest = records_digest(
        "P04_SOURCE_MAPPING_DISPOSITIONS_V1",
        list(mapping_dispositions),
        ("source_record_key",),
    )
    unresolved = tuple(
        sorted(
            (
                item["status"]
                for item in disposition_by_key.values()
                if item["status"] != "MAPPED"
            )
        )
    )
    return digest, unresolved


def _derive_canonical_rows(
    *,
    parsed_rows: Sequence[Mapping[str, object]],
    mapped_rows: Sequence[Mapping[str, object]],
    mapping_dispositions: Sequence[Mapping[str, object]],
    field_rules: Sequence[Mapping[str, object]],
    source_binding_id: str,
    p04_mapping_version: str,
    unit_version: str,
    normalization_version: str,
    allow_partial_mapping: bool,
) -> tuple[Mapping[str, object], ...]:
    """按显式字段/单位规则由P03解析行和P04映射独立计算预期P05行。"""
    if not all(
        isinstance(item, str) and item
        for item in (
            source_binding_id,
            p04_mapping_version,
            unit_version,
            normalization_version,
        )
    ):
        _fail(
            "SOURCE_SEAL_CONVERSION_CONTRACT_INVALID", "P05计算缺少source binding或版本"
        )
    if (
        not isinstance(field_rules, Sequence)
        or isinstance(field_rules, (str, bytes))
        or not field_rules
    ):
        _fail("SOURCE_SEAL_CONVERSION_CONTRACT_INVALID", "P05字段转换规则缺失")
    _, unresolved = _source_mapping_disposition_digest(
        parsed_rows, mapped_rows, mapping_dispositions
    )
    if unresolved and not allow_partial_mapping:
        _fail(
            "SOURCE_SEAL_MAPPING_DISPOSITION_UNRESOLVED",
            "存在已明确处置但未成功映射的解析记录",
        )
    parsed_by_key: dict[str, Mapping[str, object]] = {}
    for row in parsed_rows:
        key = row.get("source_record_key") if isinstance(row, Mapping) else None
        if not isinstance(key, str) or not key or key in parsed_by_key:
            _fail("SOURCE_SEAL_RECORD_KEY_INVALID", "解析行来源记录键缺失或重复")
        parsed_by_key[key] = row
    rules_by_field: dict[str, Mapping[str, object]] = {}
    for rule in field_rules:
        required = (
            "source_field",
            "canonical_field",
            "operation",
            "factor_numerator",
            "factor_denominator",
            "quality_field",
            "reason_field",
        )
        if not isinstance(rule, Mapping) or any(key not in rule for key in required):
            _fail(
                "SOURCE_SEAL_CONVERSION_CONTRACT_INVALID",
                "P05字段规则缺少字段映射、字段级质量/原因、运算或单位倍率",
            )
        source_field, canonical_field = rule["source_field"], rule["canonical_field"]
        if (
            not all(
                isinstance(item, str) and item
                for item in (
                    source_field,
                    canonical_field,
                    rule["quality_field"],
                    rule["reason_field"],
                )
            )
            or canonical_field in rules_by_field
        ):
            _fail(
                "SOURCE_SEAL_CONVERSION_CONTRACT_INVALID",
                "P05字段规则含非法字段或重复目标字段",
            )
        if rule["operation"] not in {"DECIMAL_RATIONAL", "IDENTITY"}:
            _fail(
                "SOURCE_SEAL_CONVERSION_CONTRACT_INVALID",
                "P05字段运算必须为DECIMAL_RATIONAL或IDENTITY",
            )
        if (
            not all(
                isinstance(rule[key], int) and not isinstance(rule[key], bool)
                for key in ("factor_numerator", "factor_denominator")
            )
            or rule["factor_denominator"] <= 0
        ):
            _fail("SOURCE_SEAL_CONVERSION_CONTRACT_INVALID", "P05有理单位倍率非法")
        rules_by_field[canonical_field] = rule
    expected: list[Mapping[str, object]] = []
    mapping_by_source: dict[str, tuple[str, str]] = {}
    for mapped in mapped_rows:
        key = mapped.get("source_record_key")
        parsed = parsed_by_key.get(key)
        if parsed is None:
            _fail("SOURCE_SEAL_CANONICAL_BINDING_MISMATCH", "P04映射引用不存在的解析行")
        if (
            not all(
                isinstance(mapped.get(name), str) and mapped[name]
                for name in (
                    "entity_id",
                    "session",
                    "mapping_version",
                    "identity_revision",
                    "calendar_revision",
                )
            )
            or mapped["mapping_version"] != p04_mapping_version
        ):
            _fail(
                "SOURCE_SEAL_CONVERSION_CONTRACT_INVALID",
                "P04映射行缺少实体/会话/身份或日历版本",
            )
        target = (mapped["entity_id"], mapped["session"])
        if key in mapping_by_source and mapping_by_source[key] != target:
            _fail("SOURCE_SEAL_COVERAGE_CONFLICT", "同一来源记录映射到多个实体会话")
        mapping_by_source[key] = target
        for canonical_field, rule in rules_by_field.items():
            source_field = rule["source_field"]
            quality_field, reason_field = rule["quality_field"], rule["reason_field"]
            if any(
                name not in parsed
                for name in (source_field, quality_field, reason_field)
            ):
                _fail(
                    "SOURCE_SEAL_CONVERSION_VALUE_INVALID", "解析行缺少字段规则所需输入"
                )
            quality, reason = parsed[quality_field], parsed[reason_field]
            if quality not in {"OBSERVED", "UNKNOWN_DATA", "NOT_APPLICABLE"}:
                _fail("SOURCE_SEAL_CONVERSION_VALUE_INVALID", "解析行缺少合法字段质量")
            source_value = parsed[source_field]
            if quality != "OBSERVED":
                if source_value is not None:
                    _fail(
                        "SOURCE_SEAL_CONVERSION_VALUE_INVALID",
                        "非OBSERVED解析值必须显式为空",
                    )
                canonical_value = None
            elif rule["operation"] == "DECIMAL_RATIONAL":
                canonical_value = _fraction_decimal_text(
                    source_value, rule["factor_numerator"], rule["factor_denominator"]
                )
            else:
                canonical_value = source_value
                _canonical_value(canonical_value)
            expected.append(
                {
                    "entity_id": mapped["entity_id"],
                    "session": mapped["session"],
                    "field": canonical_field,
                    "value": canonical_value,
                    "quality": quality,
                    "reason": reason,
                    "source_binding_id": source_binding_id,
                    "source_record_keys": [key],
                    "unit_version": unit_version,
                    "normalization_version": normalization_version,
                    "identity_revision": mapped["identity_revision"],
                    "calendar_revision": mapped["calendar_revision"],
                }
            )
    return tuple(expected)


def derive_canonical_rows(
    *,
    parsed_rows: Sequence[Mapping[str, object]],
    mapped_rows: Sequence[Mapping[str, object]],
    mapping_dispositions: Sequence[Mapping[str, object]],
    field_rules: Sequence[Mapping[str, object]],
    source_binding_id: str,
    p04_mapping_version: str,
    unit_version: str,
    normalization_version: str,
) -> tuple[Mapping[str, object], ...]:
    """仅为完整映射集合派生P05预期行；部分状态只能由总体验证器表达。"""
    return _derive_canonical_rows(
        parsed_rows=parsed_rows,
        mapped_rows=mapped_rows,
        mapping_dispositions=mapping_dispositions,
        field_rules=field_rules,
        source_binding_id=source_binding_id,
        p04_mapping_version=p04_mapping_version,
        unit_version=unit_version,
        normalization_version=normalization_version,
        allow_partial_mapping=False,
    )


def verify_transformation_chain(
    *,
    source_bytes: bytes | None,
    source_byte_sha256: str,
    parser_version: str,
    unit_version: str,
    p04_mapping_version: str,
    p05_conversion_version: str,
    p05_field_projection_version: str,
    source_binding_id: str,
    normalization_version: str,
    field_rules: Sequence[Mapping[str, object]],
    parsed_rows: Sequence[Mapping[str, object]],
    mapped_rows: Sequence[Mapping[str, object]],
    mapping_dispositions: Sequence[Mapping[str, object]],
    canonical_rows: Sequence[Mapping[str, object]],
    claimed_digests: Mapping[str, str],
    capture_verification: Mapping[str, object] | None = None,
    replayable_source_bytes_requested: bool = False,
) -> SealProofResult:
    """复算捕获、解析、P04映射、P05行集及转换合同的分阶段摘要。"""
    if not all(
        isinstance(value, str) and value
        for value in (
            parser_version,
            unit_version,
            p04_mapping_version,
            p05_conversion_version,
            p05_field_projection_version,
            source_binding_id,
            normalization_version,
        )
    ):
        _fail(
            "SOURCE_SEAL_CONVERSION_CONTRACT_INVALID",
            "转换链缺少parser/unit/P04/P05字段投影版本",
        )
    source_digest = _require_sha256(
        source_byte_sha256, "SOURCE_SEAL_SOURCE_DIGEST_INVALID"
    )
    replayability = "VERIFIED_AT_CAPTURE"
    if source_bytes is not None:
        if not isinstance(source_bytes, bytes):
            _fail("SOURCE_SEAL_SOURCE_BYTES_INVALID", "原始来源内容必须是实际bytes")
        actual_source_digest = sha256(source_bytes).hexdigest()
        if actual_source_digest != source_digest:
            _fail("SOURCE_SEAL_SOURCE_BYTES_MISMATCH", "原始字节SHA与封存摘要不一致")
        replayability = "SOURCE_BYTES_PRESENT_PARSER_REPLAY_PENDING"
    elif replayable_source_bytes_requested:
        _fail(
            "SOURCE_SEAL_SOURCE_BYTES_UNAVAILABLE", "缺少原始字节，不得声称可回放来源"
        )
    else:
        evidence = capture_verification
        if not isinstance(evidence, Mapping) or any(
            key not in evidence
            for key in ("source_byte_sha256", "verified_at", "method", "evidence_ref")
        ):
            _fail(
                "SOURCE_SEAL_CAPTURE_VERIFICATION_MISSING",
                "缺少原始字节时必须有结构化捕获验证记录",
            )
        if (
            _require_sha256(
                evidence["source_byte_sha256"],
                "SOURCE_SEAL_CAPTURE_VERIFICATION_INVALID",
            )
            != source_digest
        ):
            _fail(
                "SOURCE_SEAL_CAPTURE_VERIFICATION_INVALID",
                "捕获验证记录未绑定原始字节摘要",
            )
        normalize_observed_clock(evidence["verified_at"])
        if not all(
            isinstance(evidence[key], str) and evidence[key]
            for key in ("method", "evidence_ref")
        ):
            _fail(
                "SOURCE_SEAL_CAPTURE_VERIFICATION_INVALID",
                "捕获验证记录缺少算法或证据引用",
            )
    parsed_digest = records_digest(
        "PARSED_ROW_SET_V1", parsed_rows, ("source_record_key",)
    )
    mapped_digest = records_digest(
        "P04_MAPPED_FACTS_V1",
        mapped_rows,
        ("source_record_key", "entity_id", "session"),
    )
    mapping_disposition_digest, unresolved_mappings = (
        _source_mapping_disposition_digest(
            parsed_rows, mapped_rows, mapping_dispositions
        )
    )
    parsed_keys = {row["source_record_key"] for row in parsed_rows}
    mapped_by_entity_session: dict[tuple[object, object], set[object]] = {}
    for row in mapped_rows:
        if row["source_record_key"] not in parsed_keys:
            _fail(
                "SOURCE_SEAL_CANONICAL_BINDING_MISMATCH",
                "P04映射引用不存在的解析来源记录",
            )
        mapped_by_entity_session.setdefault(
            (row["entity_id"], row["session"]), set()
        ).add(row["source_record_key"])
    for row in canonical_rows:
        keys = row.get("source_record_keys")
        if not isinstance(keys, Sequence) or isinstance(keys, (str, bytes)) or not keys:
            _fail("SOURCE_SEAL_CANONICAL_BINDING_MISMATCH", "P05规范行缺少来源记录绑定")
        if any(not isinstance(key, str) or not key for key in keys) or len(
            set(keys)
        ) != len(keys):
            _fail("SOURCE_SEAL_DUPLICATE_BUSINESS_KEY", "P05来源记录绑定含非法或重复键")
        if any(
            key
            not in mapped_by_entity_session.get(
                (row.get("entity_id"), row.get("session")), set()
            )
            for key in keys
        ):
            _fail(
                "SOURCE_SEAL_CANONICAL_BINDING_MISMATCH",
                "P05规范行未绑定同实体会话的P04映射记录",
            )
    expected_canonical_rows = _derive_canonical_rows(
        parsed_rows=parsed_rows,
        mapped_rows=mapped_rows,
        mapping_dispositions=mapping_dispositions,
        field_rules=field_rules,
        source_binding_id=source_binding_id,
        p04_mapping_version=p04_mapping_version,
        unit_version=unit_version,
        normalization_version=normalization_version,
        allow_partial_mapping=True,
    )
    canonical_projection = [
        {
            **row,
            "source_record_keys": sorted(
                row["source_record_keys"], key=lambda value: str(value).encode("utf-8")
            ),
        }
        for row in canonical_rows
    ]
    canonical_digest = records_digest(
        "P05_CANONICAL_ROWS_V1", canonical_projection, ("entity_id", "session", "field")
    )
    expected_canonical_digest = records_digest(
        "P05_CANONICAL_ROWS_V1",
        expected_canonical_rows,
        ("entity_id", "session", "field"),
    )
    if canonical_digest != expected_canonical_digest:
        _fail(
            "SOURCE_SEAL_CANONICAL_BINDING_MISMATCH",
            "P05规范值/质量/原因与解析行、P04映射或转换规则不一致",
        )
    field_rules_digest = records_digest(
        "P05_FIELD_RULES_V1", field_rules, ("canonical_field",)
    )
    capture_evidence_digest = (
        semantic_digest("CAPTURE_VERIFICATION_V1", capture_verification)
        if source_bytes is None and isinstance(capture_verification, Mapping)
        else None
    )
    transformation_digest = semantic_digest(
        "CONVERSION_CONTRACT_V1",
        {
            "source_byte_sha256": source_digest,
            "parser_version": parser_version,
            "unit_version": unit_version,
            "p04_mapping_version": p04_mapping_version,
            "p05_conversion_version": p05_conversion_version,
            "p05_field_projection_version": p05_field_projection_version,
            "source_binding_id": source_binding_id,
            "normalization_version": normalization_version,
            "field_rules_digest": field_rules_digest,
            "capture_evidence_digest": capture_evidence_digest,
            "parsed_digest": parsed_digest,
            "mapped_digest": mapped_digest,
            "mapping_disposition_digest": mapping_disposition_digest,
            "canonical_digest": canonical_digest,
        },
    )
    actual = {
        "source_byte_sha256": source_digest,
        "parsed_row_set_digest": parsed_digest,
        "mapped_fact_digest": mapped_digest,
        "p05_canonical_digest": canonical_digest,
        "conversion_contract_digest": transformation_digest,
    }
    for key, digest in actual.items():
        if (
            key not in claimed_digests
            or _require_sha256(
                claimed_digests[key], "SOURCE_SEAL_CONVERSION_DIGEST_INVALID"
            )
            != digest
        ):
            _fail(
                "SOURCE_SEAL_CANONICAL_BINDING_MISMATCH", f"转换链阶段摘要不一致：{key}"
            )
    return SealProofResult(
        "PARTIAL_DECLARED_SCOPE" if unresolved_mappings else "CANONICAL_BOUND",
        transformation_digest,
        errors=("SOURCE_MAPPING_NOT_FULLY_RESOLVED",) if unresolved_mappings else (),
        replayability=replayability,
        proof_digests=(
            ("source_byte_sha256", source_digest),
            ("parsed_row_set_digest", parsed_digest),
            ("mapped_fact_digest", mapped_digest),
            ("mapping_disposition_digest", mapping_disposition_digest),
            ("p05_canonical_digest", canonical_digest),
            ("conversion_contract_digest", transformation_digest),
        ),
    )


def normalize_observed_clock(value: object) -> datetime:
    """要求显式时区并统一到UTC微秒；不得由系统本地时区推断。"""
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            _fail("SOURCE_SEAL_CLOCK_INVALID", "来源时钟不是合法ISO时间")
    elif isinstance(value, datetime):
        parsed = value
    else:
        _fail("SOURCE_SEAL_CLOCK_INVALID", "来源时钟类型非法")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail("SOURCE_SEAL_CLOCK_UNKNOWN", "来源时钟缺少明确时区")
    return parsed.astimezone(timezone.utc)


def validate_clock_order(clocks: Mapping[str, object]) -> SealProofResult:
    order = (
        "read_started_at",
        "read_completed_at",
        "validated_complete_at",
        "durable_commit_observed_at",
        "source_sealed_at",
    )
    if not isinstance(clocks, Mapping) or any(key not in clocks for key in order):
        _fail("SOURCE_SEAL_CLOCK_UNKNOWN", "来源验证/持久化/封口时钟缺项")
    normalized = {key: normalize_observed_clock(clocks[key]) for key in order}
    if "request_started_at" in clocks:
        normalized["request_started_at"] = normalize_observed_clock(
            clocks["request_started_at"]
        )
        if normalized["request_started_at"] > normalized["read_completed_at"]:
            _fail("SOURCE_SEAL_CLOCK_ORDER_INVALID", "请求开始晚于来源读取完成")
    for earlier, later in zip(order, order[1:]):
        if normalized[earlier] > normalized[later]:
            _fail(
                "SOURCE_SEAL_CLOCK_ORDER_INVALID",
                f"来源时钟顺序错误：{earlier} > {later}",
            )
    payload = {
        key: value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        for key, value in normalized.items()
    }
    return SealProofResult(
        "CLOCK_ORDERED", semantic_digest("SOURCE_CLOCK_ORDER_V1", payload)
    )


def compute_ready_at(
    required_observations: Sequence[Mapping[str, object]],
) -> SealProofResult:
    """就绪时刻取必要且实际观察到的可用/下载/读取/校验时钟最大值。"""
    allowed_roles = {
        "VERIFIED_SOURCE_AVAILABLE",
        "DOWNLOAD_COMPLETED",
        "READ_COMPLETED",
        "VALIDATION_COMPLETED",
    }
    if (
        not isinstance(required_observations, Sequence)
        or isinstance(required_observations, (str, bytes))
        or not required_observations
    ):
        return SealProofResult(
            "NEEDS_DEPENDENCY", errors=("REQUIRED_AVAILABILITY_CLOCKS_MISSING",)
        )
    observations: list[tuple[str, datetime]] = []
    for item in required_observations:
        if (
            not isinstance(item, Mapping)
            or item.get("role") not in allowed_roles
            or "observed_at" not in item
        ):
            return SealProofResult(
                "NEEDS_DEPENDENCY", errors=("REQUIRED_AVAILABILITY_CLOCK_UNPROVEN",)
            )
        observations.append(
            (item["role"], normalize_observed_clock(item["observed_at"]))
        )
    ready = max(value for _, value in observations)
    ordered_payload = sorted(
        (
            {"role": role, "observed_at": value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}
            for role, value in observations
        ),
        key=lambda item: (
            item["role"].encode("utf-8"),
            item["observed_at"].encode("utf-8"),
        ),
    )
    digest = semantic_digest(
        "SOURCE_READY_AT_V1",
        {
            "observations": ordered_payload,
            "ready_at": ready.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        },
    )
    return SealProofResult(
        "READY_AT_COMPUTED",
        digest,
        proof_digests=(("ready_at_digest", digest),),
        value=ready.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    )


def classify_temporal_eligibility(
    *,
    mode: str,
    as_of: object,
    ready_at: object | None,
    first_capture_at: object | None,
    durable_commit_observed_at: object | None,
    required_dependencies: Sequence[Mapping[str, object]],
) -> SealProofResult:
    """分离NON_PIT研究与PIT候选；本函数不签发真实PIT资格。"""
    if mode == "NON_PIT_EOD":
        return SealProofResult("NON_PIT_RESEARCH_ONLY")
    if mode not in {"LIVE", "PIT"}:
        _fail(
            "SOURCE_SEAL_TEMPORAL_MODE_INVALID",
            "时序资格模式必须为NON_PIT_EOD、LIVE或PIT",
        )
    if not isinstance(required_dependencies, Sequence) or isinstance(
        required_dependencies, (str, bytes)
    ):
        _fail("SOURCE_SEAL_TEMPORAL_DEPENDENCY_INVALID", "必需依赖必须是结构化集合")
    try:
        cutoff = normalize_observed_clock(as_of)
        observed = {
            "ready_at": normalize_observed_clock(ready_at),
            "first_capture_at": normalize_observed_clock(first_capture_at),
            "durable_commit_observed_at": normalize_observed_clock(
                durable_commit_observed_at
            ),
        }
    except SourceSealValidationError:
        return SealProofResult(
            "NEEDS_DEPENDENCY", errors=("REAL_PIT_CLOCK_EVIDENCE_MISSING",)
        )
    if any(value > cutoff for value in observed.values()):
        _fail(
            "SOURCE_SEAL_PIT_ASOF_VIOLATION", "ready/capture/durable时钟晚于声明as-of"
        )
    if (
        observed["first_capture_at"] > observed["durable_commit_observed_at"]
        or observed["ready_at"] > observed["durable_commit_observed_at"]
    ):
        _fail("SOURCE_SEAL_CLOCK_ORDER_INVALID", "PIT捕获/就绪时钟晚于持久化观测")
    for dependency in required_dependencies:
        if (
            not isinstance(dependency, Mapping)
            or dependency.get("status") != "ACCEPTED"
            or not all(
                isinstance(dependency.get(key), str) and dependency[key]
                for key in ("dependency_id", "version")
            )
        ):
            return SealProofResult(
                "NEEDS_DEPENDENCY", errors=("REQUIRED_DEPENDENCY_NOT_ACCEPTED",)
            )
        if "accepted_at" not in dependency:
            return SealProofResult(
                "NEEDS_DEPENDENCY", errors=("DEPENDENCY_ACCEPTANCE_CLOCK_MISSING",)
            )
        if normalize_observed_clock(dependency["accepted_at"]) > cutoff:
            _fail("SOURCE_SEAL_PIT_ASOF_VIOLATION", "必需依赖接受时钟晚于声明as-of")
    digest = semantic_digest(
        "SOURCE_TEMPORAL_ELIGIBILITY_V1",
        {
            "mode": mode,
            "as_of": cutoff.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            **{
                key: value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                for key, value in observed.items()
            },
            "dependencies": list(required_dependencies),
        },
    )
    return SealProofResult("PIT_ELIGIBLE_SCOPED", digest)


def validate_seal_revision(
    existing: Sequence[Mapping[str, object]], candidate: Mapping[str, object]
) -> SealProofResult:
    """同revision同内容幂等；异内容冲突；合法修订只允许追加。"""
    required = ("scope", "source_binding_id", "revision", "content_digest")
    if not isinstance(candidate, Mapping) or any(
        key not in candidate for key in required
    ):
        _fail("SOURCE_SEAL_REVISION_INVALID", "封口revision身份字段缺失")
    if any(
        not isinstance(candidate[key], str) or not candidate[key]
        for key in ("scope", "source_binding_id")
    ):
        _fail(
            "SOURCE_SEAL_REVISION_INVALID",
            "封口scope与source_binding_id必须为非空字符串",
        )
    if (
        not isinstance(existing, Sequence)
        or isinstance(existing, (str, bytes))
        or any(not isinstance(row, Mapping) for row in existing)
    ):
        _fail("SOURCE_SEAL_REVISION_INVALID", "既有封口版本集合类型非法")
    if (
        not isinstance(candidate["revision"], int)
        or isinstance(candidate["revision"], bool)
        or candidate["revision"] < 1
    ):
        _fail("SOURCE_SEAL_REVISION_INVALID", "candidate revision必须为正整数")
    digest = _require_sha256(
        candidate["content_digest"], "SOURCE_SEAL_REVISION_INVALID"
    )
    scoped_existing = [
        row
        for row in existing
        if row.get("scope") == candidate["scope"]
        and row.get("source_binding_id") == candidate["source_binding_id"]
    ]
    revision_numbers: list[int] = []
    by_revision: dict[int, str] = {}
    for row in scoped_existing:
        revision = row.get("revision")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            _fail("SOURCE_SEAL_REVISION_INVALID", "既有封口revision必须为正整数")
        row_digest = _require_sha256(
            row.get("content_digest"), "SOURCE_SEAL_REVISION_INVALID"
        )
        if revision in by_revision:
            _fail("SOURCE_SEAL_REVISION_CONFLICT", "既有封口版本集合含重复revision")
        by_revision[revision] = row_digest
        revision_numbers.append(revision)
    same = [
        row for row in scoped_existing if row.get("revision") == candidate["revision"]
    ]
    if same:
        if by_revision[candidate["revision"]] == digest:
            return SealProofResult("IDEMPOTENT_EXISTING_REVISION", digest)
        _fail(
            "SOURCE_SEAL_REVISION_CONFLICT",
            "相同scope/binding/revision出现不同封口内容",
        )
    if revision_numbers and candidate["revision"] <= max(revision_numbers):
        _fail(
            "SOURCE_SEAL_REVISION_NOT_APPEND_ONLY",
            "合法修订必须追加更高revision，不得覆盖历史",
        )
    return SealProofResult("APPEND_NEW_REVISION", digest)


def compute_seal_snapshot(
    *,
    scope: str,
    source_binding_id: str,
    revision: int,
    seal_id: str,
    rows: Sequence[Mapping[str, object]],
    references: Sequence[Mapping[str, object]],
    row_key_fields: tuple[str, ...],
) -> Mapping[str, object]:
    """计算封口快照摘要；不写库、不证明快照来自PG事务或不可变存储。"""
    if (
        not all(
            isinstance(value, str) and value
            for value in (scope, source_binding_id, seal_id)
        )
        or not isinstance(revision, int)
        or isinstance(revision, bool)
        or revision < 1
    ):
        _fail("SOURCE_SEAL_SNAPSHOT_INVALID", "封口快照身份字段非法")
    if (
        not isinstance(references, Sequence)
        or isinstance(references, (str, bytes))
        or not any(
            isinstance(item, Mapping) and item.get("reference_type") == "SOURCE_SEAL"
            for item in references
        )
    ):
        _fail("SOURCE_SEAL_SNAPSHOT_INVALID", "Owner封口引用闭包必须含source_seal引用")
    row_digest = records_digest("OWNER_SEALED_ROWS_V1", rows, row_key_fields)
    reference_digest = records_digest(
        "OWNER_SEALED_REFERENCES_V1",
        references,
        ("reference_type", "reference_id", "target_version"),
    )
    snapshot_content_digest = semantic_digest(
        "P20_SEAL_SNAPSHOT_V1",
        {
            "scope": scope,
            "source_binding_id": source_binding_id,
            "revision": revision,
            "seal_id": seal_id,
            "row_count": len(rows),
            "row_digest": row_digest,
            "reference_count": len(references),
            "reference_digest": reference_digest,
        },
    )
    return {
        "scope": scope,
        "source_binding_id": source_binding_id,
        "revision": revision,
        "seal_id": seal_id,
        "row_count": len(rows),
        "row_digest": row_digest,
        "reference_count": len(references),
        "reference_digest": reference_digest,
        "content_digest": snapshot_content_digest,
    }


def validate_seal_snapshot(
    *,
    sealed_snapshot: Mapping[str, object],
    readback_rows: Sequence[Mapping[str, object]],
    readback_references: Sequence[Mapping[str, object]],
    row_key_fields: tuple[str, ...],
) -> SealProofResult:
    """比较封口记录和显式读回行集的ID/count/digest/引用闭包。"""
    required = (
        "scope",
        "source_binding_id",
        "revision",
        "seal_id",
        "row_count",
        "row_digest",
        "reference_count",
        "reference_digest",
        "content_digest",
    )
    if not isinstance(sealed_snapshot, Mapping) or any(
        key not in sealed_snapshot for key in required
    ):
        _fail("SOURCE_SEAL_SNAPSHOT_INVALID", "封口快照缺少ID/count/digest/引用字段")
    try:
        readback = compute_seal_snapshot(
            scope=sealed_snapshot["scope"],
            source_binding_id=sealed_snapshot["source_binding_id"],
            revision=sealed_snapshot["revision"],
            seal_id=sealed_snapshot["seal_id"],
            rows=readback_rows,
            references=readback_references,
            row_key_fields=row_key_fields,
        )
    except SourceSealValidationError:
        raise
    for key in required:
        if readback[key] != sealed_snapshot[key]:
            _fail("SOURCE_SEAL_SNAPSHOT_MISMATCH", f"封口后读回与快照不一致：{key}")
    return SealProofResult("SEALED_SNAPSHOT_MATCH", sealed_snapshot["content_digest"])


OWNER_INPUT_FIELDS = (
    "owner_scope",
    "source_seal",
    "fact_inputs",
    "derived_inputs",
    "rule_inputs",
    "provenance_pit",
    "result_identity",
)


def compute_owner_input_digest(
    owner_projection: Mapping[str, object],
    required_projection: Sequence[str] = OWNER_INPUT_FIELDS,
) -> str:
    """校验Owner局部投影并将封口ID、内容摘要与时钟质量纳入身份。"""
    if (
        not isinstance(owner_projection, Mapping)
        or tuple(required_projection) != OWNER_INPUT_FIELDS
    ):
        _fail(
            "SOURCE_SEAL_OWNER_PROJECTION_CONTRACT_INVALID",
            "Owner投影字段合同不匹配OWNER_INPUT_PROJECTION_V1",
        )
    if set(owner_projection) != set(OWNER_INPUT_FIELDS):
        _fail(
            "SOURCE_SEAL_OWNER_PROJECTION_INCOMPLETE", "Owner投影字段缺失或含合同外字段"
        )
    seal = owner_projection["source_seal"]
    if not isinstance(seal, Mapping) or any(
        key not in seal for key in ("seal_id", "content_digest", "clock_quality")
    ):
        _fail(
            "SOURCE_SEAL_OWNER_PROJECTION_INCOMPLETE",
            "Owner投影未绑定封口ID、内容摘要与时钟质量",
        )
    if not isinstance(seal["seal_id"], str) or not seal["seal_id"]:
        _fail("SOURCE_SEAL_OWNER_PROJECTION_INVALID", "Owner投影封口ID必须是非空字符串")
    _require_sha256(seal["content_digest"], "SOURCE_SEAL_OWNER_PROJECTION_INVALID")
    if seal["clock_quality"] not in {"VERIFIED", "UNKNOWN", "UNPROVEN"}:
        _fail("SOURCE_SEAL_OWNER_PROJECTION_INVALID", "Owner投影时钟质量枚举非法")
    if not isinstance(owner_projection["owner_scope"], Mapping):
        _fail("SOURCE_SEAL_OWNER_PROJECTION_INVALID", "Owner scope必须是字段映射")
    scope = owner_projection["owner_scope"]
    scope_fields = (
        "domain",
        "partition",
        "variant",
        "target_session",
        "required_roles",
    )
    if any(key not in scope for key in scope_fields) or any(
        not isinstance(scope[key], str) or not scope[key] for key in scope_fields[:4]
    ):
        _fail(
            "SOURCE_SEAL_OWNER_PROJECTION_INCOMPLETE",
            "Owner scope缺少域、分区、变体、目标会话或required roles",
        )
    if (
        not isinstance(scope["required_roles"], (list, tuple))
        or not scope["required_roles"]
        or any(
            not isinstance(role, str) or not role for role in scope["required_roles"]
        )
    ):
        _fail(
            "SOURCE_SEAL_OWNER_PROJECTION_INVALID",
            "Owner required roles必须是含非空字符串的数组",
        )
    array_keys = {
        "fact_inputs": ("source_binding_id", "entity_id", "session", "field"),
        "derived_inputs": (
            "producer_id",
            "target_coordinate_id",
            "entity_id",
            "session",
            "field",
        ),
        "rule_inputs": ("algorithm_id", "algorithm_version", "parameter_version"),
        "provenance_pit": ("source_binding_id", "role", "as_of"),
        "result_identity": ("entity_id", "session", "field", "quality"),
    }
    required_row_fields = {
        "fact_inputs": (
            "source_binding_id",
            "source_content_version",
            "entity_id",
            "session",
            "field",
            "value_digest",
            "quality",
            "reason",
            "identity_revision",
            "calendar_revision",
        ),
        "derived_inputs": (
            "producer_id",
            "producer_version",
            "target_coordinate_id",
            "entity_id",
            "session",
            "field",
            "input_digest",
        ),
        "rule_inputs": (
            "algorithm_id",
            "algorithm_version",
            "parameter_version",
            "parameter_digest",
        ),
        "provenance_pit": (
            "source_binding_id",
            "role",
            "capture_id",
            "ready_at",
            "durable_at",
            "as_of",
            "availability_quality",
        ),
        "result_identity": ("entity_id", "session", "field", "quality", "value_digest"),
    }
    canonical_projection = dict(owner_projection)
    for key, business_key in array_keys.items():
        rows = owner_projection[key]
        if (
            not isinstance(rows, Sequence)
            or isinstance(rows, (str, bytes))
            or any(not isinstance(row, Mapping) for row in rows)
        ):
            _fail(
                "SOURCE_SEAL_OWNER_PROJECTION_INVALID",
                f"Owner投影字段必须是结构化数组：{key}",
            )
        for row in rows:
            if any(field not in row for field in required_row_fields[key]):
                _fail(
                    "SOURCE_SEAL_OWNER_PROJECTION_INCOMPLETE",
                    f"Owner投影行缺少必需字段：{key}",
                )
            digest_fields = (
                ("value_digest",)
                if key in {"fact_inputs", "result_identity"}
                else ("input_digest",)
                if key == "derived_inputs"
                else ("parameter_digest",)
                if key == "rule_inputs"
                else ()
            )
            for field in digest_fields:
                _require_sha256(row[field], "SOURCE_SEAL_OWNER_PROJECTION_INVALID")
        ordered = sorted(rows, key=lambda row: _record_key(row, business_key))
        records_digest(f"OWNER_{key.upper()}_V1", ordered, business_key)
        canonical_projection[key] = ordered
    return semantic_digest("OWNER_INPUT_PROJECTION_V1", canonical_projection)


def require_source_seal_identity_keys(
    identity: Mapping[str, object], required_identity: tuple[str, ...] | list[str]
) -> None:
    """拒绝缺失必需身份键；正常返回仅表示键存在，不表示身份或封口有效。

    本预检故意不校验字段值、来源真实性、时间、摘要或跨证据关系；值为
    ``None`` 或空字符串仍只会通过键存在性这一项，调用方不得据此封口或发布。
    """
    if not isinstance(identity, Mapping) or any(
        not isinstance(key, str) or not key for key in identity
    ):
        raise SourceSealValidationError(
            "SOURCE_SEAL_IDENTITY_INVALID", "来源封口身份必须是字符串键映射"
        )
    if (
        not isinstance(required_identity, (tuple, list))
        or not required_identity
        or any(not isinstance(key, str) or not key for key in required_identity)
        or len(required_identity) != len(set(required_identity))
    ):
        raise SourceSealValidationError(
            "SOURCE_SEAL_CONTRACT_INVALID", "正式来源封口必需身份键列表缺失、非法或重复"
        )
    missing = tuple(key for key in required_identity if key not in identity)
    if missing:
        raise SourceSealValidationError(
            "SOURCE_SEAL_IDENTITY_MISSING",
            "来源封口缺少必需身份键：" + ",".join(missing),
        )
