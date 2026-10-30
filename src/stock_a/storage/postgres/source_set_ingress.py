"""只读检查正式P03来源集合是否可供P04读取。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from . import source_raw


def inspect_p04_source_input(
    config: Mapping[str, Any], *, target_session: date, package_sha256: str
) -> dict[str, Any]:
    """只检查固定正式关系；缺表或未绑定schema时绝不回退到候选/latest。"""
    if type(target_session) is not date:
        raise ValueError("P04来源输入必须绑定显式目标会话")
    if (
        not isinstance(package_sha256, str)
        or len(package_sha256) != 64
        or any(char not in "0123456789abcdef" for char in package_sha256)
    ):
        raise ValueError("P04来源输入必须绑定完整小写SHA-256")

    with source_raw._transaction(config, write=False) as (
        _connection,
        cursor,
        namespace_id,
    ):
        cursor.execute(
            """SELECT to_regclass('stock.source_sets')::text,
                      to_regclass('stock.source_set_capabilities')::text"""
        )
        source_sets, source_capabilities = cursor.fetchone()
        if source_sets is None or source_capabilities is None:
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

        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM stock.source_sets LIMIT 1), "
            "EXISTS (SELECT 1 FROM stock.source_set_capabilities LIMIT 1)"
        )
        source_set_has_rows, capability_has_rows = cursor.fetchone()
        if not source_set_has_rows and not capability_has_rows:
            return {
                "status": "NO_MATCH",
                "target_session": target_session.isoformat(),
                "package_sha256": package_sha256,
                "namespace_id": str(namespace_id),
                "database_write": False,
                "ddl_executed": False,
                "candidate_fallback": False,
                "reason_code": "FORMAL_SOURCE_SET_TABLES_EMPTY",
            }

    return {
        "status": "P04_INGRESS_READER_NOT_CONNECTED",
        "target_session": target_session.isoformat(),
        "package_sha256": package_sha256,
        "namespace_id": str(namespace_id),
        "database_write": False,
        "ddl_executed": False,
        "candidate_fallback": False,
        "reason_code": "FORMAL_SOURCE_SET_TYPED_READER_SCHEMA_UNBOUND",
    }
