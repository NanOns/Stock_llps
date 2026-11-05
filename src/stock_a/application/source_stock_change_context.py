"""联结单股来源级D0变化与可选的精确画像修订变化。"""

from __future__ import annotations

from datetime import date
import re
from typing import Any, Mapping
from uuid import UUID

from stock_a.application.pg_price_profiles import (
    PgPriceProfileError,
    query_source_symbol_price_profile_changes,
)
from stock_a.application.source_d0_raw import (
    SourceD0RawError,
    query_source_d0_raw_launch,
)


_SOURCE_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class SourceStockChangeContextError(ValueError):
    """单股来源变化上下文参数或画像读取错误。"""

    def __init__(self, code: str, message: str, *, category: str = "UNAVAILABLE"):
        self.code = code
        self.category = category
        super().__init__(message)


def _validate_profile_identity(
    *,
    before_profile_revision: str | None,
    after_profile_revision: str | None,
) -> bool:
    if (before_profile_revision is None) != (after_profile_revision is None):
        raise SourceStockChangeContextError(
            "PROFILE_CHANGE_REVISION_PAIR_REQUIRED",
            "Profile变化必须同时提供前后两个精确profile_revision，或同时省略。",
            category="ARGUMENT",
        )
    if before_profile_revision is None:
        return False
    if any(
        not isinstance(value, str) or _SHA256.fullmatch(value) is None
        for value in (before_profile_revision, after_profile_revision)
    ):
        raise SourceStockChangeContextError(
            "PROFILE_CHANGE_REVISION_INVALID",
            "Profile修订必须是小写64位SHA-256。",
            category="ARGUMENT",
        )
    return True


def _validate_namespace_id(namespace_id: str) -> None:
    try:
        canonical = str(UUID(namespace_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise SourceStockChangeContextError(
            "SOURCE_CHANGE_NAMESPACE_INVALID",
            "namespace_id必须是规范小写UUID。",
            category="ARGUMENT",
        ) from error
    if canonical != namespace_id:
        raise SourceStockChangeContextError(
            "SOURCE_CHANGE_NAMESPACE_INVALID",
            "namespace_id必须是规范小写UUID。",
            category="ARGUMENT",
        )


def query_source_stock_change_context(
    config: Mapping[str, Any],
    *,
    namespace_id: str,
    before_session: date,
    target_session: date,
    research_cutoff: date,
    before_source_set_id: str,
    before_package_sha256: str,
    source_set_id: str,
    package_sha256: str,
    symbol: str,
    before_profile_revision: str | None = None,
    after_profile_revision: str | None = None,
) -> dict[str, Any]:
    """为单个来源股票读取两端完整D0三态上下文及可选Profile变化。"""
    _validate_namespace_id(namespace_id)
    if not isinstance(symbol, str) or _SOURCE_SYMBOL.fullmatch(symbol) is None:
        raise SourceStockChangeContextError(
            "D0_SOURCE_SYMBOL_INVALID",
            "精确来源股票代码须为SH./SZ.加六位数字。",
            category="ARGUMENT",
        )
    if any(
        not isinstance(value, str) or _SHA256.fullmatch(value) is None
        for value in (before_package_sha256, package_sha256)
    ):
        raise SourceStockChangeContextError(
            "D0_CHANGE_PACKAGE_SHA_INVALID",
            "前后来源包必须显式绑定小写64位SHA-256。",
            category="ARGUMENT",
        )
    profile_requested = _validate_profile_identity(
        before_profile_revision=before_profile_revision,
        after_profile_revision=after_profile_revision,
    )

    d0 = query_source_d0_raw_launch(
        config,
        scenario="LAUNCH_CONFIRM",
        action="changes",
        before_session=before_session,
        before_source_set_id=before_source_set_id,
        before_package_sha256=before_package_sha256,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_set_id=source_set_id,
        package_sha256=package_sha256,
        symbol=symbol,
        transition="ALL",
        offset=0,
        limit=1,
        expected_namespace_id=namespace_id,
    )
    for endpoint_name in ("before", "after"):
        endpoint = d0.get(endpoint_name)
        if (
            not isinstance(endpoint, Mapping)
            or endpoint.get("namespace_id") != namespace_id
        ):
            raise SourceStockChangeContextError(
                "SOURCE_CHANGE_NAMESPACE_MISMATCH",
                f"D0 {endpoint_name}端实际namespace与请求绑定不一致。",
            )
    rows = d0.get("results")
    if not isinstance(rows, list) or len(rows) != 1:
        raise SourceStockChangeContextError(
            "D0_SOURCE_SYMBOL_NOT_IN_PAIR",
            "来源股票代码未出现在前后任一封存来源池，无法构造单股变化上下文。",
            category="NOT_FOUND",
        )
    change = rows[0]
    if (
        not isinstance(change, Mapping)
        or change.get("source_symbol") != symbol
        or change.get("market") != symbol[:2]
    ):
        raise SourceStockChangeContextError(
            "D0_SOURCE_SYMBOL_RESULT_MISMATCH",
            "D0变化结果与显式来源代码不一致。",
        )

    profile_change: dict[str, Any]
    if not profile_requested:
        profile_change = {"status": "NOT_REQUESTED"}
    else:
        try:
            profile_result = query_source_symbol_price_profile_changes(
                config,
                before_session=before_session,
                after_session=target_session,
                max_target_session=research_cutoff,
                offset=0,
                limit=1,
                source_symbol=symbol,
                before_source_set_id=before_source_set_id,
                before_package_sha256=before_package_sha256,
                before_profile_revision=before_profile_revision,
                after_source_set_id=source_set_id,
                after_package_sha256=package_sha256,
                after_profile_revision=after_profile_revision,
                expected_namespace_id=namespace_id,
            )
        except PgPriceProfileError as error:
            if getattr(error, "code", None) == "PROFILE_CHANGE_SNAPSHOT_MISSING":
                profile_change = {
                    "status": "NOT_AVAILABLE",
                    "reason_code": error.code,
                    "message": str(error),
                }
            else:
                raise SourceStockChangeContextError(
                    getattr(error, "code", "PROFILE_CHANGE_READ_FAILED"),
                    str(error),
                ) from error
        else:
            if profile_result.get("namespace_id") != namespace_id:
                raise SourceStockChangeContextError(
                    "PROFILE_CHANGE_NAMESPACE_MISMATCH",
                    "Profile变化读取的实际namespace与请求绑定不一致。",
                )
            expected_snapshots = (
                (
                    profile_result.get("before_snapshot"),
                    before_session,
                    before_source_set_id,
                    before_package_sha256,
                    before_profile_revision,
                ),
                (
                    profile_result.get("after_snapshot"),
                    target_session,
                    source_set_id,
                    package_sha256,
                    after_profile_revision,
                ),
            )
            for (
                snapshot,
                session,
                source_id,
                package_sha,
                revision,
            ) in expected_snapshots:
                if not isinstance(snapshot, Mapping) or any(
                    snapshot.get(key) != expected
                    for key, expected in (
                        ("target_session", session),
                        ("source_set_id", source_id),
                        ("package_sha256", package_sha),
                        ("profile_revision", revision),
                    )
                ):
                    raise SourceStockChangeContextError(
                        "PROFILE_CHANGE_SNAPSHOT_IDENTITY_MISMATCH",
                        "Profile变化返回的修订身份与显式请求不一致。",
                    )
            profile_rows = profile_result.get("rows")
            if not isinstance(profile_rows, list):
                raise SourceStockChangeContextError(
                    "PROFILE_CHANGE_RESULT_INVALID",
                    "Profile变化读取未返回合法行集合。",
                )
            matching_rows = [
                row
                for row in profile_rows
                if isinstance(row, Mapping) and row.get("source_symbol") == symbol
            ]
            if len(matching_rows) > 1:
                raise SourceStockChangeContextError(
                    "PROFILE_CHANGE_SOURCE_SYMBOL_DUPLICATED",
                    "精确Profile修订中单股变化结果重复。",
                )
            profile_change = (
                {
                    "status": "AVAILABLE",
                    "symbol_change_status": "CHANGED",
                    "result": profile_result,
                }
                if matching_rows
                else {
                    "status": "AVAILABLE",
                    "symbol_change_status": "NO_CHANGED_PROFILE_ROW",
                    "result": profile_result,
                }
            )

    return {
        "status": "AVAILABLE",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "scenario_role": "DIAGNOSTIC_ONLY",
        "formal_admission_status": "NOT_AUTHORIZED_SOURCE_SYMBOL_DIAGNOSTIC",
        "formal_evaluation_status": "NOT_EVALUATED",
        "namespace_id": namespace_id,
        "source_symbol": symbol,
        "before_session": before_session.isoformat(),
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "d0": {"result": d0, "symbol_change": change},
        "profile_change": profile_change,
    }


__all__ = ["SourceStockChangeContextError", "query_source_stock_change_context"]
