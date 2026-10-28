"""来源封口身份键预检；不认证证据，也不批准封口或发布。"""

from collections.abc import Mapping

from stock_a.domain import ContractError


class SourceSealValidationError(ContractError):
    """来源封口预检失败，附稳定错误码。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


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
