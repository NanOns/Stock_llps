"""共享的三态归约及Seed形态语义。"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _reasons(values: tuple[Mapping[str, Any], ...]) -> list[str]:
    return list(
        dict.fromkeys(
            reason
            for value in values
            for reason in (
                [value.get("reason")]
                if value.get("reason")
                else value.get("reasons", [])
            )
            if reason
        )
    )


def tri_and(*values: Mapping[str, Any]) -> dict[str, Any]:
    """UNKNOWN保留；任一FALSE支配三态AND。"""
    state = (
        "FALSE"
        if any(value["state"] == "FALSE" for value in values)
        else "UNKNOWN"
        if any(value["state"] == "UNKNOWN" for value in values)
        else "TRUE"
    )
    return {"state": state, "reasons": _reasons(values)}


def tri_or(*values: Mapping[str, Any]) -> dict[str, Any]:
    """TRUE支配三态OR；无TRUE时UNKNOWN支配FALSE。"""
    state = (
        "TRUE"
        if any(value["state"] == "TRUE" for value in values)
        else "UNKNOWN"
        if any(value["state"] == "UNKNOWN" for value in values)
        else "FALSE"
    )
    return {"state": state, "reasons": _reasons(values)}


def reduce_source_seed_shape(
    s1: Mapping[str, Any], s2: Mapping[str, Any]
) -> dict[str, Any]:
    """用现役三态OR归约来源形态S1 OR S2。"""
    return tri_or(s1, s2)


def reduce_seed_with_safety(
    safety: Mapping[str, Any], source_seed_shape: Mapping[str, Any]
) -> dict[str, Any]:
    """用现役三态AND归约安全门与来源形态。"""
    return tri_and(safety, source_seed_shape)


__all__ = [
    "reduce_seed_with_safety",
    "reduce_source_seed_shape",
    "tri_and",
    "tri_or",
]
