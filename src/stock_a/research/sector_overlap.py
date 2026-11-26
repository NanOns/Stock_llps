"""R03 正式 Overlap、连通分量簇、parent 追溯与代表选择纯核算法（ALG-25 / §11.5）。

严格遵循 §11.5：
- 仅对同类型（INDUSTRY 或 CONCEPT）且 safety=TRUE、成员完整的合格板块计算；
- J(A, B) = |M_A ∩ M_B| / |M_A ∪ M_B|，并集为 0 时 NA；
- unique_share(A) = |M_A \\ ∪(其它同类型合格板块)| / |M_A|；
- 概念边 J >= 0.6 构成连通分量，行业与概念绝不混簇；
- 未知边可能跨已知分量合并时，受影响簇标 UNKNOWN；
- 代表按板块 Priority 精确键与 stable_id 选择；总览每类最多 10 个、每簇最多 2 个摘要不改底层分母。
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping, Sequence
from fractions import Fraction
from typing import Any


def compute_jaccard(
    members_a: frozenset[str],
    members_b: frozenset[str],
) -> Fraction | None:
    """计算两板块实体集合 Jaccard 相似度；并集为空返回 None（NA）。"""
    union_size = len(members_a | members_b)
    if union_size == 0:
        return None
    return Fraction(len(members_a & members_b), union_size)


def _cluster_id(board_kind: str, board_ids: Sequence[str]) -> str:
    """按类型与稳定排序成员集合生成确定性业务簇标识（禁止使用 SHA）。"""
    return f"{board_kind}:" + "+".join(sorted(board_ids))


_EXHAUSTION_RANK = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}


def _unknown_prio_can_beat(
    board: Mapping[str, Any] | None,
    prio_row: Mapping[str, Any],
    best_sort_key: tuple[Any, ...],
) -> bool:
    """判断 priority_status=UNKNOWN 的成员在最乐观未知字段取值下是否可能超越当前最高 RANKABLE 成员。"""
    b_neg_emg, b_neg_dq5, b_neg_bd3, b_neg_md3, b_neg_sw, b_exh, b_key = best_sort_key
    board_data = board or {}

    emg_rank = (board_data.get("emergence") or {}).get("rank")
    if emg_rank is None:
        emg_rank = prio_row.get("emergence_rank")
    if emg_rank is None:
        return True
    u_neg_emg = -int(emg_rank)
    if u_neg_emg != b_neg_emg:
        return u_neg_emg < b_neg_emg

    for field_val, b_neg_val in (
        (board_data.get("dq5", prio_row.get("dq5")), b_neg_dq5),
        (board_data.get("breadth_delta3", prio_row.get("breadth_delta3")), b_neg_bd3),
        (board_data.get("ma20_delta3", prio_row.get("ma20_delta3")), b_neg_md3),
        (
            (board_data.get("seed_wilson") or {}).get(
                "point_value", prio_row.get("seed_wilson_point")
            ),
            b_neg_sw,
        ),
    ):
        if field_val is None:
            return True
        u_neg_val = -field_val
        if u_neg_val != b_neg_val:
            return bool(u_neg_val < b_neg_val)

    exh_level = (board_data.get("exhaustion") or {}).get("level") or prio_row.get(
        "exhaustion"
    )
    if exh_level not in _EXHAUSTION_RANK:
        return True
    u_exh = _EXHAUSTION_RANK[str(exh_level)]
    if u_exh != b_exh:
        return u_exh < b_exh

    kind = str(board_data.get("board_kind", prio_row.get("board_kind", "")))
    bid = str(board_data.get("board_id", prio_row.get("board_id", "")))
    return (kind, bid) < b_key


def compute_sector_overlap(
    *,
    boards: Sequence[Mapping[str, Any]],
    priority_rows: Sequence[Mapping[str, Any]],
    previous_clusters: Sequence[Mapping[str, Any]] | None = None,
    jaccard_threshold: Fraction = Fraction(3, 5),
) -> dict[str, Any]:
    """按同类型合格/安全未知板块计算 unique_share、连通分量簇、parent 追溯、代表及展示摘要。"""
    prio_by_key = {
        (str(p.get("board_kind", "")), str(p["board_id"])): p for p in priority_rows
    }
    by_kind: dict[str, list[Mapping[str, Any]]] = {"INDUSTRY": [], "CONCEPT": []}
    for b in boards:
        if b["raw_qualification"]["safety"]["state"] in {"TRUE", "UNKNOWN"}:
            kind = str(b["board_kind"])
            if kind in by_kind:
                by_kind[kind].append(b)

    unique_shares: dict[str, dict[str, Any]] = {}
    clusters: list[dict[str, Any]] = []
    pairs: list[dict[str, Any]] = []
    overview_selected: dict[str, list[str]] = {"INDUSTRY": [], "CONCEPT": []}

    prev_by_kind: dict[str, list[Mapping[str, Any]]] = {"INDUSTRY": [], "CONCEPT": []}
    for pc in previous_clusters or ():
        prev_by_kind.setdefault(str(pc["board_kind"]), []).append(pc)

    for kind in ("INDUSTRY", "CONCEPT"):
        kind_boards = by_kind[kind]
        board_by_id = {str(b["board_id"]): b for b in kind_boards}
        entity_sets: dict[str, frozenset[str] | None] = {}
        for b in kind_boards:
            bid = str(b["board_id"])
            safety_state = b["raw_qualification"]["safety"]["state"]
            slots = b["members"]
            if (
                safety_state != "TRUE"
                or not b.get("membership_complete", True)
                or any(s.entity_id is None for s in slots)
            ):
                entity_sets[bid] = None
            else:
                entity_sets[bid] = frozenset(
                    s.entity_id for s in slots if s.entity_id is not None
                )

        # 计算 unique_share
        for b in kind_boards:
            bid = str(b["board_id"])
            ukey = (kind, bid)
            own_set = entity_sets[bid]
            if own_set is None:
                unique_shares[ukey] = {
                    "status": "UNKNOWN",
                    "value": None,
                    "reason": (
                        "SAFETY_OR_MEMBERSHIP_UNKNOWN"
                        if b["raw_qualification"]["safety"]["state"] != "TRUE"
                        or not b.get("membership_complete", True)
                        else "UNMAPPED_MEMBER_PRESENT"
                    ),
                }
                continue
            if not own_set:
                unique_shares[ukey] = {
                    "status": "NA",
                    "value": None,
                    "reason": "EMPTY_BOARD",
                }
                continue
            others_unknown = any(
                entity_sets[str(o["board_id"])] is None
                for o in kind_boards
                if str(o["board_id"]) != bid
            )
            if others_unknown:
                unique_shares[ukey] = {
                    "status": "UNKNOWN",
                    "value": None,
                    "reason": "OTHER_BOARD_MEMBERSHIP_UNKNOWN",
                }
                continue
            other_union: set[str] = set()
            for o in kind_boards:
                oid = str(o["board_id"])
                if oid != bid and entity_sets[oid] is not None:
                    other_union.update(entity_sets[oid])  # type: ignore[arg-type]
            unique_count = len(own_set - other_union)
            unique_shares[ukey] = {
                "status": "OBSERVED",
                "value": Fraction(unique_count, len(own_set)),
                "reason": None,
            }

        # 概念做连通分量聚类，行业各自独立单簇（行业不混簇）
        board_ids = sorted(str(b["board_id"]) for b in kind_boards)
        adj: dict[str, set[str]] = {bid: set() for bid in board_ids}
        unknown_edges: list[tuple[str, str]] = []
        for i, a_id in enumerate(board_ids):
            for b_id in board_ids[i + 1 :]:
                set_a = entity_sets[a_id]
                set_b = entity_sets[b_id]
                if set_a is None or set_b is None:
                    pairs.append(
                        {
                            "board_kind": kind,
                            "board_id_a": a_id,
                            "board_id_b": b_id,
                            "jaccard_status": "UNKNOWN",
                            "jaccard_value": None,
                            "reason": "MEMBERSHIP_UNKNOWN",
                        }
                    )
                    if kind == "CONCEPT":
                        unknown_edges.append((a_id, b_id))
                    continue
                j = compute_jaccard(set_a, set_b)
                pairs.append(
                    {
                        "board_kind": kind,
                        "board_id_a": a_id,
                        "board_id_b": b_id,
                        "jaccard_status": "OBSERVED" if j is not None else "NA",
                        "jaccard_value": j,
                        "reason": None if j is not None else "EMPTY_UNION",
                    }
                )
                if kind == "CONCEPT" and j is not None and j >= jaccard_threshold:
                    adj[a_id].add(b_id)
                    adj[b_id].add(a_id)

        visited: set[str] = set()
        components: list[list[str]] = []
        comp_index: dict[str, int] = {}
        for bid in board_ids:
            if bid in visited:
                continue
            comp: list[str] = []
            queue = deque([bid])
            visited.add(bid)
            while queue:
                cur = queue.popleft()
                comp.append(cur)
                comp_index[cur] = len(components)
                for nxt in sorted(adj[cur]):
                    if nxt not in visited:
                        visited.add(nxt)
                        queue.append(nxt)
            components.append(sorted(comp))

        # 若未知边跨越两个不同已知分量，则这两个分量受影响且标 UNKNOWN
        unknown_affected_comps: set[int] = set()
        for a_id, b_id in unknown_edges:
            ca, cb = comp_index[a_id], comp_index[b_id]
            if ca != cb:
                unknown_affected_comps.add(ca)
                unknown_affected_comps.add(cb)

        for idx, comp in enumerate(components):
            cid = _cluster_id(kind, comp)
            comp_set = set(comp)
            parent_ids = sorted(
                str(pc["cluster_id"])
                for pc in prev_by_kind.get(kind, [])
                if str(pc["cluster_id"]) != cid
                and (set(pc["member_board_ids"]) & comp_set)
            )
            has_unknown_safety = any(
                board_by_id[bid]["raw_qualification"]["safety"]["state"] != "TRUE"
                for bid in comp
            )
            cluster_unknown = idx in unknown_affected_comps or has_unknown_safety
            unknown_edge_count = sum(
                1
                for a_id, b_id in unknown_edges
                if a_id in comp_set or b_id in comp_set
            )
            cluster_reason = (
                "UNKNOWN_EDGE_CROSSES_COMPONENTS"
                if idx in unknown_affected_comps
                else ("SAFETY_OR_MEMBERSHIP_UNKNOWN" if has_unknown_safety else None)
            )

            # 按 Priority 选择代表；未知边界簇或未知成员可能超越最高 RANKABLE 时代表 UNKNOWN
            rankable_members = [
                prio_by_key[(kind, bid)]
                for bid in comp
                if (kind, bid) in prio_by_key
                and prio_by_key[(kind, bid)]["priority_status"] == "RANKABLE"
            ]
            rankable_members.sort(key=lambda p: p["sort_key"])
            unknown_prio_members = [
                (bid, prio_by_key[(kind, bid)])
                for bid in comp
                if (kind, bid) in prio_by_key
                and prio_by_key[(kind, bid)]["priority_status"] == "UNKNOWN"
            ]
            best_sort_key = (
                rankable_members[0]["sort_key"] if rankable_members else None
            )
            unknown_can_win = bool(unknown_prio_members) and (
                best_sort_key is None
                or any(
                    _unknown_prio_can_beat(board_by_id.get(bid), pr, best_sort_key)
                    for bid, pr in unknown_prio_members
                )
            )
            if cluster_unknown:
                rep_id = None
                rep_status = "UNKNOWN"
                rep_reason = "CLUSTER_BOUNDARY_UNKNOWN"
            elif unknown_can_win:
                rep_id = None
                rep_status = "UNKNOWN"
                rep_reason = "PRIORITY_UNKNOWN"
            elif rankable_members:
                rep_id = str(rankable_members[0]["board_id"])
                rep_status = "SELECTED"
                rep_reason = None
            else:
                rep_id = sorted(comp)[0]
                rep_status = "FALLBACK_ID"
                rep_reason = "DIAGNOSTIC_FALLBACK_ONLY"

            clusters.append(
                {
                    "cluster_id": cid,
                    "board_kind": kind,
                    "member_board_ids": comp,
                    "parent_cluster_ids": parent_ids,
                    "cluster_status": "UNKNOWN" if cluster_unknown else "KNOWN",
                    "cluster_reason": cluster_reason,
                    "representative_board_id": rep_id,
                    "representative_status": rep_status,
                    "representative_reason": rep_reason,
                    "unknown_edge_count": unknown_edge_count,
                }
            )

        # 总览摘要视图：按原 Priority 排序，每类最多 10 个，已知概念簇最多 2 个
        ordered_kind = [
            p
            for p in priority_rows
            if str(p.get("board_kind", kind)) == kind
            and str(p["board_id"]) in set(board_ids)
            and p["priority_status"] == "RANKABLE"
        ]
        ordered_kind.sort(key=lambda p: p["sort_key"])
        cluster_by_board = {
            bid: c
            for c in clusters
            if c["board_kind"] == kind
            for bid in c["member_board_ids"]
        }
        cluster_counts: dict[str, int] = {}
        selected: list[str] = []
        for p in ordered_kind:
            if len(selected) >= 10:
                break
            bid = str(p["board_id"])
            c = cluster_by_board[bid]
            if kind == "CONCEPT" and c["cluster_status"] == "KNOWN":
                used = cluster_counts.get(c["cluster_id"], 0)
                if used >= 2:
                    continue
                cluster_counts[c["cluster_id"]] = used + 1
            selected.append(bid)
        overview_selected[kind] = selected

    return {
        "unique_shares": unique_shares,
        "clusters": clusters,
        "pairs": pairs,
        "overview_selected": overview_selected,
    }
