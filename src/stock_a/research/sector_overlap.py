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


def compute_sector_overlap(
    *,
    boards: Sequence[Mapping[str, Any]],
    priority_rows: Sequence[Mapping[str, Any]],
    previous_clusters: Sequence[Mapping[str, Any]] | None = None,
    jaccard_threshold: Fraction = Fraction(3, 5),
) -> dict[str, Any]:
    """按同类型合格板块计算 unique_share、连通分量簇、parent 追溯、代表及展示摘要。"""
    prio_by_key = {
        (str(p.get("board_kind", "")), str(p["board_id"])): p for p in priority_rows
    }
    by_kind: dict[str, list[Mapping[str, Any]]] = {"INDUSTRY": [], "CONCEPT": []}
    for b in boards:
        if b["raw_qualification"]["safety"]["state"] == "TRUE":
            kind = str(b["board_kind"])
            if kind in by_kind:
                by_kind[kind].append(b)

    unique_shares: dict[str, dict[str, Any]] = {}
    clusters: list[dict[str, Any]] = []
    overview_selected: dict[str, list[str]] = {"INDUSTRY": [], "CONCEPT": []}

    prev_by_kind: dict[str, list[Mapping[str, Any]]] = {"INDUSTRY": [], "CONCEPT": []}
    for pc in previous_clusters or ():
        prev_by_kind.setdefault(str(pc["board_kind"]), []).append(pc)

    for kind in ("INDUSTRY", "CONCEPT"):
        kind_boards = by_kind[kind]
        entity_sets: dict[str, frozenset[str] | None] = {}
        for b in kind_boards:
            bid = str(b["board_id"])
            slots = b["members"]
            if any(s.entity_id is None for s in slots):
                entity_sets[bid] = None
            else:
                entity_sets[bid] = frozenset(
                    s.entity_id for s in slots if s.entity_id is not None
                )

        # 计算 unique_share
        for b in kind_boards:
            bid = str(b["board_id"])
            own_set = entity_sets[bid]
            if own_set is None:
                unique_shares[bid] = {
                    "status": "UNKNOWN",
                    "value": None,
                    "reason": "UNMAPPED_MEMBER_PRESENT",
                }
                continue
            if not own_set:
                unique_shares[bid] = {
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
                unique_shares[bid] = {
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
            unique_shares[bid] = {
                "status": "OBSERVED",
                "value": Fraction(unique_count, len(own_set)),
                "reason": None,
            }

        # 概念做连通分量聚类，行业各自独立单簇（行业不混簇）
        board_ids = sorted(str(b["board_id"]) for b in kind_boards)
        adj: dict[str, set[str]] = {bid: set() for bid in board_ids}
        unknown_edges: list[tuple[str, str]] = []
        if kind == "CONCEPT":
            for i, a_id in enumerate(board_ids):
                for b_id in board_ids[i + 1 :]:
                    set_a = entity_sets[a_id]
                    set_b = entity_sets[b_id]
                    if set_a is None or set_b is None:
                        unknown_edges.append((a_id, b_id))
                        continue
                    j = compute_jaccard(set_a, set_b)
                    if j is not None and j >= jaccard_threshold:
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
                if set(pc["member_board_ids"]) & comp_set
            )
            cluster_unknown = idx in unknown_affected_comps

            # 按 Priority 选择代表；若存在 UNKNOWN Priority 候选则代表 UNKNOWN
            rankable_members = [
                prio_by_key[(kind, bid)]
                for bid in comp
                if (kind, bid) in prio_by_key
                and prio_by_key[(kind, bid)]["priority_status"] == "RANKABLE"
            ]
            has_unknown_prio = any(
                (kind, bid) in prio_by_key
                and prio_by_key[(kind, bid)]["priority_status"] == "UNKNOWN"
                for bid in comp
            )
            if has_unknown_prio:
                rep_id = None
                rep_status = "UNKNOWN"
            elif rankable_members:
                rankable_members.sort(key=lambda p: p["sort_key"])
                rep_id = str(rankable_members[0]["board_id"])
                rep_status = "SELECTED"
            else:
                rep_id = sorted(comp)[0]
                rep_status = "FALLBACK_ID"

            clusters.append(
                {
                    "cluster_id": cid,
                    "board_kind": kind,
                    "member_board_ids": comp,
                    "parent_cluster_ids": parent_ids,
                    "cluster_status": "UNKNOWN" if cluster_unknown else "KNOWN",
                    "representative_board_id": rep_id,
                    "representative_status": rep_status,
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
        "overview_selected": overview_selected,
    }
