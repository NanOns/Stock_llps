import { byId, display, element } from "./state.js";

export function renderD1Boards(data, openBoard) {
  const rows = byId("boardRows");
  rows.replaceChildren();
  for (const board of data.boards.summaries) {
    const row = element("tr");
    const name = element("td");
    name.append(element("strong", board.board_name));
    row.append(
      name,
      element("td", board.board_kind === "INDUSTRY" ? "行业" : "GN 概念"),
      element("td", display(board.snapshot_member_count)),
      element(
        "td",
        `${display(board.members_in_source_pool)} / ${display(board.members_outside_source_pool)}`,
      ),
      element("td", display(board.material_change_distinct_symbol_count)),
    );
    const action = element("td");
    const button = element("button", "查看成员 →", "text-button");
    button.type = "button";
    button.addEventListener("click", () => openBoard(board));
    action.append(button);
    row.append(action);
    rows.append(row);
  }
  if (!data.boards.summaries.length) {
    const row = element("tr");
    const cell = element("td", "当前筛选没有可展示的板块。", "empty");
    cell.colSpan = 6;
    row.append(cell);
    rows.append(row);
  }
  byId("boardSummary").textContent =
    `完整当前板块 ${display(data.boards.total_count)} 个 · 材料变化交集；成员快照 NON_PIT`;
  byId("boardPage").textContent =
    `偏移 ${data.boards.offset} · ${data.boards.summaries.length} 个`;
  byId("boardPrev").disabled = data.boards.offset === 0;
  byId("boardNext").disabled = data.boards.next_offset == null;
}

export function renderD1Members(data, openStock) {
  const board = data.boards.selected;
  if (!board) return;
  const members = data.boards.selected_members;
  byId("boardTitle").textContent = board.board_name;
  byId("boardMeta").textContent =
    `${board.board_kind === "INDUSTRY" ? "行业" : "GN 概念"} · 当前成员快照 NON_PIT · D1材料交集`;
  byId("boardCoverage").textContent =
    `当前成员 ${display(board.snapshot_member_count)} · 来源池内 ${display(board.members_in_source_pool)} · 池外 ${display(board.members_outside_source_pool)} · 筛选 ${display(data.boards.selected_member_count)} · 本页 ${members.length}`;
  const list = byId("memberList");
  list.replaceChildren();
  list.classList.toggle("has-items", !!members.length);
  if (!members.length) {
    list.textContent = "当前筛选没有成员；状态未知不代表正式未入选。";
  } else {
    for (const member of members) {
      const button = element("button", null, "list-row");
      button.type = "button";
      const left = element("span", null, "row-main");
      left.append(
        element("strong", member.source_symbol),
        element(
          "small",
          `${member.material_group} · ${member.source_pool_status} · NON_PIT`,
        ),
      );
      const right = element("span", null, "row-value");
      right.append(
        element(
          "span",
          display(member.selected_scope_summary?.material_change_anchor_count),
        ),
        element("small", "所选范围锚点"),
      );
      button.append(left, right, element("span", "›", "row-arrow"));
      button.addEventListener("click", () =>
        openStock(member.source_symbol, member),
      );
      list.append(button);
    }
  }
  byId("memberPage").textContent =
    `偏移 ${data.boards.member_offset} · 筛选 ${display(data.boards.selected_member_count)} 个成员`;
  byId("memberPrev").disabled = data.boards.member_offset === 0;
  byId("memberNext").disabled = data.boards.member_next_offset == null;
}
