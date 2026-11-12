export const d1MaterialState = {
  data: null,
  controller: null,
  request: 0,
  startedAt: 0,
  board: null,
  boardOffset: 0,
  stockOffset: 0,
  memberOffset: 0,
};

export const byId = (id) => document.getElementById(id);

export function element(tag, text, className) {
  const item = document.createElement(tag);
  if (text !== undefined && text !== null) item.textContent = String(text);
  if (className) item.className = className;
  return item;
}

export function display(value) {
  if (value === undefined || value === null) return "—";
  if (typeof value === "object")
    return (
      value.decimal ??
      value.exact ??
      (value.numerator !== undefined
        ? `${value.numerator}/${value.denominator}`
        : JSON.stringify(value))
    );
  return String(value);
}

export function displayStockName(item) {
  return item?.display_name?.trim() || "名称未知";
}

export function displayStockCode(item, fallback = "") {
  const code = item?.source_symbol || item?.symbol || fallback;
  return typeof code === "string" ? code.replace(/^(SH|SZ|BJ)\./, "") : "";
}

export function displayStockTitle(item, fallback = "") {
  const code = displayStockCode(item, fallback);
  return `${displayStockName(item)}${code ? `（${code}）` : ""}`;
}

export function displayBoardName(item) {
  return item?.board_name?.trim() || "板块名称未知";
}

export function buildD1MaterialRequest(context, board, offsets, symbol = null) {
  return {
    target_session: context.target_session,
    research_cutoff: context.research_cutoff,
    source_package_id: context.source_package_id,
    d1_history_session_count: context.d1_history_session_count,
    industry_snapshot_id: context.industry_snapshot_id,
    concept_snapshot_id: context.concept_snapshot_id,
    structure_type: byId("d1StructureType").value,
    material_change_reason: byId("d1MaterialReason").value,
    board_kind: board?.board_kind || byId("boardKind").value,
    board_native_id: board?.board_native_id || null,
    board_offset: offsets.board,
    member_offset: offsets.member,
    stock_offset: offsets.stock,
    board_limit: 15,
    member_limit: 30,
    stock_limit: 30,
    board_search: byId("boardSearch").value.trim() || null,
    member_search: byId("memberSearch").value.trim() || null,
    member_group: byId("memberState").value,
    symbol,
  };
}
