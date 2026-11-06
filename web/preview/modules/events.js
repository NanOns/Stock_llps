/* Stock A预览页面events职责模块；保持原业务逻辑和交互。 */
"use strict";
$("reloadCatalog").addEventListener("click", async () => {
  await loadCatalog();
  await loadProfileSnapshots();
});
$("versionSelect").addEventListener("change", () => {
  state.offset = 0;
  state.stockRequest++;
  if (!state.timelineNavigation) invalidateTimeline();
  $("queryStock").disabled = false;
  clearResult();
  loadPage();
});
for (const id of ["symbolInput", "windowInput"]) {
  $(id).addEventListener("input", () => {
    state.stockRequest++;
    $("queryStock").disabled = false;
    if (!state.timelineNavigation) {
      invalidateTimeline();
      clearResult();
    }
  });
}
$("marketSelect").addEventListener("change", () => {
  state.offset = 0;
  loadPage();
});
$("boardSelect").addEventListener("change", () => {
  state.offset = 0;
  loadPage();
});
$("prevPage").addEventListener("click", () => {
  state.offset = Math.max(0, state.offset - state.limit);
  loadPage();
});
$("nextPage").addEventListener("click", () => {
  if (state.offset + state.limit < state.total) {
    state.offset += state.limit;
    loadPage();
  }
});
$("stockForm").addEventListener("submit", queryStock);
$("queryTimeline").addEventListener("click", queryTimeline);
$("chartModeDay").addEventListener("click", () => setChartMode("day"));
$("chartModeWeek").addEventListener("click", () => setChartMode("week"));
$("chartModeMonth").addEventListener("click", () => setChartMode("month"));
$("searchProfiles").addEventListener("click", () => {
  submitProfileSearch();
});
$("profileSymbolInput").addEventListener("input", () => {
  state.profileOffset = 0;
  clearProfilePageResults(
    "来源代码输入已变化；旧结果已清除，请查询新的精确条件。",
  );
});
$("profileSymbolInput").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    submitProfileSearch();
  }
});
$("profileSnapshotSelect").addEventListener("change", () => {
  preserveComparisonOnSnapshotChange(
    selectedProfileSnapshot(),
    "完整PG价格画像快照",
  );
  scheduleProfileSearch();
});
for (const id of [
  "profileMarketSelect",
  "profileBoardSelect",
  "profileBias20AtrBandSelect",
  "profileBarStatusSelect",
  "profileWindowStatusSelect",
  "profileCoreFieldSelect",
  "profileCoreQualitySelect",
  "profilePosition1Select",
  "profilePosition2Select",
]) {
  $(id).addEventListener("change", () => {
    scheduleProfileSearch();
  });
}
$("profileFieldSelect").addEventListener("change", () => {
  state.profileOffset = 0;
  $("profileConditionSelect").value = "ALL";
  scheduleProfileSearch();
});
$("profileConditionSelect").addEventListener("change", () => {
  scheduleProfileSearch();
});
$("profilePrev").addEventListener("click", () => {
  state.profileOffset = Math.max(0, state.profileOffset - state.profileLimit);
  loadProfilePage();
});
$("profileNext").addEventListener("click", () => {
  if (state.profileOffset + state.profileLimit < state.profileTotal) {
    state.profileOffset += state.profileLimit;
    loadProfilePage();
  }
});
$("profileLast").addEventListener("click", () => {
  if (state.profileTotal) {
    state.profileOffset =
      Math.floor((state.profileTotal - 1) / state.profileLimit) *
      state.profileLimit;
    loadProfilePage();
  }
});
$("profileReturnFirst").addEventListener("click", () => {
  state.profileOffset = 0;
  loadProfilePage();
});
$("searchChanges").addEventListener("click", () => {
  state.changeOffset = 0;
  loadChanges();
});
$("searchPriceRelations").addEventListener("click", () => {
  state.relationOffset = 0;
  loadPriceRelations();
});
$("profileBreadthOpenFull").addEventListener("click", () => {
  const snapshot = state.profileBreadthData?.after_snapshot;
  if (!snapshot) return;
  const exact = state.profileSnapshots.find(
    (item) =>
      item.target_session === snapshot.target_session &&
      item.package_sha256 === snapshot.package_sha256 &&
      item.profile_revision === snapshot.profile_revision,
  );
  if (!exact) {
    $("profileMessage").textContent =
      "该精确结束快照不在当前PG目录中；已停止跳转，没有回退其它版本。";
    return;
  }
  $("profileSnapshotSelect").value = profileSnapshotKey(exact);
  $("profileSymbolInput").value = "";
  $("profileMarketSelect").value = "ALL";
  $("profileBoardSelect").value = "ALL";
  $("profileBarStatusSelect").value = "ALL";
  $("profileWindowStatusSelect").value = "ALL";
  state.profileOffset = 0;
  $("returnToSourceBreadth").hidden = false;
  $("profileBrowserTitle").scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
  loadProfilePage();
});
$("profileBreadthOpenPair").addEventListener("click", () => {
  $("changeSymbolInput").focus();
  $("changeSymbolInput").scrollIntoView({
    behavior: "smooth",
    block: "center",
  });
  $("changeSymbolInput").placeholder = "SH.或SZ.加六位数字";
});
$("profileBreadthMembersPrev").addEventListener("click", () => {
  const previous = Math.max(
    0,
    state.profileBreadthMemberOffset - state.profileBreadthMemberLimit,
  );
  if (state.profileBreadthMemberQuery)
    loadProfileBreadthMembers(state.profileBreadthMemberQuery, previous);
});
$("profileBreadthMembersNext").addEventListener("click", () => {
  const next =
    state.profileBreadthMemberOffset + state.profileBreadthMemberLimit;
  if (state.profileBreadthMemberQuery)
    loadProfileBreadthMembers(state.profileBreadthMemberQuery, next);
});
$("profileBreadthMembersLast").addEventListener("click", () => {
  const last = state.profileBreadthMemberData?.page.last_offset;
  if (last !== null && last !== undefined && state.profileBreadthMemberQuery)
    loadProfileBreadthMembers(state.profileBreadthMemberQuery, last);
});
$("returnToSourceBreadth").addEventListener("click", () => {
  $("sourceBreadthTitle").scrollIntoView({
    behavior: "smooth",
    block: "center",
  });
  $("returnToSourceBreadth").hidden = true;
});
$("compareProfilePair").addEventListener("click", compareSourceProfilePair);
$("changeSymbolInput").addEventListener("input", () => {
  state.pairRequest++;
  if (state.timelineContext?.origin === "change") invalidateTimeline();
  state.changeBinding = null;
  resetChangeDetail();
});
$("changeSymbolInput").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    compareSourceProfilePair();
  }
});
for (const id of ["changeBeforeSelect", "changeAfterSelect"]) {
  $(id).addEventListener("change", () => {
    if (id === "changeAfterSelect")
      preserveComparisonOnSnapshotChange(
        selectedChangeSnapshot(id),
        "两日结束快照",
      );
    state.changeOffset = 0;
    state.relationOffset = 0;
    clearRelationScreen(
      "两日快照已变化；旧RAW关系结果已清除，正在读取新绑定。",
    );
    loadChanges();
    loadPriceRelations();
  });
}
$("versionSelect").addEventListener("change", () => {
  const selected = selectedVersion();
  const binding = state.comparisonBasket[0];
  if (
    binding &&
    (!selected ||
      selected.target_session !== binding.target_session ||
      selected.package_sha256 !== binding.package_sha256)
  )
    clearComparisonBasket(
      "预览来源日期或包SHA已切换；旧临时对照已失效并清空。",
    );
});
$("compareSymbols").addEventListener("click", compareSelectedSymbols);
$("clearComparison").addEventListener("click", () => clearComparisonBasket());
function refreshComparisonEventHistory() {
  if (!state.comparisonTimelines.length) return;
  renderComparisonEventHistory(
    comparisonAxis(state.comparisonTimelines, state.comparisonBasket.length),
  );
}
$("comparisonEventWindow").addEventListener("change", () => {
  state.comparisonEventWindow = Number($("comparisonEventWindow").value);
  refreshComparisonEventHistory();
});
$("comparisonEventField").addEventListener("change", () => {
  state.comparisonEventField = $("comparisonEventField").value;
  refreshComparisonEventHistory();
});
$("comparisonEventKind").addEventListener("change", () => {
  state.comparisonEventKind = $("comparisonEventKind").value;
  refreshComparisonEventHistory();
});
$("comparisonPeriodKind").addEventListener("change", () => {
  state.comparisonPeriodKind = $("comparisonPeriodKind").value;
  renderComparisonPeriodResults();
});
$("changeTypeSelect").addEventListener("change", () => {
  state.changeOffset = 0;
  loadChanges();
});
$("changeProfileFieldSelect").addEventListener("change", () => {
  const fieldId = $("changeProfileFieldSelect").value;
  const kindSelect = $("changeProfileKindSelect");
  kindSelect.disabled = !fieldId;
  kindSelect.value = fieldId ? "ANY_STATE_CHANGED" : "";
  state.changeOffset = 0;
  loadChanges();
});
$("changeProfileKindSelect").addEventListener("change", () => {
  state.changeOffset = 0;
  loadChanges();
});
for (const id of [
  "changeBeforeBias20AtrStateSelect",
  "changeAfterBias20AtrStateSelect",
]) {
  $(id).addEventListener("change", () => {
    state.changeOffset = 0;
    loadChanges();
  });
}
$("priceRelationFieldSelect").addEventListener("change", () => {
  state.relationOffset = 0;
  loadPriceRelations();
});
$("priceTransitionTypeSelect").addEventListener("change", () => {
  state.relationOffset = 0;
  loadPriceRelations();
});
$("ma20DirectionSelect").addEventListener("change", () => {
  state.relationOffset = 0;
  loadPriceRelations();
});
for (const [fieldId, statusId] of [
  ["endPosition1Field", "endPosition1Status"],
  ["endPosition2Field", "endPosition2Status"],
]) {
  $(fieldId).addEventListener("change", () => {
    const status = $(statusId);
    status.disabled = !$(fieldId).value;
    if (!$(fieldId).value) status.value = "";
    state.relationOffset = 0;
    loadPriceRelations();
  });
  $(statusId).addEventListener("change", () => {
    state.relationOffset = 0;
    loadPriceRelations();
  });
}
$("relationSymbolInput").addEventListener("input", () => {
  state.relationOffset = 0;
  clearRelationScreen("精确来源代码筛选已变化；点击“查询RAW关系”读取新组合。");
});
$("relationSymbolInput").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    state.relationOffset = 0;
    loadPriceRelations();
  }
});
$("priceRelationPrev").addEventListener("click", () => {
  state.relationOffset = Math.max(
    0,
    state.relationOffset - state.relationLimit,
  );
  loadPriceRelations();
});
$("priceRelationNext").addEventListener("click", () => {
  if (state.relationOffset + state.relationLimit < state.relationTotal) {
    state.relationOffset += state.relationLimit;
    loadPriceRelations();
  }
});
$("priceRelationLast").addEventListener("click", () => {
  if (state.relationTotal) {
    state.relationOffset =
      Math.floor((state.relationTotal - 1) / state.relationLimit) *
      state.relationLimit;
    loadPriceRelations();
  }
});
$("changePrev").addEventListener("click", () => {
  state.changeOffset = Math.max(0, state.changeOffset - state.changeLimit);
  loadChanges();
});
$("changeNext").addEventListener("click", () => {
  if (state.changeOffset + state.changeLimit < state.changeTotal) {
    state.changeOffset += state.changeLimit;
    loadChanges();
  }
});
$("changeLast").addEventListener("click", () => {
  if (state.changeTotal) {
    state.changeOffset =
      Math.floor((state.changeTotal - 1) / state.changeLimit) *
      state.changeLimit;
    loadChanges();
  }
});
$("priceRelationFieldFilter").addEventListener("change", () =>
  renderPriceRelationHistory(state.timeline?.price_relation_history || null),
);
$("ma20DirectionWindow").addEventListener("change", () =>
  renderMa20DirectionHistory(state.timeline?.ma20_direction_history || null),
);
$("priceRelationKindFilter").addEventListener("change", () =>
  renderPriceRelationHistory(state.timeline?.price_relation_history || null),
);
$("showPriceRelationUnknown").addEventListener("change", () =>
  renderPriceRelationHistory(state.timeline?.price_relation_history || null),
);
