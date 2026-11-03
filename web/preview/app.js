/* Stock A预览页面app职责模块；保持原业务逻辑和交互。 */
"use strict";
renderComparisonBasket();
void (async () => {
  await loadCatalog();
  await loadProfileSnapshots();
})();
