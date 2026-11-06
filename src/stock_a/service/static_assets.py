"""读取固定白名单中的本机预览页面资源。"""

from pathlib import Path
import sys
from types import MappingProxyType
from typing import Mapping


_WORKSPACE = Path("G:/codex work/Stock A")
_ASSET_TYPES = {
    "index.html": "text/html; charset=utf-8",
    "app.js": "text/javascript; charset=utf-8",
    "modules/app.js": "text/javascript; charset=utf-8",
    "app.css": "text/css; charset=utf-8",
    "styles/page.css": "text/css; charset=utf-8",
    "styles/source.css": "text/css; charset=utf-8",
    "styles/history.css": "text/css; charset=utf-8",
    "modules/shared.js": "text/javascript; charset=utf-8",
    "modules/source/catalog.js": "text/javascript; charset=utf-8",
    "modules/source/core_periods.js": "text/javascript; charset=utf-8",
    "modules/source/core_profile.js": "text/javascript; charset=utf-8",
    "modules/research/profile_comparison.js": "text/javascript; charset=utf-8",
    "modules/research/comparison_rendering.js": "text/javascript; charset=utf-8",
    "modules/research/comparison_history.js": "text/javascript; charset=utf-8",
    "modules/research/price_timeline.js": "text/javascript; charset=utf-8",
    "modules/research/history_charts.js": "text/javascript; charset=utf-8",
    "modules/research/profile_detail.js": "text/javascript; charset=utf-8",
    "modules/research/profile_breadth.js": "text/javascript; charset=utf-8",
    "modules/research/profile_search.js": "text/javascript; charset=utf-8",
    "modules/changes/highlights.js": "text/javascript; charset=utf-8",
    "modules/changes/detail.js": "text/javascript; charset=utf-8",
    "modules/changes/price_relations.js": "text/javascript; charset=utf-8",
    "modules/changes/list.js": "text/javascript; charset=utf-8",
    "modules/sector_change.js": "text/javascript; charset=utf-8",
    "modules/events.js": "text/javascript; charset=utf-8",
    "modules/app.js": "text/javascript; charset=utf-8",
}
_LEGACY_APP_MODULES = (
    "modules/shared.js",
    "modules/source/catalog.js",
    "modules/source/core_periods.js",
    "modules/source/core_profile.js",
    "modules/research/profile_comparison.js",
    "modules/research/comparison_rendering.js",
    "modules/research/comparison_history.js",
    "modules/research/price_timeline.js",
    "modules/research/history_charts.js",
    "modules/research/profile_detail.js",
    "modules/research/profile_breadth.js",
    "modules/research/profile_search.js",
    "modules/changes/highlights.js",
    "modules/changes/detail.js",
    "modules/changes/price_relations.js",
    "modules/changes/list.js",
    "modules/sector_change.js",
    "modules/events.js",
    "modules/app.js",
)
_ASSET_TARGETS = MappingProxyType({"modules/app.js": "app.js"})
_STATIC_ROUTES = MappingProxyType(
    {
        "/": "index.html",
        **{f"/{name}": name for name in _ASSET_TYPES if name != "index.html"},
    }
)


class PreviewAssetError(RuntimeError):
    """固定页面资源缺失、越界或被链接替换。"""


def static_asset_routes() -> Mapping[str, str]:
    """返回页面服务允许访问的精确静态路由。"""
    return _STATIC_ROUTES


def _asset_root() -> Path:
    module_path = Path(__file__).resolve(strict=True)
    try:
        workspace = _WORKSPACE.resolve(strict=True)
    except (FileNotFoundError, OSError):
        workspace = None
    if workspace is not None and module_path.is_relative_to(workspace):
        root = workspace / "web" / "preview"
        allowed_root = workspace
    else:
        allowed_root = Path(sys.prefix).resolve(strict=True)
        root = allowed_root / "preview"
    try:
        resolved_root = root.resolve(strict=True)
        resolved_root.relative_to(allowed_root)
    except (FileNotFoundError, OSError, ValueError) as error:
        raise PreviewAssetError("固定预览资源目录不存在或越界") from error
    if not resolved_root.is_dir():
        raise PreviewAssetError("固定预览资源路径不是目录")
    return resolved_root


def read_static_asset(name: str) -> tuple[bytes, str]:
    """按固定文件名读取页面资产，不支持任意路径或目录扫描。"""
    content_type = _ASSET_TYPES.get(name)
    if content_type is None:
        raise PreviewAssetError("预览资源不在固定白名单中")
    if name == "app.js":
        modules = [read_static_asset(module)[0] for module in _LEGACY_APP_MODULES]
        return b"\n".join(modules), content_type
    root = _asset_root()
    target_name = _ASSET_TARGETS.get(name, name)
    candidate = root / target_name
    try:
        if candidate.is_symlink():
            raise PreviewAssetError("预览资源不能是符号链接")
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except PreviewAssetError:
        raise
    except (FileNotFoundError, OSError, ValueError) as error:
        raise PreviewAssetError("固定预览资源缺失或越界") from error
    if not resolved.is_file():
        raise PreviewAssetError("预览资源不是普通文件")
    return resolved.read_bytes(), content_type
