"""读取固定白名单中的本机产品页面资源。"""

from pathlib import Path
import sys
from types import MappingProxyType
from typing import Mapping


_WORKSPACE = Path("G:/codex work/Stock A")
_ASSET_TYPES = {
    "index.html": "text/html; charset=utf-8",
    "app.js": "text/javascript; charset=utf-8",
    "app.css": "text/css; charset=utf-8",
}
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
        root = workspace / "web" / "product"
        allowed_root = workspace
    else:
        allowed_root = Path(sys.prefix).resolve(strict=True)
        root = allowed_root / "product"
    try:
        resolved_root = root.resolve(strict=True)
        resolved_root.relative_to(allowed_root)
    except (FileNotFoundError, OSError, ValueError) as error:
        raise PreviewAssetError("固定产品资源目录不存在或越界") from error
    if not resolved_root.is_dir():
        raise PreviewAssetError("固定产品资源路径不是目录")
    return resolved_root


def read_static_asset(name: str) -> tuple[bytes, str]:
    """按固定文件名读取页面资产，不支持任意路径或目录扫描。"""
    content_type = _ASSET_TYPES.get(name)
    if content_type is None:
        raise PreviewAssetError("产品资源不在固定白名单中")
    root = _asset_root()
    candidate = root / name
    try:
        if candidate.is_symlink():
            raise PreviewAssetError("产品资源不能是符号链接")
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except PreviewAssetError:
        raise
    except (FileNotFoundError, OSError, ValueError) as error:
        raise PreviewAssetError("固定产品资源缺失或越界") from error
    if not resolved.is_file():
        raise PreviewAssetError("产品资源不是普通文件")
    return resolved.read_bytes(), content_type
