"""Stock A本地Git准入维护器；持续用于提交/推送，不提交Git、不进入业务依赖。"""

from __future__ import annotations

import ast
import os
import json
from pathlib import PurePosixPath
import re
import subprocess
import sys

os.environ.update(
    TMP="G:/codex_tmp",
    TEMP="G:/codex_tmp",
    TMPDIR="G:/codex_tmp",
    PYTHONUTF8="1",
    PYTHONDONTWRITEBYTECODE="1",
)

ROOT_FILES = {
    ".gitignore",
    ".gitattributes",
    "AGENTS.md",
    "PROJECT_SPEC.md",
    "README.md",
    "pyproject.toml",
    "uv.lock",
    "requirements.txt",
    "package.json",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "tsconfig.json",
    "vite.config.ts",
}
EXTENSIONS = {
    "src": {".py", ".pyi", ".js", ".jsx", ".ts", ".tsx", ".css", ".html", ".json"},
    "web": {".js", ".jsx", ".ts", ".tsx", ".css", ".html", ".json", ".svg"},
    "config": {".json", ".toml", ".yaml", ".yml"},
    "migrations": {".sql"},
    "resources": {
        ".svg",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".ico",
        ".woff",
        ".woff2",
        ".css",
    },
}
DENIED_PARTS = {
    "tests",
    "test",
    "__tests__",
    "__snapshots__",
    "fixtures",
    "fixture",
    "mocks",
    "scripts",
    "evidence",
    "artifacts",
    "outputs",
    "output",
    "data",
    "runtime",
    "logs",
    "cache",
    "__pycache__",
    "node_modules",
    "dist",
    "build",
    "coverage",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    ".idea",
    ".vscode",
}
DENIED_TEST_NAME = re.compile(r"^(?:test_|conftest\.)|_test\.|\.test\.|\.spec\.", re.I)
DENIED_PROCESS_NAME = re.compile(
    r"阶段任务|阶段报告|任务卡|执行报告|验收报告|测试|截图|快照"
)
DENIED_RECORD_NAME = re.compile(
    r"(?:^|[_-])(?:report|receipt|results|snapshot)(?:[_-].*)?$", re.I
)
FIXED_ISO_DATE = re.compile(
    r"(?<![0-9])20[0-9]{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])(?![0-9])"
)
FIXED_DATE_CALL = re.compile(
    r"\b(?:date|datetime)\(\s*20[0-9]{2}\s*,\s*(?:[1-9]|1[0-2])\s*,\s*(?:[1-9]|[12][0-9]|3[01])\s*\)"
)
FIXED_SYMBOL = re.compile(r"\b(?:SH|SZ)[._ ]?[0-9]{6}\b", re.I)
BARE_FIXED_SYMBOL = re.compile(
    r"(?i)\b(?:symbol|security|stock|证券|股票|来源代码|证券代码|代码)\b.{0,48}(?<![0-9])[0-9]{6}(?![0-9])"
)
FIXED_BOARD_SELECTOR = re.compile(
    r"""(?i)\b(?:board_filter|board_scope|default_board|selected_board)\b\s*=\s*["'](?:MAIN_BOARD|GEM|创业板|主板)["']"""
)
FIXED_BOARD_BRANCH = re.compile(
    r"""(?im)^\s*(?:if|elif)\s+(?:args\.)?board\s*==\s*["'](?:MAIN_BOARD|GEM|创业板|主板)["']\s*:"""
)
DAILY_FILE = re.compile(r"""(?ix)
    (?:
        \bdaily(?:[_-][a-z0-9]+)*\b
        |\btrade[_-]?date(?:[_-][a-z0-9]+)*\b
        |\btarget[_-]?session(?:[_-][a-z0-9]+)*\b
        |\bsession[_-]?date(?:[_-][a-z0-9]+)*\b
    )
    [^\r\n]{0,120}?
    \\?\.(?:json|csv|parquet|sqlite|db)\b
""")
BUSINESS_SUFFIXES = {
    ".py",
    ".pyi",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".css",
    ".html",
    ".json",
    ".toml",
    ".yaml",
    ".yml",
}

DRIVE_PATH = re.compile(r"""(?i)(?<![A-Za-z0-9])([A-Z]:[\\/][^"'<>|;\r\n]+)""")
UNC_PATH = re.compile(
    r"""(?<![A-Za-z0-9:/])(?:\\\\|//)(?![?.])[^\\/\s"']+[\\/][^\s"'<>|]+"""
)
FORBIDDEN_EXTERNAL_PART = re.compile(
    r"(?i)Stock A 交付|过程资料|(?:^|[/\\])test[_ -]?temp(?:[/\\]|$)"
)
BUSINESS_FILE_HINT = re.compile(
    r"""(?ix)(?:response|archive|journal|checkpoint|resume|restore|capture|raw[_-]?source)"""
)
FORBIDDEN_CONFIG_PATH_ROLE = re.compile(
    r"""(?ix)(?:paths\s*\[\s*["'](?:delivery_root|release_root|runtime_data_root|private_configuration_root)["']\s*\]|(?:delivery_root|release_root|runtime_data_root|private_configuration_root))"""
)
PERSISTENT_SIDE_CAR_SUFFIXES = {
    ".jsonl",
    ".csv",
    ".parquet",
    ".sqlite",
    ".sqlite3",
    ".db",
    ".duckdb",
    ".feather",
    ".arrow",
    ".pkl",
    ".pickle",
}
FILE_IO_CALLS = {
    "open",
    "fdopen",
    "read_text",
    "read_bytes",
    "write_text",
    "write_bytes",
    "mkdir",
    "rename",
    "replace",
    "unlink",
    "glob",
    "rglob",
    "to_csv",
    "to_json",
    "to_parquet",
    "to_pickle",
    "read_csv",
    "read_json",
    "read_parquet",
    "read_pickle",
    "connect",
    "mkstemp",
    "NamedTemporaryFile",
    "TemporaryDirectory",
    "copyfile",
    "copy2",
    "move",
    "rmtree",
    "dump",
    "load",
}
ALLOWED_SOURCE_PATHS = {
    "src/stock_a/application/runtime_config.py": {
        "G:/codex work/Stock A",
        "G:/codex_tmp",
        "G:/Postgres/data",
        "G:/Postgres/data/pg_wal",
        "G:/Postgres/log",
        "G:/Postgres/backups",
    },
    "src/stock_a/storage/postgres/binding.py": {
        "G:/codex work/Stock A",
        "G:/codex_tmp",
        "G:/codex work/Stock A/config/runtime.defaults.json",
    },
    "src/stock_a/storage/postgres/identity.py": {
        "G:/codex work/Stock A/config/private/postgresql.credentials.json",
        "G:/codex work/Stock A/config/private",
        "G:/",
    },
    "src/stock_a/storage/postgres/source_raw.py": {"G:/codex_tmp"},
    "src/stock_a/sources/tdx/vipdata.py": {
        "G:/codex_tmp",
        "G:/codex_tmp/stock_a_vipdata",
    },
    "src/stock_a/application/cli_commands/parser.py": {"G:/codex_tmp"},
    "src/stock_a/service/static_assets.py": {"G:/codex work/Stock A"},
}
CONFIG_PATH_FIELDS = {
    "paths.workspace_root": "G:/codex work/Stock A",
    "paths.delivery_root": "G:/codex work/Stock A 交付",
    "paths.release_root": "G:/codex work/Stock A 交付/发布包",
    "paths.runtime_data_root": "G:/codex work/Stock A 交付/运行数据",
    "paths.private_configuration_root": "G:/codex work/Stock A/config/private",
    "paths.temporary_root": "G:/codex_tmp",
    "paths.configuration_root": "G:/codex work/Stock A/config",
    "postgresql.data_directory": "G:/Postgres/data",
    "postgresql.wal_directory": "G:/Postgres/data/pg_wal",
    "postgresql.log_directory": "G:/Postgres/log",
    "postgresql.manual_export_and_backup_directory": "G:/Postgres/backups",
    "source_policy.configured_tdx_source_roots.0": "D:/new_tdx",
    "configuration_policy.root": "G:/codex work/Stock A/config",
}


def _normalise_path(value: str) -> str:
    normalized = value.replace("\\\\", "/").replace("\\", "/").rstrip(" .,;:)]}")
    if len(normalized) > 3:
        normalized = normalized.rstrip("/")
    return normalized


def _external_path_errors(path: str, text: str) -> list[str]:
    allowed = ALLOWED_SOURCE_PATHS.get(path, set())
    errors = []
    for match in DRIVE_PATH.finditer(text):
        candidate = _normalise_path(match.group(1))
        if candidate not in allowed:
            errors.append(f"未登记的外部绝对路径：{candidate}")
    if UNC_PATH.search(text):
        errors.append("源码包含UNC或网络共享路径")
    return errors


def _config_path_errors(text: str) -> list[str]:
    try:
        value = json.loads(text)
    except (ValueError, UnicodeError):
        return ["运行配置无法解析，拒绝校验路径字段"]
    errors = []

    def visit(item, prefix: str) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                visit(child, f"{prefix}.{key}" if prefix else str(key))
        elif isinstance(item, list):
            for index, child in enumerate(item):
                visit(child, f"{prefix}.{index}")
        elif isinstance(item, str) and (
            DRIVE_PATH.search(item) or UNC_PATH.search(item)
        ):
            expected = CONFIG_PATH_FIELDS.get(prefix)
            if expected != item:
                errors.append(f"未登记的运行配置路径字段：{prefix}={item}")

    visit(value, "")
    return errors


def _call_name(node: ast.Call) -> str:
    function = node.func
    if isinstance(function, ast.Name):
        return function.id
    if isinstance(function, ast.Attribute):
        return function.attr
    return ""


def _file_io_path_errors(path: str, text: str) -> list[str]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []
    assignments: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            source = ast.get_source_segment(text, node.value) or ""
            for target in node.targets:
                if isinstance(target, ast.Name):
                    assignments[target.id] = source
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            assignments[node.target.id] = ast.get_source_segment(text, node.value) or ""

    def expand_source(source: str, seen: set[str] | None = None) -> str:
        seen = set() if seen is None else seen
        parts = [source]
        for name, value in assignments.items():
            if name not in seen and re.search(rf"\b{re.escape(name)}\b", source):
                parts.append(expand_source(value, seen | {name}))
        return "\n".join(parts)

    def expression_source(node: ast.AST) -> str:
        return expand_source(ast.get_source_segment(text, node) or "")

    errors = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _call_name(node) not in FILE_IO_CALLS:
            continue
        candidates = [expression_source(arg) for arg in node.args]
        candidates.extend(expression_source(keyword.value) for keyword in node.keywords)
        if isinstance(node.func, ast.Attribute):
            candidates.append(expression_source(node.func.value))
        for candidate in candidates:
            normalized = candidate.replace("\\", "/")
            errors.update(_external_path_errors(path, candidate))
            if FORBIDDEN_CONFIG_PATH_ROLE.search(candidate):
                errors.add("业务文件I/O使用交付、运行数据或私密配置路径字段")
            suffixes = {
                suffix
                for suffix in PERSISTENT_SIDE_CAR_SUFFIXES
                if re.search(re.escape(suffix) + r"(?![A-Za-z0-9])", normalized, re.I)
            }
            if suffixes:
                errors.add(
                    "业务文件I/O引用每日旁路格式：" + ", ".join(sorted(suffixes))
                )
            if re.search(r"\.json(?![A-Za-z0-9])", normalized, re.I) and (
                DAILY_FILE.search(normalized) or BUSINESS_FILE_HINT.search(normalized)
            ):
                errors.add("业务文件I/O引用每日或响应JSON旁路")
            if FORBIDDEN_EXTERNAL_PART.search(normalized):
                errors.add("业务文件I/O指向交付、过程资料或测试目录")
    return sorted(errors)


def business_content_errors(path: str, raw: bytes) -> list[str]:
    """检查现役源码/配置中的固定样例及每日旁路文件引用。"""
    if not path.startswith(("src/", "web/", "config/")):
        return []
    if PurePosixPath(path).suffix.casefold() not in BUSINESS_SUFFIXES:
        return []
    if path.startswith("src/stock_a/research/calendars/cn_equity_sessions_"):
        # 仅对正式交易所日历允许固定交易日期事实。
        date_exempt = True
    else:
        date_exempt = False
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeError:
        return ["业务文本不是有效UTF-8"]
    errors = []
    if not date_exempt and (
        FIXED_ISO_DATE.search(text) or FIXED_DATE_CALL.search(text)
    ):
        errors.append("固定交易日期进入现役源码/配置")
    if FIXED_SYMBOL.search(text) or any(
        BARE_FIXED_SYMBOL.search(line) for line in text.splitlines()
    ):
        errors.append("固定证券样例进入现役源码/配置")
    if FIXED_BOARD_SELECTOR.search(text) or FIXED_BOARD_BRANCH.search(text):
        errors.append("固定板别进入现役源码/配置")
    if path == "config/runtime.defaults.json":
        errors.extend(_config_path_errors(text))
    if path.endswith((".py", ".pyi")):
        errors.extend(_file_io_path_errors(path, text))
    elif path.startswith("web/"):
        errors.extend(_external_path_errors(path, text))
        if re.search(
            r"""(?is)(?:fetch|XMLHttpRequest|FileReader|sendBeacon)\s*\([^)]*?\.(?:jsonl|csv|parquet|sqlite|db|duckdb|feather|arrow)\b""",
            text,
        ):
            errors.append("页面API路径引用每日旁路文件格式")
    return errors


def path_error(path: str) -> str | None:
    p = PurePosixPath(path)
    if p.is_absolute() or ".." in p.parts or "\\" in path:
        return "路径不合法"
    if path in ROOT_FILES:
        return None
    if path == "devtools/stock_a_git_guard.py":
        return None
    parts = p.parts
    lower = [x.casefold() for x in parts]
    if not parts or any(x in DENIED_PARTS for x in lower):
        return "测试、过程材料、数据、产物或缓存路径禁止提交"
    if parts[0] == "config" and len(parts) > 1 and lower[1] == "private":
        return "私密配置禁止提交"
    if DENIED_TEST_NAME.search(p.name) or DENIED_PROCESS_NAME.search(p.name):
        return "测试或阶段材料命名禁止提交"
    if p.suffix.casefold() in {".md", ".json", ".html", ".xml"} and (
        DENIED_RECORD_NAME.search(p.stem) or p.stem.endswith(("回执", "报告"))
    ):
        return "执行报告、回执、快照或结果文件禁止提交"
    if parts[0] == "docs":
        if (
            len(parts) >= 3
            and parts[1] in {"requirements", "design", "changes"}
            and p.suffix == ".md"
        ):
            return None
        return "文档仅允许正式需求、设计和明确需求变更"
    if parts[0] in EXTENSIONS and len(parts) >= 2 and p.suffix in EXTENSIONS[parts[0]]:
        return None
    return "不在业务与正式规范白名单内"


def git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args], stderr=subprocess.PIPE)


def index_records():
    for raw in git("ls-files", "--stage", "-z").split(b"\0"):
        if not raw:
            continue
        meta, path = raw.split(b"\t", 1)
        mode, oid, stage = meta.decode("ascii").split()
        yield path.decode("utf-8"), mode, stage


def tree_records(commit: str):
    for raw in git("ls-tree", "-rz", commit).split(b"\0"):
        if not raw:
            continue
        meta, path = raw.split(b"\t", 1)
        mode, kind, oid = meta.decode("ascii").split()
        yield path.decode("utf-8"), mode, "0"


def check_records(
    records, errors: set[str], read_blob=None, *, allow_legacy_catalog=False
):
    records = list(records)
    allowed_config = None
    if read_blob is not None:
        try:
            defaults = json.loads(read_blob("config/runtime.defaults.json"))
            policy = defaults.get("configuration_policy")
            if policy is None and allow_legacy_catalog:
                # 兼容规则生效前的两份既有公共配置历史，不放行任意历史配置。
                allowed_config = {
                    "config/runtime.defaults.json",
                    "config/contracts/day-chain.contract.json",
                }
            else:
                if (
                    not policy
                    or policy["root"] != "G:/codex work/Stock A/config"
                    or not policy["root_only"]
                    or policy.get("test_configuration_or_fixture_allowed") is not False
                    or policy.get("unknown_files") != "REJECT"
                ):
                    raise ValueError("配置根或目录缺失")
                allowed_config = set()
                for entry in policy["public_files"]:
                    path = "config/" + entry["path"]
                    if (
                        path_error(path)
                        or entry["role"]
                        not in {
                            "BUSINESS_RUNTIME_CONFIGURATION",
                            "BUSINESS_PIPELINE_DESIGN_CONTRACT",
                            "BUSINESS_PIPELINE_DESIGN_CONTRACT_MODULE",
                            "BUSINESS_PARAMETER_CONTRACT",
                            "BUSINESS_SOURCE_CONTRACT",
                            "BUSINESS_CONFIGURATION_SCHEMA",
                            "PRICE_PROFILE_SEMANTICS_CONTRACT",
                            "PRICE_PROFILE_SEMANTICS_MODULE",
                        }
                        or not entry.get("contract")
                        or not entry.get("consumer")
                    ):
                        raise ValueError("公共配置登记不合法")
                    if path in allowed_config:
                        raise ValueError("公共配置重复登记")
                    allowed_config.add(path)
                present = {p for p, _, _ in records}
                if not allowed_config <= present:
                    raise ValueError("公共配置登记引用不存在的文件")
        except (
            KeyError,
            TypeError,
            ValueError,
            UnicodeError,
            subprocess.CalledProcessError,
        ):
            errors.add(
                "config/runtime.defaults.json：配置根/公共文件角色目录无法校验，拒绝提交/推送"
            )
            allowed_config = set()
    for path, mode, stage in records:
        reason = path_error(path)
        if read_blob is not None and path.startswith("config/") and not reason:
            if path not in allowed_config:
                reason = "未在唯一公共业务配置目录登记，禁止测试/任务配置混入"
            else:
                try:
                    if path.endswith(".json"):
                        obj = json.loads(read_blob(path))
                        if isinstance(obj, dict) and any(
                            k in obj
                            for k in (
                                "test_cases",
                                "fixtures",
                                "oracle",
                                "expected_results",
                            )
                        ):
                            reason = "公共配置包含测试向量/fixture/Oracle内容"
                except (ValueError, UnicodeError, subprocess.CalledProcessError):
                    reason = "已登记公共JSON配置无法解析"
        if read_blob is not None and path.startswith("docs/") and not reason:
            lines = read_blob(path).decode("utf-8-sig").splitlines()
            heading = lines[0] if lines else ""
            if re.search(
                r"测试(?:报告|方案|计划|记录|说明)|阶段(?:任务|报告)|回执|执行报告|验收报告|test\s+(?:plan|report)",
                heading,
                re.I,
            ):
                reason = "实际测试/阶段材料标题不能包装为正式业务设计"
        if mode not in {"100644", "100755"}:
            reason = "禁止链接/子模块绕过当前业务工作空间"
        if stage != "0":
            reason = "存在未解决的索引冲突"
        if reason:
            errors.add(f"{path}：{reason}")
        if read_blob is not None and not reason:
            try:
                content_errors = business_content_errors(path, read_blob(path))
            except (UnicodeError, subprocess.CalledProcessError):
                content_errors = ["无法读取业务内容"]
            for content_error in content_errors:
                errors.add(f"{path}：{content_error}")


def check_business_root(root_path: str, errors: set[str]) -> None:
    root = os.path.abspath(root_path)
    for family in ("src", "web", "config"):
        base = os.path.join(root, family)
        if not os.path.isdir(base):
            continue
        for current, directories, files in os.walk(base, followlinks=False):
            directories[:] = [
                name
                for name in directories
                if not os.path.islink(os.path.join(current, name))
            ]
            for name in files:
                full_path = os.path.join(current, name)
                if os.path.islink(full_path):
                    continue
                relative = os.path.relpath(full_path, root).replace("\\", "/")
                try:
                    raw = open(full_path, "rb").read()
                except OSError:
                    errors.add(f"{relative}：无法读取业务内容")
                    continue
                for content_error in business_content_errors(relative, raw):
                    errors.add(f"{relative}：{content_error}")


def main():
    errors: set[str] = set()
    root = (
        git("rev-parse", "--show-toplevel").decode("utf-8").strip().replace("\\", "/")
    )
    if root.casefold() != "g:/codex work/stock a":
        raise RuntimeError("Stock A准入只适用于批准的唯一工作空间")
    mode = sys.argv[1] if len(sys.argv) > 1 else "index"
    if mode == "business-check":
        if len(sys.argv) != 4 or sys.argv[2] != "--root":
            raise RuntimeError("business-check必须提供--root目录")
        check_business_root(sys.argv[3], errors)
    elif mode == "index":
        check_records(index_records(), errors, lambda p: git("show", ":" + p))
    elif mode == "pre-push":
        for line in sys.stdin:
            fields = line.strip().split()
            if len(fields) != 4:
                raise RuntimeError("Git推送引用格式无效")
            local_ref, local_sha, remote_ref, remote_sha = fields
            if set(local_sha) == {"0"}:
                continue
            check_records(
                tree_records(local_sha),
                errors,
                lambda p: git("show", local_sha + ":" + p),
            )
            revision = (
                local_sha if set(remote_sha) == {"0"} else remote_sha + ".." + local_sha
            )
            for commit in git("rev-list", revision).decode("ascii").splitlines():
                check_records(
                    tree_records(commit),
                    errors,
                    lambda p, c=commit: git("show", c + ":" + p),
                    allow_legacy_catalog=True,
                )
    else:
        raise RuntimeError("未知Git准入模式")
    if errors:
        print(
            "Stock A Git准入拒绝：仅允许业务代码/运行必需组成和正式需求、设计、需求变更。",
            file=sys.stderr,
        )
        for line in sorted(errors):
            print(" - " + line, file=sys.stderr)
        print(
            "请移出暂存区，按AGENTS将测试或过程材料保留在外部目录。禁止force-add/no-verify绕过。",
            file=sys.stderr,
        )
        return 1
    print("Stock A Git准入：通过路径/类型及动态日期、固定证券/板别和每日旁路文件检查。")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, subprocess.CalledProcessError, RuntimeError, UnicodeError) as exc:
        print(
            "Stock A Git准入未能完成：" + type(exc).__name__ + "；拒绝提交/推送。",
            file=sys.stderr,
        )
        raise SystemExit(1)
