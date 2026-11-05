"""Checks that app.py loads each Lambda function's modules during Init, and only those."""

import ast
import importlib
import os
import subprocess
import sys
from pathlib import Path

INGESTOR_DIR = Path(__file__).resolve().parents[2]
APP_PY = INGESTOR_DIR / "app.py"


def _app_module():
    if str(INGESTOR_DIR) not in sys.path:
        sys.path.insert(0, str(INGESTOR_DIR))
    return importlib.import_module("app")


def _handler_imports() -> dict[str, tuple[str, ...]]:
    """The chalicelib modules each handler in app.py imports in its body."""
    result = {}
    for node in ast.parse(APP_PY.read_text()).body:
        if not isinstance(node, ast.FunctionDef):
            continue
        modules = []
        for stmt in node.body:
            if isinstance(stmt, ast.ImportFrom) and stmt.module and stmt.module.startswith("chalicelib"):
                if stmt.module == "chalicelib":
                    modules += [f"chalicelib.{alias.name}" for alias in stmt.names]
                else:
                    modules.append(stmt.module)
        if modules:
            result[node.name] = tuple(modules)
    return result


def test_handler_modules_matches_handler_imports():
    """HANDLER_MODULES must list exactly what each handler imports, or Init won't preload it."""
    expected = _handler_imports()
    actual = _app_module().HANDLER_MODULES
    assert set(actual) == set(expected), "add/remove the handler in app.HANDLER_MODULES"
    for name, modules in expected.items():
        assert sorted(actual[name]) == sorted(modules), f"app.HANDLER_MODULES[{name!r}] should be {modules}"


def test_every_preloaded_module_imports():
    for modules in _app_module().HANDLER_MODULES.values():
        for module in modules:
            importlib.import_module(module)


def _loaded_after_import(handler: str | None) -> set[str]:
    env = {k: v for k, v in os.environ.items() if k != "_HANDLER"}
    if handler:
        env["_HANDLER"] = handler
    code = "import sys, app; print(' '.join(sorted(sys.modules)))"
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=INGESTOR_DIR, env=env, capture_output=True, text=True, check=True
    )
    return set(result.stdout.split())


def test_init_preloads_only_the_functions_modules():
    loaded = _loaded_after_import("app.bb_store_station_status")
    assert "chalicelib.bluebikes" in loaded
    assert "chalicelib.daily_speeds" not in loaded
    assert "pandas" not in loaded


def test_init_preloads_heavy_modules_when_the_function_needs_them():
    loaded = _loaded_after_import("app.update_agg_trip_metrics")
    assert {"chalicelib.agg_speed_tables", "pandas"} <= loaded


def test_no_preload_outside_lambda():
    loaded = _loaded_after_import(None)
    assert not {m for m in loaded if m.startswith("chalicelib.")} - {"chalicelib"}
    assert _loaded_after_import("app.not_a_function") == loaded
