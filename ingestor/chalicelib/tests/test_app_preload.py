"""Checks that app.py loads each Lambda function's modules during Init, and only those."""

import os
import subprocess
import sys
from pathlib import Path

INGESTOR_DIR = Path(__file__).resolve().parents[2]


def _run(code: str, handler: str | None = None) -> str:
    env = {k: v for k, v in os.environ.items() if k != "_HANDLER"}
    if handler:
        env["_HANDLER"] = handler
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=INGESTOR_DIR, env=env, capture_output=True, text=True, check=True
    )
    return result.stdout


def _loaded_after_import(handler: str | None) -> set[str]:
    return set(_run("import sys, app; print(' '.join(sorted(sys.modules)))", handler).split())


def test_init_preloads_only_the_functions_modules():
    loaded = _loaded_after_import("app.bb_store_station_status")
    assert "chalicelib.bluebikes" in loaded
    assert "chalicelib.daily_speeds" not in loaded
    assert "pandas" not in loaded


def test_init_preloads_heavy_modules_when_the_function_needs_them():
    loaded = _loaded_after_import("app.update_agg_trip_metrics")
    assert {"chalicelib.agg_speed_tables", "pandas"} <= loaded


def test_init_preloads_pandas_for_bluebikes_daily_jobs():
    # bluebikes imports pandas inside its functions, so these handlers import it for the preload
    for handler in ("app.bb_store_station_info", "app.bb_calc_daily_stats"):
        assert {"chalicelib.bluebikes", "pandas"} <= _loaded_after_import(handler)


def test_init_preloads_from_module_import_function():
    # store_overview_data does `from chalicelib.overview.build import store_overview`
    assert "chalicelib.overview.build" in _loaded_after_import("app.store_overview_data")


def test_no_preload_outside_lambda():
    loaded = _loaded_after_import(None)
    assert not {m for m in loaded if m.startswith("chalicelib.")}
    assert _loaded_after_import("app.not_a_function") == loaded


def test_every_handlers_imports_preload():
    """Runs each handler's preload, so a broken import fails here rather than at Init in Lambda."""
    code = (
        "import ast, app\n"
        "names = [n.name for n in ast.parse(open('app.py').read()).body if isinstance(n, ast.FunctionDef)]\n"
        "handlers = [n for n in names if n != 'preload_handler_imports']\n"
        "for name in handlers:\n"
        "    app.preload_handler_imports(name)\n"
        "print(len(handlers))\n"
    )
    assert int(_run(code)) >= 20
