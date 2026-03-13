from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from streamlit.testing.v1 import AppTest


TARGET_SPANS: dict[str, list[str]] = {
    "overview": [
        "main:overview:render_page",
        "render_exec:section:trend_chart",
        "main:prewarm:cross_view",
    ],
    "traffic": [
        "main:traffic:render_page",
        "render_traffic:section:channels",
        "main:prewarm:cross_view",
    ],
}


def _rows_to_map(rows: Any) -> dict[str, float]:
    out: dict[str, float] = {}
    if not isinstance(rows, list):
        return out
    for item in rows:
        if not isinstance(item, dict):
            continue
        span = str(item.get("span", "")).strip()
        if not span:
            continue
        try:
            out[span] = float(item.get("ms", 0.0))
        except Exception:
            continue
    return out


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(max(0, min(len(ordered) - 1, round((len(ordered) - 1) * p))))
    return float(ordered[idx])


def _stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {"n": 0, "min": 0.0, "avg": 0.0, "p95": 0.0, "max": 0.0}
    return {
        "n": float(len(values)),
        "min": float(min(values)),
        "avg": float(statistics.mean(values)),
        "p95": float(_percentile(values, 0.95)),
        "max": float(max(values)),
    }


def _run_single(script_path: str, timeout: float, username: str, password: str) -> tuple[dict[str, float], dict[str, float]]:
    app = AppTest.from_file(script_path, default_timeout=timeout)
    app.run(timeout=timeout)

    app.text_input(key="login_username").set_value(username)
    app.text_input(key="login_password").set_value(password)
    app.button[0].click()
    app.run(timeout=timeout)
    overview_map = _rows_to_map(app.session_state["dashboard_profile_last"] if "dashboard_profile_last" in app.session_state else [])

    app.button(key="nav_traffic_btn").click()
    app.run(timeout=timeout)
    traffic_map = _rows_to_map(app.session_state["dashboard_profile_last"] if "dashboard_profile_last" in app.session_state else [])
    return overview_map, traffic_map


def _build_summary(records: dict[str, list[dict[str, float]]], *, drop_first: int) -> dict[str, dict[str, dict[str, float]]]:
    summary: dict[str, dict[str, dict[str, float]]] = {}
    for view_name, rows in records.items():
        start_idx = max(0, int(drop_first))
        sampled = rows[start_idx:] if start_idx < len(rows) else []
        span_summary: dict[str, dict[str, float]] = {}
        for span in TARGET_SPANS.get(view_name, []):
            values = [float(r[span]) for r in sampled if span in r]
            span_summary[span] = _stats(values)
        summary[view_name] = span_summary
    return summary


def _print_summary(summary: dict[str, dict[str, dict[str, float]]], *, drop_first: int) -> None:
    print(f"drop_first={drop_first}")
    for view_name in ("overview", "traffic"):
        print(f"[{view_name}]")
        for span in TARGET_SPANS.get(view_name, []):
            row = summary.get(view_name, {}).get(span, {})
            print(
                f"  {span}: n={int(row.get('n', 0))} "
                f"min={row.get('min', 0.0):.1f}ms avg={row.get('avg', 0.0):.1f}ms "
                f"p95={row.get('p95', 0.0):.1f}ms max={row.get('max', 0.0):.1f}ms"
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Perf E2E del dashboard con Streamlit AppTest (Overview + Trafico).")
    parser.add_argument("--script", default="dashboard.py", help="Ruta del script Streamlit.")
    parser.add_argument("--iterations", type=int, default=6, help="Numero de iteraciones.")
    parser.add_argument("--timeout", type=float, default=60.0, help="Timeout por run de AppTest.")
    parser.add_argument("--username", default="admin", help="Usuario para login.")
    parser.add_argument("--password", default="AdminYAP2026!", help="Password para login.")
    parser.add_argument("--drop-first", type=int, default=1, help="Cuantas iteraciones iniciales omitir en resumen.")
    parser.add_argument("--output-json", default="", help="Ruta para guardar resumen en JSON.")
    args = parser.parse_args()

    script_path = Path(str(args.script)).resolve()
    os.chdir(str(script_path.parent))
    script_parent = str(script_path.parent)
    if script_parent not in sys.path:
        sys.path.insert(0, script_parent)
    existing_pythonpath = os.environ.get("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = script_parent if not existing_pythonpath else f"{script_parent}{os.pathsep}{existing_pythonpath}"

    os.environ["DASHBOARD_PROFILE"] = "1"
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")

    iterations = max(1, int(args.iterations))
    records: dict[str, list[dict[str, float]]] = {"overview": [], "traffic": []}

    for idx in range(iterations):
        overview_map, traffic_map = _run_single(str(script_path), args.timeout, args.username, args.password)
        records["overview"].append(overview_map)
        records["traffic"].append(traffic_map)
        ov = overview_map.get("main:overview:render_page", 0.0)
        tr = traffic_map.get("main:traffic:render_page", 0.0)
        print(f"iter={idx + 1}/{iterations} overview={ov:.1f}ms traffic={tr:.1f}ms")

    summary = _build_summary(records, drop_first=args.drop_first)
    _print_summary(summary, drop_first=args.drop_first)

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "script": str(script_path),
            "iterations": iterations,
            "drop_first": int(args.drop_first),
            "records": records,
            "summary": summary,
        }
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"saved_json={out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
