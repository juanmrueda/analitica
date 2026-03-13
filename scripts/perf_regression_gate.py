#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
BENCHMARK_SCRIPT = ROOT_DIR / "scripts" / "benchmark_dashboard_loaders.py"


def _extract_json_blob(text: str) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("No JSON object found in benchmark output.")
    return json.loads(text[start : end + 1])


def _load_benchmark_payload(report_path: Path, iterations: int) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(BENCHMARK_SCRIPT),
        "--report-path",
        str(report_path),
        "--iterations",
        str(max(int(iterations), 1)),
        "--mode",
        "compare",
        "--output",
        "json",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(ROOT_DIR), check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            "benchmark command failed\n"
            f"cmd={' '.join(cmd)}\n"
            f"stdout={proc.stdout}\n"
            f"stderr={proc.stderr}"
        )
    return _extract_json_blob(proc.stdout)


def _mode_result(payload: dict[str, Any], mode_name: str) -> dict[str, Any]:
    for result in payload.get("results", []):
        if str(result.get("mode", "")) == mode_name:
            return result
    raise KeyError(f"Mode '{mode_name}' not found in benchmark payload.")


def _check_limit(
    checks: list[dict[str, Any]],
    *,
    metric: str,
    value: float,
    limit: float,
) -> None:
    ok = float(value) <= float(limit)
    checks.append(
        {
            "metric": metric,
            "value": float(value),
            "limit": float(limit),
            "ok": bool(ok),
        }
    )


def _validate_mode_rows(mode_name: str, mode_result: dict[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    rows = mode_result.get("rows", {}) if isinstance(mode_result.get("rows", {}), dict) else {}
    for key in ("daily", "hourly", "campaign_unified", "piece_enriched"):
        value = int(rows.get(key, 0))
        checks.append(
            {
                "metric": f"{mode_name}.rows.{key}",
                "value": float(value),
                "limit": 1.0,
                "ok": value >= 1,
            }
        )
    return checks


def _load_profile_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _profile_metric(
    payload: dict[str, Any],
    view: str,
    span: str,
    stat: str,
) -> float:
    return float(
        payload.get("summary", {})
        .get(view, {})
        .get(span, {})
        .get(stat, 0.0)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gate de regresion de performance para dashboard (benchmark + perfil E2E opcional)."
    )
    parser.add_argument(
        "--report-path",
        default=str(ROOT_DIR / "tests" / "fixtures" / "benchmark_historical.json"),
        help="Ruta del report JSON para benchmark (idealmente fixture versionado).",
    )
    parser.add_argument("--iterations", type=int, default=2, help="Iteraciones warm para benchmark.")
    parser.add_argument("--output-json", default="", help="Ruta opcional para guardar resultado del gate.")

    parser.add_argument("--parquet-cold-max", type=float, default=400.0)
    parser.add_argument("--parquet-warm-avg-max", type=float, default=80.0)
    parser.add_argument("--parquet-warm-p95-max", type=float, default=120.0)
    parser.add_argument("--json-cold-max", type=float, default=500.0)
    parser.add_argument("--json-warm-avg-max", type=float, default=100.0)
    parser.add_argument("--json-warm-p95-max", type=float, default=150.0)

    parser.add_argument(
        "--profile-json",
        default="",
        help="JSON de profile E2E (salida de scripts/profile_dashboard_e2e.py). Opcional.",
    )
    parser.add_argument("--profile-overview-avg-max", type=float, default=150.0)
    parser.add_argument("--profile-overview-p95-max", type=float, default=250.0)
    parser.add_argument("--profile-trend-avg-max", type=float, default=90.0)
    parser.add_argument("--profile-trend-p95-max", type=float, default=150.0)
    parser.add_argument("--profile-traffic-avg-max", type=float, default=120.0)
    parser.add_argument("--profile-traffic-p95-max", type=float, default=180.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report_path = Path(args.report_path).resolve()
    if not report_path.exists():
        print(f"FAIL report-path not found: {report_path}")
        return 2

    checks: list[dict[str, Any]] = []
    bench_payload = _load_benchmark_payload(report_path, args.iterations)
    parquet_result = _mode_result(bench_payload, "parquet")
    json_result = _mode_result(bench_payload, "json-fallback")

    _check_limit(
        checks,
        metric="parquet.cold_total_ms",
        value=float(parquet_result.get("cold_total_ms", 0.0)),
        limit=float(args.parquet_cold_max),
    )
    _check_limit(
        checks,
        metric="parquet.warm_total_avg_ms",
        value=float(parquet_result.get("warm_total_avg_ms", 0.0)),
        limit=float(args.parquet_warm_avg_max),
    )
    _check_limit(
        checks,
        metric="parquet.warm_total_p95_ms",
        value=float(parquet_result.get("warm_total_p95_ms", 0.0)),
        limit=float(args.parquet_warm_p95_max),
    )
    checks.extend(_validate_mode_rows("parquet", parquet_result))

    _check_limit(
        checks,
        metric="json-fallback.cold_total_ms",
        value=float(json_result.get("cold_total_ms", 0.0)),
        limit=float(args.json_cold_max),
    )
    _check_limit(
        checks,
        metric="json-fallback.warm_total_avg_ms",
        value=float(json_result.get("warm_total_avg_ms", 0.0)),
        limit=float(args.json_warm_avg_max),
    )
    _check_limit(
        checks,
        metric="json-fallback.warm_total_p95_ms",
        value=float(json_result.get("warm_total_p95_ms", 0.0)),
        limit=float(args.json_warm_p95_max),
    )
    checks.extend(_validate_mode_rows("json-fallback", json_result))

    profile_path = Path(str(args.profile_json)).resolve() if str(args.profile_json).strip() else None
    if profile_path is not None and profile_path.exists():
        profile_payload = _load_profile_payload(profile_path)
        _check_limit(
            checks,
            metric="profile.overview.main:overview:render_page.avg",
            value=_profile_metric(profile_payload, "overview", "main:overview:render_page", "avg"),
            limit=float(args.profile_overview_avg_max),
        )
        _check_limit(
            checks,
            metric="profile.overview.main:overview:render_page.p95",
            value=_profile_metric(profile_payload, "overview", "main:overview:render_page", "p95"),
            limit=float(args.profile_overview_p95_max),
        )
        _check_limit(
            checks,
            metric="profile.overview.render_exec:section:trend_chart.avg",
            value=_profile_metric(profile_payload, "overview", "render_exec:section:trend_chart", "avg"),
            limit=float(args.profile_trend_avg_max),
        )
        _check_limit(
            checks,
            metric="profile.overview.render_exec:section:trend_chart.p95",
            value=_profile_metric(profile_payload, "overview", "render_exec:section:trend_chart", "p95"),
            limit=float(args.profile_trend_p95_max),
        )
        _check_limit(
            checks,
            metric="profile.traffic.main:traffic:render_page.avg",
            value=_profile_metric(profile_payload, "traffic", "main:traffic:render_page", "avg"),
            limit=float(args.profile_traffic_avg_max),
        )
        _check_limit(
            checks,
            metric="profile.traffic.main:traffic:render_page.p95",
            value=_profile_metric(profile_payload, "traffic", "main:traffic:render_page", "p95"),
            limit=float(args.profile_traffic_p95_max),
        )

    failed = [item for item in checks if not bool(item.get("ok", False))]
    for item in checks:
        status = "PASS" if item["ok"] else "FAIL"
        print(
            f"[{status}] {item['metric']}: {item['value']:.2f} "
            f"(limit {item['limit']:.2f})"
        )

    payload = {
        "ok": len(failed) == 0,
        "report_path": str(report_path),
        "iterations": int(max(int(args.iterations), 1)),
        "checks": checks,
        "failed": failed,
    }
    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"saved_json={out_path}")

    if failed:
        print(f"FAIL perf gate: {len(failed)} checks failed.")
        return 1
    print("PASS perf gate.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
