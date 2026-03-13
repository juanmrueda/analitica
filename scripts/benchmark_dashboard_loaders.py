#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path
from typing import Any, Callable

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import dashboard  # noqa: E402


def _parquet_bundle_detected(report_path: Path, parquet_dirname: str) -> bool:
    bundle_dir = report_path.parent / str(parquet_dirname)
    if not bundle_dir.exists():
        return False
    return any(bundle_dir.glob("*.parquet"))


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(int(math.ceil(q * len(ordered))) - 1, 0)
    return float(ordered[idx])


def _timed(metrics: dict[str, float], key: str, fn: Callable[[], Any]) -> Any:
    started = time.perf_counter()
    result = fn()
    metrics[key] = (time.perf_counter() - started) * 1000.0
    return result


def _single_run(report_path: Path) -> dict[str, Any]:
    steps: dict[str, float] = {}
    daily = _timed(steps, "load_daily_ms", lambda: dashboard.load_daily_df_from_report_path(report_path))
    hourly = _timed(steps, "load_hourly_ms", lambda: dashboard.load_hourly_df_from_report_path(report_path))
    campaigns = _timed(
        steps,
        "load_campaign_unified_ms",
        lambda: dashboard.load_campaign_unified_df_from_report_path(report_path),
    )
    pieces = _timed(
        steps,
        "load_piece_enriched_ms",
        lambda: dashboard.load_piece_enriched_df_from_report_path(report_path),
    )

    path_str, modified_ns, size_bytes = dashboard._report_cache_signature(report_path)
    if daily.empty or "date" not in daily.columns:
        start_iso = "1970-01-01"
        end_iso = "1970-01-01"
    else:
        start_iso = daily["date"].min().isoformat()
        end_iso = daily["date"].max().isoformat()
    filter_key = dashboard._campaign_filters_cache_key({})

    channels = _timed(
        steps,
        "rollup_channels_ms",
        lambda: dashboard._cached_channels_roll_from_report(
            path_str, modified_ns, size_bytes, start_iso, end_iso
        ),
    )
    pages = _timed(
        steps,
        "rollup_top_pages_ms",
        lambda: dashboard._cached_top_pages_roll_from_report(
            path_str, modified_ns, size_bytes, start_iso, end_iso
        ),
    )
    campaign_roll = _timed(
        steps,
        "rollup_campaigns_ms",
        lambda: dashboard._cached_campaign_roll_from_report(
            path_str, modified_ns, size_bytes, start_iso, end_iso, "All", filter_key
        ),
    )
    top_pieces = _timed(
        steps,
        "rollup_top_pieces_ms",
        lambda: dashboard._cached_top_pieces_roll_from_report(
            path_str, modified_ns, size_bytes, start_iso, end_iso, "All", filter_key
        ),
    )
    _timed(
        steps,
        "rollup_filter_values_ms",
        lambda: dashboard._cached_campaign_filter_values_from_report(
            path_str,
            modified_ns,
            size_bytes,
            "campaign_name",
            "All",
            start_iso,
            end_iso,
        ),
    )

    return {
        "total_ms": float(sum(steps.values())),
        "steps_ms": steps,
        "rows": {
            "daily": int(len(daily)),
            "hourly": int(len(hourly)),
            "campaign_unified": int(len(campaigns)),
            "piece_enriched": int(len(pieces)),
            "channels_rollup": int(len(channels)),
            "top_pages_rollup": int(len(pages)),
            "campaign_rollup": int(len(campaign_roll)),
            "top_pieces_rollup": int(len(top_pieces)),
        },
    }


def _clear_caches() -> None:
    dashboard.st.cache_data.clear()


def _run_mode(report_path: Path, *, mode_name: str, iterations: int, use_parquet: bool) -> dict[str, Any]:
    original_parquet_dirname = dashboard.REPORT_PARQUET_DIRNAME
    parquet_detected = _parquet_bundle_detected(report_path, original_parquet_dirname)
    dashboard.REPORT_PARQUET_DIRNAME = (
        original_parquet_dirname if use_parquet else "__benchmark_json_fallback_only__"
    )
    try:
        _clear_caches()
        cold = _single_run(report_path)
        warm_runs: list[dict[str, Any]] = []
        for _ in range(max(iterations, 1)):
            warm_runs.append(_single_run(report_path))

        warm_totals = [float(run["total_ms"]) for run in warm_runs]
        step_names = list(cold["steps_ms"].keys())
        warm_step_avg: dict[str, float] = {}
        for step in step_names:
            values = [float(run["steps_ms"].get(step, 0.0)) for run in warm_runs]
            warm_step_avg[step] = sum(values) / len(values) if values else 0.0

        return {
            "mode": mode_name,
            "uses_parquet": use_parquet,
            "parquet_bundle_detected": parquet_detected,
            "cold_total_ms": float(cold["total_ms"]),
            "warm_total_avg_ms": sum(warm_totals) / len(warm_totals) if warm_totals else 0.0,
            "warm_total_min_ms": min(warm_totals) if warm_totals else 0.0,
            "warm_total_p95_ms": _percentile(warm_totals, 0.95),
            "warm_step_avg_ms": warm_step_avg,
            "rows": cold["rows"],
        }
    finally:
        dashboard.REPORT_PARQUET_DIRNAME = original_parquet_dirname
        _clear_caches()


def _print_table(results: list[dict[str, Any]]) -> None:
    print(
        f"{'mode':<16}{'cold_ms':>12}{'warm_avg_ms':>14}{'warm_p95_ms':>14}"
        f"{'pq':>6}"
        f"{'daily':>8}{'hourly':>8}{'camp':>8}{'piece':>8}"
    )
    print("-" * 94)
    for item in results:
        rows = item["rows"]
        pq_flag = "yes" if item.get("parquet_bundle_detected") else "no"
        print(
            f"{item['mode']:<16}{item['cold_total_ms']:>12.2f}{item['warm_total_avg_ms']:>14.2f}"
            f"{item['warm_total_p95_ms']:>14.2f}{pq_flag:>6}{rows['daily']:>8d}{rows['hourly']:>8d}"
            f"{rows['campaign_unified']:>8d}{rows['piece_enriched']:>8d}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark reproducible para loaders/rollups del dashboard (cold/warm cache)."
    )
    parser.add_argument(
        "--report-path",
        default=str(ROOT_DIR / "reports" / "yap" / "yap_historical.json"),
        help="Ruta al *_historical.json del tenant.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Iteraciones warm por modo (default: 5).",
    )
    parser.add_argument(
        "--mode",
        choices=("compare", "parquet", "json-fallback"),
        default="compare",
        help="Modo benchmark: compare (ambos), parquet, o json-fallback.",
    )
    parser.add_argument(
        "--output",
        choices=("table", "json"),
        default="table",
        help="Formato de salida.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report_path = Path(args.report_path).resolve()
    if not report_path.exists():
        print(f"ERROR: report file not found: {report_path}", file=sys.stderr)
        return 2

    modes: list[tuple[str, bool]]
    if args.mode == "compare":
        modes = [("parquet", True), ("json-fallback", False)]
    elif args.mode == "parquet":
        modes = [("parquet", True)]
    else:
        modes = [("json-fallback", False)]

    results: list[dict[str, Any]] = []
    for mode_name, use_parquet in modes:
        results.append(
            _run_mode(
                report_path,
                mode_name=mode_name,
                iterations=max(int(args.iterations), 1),
                use_parquet=use_parquet,
            )
        )

    payload = {
        "report_path": str(report_path),
        "iterations": max(int(args.iterations), 1),
        "results": results,
    }
    if args.output == "json":
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        _print_table(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
