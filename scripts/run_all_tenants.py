#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_TENANTS_CONFIG_PATH = ROOT_DIR / "config" / "tenants.json"
PIPELINE_SCRIPT_PATH = ROOT_DIR / "scripts" / "yap_daily_cpl_report.py"


def _resolve_repo_path(raw_path: str) -> Path:
    p = Path(str(raw_path or "").strip())
    return p if p.is_absolute() else (ROOT_DIR / p).resolve()


def _default_tenants() -> dict[str, dict[str, Any]]:
    return {"yap": {"id": "yap", "name": "YAP"}}


def _load_tenants(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return _default_tenants()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return _default_tenants()

    entries = payload.get("tenants", [])
    if not isinstance(entries, list):
        return _default_tenants()

    out: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        tenant_id = str(entry.get("id", "")).strip().lower()
        if not tenant_id:
            continue
        out[tenant_id] = {
            "id": tenant_id,
            "name": str(entry.get("name", tenant_id.upper())),
        }
    return out if out else _default_tenants()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run YAP pipeline for all configured tenants."
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "bootstrap", "daily"],
        default="auto",
        help="Pipeline mode for each tenant run.",
    )
    parser.add_argument(
        "--bootstrap-start",
        default=date.today().replace(month=1, day=1).isoformat(),
        help="Start date for bootstrap mode (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=date.today().isoformat(),
        help="End date for extraction (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--organic-lookback-days",
        type=int,
        default=30,
        help="Organic lookback days per tenant.",
    )
    parser.add_argument(
        "--refresh-lookback-days",
        type=int,
        default=1,
        help="For mode=auto, reprocess this many days before end-date.",
    )
    parser.add_argument(
        "--tenants-config-path",
        default=str(DEFAULT_TENANTS_CONFIG_PATH),
        help="Path to tenants JSON config.",
    )
    parser.add_argument(
        "--tenant-id",
        default=None,
        help="Run only one tenant id. If omitted, runs all configured tenants.",
    )
    parser.add_argument(
        "--python-executable",
        default=sys.executable,
        help="Python executable used to run tenant pipeline.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue with next tenant if one tenant fails.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    tenants_config_path = _resolve_repo_path(args.tenants_config_path)
    tenants = _load_tenants(tenants_config_path)
    selected = (
        [str(args.tenant_id).strip().lower()]
        if args.tenant_id
        else sorted(tenants.keys())
    )
    if not selected:
        print("No tenants found to run.")
        return 1

    failures: list[str] = []
    for tenant_id in selected:
        if tenant_id not in tenants:
            msg = f"Tenant '{tenant_id}' not found in {tenants_config_path}"
            print(msg)
            failures.append(tenant_id)
            if not args.continue_on_error:
                return 1
            continue

        cmd = [
            str(args.python_executable),
            str(PIPELINE_SCRIPT_PATH),
            "--tenant-id",
            tenant_id,
            "--tenants-config-path",
            str(tenants_config_path),
            "--mode",
            str(args.mode),
            "--bootstrap-start",
            str(args.bootstrap_start),
            "--end-date",
            str(args.end_date),
            "--organic-lookback-days",
            str(args.organic_lookback_days),
            "--refresh-lookback-days",
            str(args.refresh_lookback_days),
        ]
        print(f"[{tenant_id}] Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=str(ROOT_DIR))
        if result.returncode != 0:
            failures.append(tenant_id)
            print(f"[{tenant_id}] Failed with exit code {result.returncode}")
            if not args.continue_on_error:
                return result.returncode
        else:
            print(f"[{tenant_id}] OK")

    if failures:
        print(f"Finished with failures: {', '.join(failures)}")
        return 1

    print("Finished successfully for all tenants.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
