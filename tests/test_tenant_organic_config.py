from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import dashboard


ROOT_DIR = Path(__file__).resolve().parents[1]
PIPELINE_PATH = ROOT_DIR / "scripts" / "yap_daily_cpl_report.py"


def _load_pipeline_module():
    spec = importlib.util.spec_from_file_location("test_yap_daily_cpl_report", PIPELINE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load pipeline module from {PIPELINE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PIPELINE = _load_pipeline_module()


def test_pipeline_loader_reads_organic_enabled_flag(tmp_path: Path) -> None:
    tenants_path = tmp_path / "tenants.json"
    payload = {
        "tenants": [
            {
                "id": "tenant_disabled",
                "name": "Tenant Disabled",
                "report_path": "reports/tenant_disabled/historical.json",
                "organic_report_path": "reports/tenant_disabled/organic.json",
                "organic_enabled": False,
            },
            {
                "id": "tenant_default",
                "name": "Tenant Default",
                "report_path": "reports/tenant_default/historical.json",
                "organic_report_path": "reports/tenant_default/organic.json",
            },
        ]
    }
    tenants_path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = PIPELINE._load_tenants_config(tenants_path)

    assert loaded["tenant_disabled"]["organic_enabled"] is False
    assert loaded["tenant_default"]["organic_enabled"] is True


def test_dashboard_loader_reads_organic_enabled_flag(tmp_path: Path) -> None:
    tenants_path = tmp_path / "tenants.json"
    report_path = tmp_path / "reports" / "tenant_disabled" / "historical.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("{}", encoding="utf-8")
    payload = {
        "tenants": [
            {
                "id": "tenant_disabled",
                "name": "Tenant Disabled",
                "report_path": str(report_path),
                "organic_enabled": False,
            },
            {
                "id": "tenant_default",
                "name": "Tenant Default",
                "report_path": str(report_path),
            },
        ]
    }
    tenants_path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = dashboard.load_tenants_config(tenants_path)

    assert loaded["tenant_disabled"]["organic_enabled"] is False
    assert loaded["tenant_default"]["organic_enabled"] is True


def test_save_tenant_operational_flags_updates_tenants_json(tmp_path: Path) -> None:
    tenants_path = tmp_path / "tenants.json"
    payload = {
        "tenants": [
            {
                "id": "tenant_disabled",
                "name": "Tenant Disabled",
                "report_path": "reports/tenant_disabled/historical.json",
                "organic_report_path": "reports/tenant_disabled/organic.json",
                "organic_enabled": False,
                "ga4_property_id": "",
            }
        ]
    }
    tenants_path.write_text(json.dumps(payload), encoding="utf-8")

    ok, err = dashboard.save_tenant_operational_flags(
        tenants_path,
        "tenant_disabled",
        organic_enabled=True,
    )

    assert ok is True
    assert err == ""
    saved = json.loads(tenants_path.read_text(encoding="utf-8"))
    assert saved["tenants"][0]["organic_enabled"] is True
    assert saved["tenants"][0]["name"] == "Tenant Disabled"
