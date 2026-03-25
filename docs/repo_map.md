# Repository Map

## 1) Architecture Overview

This repository is a **multi-tenant marketing analytics platform** with 3 main runtime layers:

1. **Ingestion/Pipeline layer (batch, hourly/manual)**
   - Pulls data from Meta Ads, Google Ads, and GA4 APIs.
   - Normalizes and merges datasets.
   - Writes tenant outputs as JSON + Parquet bundles.

2. **Application layer (Streamlit UI)**
   - Loads tenant reports.
   - Applies date/platform/campaign filters.
   - Renders KPI cards, trend charts, funnel, acquisition tables, traffic quality cards, source/medium views, hourly active users, country/city user views, tech pies, demographics, geo, and top pieces.
   - Provides admin settings, auth, audit logs, and COCO IA chat widget.

3. **Assistant layer (COCO IA)**
   - Deterministic resolver chain + optional intent-classifier path.
   - Uses context payloads and function-like internal tools.
   - Falls back to OpenAI chat-completions flow when deterministic resolution is insufficient.

Data persistence is file-based (no relational DB/ORM): JSON, Parquet, and JSONL logs under `reports/` and `config/`.

## 2) Main Modules

### App entrypoint and UI orchestration
- `dashboard.py`
  - Main Streamlit app (`main()`).
  - Auth/session/admin/config management.
  - Cache wrappers for report/parquet loading and heavy rollups.
  - Delegates domain UI work to specialized modules:
    - `dashboard_filters`
    - `dashboard_data`
    - `dashboard_trends`
    - `dashboard_overview_sections`
    - `dashboard_traffic_sections`
  - Integrates COCO IA (`coco_agent` package).

### Data shaping/normalization
- `dashboard_data.py`
  - Report/parquet signatures and health checks.
  - `daily_df`, `hourly_df`, acquisition table normalization.
  - Normalization for campaign unified, piece enriched, device, demographics, geo.

### Filters and date comparison UX logic
- `dashboard_filters.py`
  - Date preset resolution, comparison range resolution.
  - Top filter rendering (`render_top_filters`).
  - Platform selector + campaign-filter wiring.

### Trend chart payload and rendering
- `dashboard_trends.py`
  - Builds trend payloads from frames/report.
  - Handles single-day hourly behavior, resampling/downsampling.
  - Renders performance chart.

### Overview sections
- `dashboard_overview_sections.py`
  - Funnel, GA4 conversion card, media mix.
  - Lead demographics, lead geo map, device breakdown.
  - Audit table and top pieces sections.

### Traffic sections
- `dashboard_traffic_sections.py`
  - Traffic decision cards.
  - Source / medium rollups.
  - Top pages rollups.
  - Hourly active-users heatmap matrix.
  - Country/city user rollups and tech breakdowns.
  - Supporting traffic section calculations and compare-series shaping for clickable KPI cards.

### COCO IA package
- `coco_agent/engine.py`
  - OpenAI request loop, tool-call execution loop, usage accounting.
- `coco_agent/tools.py`
  - Tool schema + execution against analytics context sections.
- `coco_agent/context_builder.py`
  - Builds/sanitizes the context payload fed to COCO.
- `coco_agent/workflow.py`
  - Orchestrates deterministic resolver chain.
- `coco_agent/deterministic_resolvers.py`
  - Core business-answering logic (comparisons, period parsing, top piece, peak day, follow-ups).
- `coco_agent/intent_classifier.py`
  - LLM classifier for intent + normalized params.
- `coco_agent/intent_workflow.py`
  - Dispatcher from intent outputs to compute resolvers.
- `coco_agent/compute_resolvers.py`
  - Bridges intent params to deterministic resolvers through synthetic queries.

### Pipeline and operations scripts
- `scripts/yap_daily_cpl_report.py`
  - Main extraction/merge/export pipeline for each tenant.
  - Respects per-tenant toggles such as `organic_enabled`.
  - Exports GA4 traffic-acquisition datasets used by the dashboard, including hourly active users, country/city users, and browser/OS/device breakdowns.
  - External API HTTP calls + normalization + parquet export.
- `scripts/run_all_tenants.py`
  - Multi-tenant orchestrator (iterates `config/tenants.json`).
- `scripts/backfill_tenant_paid_monthly.py`
  - Month-by-month paid backfill validator for onboarding new tenants.
  - Runs `yap_daily_cpl_report.py` incrementally and compares each window against Meta/Google source totals.
- `scripts/benchmark_dashboard_loaders.py`
  - Cold/warm benchmark for data loaders + rollups.
- `scripts/perf_regression_gate.py`
  - Performance gate for CI.
- `scripts/profile_dashboard_e2e.py`
  - E2E profile using Streamlit testing API.

### Tests and CI
- `tests/`
  - Loader contracts, smoke rollups, parquet guardrails.
- `.github/workflows/ci.yml`
  - `pytest` + benchmark smoke + perf gate on push/PR.

## 3) Dependencies Between Modules

### High-level flow

```text
scripts/run_all_tenants.py
  -> scripts/yap_daily_cpl_report.py
      -> reports/<tenant>/<tenant>_historical.json
      -> reports/<tenant>/dashboard/*.parquet

dashboard.py
  -> dashboard_data.py (data loaders/normalization)
  -> dashboard_filters.py (top filters and date compare logic)
  -> dashboard_trends.py (trend payload/chart)
  -> dashboard_overview_sections.py (overview section rendering)
  -> dashboard_traffic_sections.py (traffic section rendering/calculations)
  -> coco_agent/* (assistant workflow)
```

### Internal package coupling

- `dashboard.py` depends on:
  - `dashboard_data`, `dashboard_filters`, `dashboard_trends`, `dashboard_overview_sections`, `dashboard_traffic_sections`
  - `coco_agent.workflow`, `coco_agent.context_builder`, `coco_agent.engine`

- `coco_agent/workflow.py` depends on:
  - `coco_agent/deterministic_resolvers.py`
  - `coco_agent/intent_workflow.py`

- `coco_agent/intent_workflow.py` depends on:
  - `coco_agent/intent_classifier.py`
  - `coco_agent/compute_resolvers.py`
  - `coco_agent/deterministic_resolvers.py` (guardrails/fallbacks)

- `coco_agent/compute_resolvers.py` depends on:
  - `coco_agent/deterministic_resolvers.py`

- `coco_agent/engine.py` depends on:
  - `coco_agent/tools.py`
  - OpenAI HTTP endpoint via `urllib.request`.

### Cross-cutting external dependencies

- Runtime libs (`requirements.txt`):
  - `streamlit`, `pandas`, `plotly`, `matplotlib`, `pyarrow`
- Dev/test:
  - `pytest`

## 4) Where Business Logic Lives

Business logic is concentrated in:

- **Data/business metrics normalization**
  - `dashboard_data.py`
  - Key areas: daily/hourly/acquisition transformations, campaign/piece/device/demographic/geo normalization.

- **Date range and comparison semantics**
  - `dashboard_filters.py`
  - Key areas: preset ranges, previous period, YoY, custom compare ranges.

- **KPI and section behavior in dashboard flow**
  - `dashboard.py` (summary/KPI helpers, settings logic, tenant/user scope logic, cache strategy).
  - Admin settings now persist explicit order for KPI cards and section blocks per scope/tenant.

- **Trend logic**
  - `dashboard_trends.py` (hourly-vs-daily trend behavior, downsampling rules).

- **Traffic quality and acquisition section logic**
  - `dashboard_traffic_sections.py`
  - Key areas: decision-card metrics, clickable KPI trend helpers, source/medium aggregation, traffic page rollups, hourly/country/city/tech aggregations, platform-aware GA4 filtering.

- **COCO IA deterministic analytics interpretation**
  - `coco_agent/deterministic_resolvers.py`
  - Key areas: period parsing in Spanish, follow-up context handling, structured comparisons.

- **Pipeline domain logic**
  - `scripts/yap_daily_cpl_report.py`
  - Key areas: API fetch/merge and canonical report generation.

## 5) Where API Routes Live

- **No API route layer is implemented**.
- There is no FastAPI/Flask/Django router in this repository.
- The primary user-facing interface is Streamlit (`dashboard.py`), and external APIs are consumed directly from pipeline/assistant modules.

## 6) Where Database Models Live

- **No ORM/database model layer exists** (no SQLAlchemy/Django/Peewee models).
- Data model is file-backed and implicit in JSON/Parquet schemas:
  - Main report files: `reports/<tenant>/*_historical.json`
  - Parquet bundle: `reports/<tenant>/dashboard/*.parquet`
  - Config/runtime entities:
    - `config/tenants.json` (`historical_start_date`, IDs, and per-tenant toggles like `organic_enabled`)
    - `config/users.json` (runtime; template versioned)
    - `config/dashboard_settings.json` (runtime; template versioned)
  - Event/audit logs (JSONL):
    - `config/admin_audit.jsonl`
    - `config/ai_usage.jsonl`
    - `config/coco_chat_memory.jsonl`

## 7) Key Services and Utilities

### Services (runtime/ops)
- **Dashboard service**
  - Streamlit app (`dashboard.py`) typically run via systemd service (`config/systemd/yap-dashboard.service`).
- **Pipeline service + timer**
  - Hourly pipeline via `config/systemd/yap-pipeline.service` + `config/systemd/yap-pipeline.timer`.
- **Reverse proxy**
  - Caddy config at `config/caddy/Caddyfile`.

### Utilities and support scripts
- `scripts/run_all_tenants.py`: multi-tenant orchestration.
- `scripts/yap_daily_cpl_report.py`: ETL core.
- `scripts/backfill_tenant_paid_monthly.py`: month-by-month onboarding backfill with paid-source validation.
- `scripts/benchmark_dashboard_loaders.py`: loader benchmark.
- `scripts/perf_regression_gate.py`: regression gate.
- `scripts/profile_dashboard_e2e.py`: E2E profiling.
- `scripts/sync_caddy.sh`: deploy/sync Caddy config.

## 8) Practical Notes for New Contributors

- Start from `dashboard.py` for end-to-end app flow.
- For data correctness issues, inspect in this order:
  1. `reports/<tenant>/*_historical.json`
  2. parquet bundle generation in `scripts/yap_daily_cpl_report.py`
  3. normalization in `dashboard_data.py`
- For date/filter issues, inspect `dashboard_filters.py` + filter session-state wiring in `dashboard.py`.
- For traffic-section issues, inspect `dashboard_traffic_sections.py` and then the `render_traffic()` wiring in `dashboard.py`.
- For COCO answers, inspect `coco_agent/workflow.py` and then:
  - `deterministic_resolvers.py` (primary)
  - `intent_workflow.py` / `intent_classifier.py` (if intent path enabled)

## 9) Responsibility Index

### Core app modules
- `dashboard.py`
  - Responsibility: Streamlit application composition, auth/session state, tenant selection, admin panel, cache orchestration, page-level rendering flow, and per-tenant ordering of KPI cards/section blocks.
- `dashboard_data.py`
  - Responsibility: canonical loading/parsing/normalization of report JSON and parquet datasets into stable DataFrame contracts.
- `dashboard_filters.py`
  - Responsibility: top-filter UX and filter state transitions (platform, date presets, compare ranges, campaign filters).
- `dashboard_trends.py`
  - Responsibility: build/render trend payloads (daily/hourly), compare-series shaping, and chart downsampling.
- `dashboard_overview_sections.py`
  - Responsibility: rendering and local calculations for major overview sections (funnel, media mix, lead demographics/geo/device, audit table, top pieces).
- `dashboard_traffic_sections.py`
  - Responsibility: traffic decision cards, source/medium aggregation, top pages rollups, hourly/country/city/tech traffic computations, and related traffic-quality helpers.

### COCO assistant modules
- `coco_agent/engine.py`
  - Responsibility: OpenAI chat-completion loop, tool-call handling, and turn execution lifecycle.
- `coco_agent/tools.py`
  - Responsibility: expose analytics context through structured callable tools (`list_available_metrics`, `get_scope_and_coverage`, `get_metric_values`, `compare_metrics`).
- `coco_agent/context_builder.py`
  - Responsibility: construct and sanitize the assistant context payload from dashboard summaries and scope metadata.
- `coco_agent/workflow.py`
  - Responsibility: orchestrate deterministic resolvers and intent-based resolver path selection/fallback.
- `coco_agent/deterministic_resolvers.py`
  - Responsibility: deterministic business Q&A logic (period parsing, comparisons, peak/top analyses, follow-ups).
- `coco_agent/intent_classifier.py`
  - Responsibility: classify question intent and normalize intent params via constrained JSON output.
- `coco_agent/intent_workflow.py`
  - Responsibility: dispatch validated intents to compute resolvers, apply guardrails, and build resolver metadata.
- `coco_agent/compute_resolvers.py`
  - Responsibility: transform intent params into deterministic resolver calls (synthetic-query bridge).

### Pipeline and ops modules
- `scripts/yap_daily_cpl_report.py`
  - Responsibility: tenant ETL pipeline (extract from APIs, transform, aggregate, export JSON/parquet bundles, and skip optional modules such as organic when disabled per tenant).
- `scripts/run_all_tenants.py`
  - Responsibility: iterate configured tenants and invoke per-tenant pipeline with consistent CLI arguments.
- `scripts/backfill_tenant_paid_monthly.py`
  - Responsibility: orchestrate month-by-month paid backfill for new tenants and stop on validation mismatches before advancing.
- `scripts/benchmark_dashboard_loaders.py`
  - Responsibility: cold/warm performance measurements for dashboard loader and rollup paths.
- `scripts/perf_regression_gate.py`
  - Responsibility: enforce threshold-based performance quality gate from benchmark/profile outputs.
- `scripts/profile_dashboard_e2e.py`
  - Responsibility: E2E render profiling via Streamlit testing harness.

### Test and CI modules
- `tests/conftest.py`
  - Responsibility: fixture setup for synthetic reports and cache isolation.
- `tests/test_dashboard_data_contracts.py`
  - Responsibility: verify DataFrame schema/contracts and parquet-vs-JSON loader behavior.
- `tests/test_dashboard_smoke_rollups.py`
  - Responsibility: smoke-check rollup outputs used by UI sections.
- `tests/test_dashboard_parquet_guardrail.py`
  - Responsibility: verify parquet bundle health, stale/missing detection.
- `tests/test_dashboard_traffic_sections.py`
  - Responsibility: validate traffic decision-card metrics, source/medium filtering, top-pages rollup behavior, and hourly/country/tech aggregations.
- `.github/workflows/ci.yml`
  - Responsibility: run tests, benchmark smoke, and perf gate in automated CI.

## 10) Typical Data Flow

```text
[External APIs]
Meta Ads Graph API
Google Ads API
GA4 Data API
        |
        v
scripts/yap_daily_cpl_report.py
  - extract + normalize + aggregate
  - write reports/<tenant>/*_historical.json
  - write reports/<tenant>/dashboard/*.parquet
        |
        v
dashboard.py
  -> dashboard_data.py (load + normalize DataFrames)
  -> dashboard_filters.py (resolve active ranges/platform/filter state)
  -> dashboard_trends.py + dashboard_overview_sections.py + dashboard_traffic_sections.py (render content)
        |
        +--> coco_agent/context_builder.py (context payload)
                -> coco_agent/workflow.py
                   -> deterministic_resolvers / intent workflow
                   -> coco_agent/engine.py (OpenAI + tool-calls if needed)
```

Operational cadence:
- Hourly/automated: systemd timer runs `scripts/run_all_tenants.py` -> per-tenant ETL.
- Interactive: Streamlit request/rerun loads latest report/parquet and renders filtered views.

## 11) Entry Points (API / CLI / Jobs)

### API entry points
- HTTP API routes: **none** (no FastAPI/Flask/Django route layer).
- UI entry point: Streamlit app via `dashboard.py` (`main()`).

### CLI entry points
- Multi-tenant orchestrator:
  - `python scripts/run_all_tenants.py --mode auto ...`
- Per-tenant ETL:
  - `python scripts/yap_daily_cpl_report.py --tenant-id <id> --mode auto ...`
- Monthly onboarding backfill:
  - `python scripts/backfill_tenant_paid_monthly.py --tenant-id <id> --bootstrap-start 2025-01-01 --monthly-through 2025-12-31 --catchup-end <date> ...`
- Benchmark:
  - `python scripts/benchmark_dashboard_loaders.py --report-path ...`
- Perf gate:
  - `python scripts/perf_regression_gate.py --report-path ...`
- E2E profile:
  - `python scripts/profile_dashboard_e2e.py ...`

### Job entry points
- Systemd timer job:
  - `config/systemd/yap-pipeline.timer` triggers `yap-pipeline.service`.
- Systemd app service:
  - `config/systemd/yap-dashboard.service` runs Streamlit app.
- CI job:
  - `.github/workflows/ci.yml` job `test-and-smoke`.

## 12) Shared Utilities

### Shared helper domains
- Numeric safety and formatting helpers
  - Examples: `safe_float`, `series_safe_div`, KPI format helpers in `dashboard.py` and `coco_agent/deterministic_resolvers.py`.
- Date coercion/range normalization
  - `dashboard_filters.py` (`_coerce_date_value`, `_normalize_date_range`, compare-range resolvers).
- Cache/signature helpers
  - `dashboard_data.py` (`report_cache_signature`, parquet signature/health helpers).
  - `dashboard.py` (`@st.cache_data`, `@st.cache_resource` wrappers for loaders and rollups).
- Text normalization utilities
  - Campaign/piece/location and intent text normalization in `dashboard_data.py` and `coco_agent/deterministic_resolvers.py`.

### Shared configuration and runtime stores
- Tenant and app config:
  - `config/tenants.json`
  - `config/dashboard_settings.json` (runtime) + template
- User/auth config:
  - `config/users.json` (runtime) + template
- Operational logs:
  - `config/admin_audit.jsonl`
  - `config/ai_usage.jsonl`
  - `config/coco_chat_memory.jsonl`
  - `config/coco_chat_state.json`
