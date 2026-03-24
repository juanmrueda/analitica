# MCP Juan (GA4 + Google Ads + Meta Ads)

Repositorio operativo para analitica de performance multi-tenant (YAP + Hyundai HN + RACSA):

- `analytics-mcp` (GA4)
- `google-ads-mcp` (Google Ads)
- `meta-ads-mcp` (Meta Ads)
- Dashboard en Streamlit (`dashboard.py`)
- Pipeline orquestado por tenant (`scripts/run_all_tenants.py`)

## Estado actual (2026-03-24)

- Produccion desplegada en DigitalOcean (Droplet Ubuntu).
- Dominio: `analitica.ipalmera.com` (Cloudflare al frente).
- Dashboard activo como servicio systemd: `yap-dashboard.service`.
- Pipeline automatico cada hora: `yap-pipeline.timer` -> `yap-pipeline.service`.
- El servicio de pipeline ejecuta `scripts/run_all_tenants.py --mode auto --organic-lookback-days 30`.
- La extraccion organica ahora es opcional por tenant via `config/tenants.json -> organic_enabled`.
- Tenants activos en `config/tenants.json`:
  - `yap`
  - `hyundai_hn`
  - `racsa`
- El tenant `ipalmera_regional` fue retirado de la configuracion activa.
- Cambios locales recientes:
  - `edd3a3a` (`feat: onboard RACSA tenant with monthly backfill workflow`)
  - `dc6e953` (`feat: revamp traffic dashboard sections`)

### Avance local COCO IA y arquitectura (2026-03-13)

- Se migro la logica de agente a `coco_agent/` para sacar complejidad de `dashboard.py`.
- Se separaron capas:
  - `coco_agent/engine.py` -> llamada a modelo y orquestacion de tool-calls.
  - `coco_agent/tools.py` -> definicion de herramientas para respuestas con datos.
  - `coco_agent/context_builder.py` -> construccion/saneamiento de contexto.
  - `coco_agent/workflow.py` -> cadena de resolvers deterministas + fallback.
  - `coco_agent/deterministic_resolvers.py` -> reglas de interpretacion/calculo.
- COCO IA resuelve de forma determinista:
  - comparativos interanuales por periodos (ej. ene-feb 2025 vs 2026),
  - follow-up "ponlo en tabla" reutilizando contexto previo,
  - comparativos por ventanas de dias dentro de mes (ej. primeros 10 dias de marzo).
- Se incorporo documentacion estructural viva del repo:
  - `docs/repo_map.md` con arquitectura, responsabilidades, flujo y entry points.
  - `AGENT.md` con reglas de implementacion (leer mapa, identificar modulo, tocar solo archivos necesarios).
- Pendiente en curso:
  - robustecer parser semantico para mas variaciones de lenguaje natural,
  - validacion numerica cruzada para evitar inconsistencias en respuestas libres.

## Estructura principal

- `dashboard.py` -> UI principal.
- `dashboard_data.py` -> capa de datos (IO de reportes/parquet + normalizacion de tablas para dashboard).
- `dashboard_traffic_sections.py` -> secciones especializadas de Trafico y Adquisicion (decision cards, canales, source/medium, top pages).
- `assets/dashboard.css` -> estilos principales del dashboard (antes embebidos en `apply_theme()`).
- `coco_agent/` -> modulo de agente COCO IA (engine, workflow, resolvers, context builder, tools).
- `scripts/run_all_tenants.py` -> ejecuta el pipeline para todos los tenants configurados.
- `scripts/yap_daily_cpl_report.py` -> extraccion y consolidacion por tenant.
- `scripts/backfill_tenant_paid_monthly.py` -> bootstrap pagado mensual por tenant nuevo con validacion por mes contra Meta/Google.
- `reports/yap/yap_historical.json` -> fuente principal tenant YAP.
- `reports/hyundai_hn/hyundai_hn_historical.json` -> fuente principal tenant Hyundai.
- `reports/racsa/racsa_historical.json` -> fuente principal tenant RACSA.
- `reports/*/*_organic_historical.json` -> modulo organico por tenant, solo si `organic_enabled=true`.
- `config/tenants.json` -> ids, rutas y piso historico (`historical_start_date`) por tenant.
- `config/users.json` -> usuarios y permisos.
- `config/dashboard_settings.template.json` -> plantilla versionada de variables de dashboard.
- `config/dashboard_settings.json` -> runtime local/no versionado (se crea desde template si no existe).
- `config/admin_audit.jsonl` -> bitacora de cambios administrativos.
- `config/backups/` -> backups automaticos antes de guardar config.

## Operacion local (desarrollo)

1. Instalar dependencias:

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
```

2. Ejecutar pipeline multi-tenant:

```powershell
.\.venv\Scripts\python scripts\run_all_tenants.py --mode auto --bootstrap-start 2025-01-01 --organic-lookback-days 30
```

Nota:
- `--organic-lookback-days` solo aplica a tenants con `organic_enabled=true`.
- Si `organic_enabled=false`, el pipeline omite completamente la extraccion organica para ese tenant.

3. Ejecutar pipeline para un tenant puntual:

```powershell
.\.venv\Scripts\python scripts\yap_daily_cpl_report.py --tenant-id hyundai_hn --mode auto
```

4. Bootstrap mensual para un tenant nuevo:

```powershell
.\.venv\Scripts\python scripts\backfill_tenant_paid_monthly.py --tenant-id racsa --bootstrap-start 2025-01-01 --monthly-through 2025-12-31 --catchup-end 2026-03-24 --python-executable .\.venv\Scripts\python --refresh-lookback-days 0
```

5. Levantar dashboard local:

```powershell
.\.venv\Scripts\streamlit run dashboard.py
```

Opcional (Windows): auto-arrancar Streamlit cuando abras Antigravity:

```powershell
.\scripts\auto-start-streamlit-on-antigravity.ps1
```

Por defecto vigila el proceso `antigravity` y, al detectarlo, ejecuta:
`python -m streamlit run dashboard.py`.

Flags utiles:
- `-RunOnce`: sale despues del primer arranque.
- `-RelaunchOnAntigravityRestart`: vuelve a disparar Streamlit si cierras y reabres Antigravity.
- `-AntigravityProcess <nombre>`: cambia el nombre del proceso a vigilar.

6. Login local:

```powershell
copy config\users.template.json config\users.json
copy config\dashboard_settings.template.json config\dashboard_settings.json
```

Usuarios base del template:
- `admin` (rol `admin`)
- `hyundai` (rol `viewer`, tenant `hyundai_hn`)
- `yap` (rol `viewer`, tenant `yap`)

Definir o resetear passwords desde `config/users.json` o desde el panel de Administracion.

## Testing y Benchmark (Fase 2.1)

1. Instalar dependencias de desarrollo:

```powershell
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

2. Ejecutar tests automatizados:

```powershell
python -m pytest
```

3. Ejecutar benchmark reproducible (Parquet vs JSON fallback):

```powershell
python scripts\benchmark_dashboard_loaders.py --report-path reports\yap\yap_historical.json --iterations 5 --mode compare
```

Opciones utiles del benchmark:
- `--mode parquet` para medir solo ruta Parquet.
- `--mode json-fallback` para forzar carga desde JSON.
- `--output json` para exportar resultados en formato JSON.

4. Ejecutar gate de regresion de performance (P3.2):

```powershell
python scripts\perf_regression_gate.py --report-path tests\fixtures\benchmark_historical.json --iterations 2
```

Opcional (agrega chequeo E2E usando JSON de `profile_dashboard_e2e.py`):

```powershell
python scripts\perf_regression_gate.py --report-path tests\fixtures\benchmark_historical.json --iterations 2 --profile-json artifacts\perf\p3_1_e2e_profile_20260313.json
```

## CI minima (Fase 2.3)

- Workflow: `.github/workflows/ci.yml`
- Ejecuta en `push` a `main` y en `pull_request`.
- Pasos:
  - Instalacion de dependencias runtime + dev.
  - `pytest` completo.
  - benchmark smoke con fixture versionado:
    `python scripts/benchmark_dashboard_loaders.py --report-path tests/fixtures/benchmark_historical.json --iterations 1 --mode compare`
  - gate de performance:
    `python scripts/perf_regression_gate.py --report-path tests/fixtures/benchmark_historical.json --iterations 2`

## Panel de Administracion (dashboard)

- Gestion de usuarios por tenant.
- Activacion/desactivacion de vistas del menu lateral.
- Configuracion dinamica de KPIs y secciones por tenant.
- Vista/plataforma por defecto por tenant.
- Upload de logo por tenant (se guarda en `assets/logos`).
- Toggle de extraccion organica por tenant (persiste en `config/tenants.json`).
- Toggle de salud de token Meta en sidebar.
- Auditoria de cambios y backups de configuracion.

## Operacion en produccion (DigitalOcean)

### Servicios systemd

- `yap-dashboard.service` -> Streamlit en `127.0.0.1:8501`
- `yap-pipeline.service` -> pipeline multi-tenant (oneshot)
- `yap-pipeline.timer` -> `OnCalendar=hourly`

### Validaciones utiles

Estado:

```bash
sudo systemctl status yap-dashboard --no-pager
sudo systemctl status yap-pipeline.timer --no-pager
sudo systemctl status yap-pipeline.service --no-pager
```

Ver comando real del pipeline:

```bash
systemctl cat yap-pipeline.service
```

Proxima corrida:

```bash
systemctl list-timers | grep yap-pipeline
```

Forzar corrida manual:

```bash
sudo systemctl start yap-pipeline.service
```

Logs:

```bash
journalctl -u yap-dashboard -n 120 --no-pager
journalctl -u yap-pipeline.service -n 120 --no-pager
```

## Flujo de datos horario

1. Cada hora `yap-pipeline.timer` dispara `yap-pipeline.service`.
2. `run_all_tenants.py` recorre los tenants de `config/tenants.json`.
3. Cada tenant corre `yap_daily_cpl_report.py --mode auto`.
4. Se actualizan JSONs por tenant en `reports/<tenant_id>/`.
5. Streamlit consume esos JSONs en cada recarga.

Nota:
- Si un tenant no tiene `ga4_property_id` (ejemplos actuales `hyundai_hn` y `racsa`), GA4 se omite para ese tenant y el pipeline continua normalmente.
- Si un tenant tiene `organic_enabled=false` (config actual de `yap`, `hyundai_hn` y `racsa`), el modulo organico se omite para ese tenant y no se reescribe `*_organic_historical.json`.
- `dashboard.py` bootstrapea `config/dashboard_settings.json` desde `config/dashboard_settings.template.json` cuando falta el runtime.
- Para vista de miniaturas remotas (Meta) en "Top 10 Piezas", Caddy debe permitir `img-src ... https:`.

## Deploy de cambios

1. Commit y push en local.
2. En servidor:

```bash
cd /opt/yap/app
git pull --ff-only origin main
/opt/yap/venv/bin/pip install -r requirements.txt
sudo cp /opt/yap/app/config/systemd/yap-pipeline.service /etc/systemd/system/yap-pipeline.service
sudo cp /opt/yap/app/config/systemd/yap-pipeline.timer /etc/systemd/system/yap-pipeline.timer
sudo systemctl daemon-reload
sudo systemctl restart yap-pipeline.timer
sudo systemctl restart yap-dashboard
sudo systemctl start yap-pipeline.service
```

Nota deploy:
- `config/users.json` y `config/dashboard_settings.json` no se versionan, por lo que `git pull` no los reemplaza.
- Guardrail de historico: el pipeline protege contra bootstrap-start mayor al piso historico del tenant (por defecto `2025-01-01`) salvo que se use `--allow-historical-truncation`.

### Infraestructura (Caddy) versionada

- Config repo: `config/caddy/Caddyfile`
- Sync a `/etc/caddy/Caddyfile` en DO:

```bash
cd /opt/yap/app
bash scripts/sync_caddy.sh
```

### Smoke test rapido post-deploy

```bash
sudo systemctl status yap-dashboard --no-pager
sudo systemctl status yap-pipeline.timer --no-pager
curl -I https://analitica.ipalmera.com
```

Validar datos para rango de un dia (hora real + top piezas):

```bash
cd /opt/yap/app
jq '.hourly|length' reports/yap/yap_historical.json
jq '.traffic_acquisition.paid_piece_daily|length' reports/yap/yap_historical.json
```

## Configuracion sensible (no versionar)

- `~/.codex/config.toml` real con tokens.
- `ga4_user_oauth_analytics_ipalmera.json` real.
- `config/users.json` runtime.
- `config/dashboard_settings.json` runtime.
- Backups o archivos privados con credenciales (`*.bak`, `*.private`, etc.).

## Cloudflare recomendado

- SSL/TLS: `Full` (evitar `Flexible`).
- DNS de `analitica.ipalmera.com` apuntando al Droplet.
- Para troubleshooting de certificado, usar temporalmente `DNS only`.

## Seguridad minima recomendada

- Usuario operativo no root (`juanm`).
- `ufw` activo para `22`, `80`, `443`.
- SSH por llaves.
- En `sshd_config`: `PermitRootLogin no` y `PasswordAuthentication no`.
