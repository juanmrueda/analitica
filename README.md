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
  - `0b9ee15` (`feat: expand traffic acquisition insights`)
  - `fa76022` (`feat: allow manual KPI ordering in admin`)
  - `39346c8` (`feat: connect RACSA GA4 property and traffic view`)
  - `aea4849` (`perf: smooth traffic view transitions and document runtime`)

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
- `dashboard_traffic_sections.py` -> secciones especializadas de Trafico y Adquisicion (decision cards, canales, source/medium, top pages, heatmap horario, pais/ciudad y tecnologia de usuario).
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

Para corrida local headless con puerto fijo:

```powershell
.\.venv\Scripts\streamlit run dashboard.py --server.headless true --server.port 8501
```

Notas operativas de esta corrida local:
- URL esperada: `http://localhost:8501`
- stdout manual: `tmp_streamlit_stdout_manual.log`
- stderr manual: `tmp_streamlit_stderr_manual.log`

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
- Orden manual de cards KPI en `Overview` y `Trafico y Adquisicion`.
- Orden manual de bloques/secciones por tenant para `Overview` y `Trafico y Adquisicion`.
- Vista/plataforma por defecto por tenant.
- Upload de logo por tenant (se guarda en `assets/logos`).
- Toggle de extraccion organica por tenant (persiste en `config/tenants.json`).
- Toggle de salud de token Meta en sidebar.
- Auditoria de cambios y backups de configuracion.

## Overview / GA4

- La card `Usuarios GA4 por evento` en `Overview` puede configurarse por tenant con `ga4_overview_conversion_event_names` en `config/tenants.json`.
- Para evitar sobreconteo al sumar `totalUsers` diario, esa card consulta GA4 directo por rango con `dimensions=eventName` y `metrics=totalUsers`.
- Si la consulta directa falla, la card puede caer al dataset local `ga4_event_daily` como respaldo.
- La card representa usuarios unicos del periodo en GA4. Cuando se usa consulta directa, no se segmenta por selector `Google/Meta`.

## Trafico y Adquisicion

- Las cards superiores son interactivas: al hacer clic en una card cambia la tendencia inferior, igual que en `Overview`.
- La vista usa tablas GA4 del bundle `traffic_acquisition` cuando el tenant tiene `ga4_property_id`.
- Cuando existen bundles Parquet en `reports/<tenant>/dashboard/`, la vista prioriza esas tablas sobre el JSON historico para reducir latencia.
- La tabla `Source / Medium` evita `pandas.Styler` en runtime para recortar el costo de primera carga/cambio de vista en tenants grandes como `yap`.
- El body principal usa un placeholder temporal al cambiar entre `Overview` y `Trafico y Adquisicion` para disminuir la mezcla visual de bloques viejos mientras Streamlit completa el rerun.
- El cambio de vista marca una transicion interna y omite una sola vez el `cross_view prewarm` para que el primer paint de la vista destino llegue antes.
- El contenedor principal del body cambia de `key` por `view_mode`/transicion para reducir reciclaje visual de nodos entre `Overview` y `Trafico y Adquisicion`.
- `render_traffic()` precrea `slots` vacios por seccion antes de pintar KPI, canales, `source_medium`, top pages y breakdowns; eso ayuda a que desaparezcan antes los bloques bajos de `Overview` mientras las secciones nuevas terminan de renderizar.
- Secciones nuevas soportadas:
  - `Usuarios Activos por Hora`
  - `Usuarios por Pais` (top 5)
  - `Top 10 Ciudades`
  - `Tecnologia de Usuario`:
    - tipo de dispositivo
    - sistema operativo
    - navegador
- Si el tenant no tiene GA4, estas secciones no cargan data y la vista puede limitarse a paid/acquisition segun configuracion.

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
- Si un tenant no tiene `ga4_property_id` (ejemplo actual `hyundai_hn`), GA4 se omite para ese tenant y el pipeline continua normalmente.
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
- Los bundles de `reports/<tenant>/` son estado runtime. Un `git pull` no backfillea datasets nuevos ni crea historicos faltantes por tenant.
- Guardrail de historico: el pipeline protege contra bootstrap-start mayor al piso historico del tenant (por defecto `2025-01-01`) salvo que se use `--allow-historical-truncation`.
- Si sincronizas JSONs de produccion a local o a DO, sincroniza tambien `reports/<tenant>/dashboard/*.parquet`; el dashboard prioriza parquet sobre JSON y un bundle stale puede recortar la fecha maxima visible en filtros.

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

### Sync manual de runtime y reportes a DO

Usar este flujo cuando:
- se crea un tenant nuevo y ya existe historico local en `reports/<tenant>/`
- se agregan datasets nuevos al pipeline/dashboard y produccion no tiene cobertura historica para un tenant
- se ajusta `config/dashboard_settings.json`, logos u otros archivos runtime no versionados

Flujo recomendado:

1. Validar localmente el tenant afectado.
2. Respaldar en DO antes de pisar archivos runtime o `reports/<tenant>/`.
3. Subir solo el tenant afectado, no todo `reports/`.
4. Ejecutar un refresh corto del tenant en DO para traer el dia actual.
5. Verificar cobertura y UI.

Ejemplo operativo:

```bash
# 1) Backup remoto
ssh juanm@<droplet_ip> "mkdir -p /opt/yap/app/reports_backups && cp -a /opt/yap/app/reports/yap /opt/yap/app/reports_backups/yap_$(date +%Y%m%d_%H%M%S)"

# 2) Subir reportes del tenant desde local
scp reports/yap/yap_historical.json juanm@<droplet_ip>:/tmp/yap_sync/yap_historical.json
scp reports/yap/dashboard/*.parquet juanm@<droplet_ip>:/tmp/yap_sync/dashboard/

# 3) Aplicar en DO
ssh juanm@<droplet_ip> "cp /tmp/yap_sync/yap_historical.json /opt/yap/app/reports/yap/yap_historical.json && cp /tmp/yap_sync/dashboard/*.parquet /opt/yap/app/reports/yap/dashboard/"

# 4) Refresh corto del tenant
ssh juanm@<droplet_ip> "/opt/yap/venv/bin/python /opt/yap/app/scripts/yap_daily_cpl_report.py --tenant-id yap --tenants-config-path /opt/yap/app/config/tenants.json --mode auto --bootstrap-start 2025-01-01 --end-date $(date +%F) --refresh-lookback-days 1"
```

Notas:
- Para `config/dashboard_settings.json`, logos o `config/users.json`, subir esos archivos por separado porque son runtime y no entran por `git pull`.
- Si el tenant no existe todavia en `reports/` de DO, copiar primero el folder completo local del tenant (`reports/<tenant>/`) evita que el hourly intente bootstrap completo.
- Si un rerun historico local falla por Meta/Google pero el JSON/parquet local ya quedo correcto, se puede sincronizar ese estado validado a DO y luego hacer solo un refresh corto.
- Si despues de copiar `*_historical.json` el dashboard sigue mostrando una fecha vieja, revisar primero `reports/<tenant>/dashboard/daily.parquet` y el resto del bundle parquet antes de asumir un problema de filtros.

Validar datos para rango de un dia (hora real + top piezas):

```bash
cd /opt/yap/app
jq '.hourly|length' reports/yap/yap_historical.json
jq '.traffic_acquisition.paid_piece_daily|length' reports/yap/yap_historical.json
```

Validar cobertura de datasets nuevos por tenant:

```bash
cd /opt/yap/app
python3 - <<'PY'
import json
from pathlib import Path
p = Path("reports/yap/yap_historical.json")
data = json.loads(p.read_text(encoding="utf-8"))
for key in [
    "ga4_active_users_hourly",
    "ga4_country_users_daily",
    "ga4_city_users_daily",
    "ga4_device_users_daily",
    "ga4_operating_system_users_daily",
    "ga4_browser_users_daily",
]:
    rows = data.get("traffic_acquisition", {}).get(key, [])
    dates = sorted({str(r.get("date", ""))[:10] for r in rows if r.get("date")})
    print(key, dates[0] if dates else None, dates[-1] if dates else None, len(rows))
PY
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
