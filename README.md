# MCP Juan (GA4 + Google Ads + Meta Ads)

Repositorio operativo para analitica de performance multi-tenant (YAP + Hyundai HN):

- `analytics-mcp` (GA4)
- `google-ads-mcp` (Google Ads)
- `meta-ads-mcp` (Meta Ads)
- Dashboard en Streamlit (`dashboard.py`)
- Pipeline orquestado por tenant (`scripts/run_all_tenants.py`)

## Estado actual (2026-03-10)

- Produccion desplegada en DigitalOcean (Droplet Ubuntu).
- Dominio: `analitica.ipalmera.com` (Cloudflare al frente).
- Version desplegada en app: commit `f9e2f88` (main).
- Dashboard activo como servicio systemd: `yap-dashboard.service`.
- Pipeline automatico cada hora: `yap-pipeline.timer` -> `yap-pipeline.service`.
- El servicio de pipeline ejecuta `scripts/run_all_tenants.py --mode auto --organic-lookback-days 30`.
- Tenants activos en `config/tenants.json`:
  - `yap`
  - `hyundai_hn`
- El tenant `ipalmera_regional` fue retirado de la configuracion activa.

## Estructura principal

- `dashboard.py` -> UI principal.
- `scripts/run_all_tenants.py` -> ejecuta el pipeline para todos los tenants configurados.
- `scripts/yap_daily_cpl_report.py` -> extraccion y consolidacion por tenant.
- `reports/yap/yap_historical.json` -> fuente principal tenant YAP.
- `reports/hyundai_hn/hyundai_hn_historical.json` -> fuente principal tenant Hyundai.
- `reports/*/*_organic_historical.json` -> modulo organico por tenant.
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

3. Ejecutar pipeline para un tenant puntual:

```powershell
.\.venv\Scripts\python scripts\yap_daily_cpl_report.py --tenant-id hyundai_hn --mode auto
```

4. Levantar dashboard local:

```powershell
.\.venv\Scripts\streamlit run dashboard.py
```

5. Login local:

```powershell
copy config\users.template.json config\users.json
copy config\dashboard_settings.template.json config\dashboard_settings.json
```

Usuarios base del template:
- `admin` (rol `admin`)
- `hyundai` (rol `viewer`, tenant `hyundai_hn`)
- `yap` (rol `viewer`, tenant `yap`)

Definir o resetear passwords desde `config/users.json` o desde el panel de Administracion.

## Panel de Administracion (dashboard)

- Gestion de usuarios por tenant.
- Activacion/desactivacion de vistas del menu lateral.
- Configuracion dinamica de KPIs y secciones por tenant.
- Vista/plataforma por defecto por tenant.
- Upload de logo por tenant (se guarda en `assets/logos`).
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
- Si un tenant no tiene `ga4_property_id` (ejemplo actual `hyundai_hn`), GA4 se omite para ese tenant y el pipeline continua normalmente.
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
