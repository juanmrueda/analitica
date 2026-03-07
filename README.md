# MCP Juan (GA4 + Google Ads + Meta Ads)

Repositorio operativo para analitica de performance multi-tenant (YAP + Hyundai HN):

- `analytics-mcp` (GA4)
- `google-ads-mcp` (Google Ads)
- `meta-ads-mcp` (Meta Ads)
- Dashboard en Streamlit (`dashboard.py`)
- Pipeline orquestado por tenant (`scripts/run_all_tenants.py`)

## Estado actual (2026-03-06)

- Produccion desplegada en DigitalOcean (Droplet Ubuntu).
- Dominio: `analitica.ipalmera.com` (Cloudflare al frente).
- Version desplegada: `Multitenant V2-adminpanel` (commit `9ca64ae`).
- Dashboard activo como servicio systemd: `yap-dashboard.service`.
- Pipeline automatico cada hora: `yap-pipeline.timer` -> `yap-pipeline.service`.
- El servicio de pipeline ejecuta `scripts/run_all_tenants.py --mode auto`.
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
- `config/tenants.json` -> ids y rutas por tenant.
- `config/users.json` -> usuarios y permisos.
- `config/dashboard_settings.json` -> variables dinamicas del dashboard por tenant.
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
.\.venv\Scripts\python scripts\run_all_tenants.py --mode auto --organic-lookback-days 30
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

## Deploy de cambios

1. Commit y push en local.
2. En servidor:

```bash
cd /opt/yap/app
git pull --ff-only origin main
/opt/yap/venv/bin/pip install -r requirements.txt
sudo cp /opt/yap/app/config/systemd/yap-pipeline.timer /etc/systemd/system/yap-pipeline.timer
sudo systemctl daemon-reload
sudo systemctl restart yap-pipeline.timer
sudo systemctl restart yap-dashboard
sudo systemctl start yap-pipeline.service
```

## Configuracion sensible (no versionar)

- `~/.codex/config.toml` real con tokens.
- `ga4_user_oauth_analytics_ipalmera.json` real.
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
