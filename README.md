# MCP Juan (GA4 + Google Ads + Meta Ads)

Repositorio operativo para analitica y performance de YAP:

- `analytics-mcp` (GA4)
- `google-ads-mcp` (Google Ads)
- `meta-ads-mcp` (Meta Ads)
- Dashboard en Streamlit (`dashboard.py`)
- Pipeline horario (`scripts/yap_daily_cpl_report.py`)

## Estado actual (2026-03-01)

- Produccion desplegada en DigitalOcean (Droplet Ubuntu).
- Dominio objetivo: `analitica.ipalmera.com` (Cloudflare al frente).
- Dashboard corriendo como servicio systemd: `yap-dashboard.service`.
- Pipeline automatico corriendo cada hora con timer systemd: `yap-pipeline.timer`.
- Actualizacion programada: cada hora (zona `America/Bogota`), con corte al dia actual.
- Historico reconstruido incluyendo 2025.

## Bitacora operativa (2026-03-05)

- UI/login:
  - Se elimino el background del login para que no herede el fondo del dashboard (`dashboard.py`).
  - Se migraron componentes Streamlit de `use_container_width` a `width="stretch"` para compatibilidad post-2025.
- Accesos:
  - Se agrego usuario local `yap` (rol `viewer`) con acceso restringido a tenant `yap` en `config/users.json`.
- Red local:
  - Se valido publicacion LAN de Streamlit con `--server.address 0.0.0.0` y apertura de puerto `8501` en firewall de Windows.
- Datos refrescados:
  - Corrida multi-tenant con corte al dia actual (2026-03-05).
  - Corrida puntual de YAP con corte al dia anterior (2026-03-04) para conciliacion MCP vs JSON.
- Scheduler:
  - `config/systemd/yap-pipeline.timer` actualizado a `OnCalendar=hourly`.
  - Defaults de `--end-date` cambiados a hoy en `scripts/run_all_tenants.py` y `scripts/yap_daily_cpl_report.py`.
- Validaciones de analitica:
  - GTM: evento `sesion_encript` confirmado como `eventName` de un tag GA4 Event (no user property).
  - GTM cross-domain/linker incluye `yap.com.gt` y `gtc.com.gt`.
  - GA4 (propiedad YAP): para febrero 2026, `sesion_encript` reporto `eventCount=921` y `totalUsers=696`.
- Validaciones de Meta:
  - Se listaron audiencias en `act_352965073056857` y `act_1808641036591815`.
  - En la cuenta operativa actual (`act_1808641036591815`) los ad sets activos usan intereses/Advantage; no se observaron `custom_audiences` activas.

Comandos usados en esta sesion:

```powershell
.\.venv\Scripts\python.exe scripts\run_all_tenants.py --mode auto --end-date 2026-03-05 --organic-lookback-days 30
.\.venv\Scripts\python.exe scripts\yap_daily_cpl_report.py --tenant-id yap --mode daily --end-date 2026-03-04
streamlit run dashboard.py --server.address 0.0.0.0 --server.port 8501
```

## Estructura principal

- `dashboard.py` -> UI principal.
- `scripts/yap_daily_cpl_report.py` -> extraccion y consolidacion.
- `reports/yap/yap_historical.json` -> fuente principal del dashboard.
- `reports/yap/yap_organic_historical.json` -> modulo organico (si hay acceso/permisos).
- `config/dashboard_settings.json` -> variables dinamicas del dashboard por tenant.
- `config/admin_audit.jsonl` -> bitacora local de cambios administrativos (usuarios/settings).
- `config/backups/` -> backups automaticos previos a cada guardado de configuracion.
- `requirements.txt` -> dependencias de runtime.
- `scripts/meta-token-guard.ps1` -> utilidad opcional de refresh de token Meta (flujo local Windows).

## Dependencias (repo)

Archivo `requirements.txt`:

- `streamlit==1.54.0`
- `pandas==2.3.3`
- `plotly==6.5.2`
- `matplotlib>=3.8,<4`

## Configuracion sensible (NO versionar)

No subir al repo:

- `~/.codex/config.toml` real con tokens.
- `ga4_user_oauth_analytics_ipalmera.json` real.
- respaldos con tokens (`backups/`, `*.bak`, `*.private`, etc).

En servidor se usan:

- `/home/juanm/.codex/config.toml`
- `/opt/yap/app/ga4_user_oauth_analytics_ipalmera.json`

## Operacion local (desarrollo)

1. Ejecutar pipeline:

```powershell
python scripts\yap_daily_cpl_report.py --mode auto --output-path reports\yap\yap_historical.json --organic-output-path reports\yap\yap_organic_historical.json --organic-lookback-days 30
```

Pipeline por tenant (multi-tenant, fase local):

```powershell
python scripts\yap_daily_cpl_report.py --tenant-id yap --mode auto
```

Pipeline para todos los tenants definidos en `config/tenants.json`:

```powershell
python scripts\run_all_tenants.py --mode auto
```

Nota multi-tenant:
- Puedes definir por cliente `ga4_conversion_event_name` en `config/tenants.json`.
- Ese evento alimenta el bloque GA4 del embudo (`Conversiones GA4` y `CPL (GA4)`), sin alterar conversiones nativas de Meta/Google.

2. Levantar dashboard local:

```powershell
streamlit run dashboard.py
```

3. Login local (bloque multi-tenant):

```powershell
copy config\users.template.json config\users.json
```

- Usuarios iniciales de ejemplo:
  - `admin` / `AdminYAP2026!` (rol `admin`, ve todos los tenants)
  - `hyundai` (rol `viewer`, solo `hyundai_hn`)
  - `yap` (rol `viewer`, solo `yap`)
- Ajustar `config/users.json` antes de usar en produccion.

4. Panel administrativo (Fase 6 hardening):

- Vista `Administración`:
  - Gestion de usuarios por tenant (crear/editar/eliminar, activar/desactivar, reset de password).
  - Variables dinamicas por tenant para dashboard (`KPIs`, `secciones`, `vista/plataforma por defecto`).
  - Toggle de `Meta Token Health` en sidebar por tenant.
  - Tab `Auditoría` con:
    - chequeo de integridad de configuracion;
    - log de cambios administrativos (`config/admin_audit.jsonl`);
    - descarga de auditoria en formato `jsonl`.

- Persistencia segura:
  - Antes de guardar `users.json` o `dashboard_settings.json`, se crea backup en `config/backups/`.

## Operacion en produccion (DigitalOcean)

### Servicios systemd

- Dashboard:
  - `yap-dashboard.service`
  - expone Streamlit en `127.0.0.1:8501`
- Pipeline diario:
  - `yap-pipeline.service` (oneshot)
  - `yap-pipeline.timer` con `OnCalendar=hourly`

### Comandos utiles

Ver estado:

```bash
sudo systemctl status yap-dashboard --no-pager
sudo systemctl status yap-pipeline.timer --no-pager
sudo systemctl status yap-pipeline.service --no-pager
```

Ver proxima corrida:

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

1. Cada hora el timer dispara `yap-pipeline.service`.
2. El script corre en `--mode auto` y toma `end-date = hoy`.
3. Actualiza `reports/yap/yap_historical.json`.
4. Streamlit lee ese JSON en cada recarga del dashboard.

No hay upload manual al dashboard: el dashboard consume el JSON local actualizado.

## Bootstrap historico (ejemplo 2025+)

Reconstruccion completa:

```bash
/opt/yap/venv/bin/python /opt/yap/app/scripts/yap_daily_cpl_report.py --mode bootstrap --bootstrap-start 2025-01-01 --end-date $(date -d "yesterday" +%F) --output-path /opt/yap/app/reports/yap/yap_historical.json --organic-output-path /opt/yap/app/reports/yap/yap_organic_historical.json --organic-lookback-days 30
```

## Publicar cambios de UI/codigo

1. Cambiar y validar en local.
2. Commit/push:

```bash
git add .
git commit -m "tu cambio"
git push origin main
```

3. Deploy en servidor:

```bash
cd /opt/yap/app
git pull origin main
/opt/yap/venv/bin/pip install -r requirements.txt
sudo cp /opt/yap/app/config/systemd/yap-pipeline.timer /etc/systemd/system/yap-pipeline.timer
sudo systemctl daemon-reload
sudo systemctl restart yap-pipeline.timer
sudo systemctl restart yap-dashboard
```

## Cloudflare recomendado

- SSL/TLS: `Full` (evitar `Flexible` para este setup).
- DNS de `analitica.ipalmera.com` apuntando al Droplet.
- Durante troubleshooting de certificado, usar temporalmente `DNS only`.

## Seguridad minima recomendada

- Usuario operativo no root (`juanm`).
- `ufw` activo con `22`, `80`, `443`.
- SSH por llaves.
- En `sshd_config`: `PermitRootLogin no` y `PasswordAuthentication no` (si ya validaste acceso por llave).
