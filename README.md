# MCP Juan (GA4 + Google Ads + Meta Ads)

Repositorio operativo para analitica y performance de YAP:

- `analytics-mcp` (GA4)
- `google-ads-mcp` (Google Ads)
- `meta-ads-mcp` (Meta Ads)
- Dashboard en Streamlit (`dashboard.py`)
- Pipeline diario (`scripts/yap_daily_cpl_report.py`)

## Estado actual (2026-03-01)

- Produccion desplegada en DigitalOcean (Droplet Ubuntu).
- Dominio objetivo: `analitica.ipalmera.com` (Cloudflare al frente).
- Dashboard corriendo como servicio systemd: `yap-dashboard.service`.
- Pipeline automatico diario corriendo con timer systemd: `yap-pipeline.timer`.
- Actualizacion programada: todos los dias a las `02:00` (zona `America/Bogota`), con datos del dia anterior.
- Historico reconstruido incluyendo 2025.

## Estructura principal

- `dashboard.py` -> UI principal.
- `scripts/yap_daily_cpl_report.py` -> extraccion y consolidacion.
- `reports/yap/YAP_historical.json` -> fuente principal del dashboard.
- `reports/yap/YAP_organic_historical.json` -> modulo organico (si hay acceso/permisos).
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
python scripts\yap_daily_cpl_report.py --mode auto --output-path reports\yap\YAP_historical.json --organic-output-path reports\yap\YAP_organic_historical.json --organic-lookback-days 30
```

2. Levantar dashboard local:

```powershell
streamlit run dashboard.py
```

## Operacion en produccion (DigitalOcean)

### Servicios systemd

- Dashboard:
  - `yap-dashboard.service`
  - expone Streamlit en `127.0.0.1:8501`
- Pipeline diario:
  - `yap-pipeline.service` (oneshot)
  - `yap-pipeline.timer` con `OnCalendar=*-*-* 02:00:00`

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

## Flujo de datos diario

1. A las 2:00 a. m. el timer dispara `yap-pipeline.service`.
2. El script corre en `--mode auto` y toma `end-date = ayer`.
3. Actualiza `reports/yap/YAP_historical.json`.
4. Streamlit lee ese JSON en cada recarga del dashboard.

No hay upload manual al dashboard: el dashboard consume el JSON local actualizado.

## Bootstrap historico (ejemplo 2025+)

Reconstruccion completa:

```bash
/opt/yap/venv/bin/python /opt/yap/app/scripts/yap_daily_cpl_report.py --mode bootstrap --bootstrap-start 2025-01-01 --end-date $(date -d "yesterday" +%F) --output-path /opt/yap/app/reports/yap/YAP_historical.json --organic-output-path /opt/yap/app/reports/yap/YAP_organic_historical.json --organic-lookback-days 30
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
