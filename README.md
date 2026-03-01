# MCP Juan (GA4 + Google Ads + Meta Ads)

Repositorio operativo para analitica y performance con:

- `analytics-mcp` (GA4)
- `google-ads-mcp` (Google Ads)
- `meta-ads-mcp` (Meta Ads)

## Estado actual (2026-03-01)

- Conectividad MCP validada en esta maquina para GA4, Google Ads y Meta Ads.
- JSON principal del dashboard YAP actualizado con corte completo al `2026-02-28`.
- Dashboard Streamlit corriendo en `http://localhost:8501`.
- Token de Meta validado como vigente (detalle en seccion de token).

## Archivos clave

- `dashboard.py` (dashboard principal)
- `scripts/yap_daily_cpl_report.py` (pipeline de datos YAP)
- `reports/yap/YAP_historical.json` (fuente principal del dashboard)
- `reports/yap/YAP_organic_historical.json` (modulo organico IG/FB)
- `reports/dashboard_streamlit.log` (log de Streamlit)
- `reports/dashboard_streamlit.err.log` (errores de Streamlit)
- `scripts/install-mcp-juan.ps1`
- `scripts/verify-mcp-juan.ps1`
- `scripts/meta-token-guard.ps1`

## Flujo operativo del dashboard YAP

1. Regenerar JSON principal (bootstrap completo):
```powershell
python scripts\yap_daily_cpl_report.py --mode bootstrap --bootstrap-start 2026-01-01 --end-date 2026-02-28 --output-path reports\yap\YAP_historical.json
```

Este comando tambien genera/actualiza:
- `reports/yap/YAP_organic_historical.json` (lookback configurable, por defecto 30 dias).

2. Actualizacion diaria incremental (modo auto):
```powershell
python scripts\yap_daily_cpl_report.py --mode auto --output-path reports\yap\YAP_historical.json
```

Opcional para organico:
```powershell
python scripts\yap_daily_cpl_report.py --mode auto --organic-output-path reports\yap\YAP_organic_historical.json --organic-lookback-days 30
```

3. Correr dashboard:
```powershell
streamlit run dashboard.py
```

## Configuracion Meta recomendada (config.toml)

En `~/.codex/config.toml`, bajo `[mcp_servers.meta-ads-mcp.env]`, definir:

- `META_ADS_ACCESS_TOKEN`
- `META_AD_ACCOUNT_ID`
- `META_FACEBOOK_PAGE_ID` (opcional, se puede descubrir desde ad account)
- `META_INSTAGRAM_BUSINESS_ACCOUNT_ID` (opcional, se puede descubrir)
- `META_FACEBOOK_PAGE_ACCESS_TOKEN` (recomendado para metricas organicas de Facebook)
- `META_APP_ID`
- `META_APP_SECRET`

## Ultimo corte ejecutado (completo)

- Archivo: `reports/yap/YAP_historical.json`
- `updated_range`: `2026-01-01` a `2026-02-28`
- `run_kind`: `bootstrap`
- Fecha maxima en `daily`: `2026-02-28`
- Huecos de fecha en `2026-01-01..2026-02-28`: `0`

## Estado token Meta (ultima validacion)

Validado el `2026-03-01`:

- `is_valid`: `true`
- `type`: `USER`
- `application`: `mcp-antigravity`
- `expires_at_utc`: `2026-04-29T12:58:27+00:00`
- `days_left` al momento de la validacion: `59`
- `check_method`: `self_debug_token`

## Uso rapido en otro ordenador

1. Edita primero los archivos `*.private` y completa:
- rutas de binarios (`analytics-mcp.exe`, `mcp-google-ads.exe`) si no estan en `PATH`
- credenciales y variables de entorno

2. Instala la configuracion local:
```powershell
Set-ExecutionPolicy -Scope Process Bypass
H:\My Drive\MCP_Portable\MCP Juan\scripts\install-mcp-juan.ps1 -Mode Symlink
```

3. Valida:
```powershell
H:\My Drive\MCP_Portable\MCP Juan\scripts\verify-mcp-juan.ps1
```

Si Symlink falla por politicas de Windows:
```powershell
H:\My Drive\MCP_Portable\MCP Juan\scripts\install-mcp-juan.ps1 -Mode Copy
```

Si Claude estaba abierto:
```powershell
H:\My Drive\MCP_Portable\MCP Juan\scripts\install-mcp-juan.ps1 -Mode Copy -StopClaude
```

## Meta Token Guard (opcional)

Si usas Meta Ads MCP en tu entorno local, este script ayuda a evitar caidas por expiracion de token:

- valida token actual con `debug_token`
- refresca con `fb_exchange_token` si faltan pocos dias
- actualiza `META_ADS_ACCESS_TOKEN` en `~/.codex/config.toml` (o ruta indicada)
- crea backup antes de escribir
- valida scopes requeridos para organico: `instagram_basic` y `instagram_manage_insights`

Ejemplo manual:
```powershell
$env:META_APP_ID = "TU_APP_ID"
$env:META_APP_SECRET = "TU_APP_SECRET"
$env:META_ADS_ACCESS_TOKEN = "TU_TOKEN_ACTUAL"

.\scripts\meta-token-guard.ps1 -RefreshThresholdDays 10 -ConfigPath "$env:USERPROFILE\.codex\config.toml"
```

Para probar sin tocar archivo:
```powershell
.\scripts\meta-token-guard.ps1 -SkipConfigUpdate
```

Programar ejecucion diaria (Task Scheduler):
```powershell
$script = "G:\Mi unidad\CO\IA\MCP Juan\scripts\meta-token-guard.ps1"
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$script`" -RefreshThresholdDays 10 -ConfigPath `"$env:USERPROFILE\.codex\config.toml`""
$trigger = New-ScheduledTaskTrigger -Daily -At 6:00am
Register-ScheduledTask -TaskName "MetaTokenGuardDaily" -Action $action -Trigger $trigger -Description "Auto refresh Meta token"
```
