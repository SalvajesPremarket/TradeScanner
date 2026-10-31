# TradeScanner

Scanner de acciones basado en cotizaciones e histórico de **Schwab**, con persistencia SQLite/PostgreSQL, evaluación de estrategias restringidas y datos FLOAT de Massive. TradeBoot es un proyecto separado y no forma parte de este repositorio.

## Arquitectura actual

- `app.py`: aplicación FastAPI, rutas HTTP y ciclo de vida del worker.
- `index.html`: interfaz única. El gráfico técnico está integrado dentro de **Screener / Acciones**; no existe una pestaña independiente de gráficos.
- `tradescanner/`: módulos del motor, proveedor Schwab, velas, persistencia, universo, estrategias, noticias y FLOAT.
- `tests/`: pruebas automatizadas del backend.
- `.github/workflows/tests.yml`: ejecuta `python -m unittest discover -s tests -v` en cada push y pull request de las ramas configuradas.

## Proveedores y datos

- **Schwab**: OAuth, cotizaciones, velas e histórico diario.
- **Massive**: FLOAT exclusivamente; se utiliza caché local y no se consulta al refrescar la tabla.
- **Google News RSS**: titulares auxiliares con fecha publicada; no se inventan noticias.
- **Nasdaq Trader**: universo público de símbolos.
- **Infisical**: almacén opcional del refresh token de Schwab.

Los valores ausentes permanecen ausentes. El scanner no sustituye FLOAT por shares outstanding ni fabrica precios, velas, volumen, RVOL o noticias.

## Rutas principales

- `GET /health`: salud básica.
- `GET /api/status`: configuración y frescura de cotizaciones Schwab.
- `GET /api/engine/status`: estado del worker, ciclo, cursor y métricas.
- `GET /api/universe/status`: universo de símbolos.
- `GET /api/snapshots/status`: almacenamiento y métricas de snapshots.
- `GET /api/snapshots/latest?limit=100&timeframe=1m`: snapshots más recientes con EMA20/MACD calculados en la temporalidad seleccionada; admite `1m`, `5m`, `15m`, `30m`, `1h`, `1d`, `1w`.
- `GET /api/candles/{symbol}?limit=500&timeframe=1m`: histórico Schwab; temporalidades admitidas: `1m`, `5m`, `15m`, `1h`, `day`, `week`.
- `GET /api/quotes?symbols=AAPL,MSFT`: cotizaciones y filtros básicos.
- `POST /api/scan?symbols=AAPL,MSFT`: escaneo manual, máximo 100 símbolos únicos.
- `GET /api/strategies`: estrategias guardadas.
- `POST /api/strategies`: guardar o editar estrategia.
- `POST /api/strategies/{id}/activate`: activar o desactivar estrategia.
- `DELETE /api/strategies/{id}`: eliminar estrategia.
- `GET /api/strategies/active-matches`: evaluaciones y coincidencias de estrategias activas.
- `POST /api/diagnostics/float/massive?symbol=AAPL`: consulta diagnóstica explícita de FLOAT Massive.

Las rutas de administración de estrategias requieren un JWT válido de Neon Auth (`Authorization: Bearer ...`) y una sesión de dispositivo activo.

## Configuración

Variables privadas de entorno:

- `SCHWAB_CLIENT_ID`
- `SCHWAB_CLIENT_SECRET`
- `SCHWAB_REFRESH_TOKEN` (bootstrap si no se utiliza Infisical)
- `INFISICAL_CLIENT_ID`
- `INFISICAL_CLIENT_SECRET`
- `INFISICAL_PROJECT_ID`
- `INFISICAL_ENVIRONMENT` (slug exacto del entorno)
- `INFISICAL_SECRET_PATH` (opcional; predeterminado `/`)
- `MASSIVE_API_KEY`
- `DATABASE_URL` (PostgreSQL administrado; prioridad sobre SQLite)
- `TRADESCANNER_DB_PATH` (SQLite local cuando no hay `DATABASE_URL`)
- `TRADESCANNER_AUTO_SCAN_ENABLED` (predeterminado `false`)
- `TRADESCANNER_SCAN_INTERVAL_SECONDS` (predeterminado 30; mínimo 10)
- `NEON_AUTH_JWKS_URL`
- `NEON_AUTH_ISSUER`

No guardar credenciales, tokens ni cadenas de conexión en el repositorio. Si una credencial se expuso, debe rotarse.

## Ejecución local

```bash
python -m pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

Abrir `http://localhost:8000`.

## Render

- Build: `pip install -r requirements.txt`
- Start: `uvicorn app:app --host 0.0.0.0 --port $PORT`

El despliegue automático está deshabilitado en el servicio configurado; los despliegues se realizan manualmente. Antes de habilitar el escaneo automático en producción, verificar credenciales Schwab, frescura de cotizaciones, persistencia PostgreSQL y comportamiento del worker tras reinicios.

## Política de mantenimiento del repositorio

- Modificar la implementación existente en lugar de crear una segunda versión paralela.
- Al sustituir una pantalla, ruta, módulo o proceso, retirar el anterior solo después de comprobar que no quedan referencias, imports, llamadas, tareas, pruebas ni dependencias activas.
- No conservar documentación o pruebas que describan proveedores, autenticación, pantallas o contratos ya retirados; actualizarlas al comportamiento real.
- No borrar módulos solo por su antigüedad o nombre: primero verificar el grafo de imports y las referencias desde la aplicación y los tests.
- Ejecutar la suite automatizada después de cada cambio y no declarar un cambio validado mientras CI siga fallando.
