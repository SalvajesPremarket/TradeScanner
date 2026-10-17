# TradeScanner

Implementación nueva desde cero. No importar ni recuperar código de sistemas anteriores.

## Estado implementado

- Interfaz limpia servida por FastAPI.
- `GET /health`: health check básico.
- `GET /api/status`: indica si están configuradas las credenciales Schwab, sin exponer secretos.
- `GET /api/universe/status`: obtiene el directorio público de Nasdaq Trader, informa la cantidad de símbolos soportados, fecha del archivo y una muestra. Incluye ETFs y excluye issues de prueba; formatos de símbolo no compatibles se omiten.
- `GET /api/quotes?symbols=AAPL,MSFT`: consulta cotizaciones reales a Schwab usando OAuth refresh token.
- Normalización de precio, cierre anterior, cambio, cambio porcentual, volumen y hora de cotización cuando el proveedor los devuelve.
- Cálculo de GAP solo cuando existen precio y cierre anterior válidos.
- Filtros iniciales de precio, GAP, RVOL y volumen; los filtros activos excluyen datos ausentes en lugar de inventarlos.
- Pruebas unitarias para validación de símbolos, normalización y filtros.
- Almacenamiento SQLite de snapshots observados y colector por lotes de hasta 100 símbolos (`tradescanner/snapshots.py`).
- `GET /api/snapshots/status`: muestra cantidad de snapshots guardados, símbolos vistos y hora del último registro.
- `GET /api/snapshots/latest?limit=100`: devuelve los snapshots más recientes por símbolo (límite de 1 a 1000).
- `POST /api/scan?symbols=AAPL,MSFT`: ejecuta un ciclo bajo demanda y guarda solo cotizaciones recibidas.
- Motor automático incremental: usa el universo listado de Nasdaq Trader y consulta un lote de hasta 100 símbolos por ciclo; persiste el cursor en SQLite para continuar después de reinicios.
- `GET /api/engine/status`: expone estado, último ciclo, cursor, contadores y errores sin incluir secretos.
- Caché en memoria del access token de Schwab, con renovación anticipada al vencimiento para no solicitar un token por cada lote.
- Tabla UI conectada a snapshots reales guardados y estado del motor; no muestra filas inventadas.
- Pruebas unitarias de normalización, filtros, snapshots y parser del universo.
- GitHub Actions configurado para ejecutar las pruebas en push y pull request.
- `.gitignore` evita subir archivos de entorno locales.

## Pendiente antes de considerar el motor operativo

- Validar la autenticación y cotizaciones con credenciales Schwab nuevas y válidas.
- Validar la autenticación y cotizaciones con credenciales Schwab nuevas y válidas.
- Implementar ingestión de velas live, asociación vela/snapshot y cálculo de frescura.
- Implementar ingestión de velas live, asociación vela/snapshot y cálculo de frescura.
- Completar RVOL con una fuente histórica verificable y FLOAT con una fuente fiable.
- Configurar `TRADESCANNER_DB_PATH` apuntando a almacenamiento persistente de Render; la ruta local predeterminada no garantiza conservar datos tras reinicios o despliegues.
- Validar límites y ritmo del proveedor con datos reales; el ciclo inicial es deliberadamente secuencial y conservador.
- Conectar la tabla de la interfaz a la API y comprobar el flujo end-to-end.

## Ejecutar localmente

```bash
python -m pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

Abrir `http://localhost:8000`. Documentación: `http://localhost:8000/docs`.

## Variables de entorno

Configurar solo en el entorno seguro del servicio:

- `SCHWAB_CLIENT_ID`
- `SCHWAB_CLIENT_SECRET`
- `SCHWAB_REFRESH_TOKEN`
- `TRADESCANNER_DB_PATH` (opcional; recomendado en producción para usar un disco persistente)
- `TRADESCANNER_AUTO_SCAN_ENABLED` (opcional; predeterminado `true`; usar `false` para desactivar el ciclo automático)
- `TRADESCANNER_SCAN_INTERVAL_SECONDS` (opcional; predeterminado 30 segundos, mínimo 10)

Nunca guardar credenciales en GitHub. Rota cualquier clave o token compartido en el chat o almacenado en un servicio antiguo antes de volver a usarlo.

## Render

- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
