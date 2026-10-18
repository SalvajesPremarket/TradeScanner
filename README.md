# TradeScanner

Reconstrucción limpia e independiente. No importar ni recuperar código de sistemas anteriores. TradeBoot permanece separado y fuera de esta fase.

## Motor implementado en esta rama

- FastAPI con health check, estado del motor, universo, cotizaciones y snapshots.
- Universo de símbolos listados desde Nasdaq Trader; no se generan símbolos ficticios.
- Consulta Schwab OAuth y cotizaciones reales por lotes de hasta 100 símbolos.
- Motor incremental que persiste el cursor y el estado en SQLite para continuar tras reinicios.
- Ciclo automático configurable por variables de entorno; solo avanza el cursor después de guardar snapshots.
- Caché temporal del token de acceso Schwab con renovación anticipada.
- Captura acotada de velas intradía para un candidato de cada lote de cotizaciones; se guarda OHLCV de Schwab en SQLite.
- RVOL intradía calculado como volumen acumulado de la sesión actual frente al promedio acumulado de hasta cinco sesiones anteriores a la misma hora de Nueva York. Se excluyen sesiones históricas incompletas; si no hay historial suficiente, devuelve `null`.
- FLOAT: se acepta únicamente si Schwab devuelve un campo explícito `floatShares`. Schwab normalmente expone `sharesOutstanding`, que no equivale a FLOAT; nunca se usa como sustituto. Si no hay un campo FLOAT verificable, el valor queda `null` y el filtro FLOAT no incluye ese símbolo.
- Interfaz conectada a snapshots guardados, estado del motor y RVOL calculado cuando existe historial.
- Las credenciales nunca se guardan en el repositorio.

## Endpoints

- `GET /health`: estado básico del servicio.
- `GET /api/status`: configuración y estado de conexión con Schwab.
- `GET /api/universe/status`: tamaño y muestra del universo.
- `GET /api/engine/status`: estado, último lote, cursor, ciclos y estado de velas.
- `GET /api/snapshots/status`: métricas de almacenamiento.
- `GET /api/snapshots/latest?limit=100`: snapshots más recientes por símbolo, con RVOL solo si puede calcularse.
- `GET /api/candles/AAPL?limit=500`: velas guardadas y cálculo RVOL de ese símbolo.
- `GET /api/quotes?symbols=AAPL,MSFT`: consulta de cotizaciones y filtros básicos.
- `POST /api/scan?symbols=AAPL,MSFT`: captura de snapshots bajo demanda.

## Pendiente antes de declarar producción

- Validar autenticación y cotizaciones con credenciales Schwab válidas en un entorno seguro.
- Probar autenticación y cotizaciones reales de Schwab en un entorno seguro antes de declarar la conexión validada.
- Mejorar la cobertura de velas: por límite de solicitudes, el ciclo captura velas para un candidato por lote, no para todo el universo en cada ciclo.
- Verificar frescura de las velas y el comportamiento de RVOL con sesiones reales y datos extendidos de Schwab.
- Confirmar con respuestas reales de Schwab si el endpoint autorizado para esta cuenta devuelve un campo FLOAT explícito; la documentación habitual de cotizaciones expone `sharesOutstanding`, no un número de float shares. Si no lo devuelve, FLOAT permanece `null` sin proveedor externo.
- Conectar almacenamiento SQLite a disco persistente de Render antes de despliegues productivos.
- Ejecutar y verificar CI después de estos cambios, revisar límites de API y completar prueba end-to-end.
- Los filtros actuales funcionan en la interfaz sobre snapshots reales almacenados; solo filtran los datos que estén disponibles y no fabrican valores.

## Ejecutar localmente

```bash
python -m pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

Abrir `http://localhost:8000`; documentación API en `http://localhost:8000/docs`.

## Variables de entorno

- `SCHWAB_CLIENT_ID`
- `SCHWAB_CLIENT_SECRET`
- `SCHWAB_REFRESH_TOKEN`
- `TRADESCANNER_DB_PATH` (recomendado en producción, apuntando a almacenamiento persistente)
- `TRADESCANNER_AUTO_SCAN_ENABLED` (predeterminado `false`; establecer `true` explícitamente solo después de validar Schwab y almacenamiento persistente)
- `TRADESCANNER_SCAN_INTERVAL_SECONDS` (predeterminado 30; mínimo 10)

No publicar claves o tokens en GitHub ni en el chat. Si una credencial se compartió previamente en texto, rotarla antes de utilizarla.

## Render (cuando el motor esté validado)

- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`

No desplegar todavía: falta validar datos reales y configurar almacenamiento persistente.
