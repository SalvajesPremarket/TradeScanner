# TradeScanner

Reconstrucción limpia e independiente. No importar ni recuperar código de sistemas anteriores. TradeBoot permanece separado y fuera de esta fase.

## Motor implementado en esta rama

- FastAPI con health check, estado del motor, universo, cotizaciones y snapshots.
- Universo de símbolos listados desde Nasdaq Trader; no se generan símbolos ficticios.
- Consulta Schwab OAuth y cotizaciones reales por lotes de hasta 100 símbolos.
- Motor incremental que persiste el cursor y el estado en SQLite o PostgreSQL (`DATABASE_URL`) para continuar tras reinicios.
- Ciclo automático configurable por variables de entorno; solo avanza el cursor después de guardar snapshots.
- Caché temporal del token de acceso Schwab con renovación anticipada.
- Captura acotada de velas intradía para un candidato de cada lote de cotizaciones; se guarda OHLCV de Schwab en SQLite o PostgreSQL.
- RVOL intradía calculado como volumen acumulado de la sesión actual frente al promedio acumulado de hasta cinco sesiones anteriores a la misma hora de Nueva York. Se excluyen sesiones históricas incompletas; si no hay historial suficiente, devuelve `null`.
- FLOAT: primero se utiliza un campo FLOAT explícito de Schwab si está disponible. Como fuente secundaria opcional, FMP se consulta exclusivamente para FLOAT mediante su endpoint de flotación; nunca para cotizaciones, volumen, GAP ni RVOL. El conjunto de flotación se guarda en SQLite y se refresca como máximo una vez cada 24 horas; si FMP falla, se aplica una pausa de seis horas antes de reintentar. La interfaz solo lee la caché local, por lo que actualizar la tabla no genera solicitudes FMP. `sharesOutstanding` nunca se usa como sustituto de FLOAT.
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
- `GET /api/strategies`: lista las estrategias guardadas.
- `POST /api/strategies`: guarda o edita una estrategia (`name`, `language`, `source`, opcionalmente `id`).
- `POST /api/strategies/{id}/activate`: activa/desactiva una estrategia; solo una puede estar activa.
- `DELETE /api/strategies/{id}`: elimina una estrategia.
- `GET /api/strategies/active-matches`: símbolos que cumplen la fórmula activa y tienen cotización no marcada como desactualizada.

## Pendiente antes de declarar producción

- Validar autenticación y cotizaciones con credenciales Schwab válidas en un entorno seguro.
- Probar autenticación y cotizaciones reales de Schwab en un entorno seguro antes de declarar la conexión validada.
- Mejorar la cobertura de velas: por límite de solicitudes, el ciclo captura velas para un candidato por lote, no para todo el universo en cada ciclo.
- Verificar frescura de las velas y el comportamiento de RVOL con sesiones reales y datos extendidos de Schwab.
- Configurar `FMP_API_KEY` como secreto privado en Render para habilitar el proveedor secundario de FLOAT. Sin esa variable, FMP permanece deshabilitado y el scanner sigue funcionando con Schwab; no se necesita ni se consulta FMP para otros campos.
- Configurar `DATABASE_URL` con la cadena privada de Neon en Render y comprobar snapshots, velas, FLOAT y estado tras reiniciar antes de producción.
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
- `SCHWAB_REFRESH_TOKEN` (solo bootstrap inicial; con Infisical configurado, el valor del vault prevalece)
- `INFISICAL_CLIENT_ID` (ID de la identidad de máquina)
- `INFISICAL_CLIENT_SECRET` (secreto de la identidad de máquina; solo en variables privadas de Render)
- `INFISICAL_PROJECT_ID` (ID del proyecto TradeScanner en Infisical)
- `INFISICAL_ENVIRONMENT` (obligatorio: slug exacto del entorno Infisical; confirmar si es `production`, `prod` u otro valor)
- `INFISICAL_SECRET_PATH` (ruta de secretos; por defecto `/`)
- `DATABASE_URL` (opcional; URL PostgreSQL administrada, por ejemplo Neon; tiene prioridad sobre SQLite)
- `TRADESCANNER_DB_PATH` (ruta SQLite local cuando no hay `DATABASE_URL`)
- `TRADESCANNER_AUTO_SCAN_ENABLED` (predeterminado `false`; mantener desactivado hasta validar cotizaciones reales)

Con Infisical configurado, el callback OAuth guarda el refresh token directamente en el vault y nunca lo muestra en la página. El adaptador lee el token del vault, conserva temporalmente el access token en memoria y persiste cualquier refresh token rotado. Sin Infisical configurado, el callback OAuth no comienza.
- `TRADESCANNER_SCAN_INTERVAL_SECONDS` (predeterminado 30; mínimo 10)
- `TRADESCANNER_STRATEGY_ADMIN_TOKEN` (obligatorio para habilitar el editor; crea un secreto largo y aleatorio en variables privadas de Render; no lo publiques ni lo compartas)

No publicar claves o tokens en GitHub ni en el chat. Si una credencial se compartió previamente en texto, rotarla antes de utilizarla.

## PostgreSQL / Neon

Si `DATABASE_URL` contiene una URL `postgres://` o `postgresql://`, el servicio utiliza PostgreSQL para snapshots, estado del motor, velas, estrategias y caché FLOAT. Sin esa variable, conserva SQLite. Nunca guardes la cadena de conexión en el repositorio ni en el chat.

La migración del código no copia automáticamente los datos de la base SQLite anterior a PostgreSQL; el nuevo almacenamiento comienza con sus propias tablas y datos.

## Render (cuando el motor esté validado)

- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`

No desplegar todavía: falta validar datos reales y configurar almacenamiento persistente.


## Interfaz de estrategias (rama en revisión)

- Las pestañas Screener/Acciones, Estrategias, Gráficos, Listas y Configuración consultan datos del servicio; gráficos muestra velas almacenadas, listas se guardan en el navegador y configuración es de solo lectura.
- Las estrategias con lenguaje **Fórmula** usan expresiones restringidas y una lista permitida de operadores y campos.
- El modo **Python seguro** solo acepta una expresión o `def evaluate(stock): return ...`; únicamente permite leer campos conocidos con `stock["gap_pct"]` y expresiones del evaluador restringido. No ejecuta imports, loops, llamadas arbitrarias ni acceso al sistema.
- El modo **ThinkScript** acepta solo expresiones simples de escaneo, como `plot scan = close > 5;`. No implementa indicadores que requieren historial, como `Average()`, ni todo el lenguaje ThinkScript.
- Todas las rutas de administración de estrategias requieren `X-Strategy-Admin-Token` y la variable privada `TRADESCANNER_STRATEGY_ADMIN_TOKEN`. Si no se configura, las rutas quedan bloqueadas. El token se introduce en el editor y no se guarda en localStorage.
- El token compartido es una barrera de acceso a la administración, no un sistema de cuentas individuales. No se deben compartir enlaces o claves en público; HTTPS es obligatorio en producción.
