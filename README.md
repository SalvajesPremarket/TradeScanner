# TradeScanner

Implementación nueva desde cero. No importar ni recuperar código de sistemas anteriores.

## Estado implementado

- Interfaz limpia servida por FastAPI.
- `GET /health`: health check básico.
- `GET /api/status`: indica si están configuradas las credenciales Schwab, sin exponer secretos.
- `GET /api/quotes?symbols=AAPL,MSFT`: consulta cotizaciones reales a Schwab usando OAuth refresh token.
- Normalización de precio, cierre anterior, cambio, cambio porcentual, volumen y hora de cotización cuando el proveedor los devuelve.
- Cálculo de GAP solo cuando existen precio y cierre anterior válidos.
- Filtros iniciales de precio, GAP, RVOL y volumen; los filtros activos excluyen datos ausentes en lugar de inventarlos.
- Pruebas unitarias para validación de símbolos, normalización y filtros.
- Almacenamiento SQLite de snapshots observados y colector por lotes de hasta 100 símbolos (`tradescanner/snapshots.py`).
- `GET /api/snapshots/status`: muestra cantidad de snapshots guardados, símbolos vistos y hora del último registro.
- GitHub Actions configurado para ejecutar las pruebas en push y pull request.
- `.gitignore` evita subir archivos de entorno locales.

## Pendiente antes de considerar el motor operativo

- Ejecutar y verificar CI; la existencia del workflow no significa que las pruebas ya hayan pasado.
- Validar la autenticación y cotizaciones con credenciales Schwab nuevas y válidas.
- Construir el universo de símbolos y el ciclo de escaneo completo.
- Implementar ingestión de velas live, asociación vela/snapshot y cálculo de frescura.
- Completar RVOL con una fuente histórica verificable y FLOAT con una fuente fiable.
- Configurar `TRADESCANNER_DB_PATH` apuntando a almacenamiento persistente de Render; la ruta local predeterminada no garantiza conservar datos tras reinicios o despliegues.
- Recuperación ante fallos, métricas y despliegue de producción.
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

Nunca guardar credenciales en GitHub. Rota cualquier clave o token compartido en el chat o almacenado en un servicio antiguo antes de volver a usarlo.

## Render

- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
