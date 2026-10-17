# TradeScanner

Implementación nueva desde cero en este repositorio. No se importa código de repositorios antiguos.

## Estado actual

- Interfaz estática servida por FastAPI.
- `GET /health`: comprobación básica del proceso.
- `GET /api/status`: indica si están configuradas las credenciales nuevas de Schwab, sin exponer secretos.
- No se generan cotizaciones falsas, tickers de muestra ni resultados simulados.
- La conexión de datos de mercado en vivo y los cálculos del screener todavía están pendientes.

## Ejecutar localmente

```bash
python -m pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

Abrir `http://localhost:8000`.

## Variables de entorno

El adaptador nuevo de Schwab utilizará variables de entorno del servidor:

- `SCHWAB_CLIENT_ID`
- `SCHWAB_CLIENT_SECRET`
- `SCHWAB_REFRESH_TOKEN`

Nunca guardar credenciales en el repositorio. Rota las credenciales que se hayan compartido en el chat o guardado en servicios antiguos antes de reutilizarlas.

## Despliegue en Render

- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
