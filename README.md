# TradeScanner

Implementación nueva desde cero en este repositorio. No se importa código de repositorios antiguos.

## Estado actual

- Interfaz estática servida por FastAPI.
- `GET /health`: comprobación básica del proceso.
- `GET /api/status`: informa si existen las tres variables de credenciales Schwab, sin exponer sus valores.
- `GET /api/quotes?symbols=AAPL,MSFT`: solicita cotizaciones reales a Schwab usando OAuth refresh token.
- Límite por petición: 100 símbolos únicos como máximo.
- No se generan cotizaciones falsas, tickers de muestra ni resultados simulados.
- El universo completo, el flujo de velas en vivo, el motor de snapshots y los filtros del screener aún no están implementados.

## Ejecutar localmente

```bash
python -m pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000
```

Abrir `http://localhost:8000`. Documentación de la API: `http://localhost:8000/docs`.

## Variables de entorno

Configurar únicamente en el entorno seguro del servicio:

- `SCHWAB_CLIENT_ID`
- `SCHWAB_CLIENT_SECRET`
- `SCHWAB_REFRESH_TOKEN`

Nunca guardar credenciales en el repositorio. Rota cualquier clave o token compartido en el chat o guardado en servicios antiguos antes de reutilizarlo.

## Despliegue en Render

- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
