# Nova Gold IA · Copiloto XAU/USD M15

## Estructura
```
nova_gold_ia/
├── main.py            # backend FastAPI (visión, riesgo, noticias, sesiones, Telegram, informe 06:45)
├── requirements.txt
├── .env.example       # copia a .env y completa tus claves
└── static/index.html  # la web (la sirve el mismo backend)
```

## Probar en tu computadora
```bash
pip install -r requirements.txt
cp .env.example .env        # y completa GEMINI_API_KEY (y TWELVEDATA_API_KEY si la tienes)
export $(grep -v '^#' .env | xargs)
uvicorn main:app --host 0.0.0.0 --port 8000
```
Abre http://localhost:8000

## Publicar en Render
1. Sube esta carpeta a un repositorio de GitHub.
2. En Render: New → Web Service → elige el repositorio.
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. En "Environment" añade las variables de `.env.example`.

La web y la API quedan en la misma dirección, así que no hay que configurar URLs ni CORS.

### Importante: plan gratuito de Render
El plan gratuito "duerme" el servidor tras unos 15 minutos sin visitas. Mientras duerme,
el informe de las 06:45 NO se genera. Dos soluciones:
- Usar un plan de pago (siempre encendido), o
- Programar en https://cron-job.org una llamada POST a `https://TU-APP.onrender.com/api/v1/brief/run`
  a las 06:44 (hora de Ecuador) de lunes a viernes, con la cabecera `X-Notify-Key: TU_NOTIFY_KEY`.

## Endpoints
| Método | Ruta | Uso |
| --- | --- | --- |
| POST | /api/v1/analyze-chart | Analiza la captura y devuelve el plan |
| GET | /api/v1/calendar | Calendario Forex Factory (en caché 1 h) |
| GET | /api/v1/price | Precio de XAU/USD (requiere Twelve Data) |
| GET | /api/v1/config | Qué servicios están configurados |
| POST | /api/v1/notify | Reenvía una alarma a Telegram |
| GET / POST | /api/v1/brief, /api/v1/brief/run | Informe pre-mercado |

Herramienta educativa. No es asesoría financiera.
