"""
Nova Gold IA - backend (FastAPI)
Copiloto XAU/USD en M15: visión con Gemini + motor de riesgo + escudo Forex Factory
+ sesiones institucionales + informe pre-mercado de las 06:45 (Ecuador).

Cambios clave frente a la versión anterior:
  * La IA puede responder ESPERAR: ya no se fuerza una compra o venta en rangos.
  * Precio y ATR(14) reales desde Twelve Data (si hay API key); la IA solo lee la estructura.
  * Se rechaza la captura si el precio leído no coincide con el real (error de escala o de activo).
  * Calendario de Forex Factory en caché (respeta el límite de ~2 descargas / 5 min).
  * Sesiones calculadas con horario de verano/invierno real de Londres y Nueva York.
  * Llamada asíncrona a Gemini (no bloquea el servidor).
  * Aviso cuando el lote mínimo supera el riesgo elegido.
"""
import asyncio
import logging
import math
import os
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal, Optional
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

try:
    from google import genai
    from google.genai import types
except ImportError:  # permite arrancar sin la librería (modo demo)
    genai = None
    types = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("novagold")

# ---------------------------------------------------------------------------
# CONFIGURACIÓN (variables de entorno)
# ---------------------------------------------------------------------------
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")          # precio y velas M15 reales
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
NOTIFY_KEY = os.getenv("NOTIFY_KEY")                            # protege /notify y /brief/run
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
BRIEF_ENABLED = os.getenv("BRIEF_ENABLED", "true").lower() == "true"

ai_client = genai.Client(api_key=GEMINI_API_KEY) if (genai and GEMINI_API_KEY) else None

EC = ZoneInfo("America/Guayaquil")
NY = ZoneInfo("America/New_York")
LON = ZoneInfo("Europe/London")

# Parámetros del sistema Nova Gold (XAU/USD M15)
K_SL = 1.8               # stop = 1.8 x ATR(14)
MIN_SL = 3.50            # distancia mínima de stop en dólares
RR1, RR2 = 1.8, 3.0      # objetivos en múltiplos del riesgo
CONTRACT = 100           # 1 lote = 100 oz
MIN_LOT = 0.01
TP1_CLOSE = 0.60         # se cierra el 60 % en TP1 (si el lote lo permite)
BE_OFFSET = 0.20         # breakeven + spread
PENDING_CANDLES = 2      # la orden pendiente vale 2 velas (30 min)
POSITION_CANDLES = 8     # time-stop de la posición: 8 velas (2 h)
MAX_UPLOAD_BYTES = 8 * 1024 * 1024
PRICE_TOLERANCE = 0.01   # 1 % máximo entre precio leído y precio real

# ---------------------------------------------------------------------------
# UTILIDADES HTTP CON CACHÉ
# ---------------------------------------------------------------------------
_http: Optional[httpx.AsyncClient] = None


def http() -> httpx.AsyncClient:
    global _http
    if _http is None:
        _http = httpx.AsyncClient(timeout=15, headers={"User-Agent": "NovaGoldIA/2.0"})
    return _http


# ---------------------------------------------------------------------------
# MÓDULO 1: CALENDARIO FOREX FACTORY (con caché)
# ---------------------------------------------------------------------------
FF_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
_ff = {"data": None, "at": 0.0, "last_attempt": 0.0}
_ff_lock = asyncio.Lock()


async def get_calendar(force: bool = False) -> Optional[list]:
    async with _ff_lock:
        fresh = _ff["data"] is not None and time.time() - _ff["at"] < 3600
        if fresh and not force:
            return _ff["data"]
        if time.time() - _ff["last_attempt"] < 300:        # máximo 1 intento cada 5 min
            return _ff["data"]
        _ff["last_attempt"] = time.time()
        try:
            r = await http().get(FF_URL)
            body = r.text.strip()
            if r.status_code != 200 or body.startswith("<"):   # HTML = límite superado
                raise ValueError(f"HTTP {r.status_code}")
            data = r.json()
            if not isinstance(data, list):
                raise ValueError("formato inesperado")
            _ff.update(data=data, at=time.time())
            log.info("Calendario Forex Factory actualizado (%d eventos)", len(data))
        except Exception as exc:
            log.warning("No se pudo actualizar Forex Factory: %s", exc)
        return _ff["data"]


def _ev_time(ev: dict) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ev["date"])
    except (KeyError, ValueError, TypeError):
        return None


async def shield_status(window_min: int = 30, include_medium: bool = False) -> dict:
    events = await get_calendar()
    if events is None:
        return {"available": False, "is_blocked": False,
                "message": "Sin datos de Forex Factory: el filtro de noticias no está activo."}
    now = datetime.now(timezone.utc)
    impacts = {"High", "Medium"} if include_medium else {"High"}
    upcoming = None
    for ev in sorted(events, key=lambda e: _ev_time(e) or now):
        t = _ev_time(ev)
        if not t or ev.get("country") != "USD" or ev.get("impact") not in impacts:
            continue
        diff = (t - now).total_seconds() / 60
        if -window_min <= diff <= window_min:
            pre = diff >= 0
            return {
                "available": True, "is_blocked": True,
                "type": "PRE_NEWS_LOCKOUT" if pre else "POST_NEWS_COOLDOWN",
                "event_title": ev.get("title"), "impact": ev.get("impact"),
                "minutes_to_event": round(diff, 1),
                "forecast": ev.get("forecast") or "-", "previous": ev.get("previous") or "-",
                "message": (f"'{ev.get('title')}' sale en {round(diff)} min. No se abren operaciones."
                            if pre else
                            f"'{ev.get('title')}' salió hace {round(-diff)} min. Esperando a que se normalice el spread."),
            }
        if diff > 0 and upcoming is None:
            upcoming = {"title": ev.get("title"), "date": ev.get("date"), "minutes": round(diff)}
    return {"available": True, "is_blocked": False, "next_event": upcoming}


# ---------------------------------------------------------------------------
# MÓDULO 2: SESIONES (con horario de verano/invierno real)
# ---------------------------------------------------------------------------
def _at(tz: ZoneInfo, d: date, hh: int, mm: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=tz)


def session_phase(now: Optional[datetime] = None) -> dict:
    """Fase del mercado del oro. El 'día de trading' empieza a las 18:00 de Nueva York."""
    now = now or datetime.now(timezone.utc)
    ny_now = now.astimezone(NY)
    d = ny_now.date() + (timedelta(days=1) if ny_now.hour >= 18 else timedelta(0))
    fri_close = ny_now.weekday() == 4 and ny_now.hour >= 17
    if d.weekday() >= 5 or fri_close:
        return {"key": "closed", "session_name": "Mercado cerrado (fin de semana)", "is_optimal": False, "liquidity": "NULA"}
    phases = [
        (_at(LON, d, 8), "asia", "Sesión asiática", False, "BAJA (no operar)"),
        (_at(LON, d, 11, 30), "london", "Apertura de Londres", True, "ALTA (barrido del rango asiático)"),
        (_at(NY, d, 8), "pause", "Pausa europea", False, "MEDIA (evitar entradas nuevas)"),
        (_at(LON, d, 16, 30), "overlap", "Solapamiento Londres / Nueva York", True, "MÁXIMA (ventana estelar)"),
        (_at(NY, d, 16), "ny_pm", "Tarde de Nueva York", False, "MODERADA (gestionar, no abrir)"),
        (_at(NY, d, 18), "break", "Cierre diario", False, "MUY BAJA (spreads altos)"),
    ]
    for end, key, name, optimal, liq in phases:
        if now < end:
            return {"key": key, "session_name": name, "is_optimal": optimal, "liquidity": liq}
    return {"key": "asia", "session_name": "Sesión asiática", "is_optimal": False, "liquidity": "BAJA (no operar)"}


def session_info(user_tz: str) -> dict:
    try:
        tz = ZoneInfo(user_tz)
    except Exception:
        tz = EC
    info = session_phase()
    info["local_time"] = datetime.now(tz).strftime("%H:%M")
    return info


# ---------------------------------------------------------------------------
# MÓDULO 3: DATOS DE MERCADO REALES (Twelve Data)
# ---------------------------------------------------------------------------
_md = {"candles": None, "c_at": 0.0, "price": None, "p_at": 0.0, "src": None}
# El plan gratuito de Twelve Data limita las consultas diarias: se lleva la cuenta y se deja margen.
TD_DAILY_LIMIT = int(os.getenv("TWELVEDATA_DAILY_LIMIT", "700"))
_td_budget = {"day": None, "used": 0}


def td_allowed() -> bool:
    today = datetime.now(timezone.utc).date()
    if _td_budget["day"] != today:
        _td_budget.update(day=today, used=0)
    if _td_budget["used"] >= TD_DAILY_LIMIT:
        return False
    _td_budget["used"] += 1
    return True


async def get_candles(n: int = 120) -> Optional[list]:
    """Velas M15 de XAU/USD, de la más antigua a la más reciente. Caché de 60 s."""
    if not TWELVEDATA_API_KEY:
        return None
    if _md["candles"] and time.time() - _md["c_at"] < 60:
        return _md["candles"]
    if not td_allowed():
        log.warning("Límite diario de Twelve Data alcanzado: se usan las últimas velas guardadas")
        return _md["candles"]
    try:
        r = await http().get("https://api.twelvedata.com/time_series", params={
            "symbol": "XAU/USD", "interval": "15min", "outputsize": n,
            "timezone": "UTC", "apikey": TWELVEDATA_API_KEY})
        js = r.json()
        if js.get("status") != "ok":
            raise ValueError(js.get("message", "respuesta no válida"))
        candles = [{"t": datetime.fromisoformat(v["datetime"]).replace(tzinfo=timezone.utc),
                    "o": float(v["open"]), "h": float(v["high"]), "l": float(v["low"]), "c": float(v["close"])}
                   for v in reversed(js["values"])]
        _md.update(candles=candles, c_at=time.time())
        return candles
    except Exception as exc:
        log.warning("Twelve Data (velas) no disponible: %s", exc)
        return _md["candles"]


async def get_price() -> Optional[float]:
    """Precio de XAU/USD para las alarmas. Primero una fuente gratuita sin límite diario
    (gold-api.com, cada 10 s); si falla, Twelve Data con caché de 60 s para no gastar el cupo."""
    if _md["price"] and time.time() - _md["p_at"] < (10 if _md["src"] == "gold-api" else 60):
        return _md["price"]
    try:
        r = await http().get("https://api.gold-api.com/price/XAU")
        p = float(r.json()["price"])
        _md.update(price=p, p_at=time.time(), src="gold-api")
        return p
    except Exception as exc:
        log.info("gold-api no disponible (%s); se intenta Twelve Data", exc)
    if TWELVEDATA_API_KEY and td_allowed():
        try:
            r = await http().get("https://api.twelvedata.com/price",
                                 params={"symbol": "XAU/USD", "apikey": TWELVEDATA_API_KEY})
            p = float(r.json()["price"])
            _md.update(price=p, p_at=time.time(), src="twelvedata")
            return p
        except Exception as exc:
            log.warning("Twelve Data (precio) no disponible: %s", exc)
    return _md["price"]


def atr14(candles: list) -> Optional[float]:
    """ATR(14) de Wilder."""
    if not candles or len(candles) < 15:
        return None
    trs = [max(c["h"] - c["l"], abs(c["h"] - p["c"]), abs(c["l"] - p["c"])) for p, c in zip(candles, candles[1:])]
    atr = sum(trs[:14]) / 14
    for tr in trs[14:]:
        atr = (atr * 13 + tr) / 14
    return round(atr, 2)


# ---------------------------------------------------------------------------
# MÓDULO 4: MOTOR DE RIESGO Y LOTAJE
# ---------------------------------------------------------------------------
def compute_trade_risk(action: Literal["BUY", "SELL"], entry: float, atr: float, balance: float,
                       risk_pct: float, swing_low: Optional[float], swing_high: Optional[float],
                       order_type: str) -> dict:
    dist = max(round(atr * K_SL, 2), MIN_SL)
    sign = 1 if action == "BUY" else -1
    sl = round(entry - sign * dist, 2)
    # Stop estructural: detrás del swing si queda más lejos (y no absurdo: máx. 3 ATR)
    if action == "BUY" and swing_low and swing_low < entry and entry - swing_low <= 3 * atr:
        sl = min(sl, round(swing_low - 0.40, 2))
    if action == "SELL" and swing_high and swing_high > entry and swing_high - entry <= 3 * atr:
        sl = max(sl, round(swing_high + 0.40, 2))
    risk = round(abs(entry - sl), 2)
    tp1 = round(entry + sign * risk * RR1, 2)
    tp2 = round(entry + sign * risk * RR2, 2)

    budget = balance * risk_pct / 100
    raw_lots = budget / (risk * CONTRACT)
    lots = max(MIN_LOT, math.floor(raw_lots * 100) / 100)
    loss = round(lots * CONTRACT * risk, 2)
    loss_pct = round(loss / balance * 100, 2)
    fits = loss_pct <= risk_pct * 1.10

    split = lots >= 0.02
    lots_tp1 = round(math.floor(lots * TP1_CLOSE * 100) / 100, 2) if split else 0.0
    if split and lots_tp1 < MIN_LOT:
        lots_tp1 = MIN_LOT
    lots_tp2 = round(lots - lots_tp1, 2) if split else lots
    be_price = round(entry + sign * BE_OFFSET, 2)

    step1 = (f"Al tocar TP1 (${tp1:,.2f}): cierra {lots_tp1} lotes (60 %) y mueve el stop a ${be_price:,.2f} (breakeven + spread)."
             if split else
             f"Al tocar TP1 (${tp1:,.2f}): con 0.01 no se puede dividir; mueve el stop a ${be_price:,.2f} (breakeven + spread) y deja correr a TP2.")
    return {
        "action": f"{action} LIMIT" if order_type == "LIMIT" else action,
        "order_type": order_type,
        "entry_price": entry, "stop_loss": sl, "tp1": tp1, "tp2": tp2,
        "risk_distance_dollars": risk, "atr_used": atr,
        "recommended_lots": lots, "lots_tp1": lots_tp1, "lots_tp2": lots_tp2, "split": split,
        "breakeven_price": be_price,
        "loss_at_sl_usd": loss, "real_risk_percentage": loss_pct,
        "fits_risk": fits, "tradable": fits,
        "risk_message": None if fits else (
            f"Con el lote mínimo ({lots}) perderías ${loss:.2f} ({loss_pct}% de tu cuenta), más que tu {risk_pct}%. "
            f"El stop necesario (${risk:.2f}) es demasiado amplio para este balance."),
        "plan_execution": {
            "step_1": step1,
            "step_2": f"Al tocar TP2 (${tp2:,.2f}): cierra el resto de la posición.",
            "time_stop": (f"La orden pendiente vale {PENDING_CANDLES} velas M15 ({PENDING_CANDLES * 15} min). "
                          f"Ya dentro, si tras {POSITION_CANDLES} velas no avanzó el 50 % hacia TP1, cierra manualmente."),
            "pending_candles": PENDING_CANDLES, "position_candles": POSITION_CANDLES,
        },
    }


# ---------------------------------------------------------------------------
# MÓDULO 5: VISIÓN (Gemini)
# ---------------------------------------------------------------------------
class VisualChartAnalysis(BaseModel):
    is_valid_candlestick_chart: bool = Field(description="True solo si es un gráfico de velas japonesas legible con el eje de precios visible.")
    detected_ticker: Optional[str] = Field(None, description="Símbolo tal como aparece (ej. XAUUSD, GOLD). null si no se ve.")
    detected_timeframe: Optional[str] = Field(None, description="Temporalidad tal como aparece (ej. 15m, M15). null si no se ve.")
    current_price: float = Field(description="Último precio, leído de la etiqueta del eje de precios.")
    recent_swing_high: float = Field(description="Máximo estructural reciente.")
    recent_swing_low: float = Field(description="Mínimo estructural reciente.")
    estimated_atr_15m: float = Field(description="Estimación del rango medio de las últimas 14 velas, en dólares.")
    market_structure: Literal["SWEEP_LOW_REJECTION", "SWEEP_HIGH_REJECTION", "RANGE_BOUND", "TRENDING_EXPANSION", "UNCLEAR"]
    proposed_entry: Optional[float] = Field(None, description="Nivel de confluencia para entrar (retesteo del nivel barrido). null si no hay setup.")
    axis_top_price: Optional[float] = Field(None, description="Precio de la etiqueta más alta visible en el eje derecho.")
    axis_top_y: Optional[float] = Field(None, description="Posición vertical de esa etiqueta como fracción de la altura de la imagen (0 arriba, 1 abajo).")
    axis_bottom_price: Optional[float] = Field(None, description="Precio de la etiqueta más baja visible en el eje derecho.")
    axis_bottom_y: Optional[float] = Field(None, description="Posición vertical de esa etiqueta (0 arriba, 1 abajo).")
    rationale: str = Field(description="Explicación breve en español del patrón observado (mechas, barridos, estructura).")


VISION_PROMPT = """Eres el analista de gráficos de Nova Gold IA (XAU/USD, velas de 15 minutos).
Reglas:
1. Si la imagen no es un gráfico de velas japonesas legible con eje de precios, marca is_valid_candlestick_chart=false.
2. Lee los precios SOLO de las etiquetas del eje de precios. No inventes decimales que no se vean.
3. SWEEP_LOW_REJECTION: el precio barrió un mínimo reciente y la vela cerró de vuelta por encima con mecha inferior larga.
   SWEEP_HIGH_REJECTION: lo mismo por arriba. Si no ves claramente un barrido con rechazo, usa RANGE_BOUND,
   TRENDING_EXPANSION o UNCLEAR: en esos casos NO hay operación.
4. proposed_entry: nivel de retesteo del barrido, cerca del precio actual. null si no hay setup.
5. Para el eje: localiza la etiqueta de precio más alta y la más baja del eje derecho y su posición vertical (0 = borde superior, 1 = borde inferior).
6. rationale en español, máximo 3 frases.
{context}
Responde solo con el JSON del esquema."""


async def run_vision(image: bytes, mime: str, context: str) -> VisualChartAnalysis:
    resp = await ai_client.aio.models.generate_content(
        model=GEMINI_MODEL,
        contents=[types.Part.from_bytes(data=image, mime_type=mime), VISION_PROMPT.format(context=context)],
        config=types.GenerateContentConfig(response_mime_type="application/json",
                                           response_schema=VisualChartAnalysis, temperature=0.1),
    )
    parsed = getattr(resp, "parsed", None)
    if isinstance(parsed, VisualChartAnalysis):
        return parsed
    return VisualChartAnalysis.model_validate_json(resp.text)


# ---------------------------------------------------------------------------
# APLICACIÓN
# ---------------------------------------------------------------------------
_brief = {"text": None, "generated_at": None, "data": None}
scheduler = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler
    if BRIEF_ENABLED:
        try:
            from apscheduler.schedulers.asyncio import AsyncIOScheduler
            from apscheduler.triggers.cron import CronTrigger
            scheduler = AsyncIOScheduler()
            scheduler.add_job(generate_brief, CronTrigger(day_of_week="mon-fri", hour=6, minute=45, timezone=EC),
                              id="premarket", replace_existing=True)
            scheduler.start()
            log.info("Informe pre-mercado programado: lunes a viernes 06:45 (Ecuador)")
        except ImportError:
            log.warning("APScheduler no instalado: sin informe automático de las 06:45")
    asyncio.create_task(get_calendar())
    yield
    if scheduler:
        scheduler.shutdown(wait=False)
    if _http:
        await _http.aclose()


app = FastAPI(title="Nova Gold IA", version="2.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS, allow_credentials=False,
                   allow_methods=["GET", "POST"], allow_headers=["*"])


@app.get("/api/health")
async def health():
    return {"ok": True}


@app.get("/api/v1/config")
async def config():
    return {"gemini": ai_client is not None, "model": GEMINI_MODEL,
            "market_data": True,  # /price usa gold-api y, si falla, Twelve Data
            "candles": bool(TWELVEDATA_API_KEY),
            "twelvedata_used_today": _td_budget["used"], "twelvedata_daily_limit": TD_DAILY_LIMIT,
            "telegram": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
            "notify_key_required": bool(NOTIFY_KEY), "brief": BRIEF_ENABLED,
            "session": session_phase()}


@app.get("/api/v1/calendar")
async def calendar():
    data = await get_calendar()
    if data is None:
        raise HTTPException(502, "No se pudo obtener el calendario de Forex Factory.")
    return data


@app.get("/api/v1/price")
async def price():
    p = await get_price()
    if p is None:
        raise HTTPException(503, "Precio no disponible en este momento.")
    return {"price": p, "source": _md["src"], "at": datetime.now(timezone.utc).isoformat()}


@app.post("/api/v1/analyze-chart")
async def analyze_chart(
    file: UploadFile = File(...),
    account_balance: float = Form(500.0),
    risk_percentage: float = Form(1.0),
    timezone_name: str = Form("America/Guayaquil", alias="timezone"),
    current_price: Optional[float] = Form(None),
    news_window: int = Form(30),
    include_medium: bool = Form(False),
    timeframe: str = Form("M15"),
):
    if not (50 <= account_balance <= 1_000_000) or not (0.1 <= risk_percentage <= 3):
        raise HTTPException(422, "Balance o porcentaje de riesgo fuera de rango.")
    news_window = min(max(news_window, 5), 120)

    # 1. Escudo de noticias (antes de gastar tokens)
    shield = await shield_status(news_window, include_medium)
    if shield["is_blocked"]:
        return {"success": False, "status": "SHIELD_BLOCKED", "shield": shield, "message": shield["message"]}

    session = session_info(timezone_name)

    if ai_client is None:
        return {"success": False, "status": "CONFIG_ERROR", "message": "GEMINI_API_KEY no está configurada en el servidor."}

    image = await file.read()
    if len(image) > MAX_UPLOAD_BYTES:
        return {"success": False, "status": "INVALID_IMAGE", "message": "La imagen supera 8 MB. Recorta la captura al gráfico."}
    mime = file.content_type if (file.content_type or "").startswith("image/") else "image/png"

    # 2. Datos reales de mercado (si hay API key)
    candles, live_price = await asyncio.gather(get_candles(), get_price())
    real_atr = atr14(candles) if candles else None
    ref_price = live_price or current_price
    context = ""
    if ref_price:
        context += f"\nContexto: el precio real actual del oro es aproximadamente {ref_price:.2f}."

    # 3. Visión
    try:
        v = await run_vision(image, mime, context)
    except Exception as exc:
        log.exception("Error de visión")
        return {"success": False, "status": "PROCESSING_ERROR", "message": f"No se pudo analizar la imagen: {exc}"}

    if not v.is_valid_candlestick_chart:
        return {"success": False, "status": "INVALID_IMAGE",
                "message": "No es un gráfico de velas legible con eje de precios. Sube una captura nítida de TradingView o MetaTrader en M15."}
    tk = (v.detected_ticker or "").upper()
    if tk and not any(s in tk for s in ("XAU", "GOLD", "ORO")):
        return {"success": False, "status": "INVALID_ASSET", "message": f"El gráfico parece ser de {v.detected_ticker}, no de XAU/USD."}
    tf = (v.detected_timeframe or "").lower().replace(" ", "")
    warnings = []
    if tf and "15" not in tf:
        return {"success": False, "status": "INVALID_TIMEFRAME", "message": f"La temporalidad detectada es {v.detected_timeframe}. Nova Gold trabaja solo en M15."}
    if not tf:
        warnings.append("No se pudo confirmar en la imagen que la temporalidad sea M15.")

    # 4. Verificación cruzada del precio (detecta errores de escala)
    if ref_price and abs(v.current_price - ref_price) / ref_price > PRICE_TOLERANCE:
        return {"success": False, "status": "PRICE_MISMATCH",
                "message": f"La IA leyó {v.current_price:,.2f} pero el precio real es {ref_price:,.2f}. "
                           "Probable error de lectura del eje o captura antigua. Sube una captura actual con el eje visible."}

    price_now = ref_price or v.current_price
    atr = real_atr or v.estimated_atr_15m
    atr_source = "twelvedata" if real_atr else "estimado_por_ia"
    if not real_atr:
        warnings.append("ATR estimado a partir de la imagen: configura TWELVEDATA_API_KEY para usar el ATR real.")
    if not session["is_optimal"]:
        warnings.append(f"Estás en {session['session_name']}: liquidez {session['liquidity'].lower()}.")
    if not shield.get("available"):
        warnings.append(shield["message"])

    axis = None
    if None not in (v.axis_top_price, v.axis_top_y, v.axis_bottom_price, v.axis_bottom_y) and v.axis_top_price != v.axis_bottom_price:
        axis = {"top": {"y": v.axis_top_y, "price": v.axis_top_price},
                "bottom": {"y": v.axis_bottom_y, "price": v.axis_bottom_price}, "source": "ia"}

    tech = v.model_dump()
    tech.update(price_axis=axis, atr=atr, atr_source=atr_source, reference_price=price_now)

    # 5. Dirección: solo con barrido + rechazo; en otro caso, ESPERAR
    if v.market_structure == "SWEEP_LOW_REJECTION":
        direction = "BUY"
    elif v.market_structure == "SWEEP_HIGH_REJECTION":
        direction = "SELL"
    else:
        return {"success": True, "status": "WAIT", "session": session, "shield": shield, "warnings": warnings,
                "technical_extraction": tech,
                "trade_setup": {"action": "WAIT", "reason": "No hay barrido de liquidez con rechazo claro. Esperar."}}

    # 6. Entrada: límite en el retesteo si está a menos de 1 ATR a favor; si no, a mercado
    entry, order_type = round(price_now, 2), "MARKET"
    pe = v.proposed_entry
    if pe and direction == "BUY" and price_now - atr <= pe < price_now:
        entry, order_type = round(pe, 2), "LIMIT"
    if pe and direction == "SELL" and price_now < pe <= price_now + atr:
        entry, order_type = round(pe, 2), "LIMIT"

    setup = compute_trade_risk(direction, entry, atr, account_balance, risk_percentage,
                               v.recent_swing_low, v.recent_swing_high, order_type)
    return {"success": True, "status": "SETUP_CALCULATED" if setup["tradable"] else "RISK_TOO_HIGH",
            "session": session, "shield": shield, "warnings": warnings,
            "technical_extraction": tech, "trade_setup": setup}


# ---------------------------------------------------------------------------
# TELEGRAM: reenvío de alarmas
# ---------------------------------------------------------------------------
class NotifyIn(BaseModel):
    title: str = Field(max_length=120)
    body: str = Field(max_length=600)


_last_notify = {"at": 0.0}


async def send_telegram(text: str) -> bool:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        log.info("Telegram no configurado. Mensaje:\n%s", text)
        return False
    r = await http().post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000]})
    return r.status_code == 200


@app.post("/api/v1/notify")
async def notify(msg: NotifyIn, x_notify_key: Optional[str] = Header(None)):
    if NOTIFY_KEY and x_notify_key != NOTIFY_KEY:
        raise HTTPException(401, "Clave de notificación incorrecta.")
    if time.time() - _last_notify["at"] < 3:
        raise HTTPException(429, "Demasiadas notificaciones seguidas.")
    _last_notify["at"] = time.time()
    ok = await send_telegram(f"🔔 Nova Gold · {msg.title}\n{msg.body}")
    return {"sent": ok}


# ---------------------------------------------------------------------------
# INFORME PRE-MERCADO (06:45 Ecuador, lunes a viernes)
# ---------------------------------------------------------------------------
async def collect_brief_data() -> dict:
    candles = await get_candles(200)
    events = await get_calendar() or []
    now = datetime.now(timezone.utc)
    ny_today = now.astimezone(NY).date()
    asia_start = _at(NY, ny_today - timedelta(days=1), 18)   # 18:00 NY del día anterior
    asia_end = _at(LON, ny_today, 8)                          # apertura de Londres
    data: dict[str, Any] = {"market_data": bool(candles)}
    if candles:
        asia = [c for c in candles if asia_start <= c["t"] < asia_end]
        london = [c for c in candles if c["t"] >= asia_end]
        data.update(price=candles[-1]["c"], atr_15m=atr14(candles))
        if asia:
            data.update(asian_high=max(c["h"] for c in asia), asian_low=min(c["l"] for c in asia))
        if london:
            data.update(london_high=max(c["h"] for c in london), london_low=min(c["l"] for c in london))
    today_ec = now.astimezone(EC).date()
    data["news_today"] = [
        {"hora_ecuador": t.astimezone(EC).strftime("%H:%M"), "evento": e.get("title"), "impacto": e.get("impact"),
         "previsto": e.get("forecast") or "-", "anterior": e.get("previous") or "-"}
        for e in events
        if (t := _ev_time(e)) and e.get("country") == "USD" and e.get("impact") in ("High", "Medium")
        and t.astimezone(EC).date() == today_ec
    ]
    data["calendar_available"] = bool(events)
    return data


async def generate_brief() -> dict:
    data = await collect_brief_data()
    today = datetime.now(EC)
    header = f"🌅 XAU/USD · Informe pre-mercado {today.strftime('%d/%m/%Y')} (06:45 Ecuador)"
    fmt = lambda k: f"{data[k]:,.2f}" if isinstance(data.get(k), (int, float)) else "sin dato"
    news = "\n".join(f"• {n['hora_ecuador']} {n['evento']} ({n['impacto']}) prev. {n['previsto']}" for n in data["news_today"]) \
        or "• Sin noticias USD de impacto alto o medio hoy."
    facts = (f"Precio: {fmt('price')} | ATR M15: {fmt('atr_15m')}\n"
             f"Rango asiático: {fmt('asian_low')} – {fmt('asian_high')}\n"
             f"Londres hasta ahora: {fmt('london_low')} – {fmt('london_high')}\nNoticias USD de hoy:\n{news}")
    text = f"{header}\n\n{facts}"
    if ai_client and data["market_data"]:
        prompt = ("Eres analista de oro (XAU/USD, M15). Con ESTOS datos y ningún otro número, escribe en español un plan breve "
                  "para la sesión de Nueva York (07:00–10:30 Ecuador): 1) sesgo (alcista, bajista o esperar barrido), "
                  "2) qué nivel del rango asiático o de Londres vigilar para un barrido con rechazo, 3) advertencia de noticias "
                  "(el escudo bloquea ±30 min). No inventes precios ni probabilidades. Máximo 120 palabras.\n\n" + facts)
        try:
            r = await ai_client.aio.models.generate_content(model=GEMINI_MODEL, contents=prompt,
                                                            config=types.GenerateContentConfig(temperature=0.2))
            text += "\n\n" + (r.text or "").strip()
        except Exception as exc:
            log.warning("Gemini no pudo redactar el informe: %s", exc)
    elif not data["market_data"]:
        text += "\n\n(Sin datos de mercado: configura TWELVEDATA_API_KEY para incluir precio, ATR y rangos.)"
    text += "\n\nHerramienta educativa. No es asesoría financiera."
    _brief.update(text=text, generated_at=datetime.now(timezone.utc).isoformat(), data=data)
    await send_telegram(text)
    return _brief


@app.get("/api/v1/brief")
async def get_brief():
    return _brief


@app.post("/api/v1/brief/run")
async def run_brief(x_notify_key: Optional[str] = Header(None)):
    if NOTIFY_KEY and x_notify_key != NOTIFY_KEY:
        raise HTTPException(401, "Clave incorrecta.")
    return await generate_brief()


# ---------------------------------------------------------------------------
# FRONTEND
# ---------------------------------------------------------------------------
if os.path.isdir("static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
