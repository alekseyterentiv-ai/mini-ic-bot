from flask import Flask, request
import os
import json
import time
import re
import tempfile
import requests
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# --- ENV ---
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_API = f"https://api.telegram.org/bot{TOKEN}"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "").strip()  # например: ОПЕРАЦИИ

GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# --- Anti-dup settings ---
DEDUP_TTL_SECONDS = 6 * 60 * 60          # MessageID защита (6 часов)
CONTENT_DEDUP_WINDOW_SECONDS = 30         # окно антидубля по тексту

# In-memory caches (Cloud Run может перезапускать/масштабировать — это ок для базовой защиты)
_seen_message_ids = {}        # message_id -> ts
_seen_content = {}            # (chat_id, normalized_text) -> ts


def _cleanup_caches(now_ts: float) -> None:
    # чистим message_id
    to_del = [k for k, ts in _seen_message_ids.items() if now_ts - ts > DEDUP_TTL_SECONDS]
    for k in to_del:
        _seen_message_ids.pop(k, None)

    # чистим контент
    to_del = [k for k, ts in _seen_content.items() if now_ts - ts > CONTENT_DEDUP_WINDOW_SECONDS]
    for k in to_del:
        _seen_content.pop(k, None)


def send_message(chat_id: int, text: str) -> None:
    try:
        requests.post(
            f"{TG_API}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=20,
        )
    except Exception as e:
        print("send_message error:", repr(e))


def build_sheets_service():
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is empty")

    # GOOGLE_SA_JSON может быть строкой JSON
    sa_info = json.loads(GOOGLE_SA_JSON)

    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def append_row(row):
    service = build_sheets_service()
    # ВАЖНО: range = только имя листа (без !A), иначе возможны ошибки парсинга
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_OPS,
        valueInputOption="USER_ENTERED",
        body={"values": [row]},
    ).execute()


def validate_and_parse(text: str):
    # ожидаем: 9 полей
    # ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; СПОСОБ; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТАРИЙ
    parts = [p.strip() for p in text.split(";")]

    if len(parts) != 9:
        return None, "❌ Ошибка формата: должно быть 9 полей через ;\nПример:\nОБУХОВО; РАСХОД; КВАРТИРА; 1000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; тест"

    object_, type_, article, amount_raw, pay_type, vat, period_raw, employee, comment = parts

    if not object_ or not type_ or not article or not amount_raw:
        return None, "❌ Не хватает обязательных полей: ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА"

    # Тип
    type_up = type_.upper()
    if type_up not in ("РАСХОД", "ПРИХОД"):
        return None, "❌ Тип должен быть РАСХОД или ПРИХОД"

    # Сумма
    try:
        amt = amount_raw.replace(" ", "").replace(",", ".")
        amount = float(amt)
        if amount <= 0:
            return None, "❌ Сумма должна быть больше 0"
    except:
        return None, "❌ Сумма должна быть числом"

    # НДС
    vat_up = vat.upper()
    if vat_up not in ("ДА", "НЕТ"):
        return None, "❌ НДС только ДА или НЕТ"

    # Период: YYYY-MM-1 или YYYY-MM-2
    # (важно: именно так, это НЕ дата)
    if not re.match(r"^\d{4}-\d{2}-[12]$", period_raw):
        return None, "❌ Период только YYYY-MM-1 или YYYY-MM-2"

    period = period_raw

    return {
        "object": object_,
        "type": type_up,
        "article": article,
        "amount": amount,
        "pay_type": pay_type,
        "vat": vat_up,
        "period": period,
        "employee": employee,
        "comment": comment,
    }, None


@app.get("/")
def index():
    return "ok", 200


@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    # print("update:", json.dumps(data, ensure_ascii=False))

    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return "no message", 200

    chat_id = (msg.get("chat") or {}).get("id")
    if not chat_id:
        return "no chat", 200

    message_id = msg.get("message_id")
    text = (msg.get("text") or "").strip()

    # /start
    if text.startswith("/start"):
        send_message(
            chat_id,
            "Привет! Я на связи.\nФормат:\nОБУХОВО; РАСХОД; КВАРТИРА; 1000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; комментарий",
        )
        return "ok", 200

    now_ts = time.time()
    _cleanup_caches(now_ts)

    # --- Anti-dup by MessageID ---
    if message_id is not None:
        if message_id in _seen_message_ids:
            # уже обрабатывали
            return "dup message_id", 200
        _seen_message_ids[message_id] = now_ts

    # --- Anti-dup by content within 30s (per chat) ---
    norm_text = re.sub(r"\s+", " ", text).strip().lower()
    key = (chat_id, norm_text)
    if norm_text:
        last_ts = _seen_content.get(key)
        if last_ts and (now_ts - last_ts) <= CONTENT_DEDUP_WINDOW_SECONDS:
            return "dup content", 200
        _seen_content[key] = now_ts

    parsed, err = validate_and_parse(text)
    if err:
        send_message(chat_id, err)
        return "bad format", 200

    try:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        row = [
            now_str,                 # A DateTime
            parsed["object"],        # B Объект
            parsed["type"],          # C Тип
            parsed["article"],       # D Статья
            parsed["amount"],        # E СуммаБаза
            parsed["pay_type"],      # F СпособОплаты
            parsed["vat"],           # G НДС
            "",                      # H Категория (пусто)
            parsed["period"],        # I ПЕРИОД (YYYY-MM-1/2)
            parsed["employee"],      # J Сотрудник
            "",                      # K Статус (пусто)
            "TELEGRAM",              # L Источник
            str(message_id or ""),   # M MessageID (не пустой если есть)
        ]

        append_row(row)
        send_message(chat_id, "✅ Записал")
    except Exception as e:
        print("append error:", repr(e))
        send_message(chat_id, f"Ошибка записи в таблицу: {e}")

    return "ok", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
