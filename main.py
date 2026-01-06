import os
import json
import re
from datetime import datetime

import requests
from flask import Flask, request

from google.oauth2 import service_account
from googleapiclient.discovery import build


app = Flask(__name__)

# --- ENV ---
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_API = f"https://api.telegram.org/bot{TOKEN}"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ").strip()

# GOOGLE_SA_JSON = json string (из Secret Manager exposed as env)
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Период: YYYY-MM-1 или YYYY-MM-2
PERIOD_RE = re.compile(r"^\d{4}-\d{2}-[12]$")


def tg_send(chat_id: int, text: str):
    try:
        requests.post(
            f"{TG_API}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10,
        )
    except Exception:
        pass


def get_sheets_service():
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is empty")
    info = json.loads(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def validate_and_parse(text: str):
    parts = [p.strip() for p in text.split(";")]

    if len(parts) != 9:
        return None, "❌ Ошибка формата: должно быть 9 полей через ;"

    object_, type_, article, amount_raw, pay_type, vat, period_raw, employee, comment = parts

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
    except Exception:
        return None, "❌ Сумма должна быть числом"

    # НДС
    vat_up = vat.upper()
    if vat_up not in ("ДА", "НЕТ"):
        return None, "❌ НДС только ДА или НЕТ"

    # Период (НЕ дата!)
    period = period_raw.strip()
    if not PERIOD_RE.match(period):
        return None, "❌ Период только YYYY-MM-1 или YYYY-MM-2"

    # Минимальная проверка обязательных
    if not object_ or not article or not pay_type:
        return None, "❌ Не хватает обязательных полей: объект; статья; способ оплаты"

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


def load_recent_message_keys(service, limit=500):
    """
    Берем колонку M (MessageID) и делаем set последних значений.
    Чтобы быстро и стабильно.
    """
    resp = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_OPS}!M2:M")
        .execute()
    )
    values = resp.get("values", [])
    flat = [row[0] for row in values if row]
    if len(flat) > limit:
        flat = flat[-limit:]
    return set(flat)


def append_row(service, row):
    # ВАЖНО: range = только имя листа (без !A)
    return (
        service.spreadsheets()
        .values()
        .append(
            spreadsheetId=SPREADSHEET_ID,
            range=SHEET_OPS,
            valueInputOption="USER_ENTERED",
            body={"values": [row]},
        )
        .execute()
    )


@app.get("/")
def index():
    return "ok"


@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}

    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return ("ok", 200)

    chat_id = (msg.get("chat") or {}).get("id")
    text = (msg.get("text") or "").strip()

    if not chat_id:
        return ("ok", 200)

    # /start
    if text.startswith("/start"):
        tg_send(
            chat_id,
            "Привет! Я на связи.\nФормат:\nОБУХОВО; РАСХОД; КВАРТИРА; 1000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; комментарий",
        )
        return ("ok", 200)

    parsed, err = validate_and_parse(text)
    if err:
        tg_send(chat_id, err)
        return ("ok", 200)

    # message_id + chat_id => уникальный ключ
    message_id = msg.get("message_id")
    message_key = f"{chat_id}:{message_id}" if message_id is not None else ""

    # now
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Row строго под колонки:
    # A Datetime
    # B Объект
    # C Тип
    # D Статья
    # E СуммаБаза
    # F СпособОплаты
    # G НДС
    # H Категория (пусто)
    # I ПЕРИОД
    # J Сотрудник
    # K Статус (пусто)
    # L Источник
    # M MessageID
    # N Комментарий
    row = [
        now,
        parsed["object"],
        parsed["type"],
        parsed["article"],
        parsed["amount"],
        parsed["pay_type"],
        parsed["vat"],
        "",
        parsed["period"],
        parsed["employee"],
        "",
        "TELEGRAM",
        message_key,
        parsed["comment"],
    ]

    try:
        service = get_sheets_service()

        # антидубли
        if message_key:
            existing = load_recent_message_keys(service, limit=500)
            if message_key in existing:
                tg_send(chat_id, "⚠️ Уже записано (дубль).")
                return ("ok", 200)

        append_row(service, row)
        tg_send(chat_id, "✅ Записал")

    except Exception as e:
        tg_send(chat_id, f"Ошибка записи в таблицу: {e}")

    return ("ok", 200)
