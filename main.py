from flask import Flask, request
import os
import json
import requests
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# ---------------- ENV ----------------
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_API = f"https://api.telegram.org/bot{TOKEN}"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "").strip()

GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ---------------- GOOGLE SHEETS ----------------
creds = service_account.Credentials.from_service_account_info(
    json.loads(GOOGLE_SA_JSON),
    scopes=SCOPES
)
sheets_service = build("sheets", "v4", credentials=creds)

# ---------------- HELPERS ----------------
def send_message(chat_id: int, text: str):
    requests.post(
        f"{TG_API}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=10
    )

def get_period(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    half = "1" if dt.day <= 15 else "2"
    return f"{dt.year}-{dt.month:02d}-{half}"

def is_duplicate(message_id: int) -> bool:
    res = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_OPS}!M:M"
    ).execute()

    values = res.get("values", [])
    return any(str(message_id) == row[0] for row in values if row)

def validate_and_parse(text: str):
    parts = [p.strip() for p in text.split(";")]
    if len(parts) != 9:
        return None, "❌ Нужно 9 полей через ;"

    object_, type_, article, amount_raw, pay_type, vat, date_raw, employee, comment = parts

    if type_.upper() not in ("РАСХОД", "ПРИХОД"):
        return None, "❌ Тип только РАСХОД или ПРИХОД"

    try:
        amount = float(amount_raw.replace(" ", "").replace(",", "."))
        if amount <= 0:
            return None, "❌ Сумма должна быть > 0"
    except:
        return None, "❌ Некорректная сумма"

    vat = vat.upper()
    if vat not in ("ДА", "НЕТ"):
        return None, "❌ НДС только ДА или НЕТ"

    try:
        period = get_period(date_raw)
    except:
        return None, "❌ Дата в формате YYYY-MM-DD"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return [
        now,                # A Datetime
        object_,            # B Объект
        type_.upper(),      # C Тип
        article,            # D Статья
        amount,             # E Сумма
        pay_type,           # F Способ оплаты
        vat,                # G НДС
        "",                 # H Категория
        period,             # I Период (1 / 2)
        employee,           # J Сотрудник
        "",                 # K Статус
        "TELEGRAM",         # L Источник
        "",                 # M MessageID (заполним ниже)
        comment              # N Комментарий
    ], None

# ---------------- ROUTES ----------------
@app.get("/")
def index():
    return "OK", 200

@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    msg = data.get("message")

    if not msg:
        return "ok", 200

    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()
    message_id = msg.get("message_id")

    if text == "/start":
        send_message(
            chat_id,
            "Формат:\nОБУХОВО; РАСХОД; КВАРТИРА; 10000; НАЛ; НЕТ; 2026-01-01; ИВАНОВ; комментарий"
        )
        return "ok", 200

    if is_duplicate(message_id):
        send_message(chat_id, "⚠️ Это сообщение уже обработано")
        return "ok", 200

    row, error = validate_and_parse(text)
    if error:
        send_message(chat_id, error)
        return "ok", 200

    row[12] = message_id  # MessageID

    sheets_service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_OPS,   # ТОЛЬКО имя листа
        valueInputOption="USER_ENTERED",
        body={"values": [row]}
    ).execute()

    send_message(chat_id, "✅ Записал")
    return "ok", 200
