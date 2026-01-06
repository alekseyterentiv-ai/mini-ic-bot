from flask import Flask, request
import os
import json
import requests
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# --- ENV ---
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_API = f"https://api.telegram.org/bot{TOKEN}"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "").strip()
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# --- Google Sheets ---
creds = service_account.Credentials.from_service_account_info(
    json.loads(GOOGLE_SA_JSON),
    scopes=SCOPES
)
gs = build("sheets", "v4", credentials=creds).spreadsheets()

# --- helpers ---
def tg_send(chat_id, text):
    requests.post(
        f"{TG_API}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=10
    )

def period_ok(p: str):
    return (
        len(p) == 8 and
        p[4] == "-" and
        p[7] in ("1", "2") and
        p[:4].isdigit() and
        p[5:7].isdigit()
    )

def is_duplicate(message_id: int) -> bool:
    resp = gs.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_OPS}!M:M"
    ).execute()
    values = resp.get("values", [])
    return any(str(message_id) == row[0] for row in values if row)

def parse_message(text: str):
    parts = [p.strip() for p in text.split(";")]
    if len(parts) != 9:
        return None, "❌ Нужно ровно 9 полей через ;"

    object_, type_, article, amount_raw, pay_type, vat, period, employee, comment = parts

    if type_.upper() not in ("РАСХОД", "ПРИХОД"):
        return None, "❌ Тип только РАСХОД или ПРИХОД"

    try:
        amount = float(amount_raw.replace(" ", "").replace(",", "."))
        if amount <= 0:
            raise ValueError
    except:
        return None, "❌ Сумма некорректна"

    vat = vat.upper()
    if vat not in ("ДА", "НЕТ"):
        return None, "❌ НДС только ДА или НЕТ"

    if not period_ok(period):
        return None, "❌ Период только YYYY-MM-1 или YYYY-MM-2"

    return {
        "object": object_,
        "type": type_.upper(),
        "article": article,
        "amount": amount,
        "pay_type": pay_type,
        "vat": vat,
        "period": period,
        "employee": employee,
        "comment": comment
    }, None

# --- routes ---
@app.get("/")
def index():
    return "OK", 200

@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    msg = data.get("message")
    if not msg or "text" not in msg:
        return "ok", 200

    chat_id = msg["chat"]["id"]
    text = msg["text"].strip()
    message_id = msg["message_id"]

    if text == "/start":
        tg_send(
            chat_id,
            "Формат:\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 1000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; комментарий"
        )
        return "ok", 200

    if is_duplicate(message_id):
        tg_send(chat_id, "⚠️ Это сообщение уже записано")
        return "ok", 200

    parsed, error = parse_message(text)
    if error:
        tg_send(chat_id, error)
        return "ok", 200

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    row = [
        now,                       # A DateTime
        parsed["object"],          # B Объект
        parsed["type"],            # C Тип
        parsed["article"],         # D Статья
        parsed["amount"],          # E Сумма
        parsed["pay_type"],        # F Способ оплаты
        parsed["vat"],             # G НДС
        "",                         # H Категория
        parsed["period"],          # I Период
        parsed["employee"],        # J Сотрудник
        "",                         # K Статус
        "TELEGRAM",                 # L Источник
        message_id                 # M MessageID (АНТИДУБЛЬ)
    ]

    gs.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_OPS,
        valueInputOption="USER_ENTERED",
        body={"values": [row]}
    ).execute()

    tg_send(chat_id, "✅ Записал")
    return "ok", 200
