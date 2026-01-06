from flask import Flask, request
import os
import json
import requests
import re
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# ---------------- ENV ----------------
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_API = f"https://api.telegram.org/bot{TOKEN}"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ").strip()
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


@app.get("/")
def index():
    return "OK", 200


# ---------------- HELPERS ----------------
def send_message(chat_id: int, text: str):
    if not TOKEN:
        print("ERROR: TELEGRAM_TOKEN is not set")
        return
    requests.post(
        f"{TG_API}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=15
    )


def get_sheets_service():
    info = json.loads(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def append_row(row: list):
    service = get_sheets_service()
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_OPS,              # ТОЛЬКО имя листа
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()


# ---------------- АНТИДУБЛИ ----------------
def message_id_exists(message_id: str) -> bool:
    if not message_id:
        return False

    service = get_sheets_service()
    rng = f"'{SHEET_OPS}'!M:M"  # колонка MessageID

    res = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=rng
    ).execute()

    for r in res.get("values", []):
        if r and r[0] == message_id:
            return True
    return False


# ---------------- ВАЛИДАЦИЯ ----------------
def validate_and_parse(text: str):
    parts = [p.strip() for p in (text or "").split(";")]

    if len(parts) != 9:
        return None, "❌ Нужно 9 полей через ;\nПример:\nОБУХОВО; РАСХОД; КВАРТИРА; 10000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; тест"

    object_, type_, article, amount, pay_type, vat, period_raw, employee, comment = parts

    if type_.upper() not in ("РАСХОД", "ПРИХОД"):
        return None, "❌ Тип: РАСХОД или ПРИХОД"

    try:
        amount = float(amount.replace(" ", "").replace(",", "."))
        if amount <= 0:
            raise ValueError
    except:
        return None, "❌ Сумма должна быть числом > 0"

    vat = vat.upper()
    if vat not in ("ДА", "НЕТ"):
        return None, "❌ НДС: ДА или НЕТ"

    if not re.fullmatch(r"\d{4}-\d{2}-[12]", period_raw):
        return None, "❌ ПЕРИОД: YYYY-MM-1 или YYYY-MM-2"

    return {
        "object": object_,
        "type": type_.upper(),
        "article": article,
        "amount": amount,
        "pay_type": pay_type,
        "vat": vat,
        "period": period_raw,
        "employee": employee,
        "comment": comment,
    }, None


# ---------------- WEBHOOK ----------------
@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return "ok", 200

    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()
    message_id = str(msg.get("message_id", ""))

    if text == "/start":
        send_message(chat_id, "Бот готов. Формат:\nОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; ОПЛАТА; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТАРИЙ")
        return "ok", 200

    parsed, err = validate_and_parse(text)
    if err:
        send_message(chat_id, err)
        return "ok", 200

    if message_id_exists(message_id):
        send_message(chat_id, "⚠️ Уже записано (антидубль)")
        return "ok", 200

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    row = [
        now,                    # A Datetime
        parsed["object"],        # B Объект
        parsed["type"],          # C Тип
        parsed["article"],       # D Статья
        parsed["amount"],        # E Сумма
        parsed["pay_type"],      # F Оплата
        parsed["vat"],           # G НДС
        "",                      # H Категория
        parsed["period"],        # I Период
        parsed["employee"],      # J Сотрудник
        "",                      # K Статус
        "TELEGRAM",              # L Источник
        message_id,              # M MessageID
        parsed["comment"],       # N Комментарий
    ]

    append_row(row)
    send_message(chat_id, "✅ Записал")

    return "ok", 200
