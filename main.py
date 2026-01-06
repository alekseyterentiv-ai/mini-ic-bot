from flask import Flask, request
import os
import requests
import json
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

# ================== НАСТРОЙКИ ==================

TOKEN = os.environ.get("TELEGRAM_TOKEN")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ")

app = Flask(__name__)

TG_API = f"https://api.telegram.org/bot{TOKEN}"

# ================== GOOGLE SHEETS ==================

def get_sheet():
    creds_info = json.loads(os.environ["GOOGLE_SA_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(SHEET_OPS)

# ================== TELEGRAM ==================

def send_message(chat_id, text):
    requests.post(
        f"{TG_API}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=10
    )

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

    # ===== /start =====
    if text == "/start":
        send_message(chat_id, "Привет! Я на связи.\nФормат:\nОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; ОПЛАТА; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТАРИЙ")
        return "ok", 200

    # ===== разбор строки =====
    parts = [p.strip() for p in text.split(";")]

    if len(parts) < 9:
        send_message(chat_id, "❌ Ошибка формата. Нужно 9 полей через ;")
        return "ok", 200

    object_, type_, article, amount, pay_type, vat, period, employee, comment = parts[:9]

    try:
        amount = float(amount.replace(",", "."))
    except:
        send_message(chat_id, "❌ Сумма не число")
        return "ok", 200

    # ===== Datetime =====
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ===== СТРОКА В ТАБЛИЦУ (БЕЗ MessageID) =====
    row = [
        now,            # A Datetime
        object_,        # B Объект
        type_,          # C Тип
        article,        # D Статья
        amount,         # E СуммаБаза
        pay_type,       # F СпособОплаты
        vat,            # G НДС
        "",             # H Категория (пусто)
        period,         # I ПЕРИОД
        employee,       # J Сотрудник
        "",             # K Статус (ПУСТО)
        "TELEGRAM",     # L Источник
        ""              # M MessageID — ПУСТО
    ]

    try:
        sheet = get_sheet()
        sheet.append_row(row, value_input_option="USER_ENTERED")
        send_message(chat_id, "✅ Записал строку")
    except Exception as e:
        send_message(chat_id, f"❌ Ошибка записи: {e}")

    return "ok", 200
