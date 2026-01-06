from flask import Flask, request
import os
import json
import re
from datetime import datetime

import requests

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# ===== ENV =====
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_API = f"https://api.telegram.org/bot{TOKEN}"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ").strip()

# Секрет из Secret Manager, проброшенный как env-переменная GOOGLE_SA_JSON
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# ===== Google Sheets client =====
def get_sheets_service():
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is not set (Secret Manager -> env var).")

    info = json.loads(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def append_row(values: list):
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is not set.")
    if not SHEET_OPS:
        raise RuntimeError("SHEET_OPS is not set.")

    service = get_sheets_service()
    body = {"values": [values]}
    # Пишем в лист (таб) SHEET_OPS, начиная с A
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_OPS}!A:K",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


# ===== Telegram helpers =====
def send_message(chat_id: int, text: str):
    if not TOKEN:
        print("ERROR: TELEGRAM_TOKEN is not set")
        return
    r = requests.post(
        f"{TG_API}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=20,
    )
    print("sendMessage:", r.status_code, r.text)


@app.get("/")
def index():
    return "OK", 200


def parse_message_to_row(text: str):
    """
    Ожидаем формат:
    ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; СПОСОБ_ОПЛАТЫ; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТ
    Пример:
    ОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-01; ИВАНОВ И.И.; жильё
    """
    parts = [p.strip() for p in text.split(";")]

    if len(parts) < 9:
        return None, (
            "Формат не распознан.\n"
            "Нужно 9 полей через `;`:\n"
            "Объект; Тип; Статья; Сумма; Оплата; НДС(ДА/НЕТ); Период(YYYY-MM-DD); Сотрудник; Коммент"
        )

    object_ = parts[0]
    type_ = parts[1]
    article = parts[2]

    # сумма: допускаем пробелы/запятые
    amount_raw = parts[3].replace(" ", "").replace(",", ".")
    try:
        amount = float(amount_raw)
    except ValueError:
        return None, "Сумма не число. Пример суммы: 10000 или 10000.50"

    pay_type = parts[4]
    vat = parts[5].upper()
    period = parts[6]
    employee = parts[7]
    comment = parts[8]

    # Datetime в 1-й колонке
  now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

row = [
    now,          # A DateTime
    object_,      # B Объект
    type_,        # C Тип
    article,      # D Статья
    amount,       # E СуммаБаза
    pay_type,     # F СпособОплаты
    vat,          # G НДС
    "",           # H Категория (пока пусто, потом подтянем автоматически)
    period,       # I ПЕРИОД
    employee,     # J Сотрудник
    "",           # K Статус (пока пусто)
    "TELEGRAM",   # L Источник
    str(message_id),  # M MessageID
]
    return row, None


@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    print("update:", json.dumps(data, ensure_ascii=False))

    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return "ok", 200

    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()

    if text == "/start":
        send_message(
            chat_id,
            "Привет! Я на связи.\n"
            "Отправь строку формата:\n"
            "Объект; Тип; Статья; Сумма; Оплата; НДС; Период; Сотрудник; Коммент\n"
            "Пример:\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-01; ИВАНОВ И.И.; жильё",
        )
        return "ok", 200

    # Если пользователь прислал несколько строк — пишем каждую
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    ok_count = 0
    for ln in lines:
        row, err = parse_message_to_row(ln)
        if err:
            send_message(chat_id, err)
            continue
        try:
            append_row(row)
            ok_count += 1
        except Exception as e:
            print("append error:", str(e))
            send_message(chat_id, f"Ошибка записи в таблицу: {e}")
            break

    if ok_count:
        send_message(chat_id, f"Записал строк: {ok_count}")

    return "ok", 200
