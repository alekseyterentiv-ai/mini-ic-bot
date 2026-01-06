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
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ").strip()

# Секрет с JSON сервис-аккаунта (в Cloud Run -> Variables & Secrets -> "Secrets exposed as env vars")
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()

TG_API = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else ""


@app.get("/")
def index():
    return "OK", 200


def tg_send(chat_id: int, text: str):
    if not TG_API:
        print("ERROR: TELEGRAM_TOKEN is not set")
        return
    try:
        r = requests.post(
            f"{TG_API}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=10
        )
        print("sendMessage:", r.status_code, r.text)
    except Exception as e:
        print("sendMessage exception:", repr(e))


def get_sheets_service():
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is not set")

    info = json.loads(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def append_row_to_sheet(row: list):
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is not set")

    service = get_sheets_service()
    # Пишем в лист ОПЕРАЦИИ, начиная с A
    rng = f"{SHEET_OPS}!A:Z"
    body = {"values": [row]}

    resp = service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=rng,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

    return resp


def parse_line(text: str):
    """
    Ожидаем формат через ;
    ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; СПОСОБ; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТ
    Коммент может быть пустым.
    """
    parts = [p.strip() for p in text.split(";")]
    # убираем пустые хвосты
    while parts and parts[-1] == "":
        parts.pop()

    if len(parts) < 8:
        return None

    object_ = parts[0]
    type_ = parts[1]
    article = parts[2]
    amount_raw = parts[3].replace(" ", "").replace(",", ".")
    pay_type = parts[4]
    vat = parts[5]
    period = parts[6]
    employee = parts[7]
    comment = parts[8] if len(parts) >= 9 else ""

    try:
        amount = float(amount_raw)
    except:
        amount = amount_raw  # пусть Google Sheets сам попробует

    return object_, type_, article, amount, pay_type, vat, period, employee, comment


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
        tg_send(chat_id, "Привет! Я на связи.\nФормат:\nОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; СПОСОБ; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТ")
        return "ok", 200

    parsed = parse_line(text)
    if not parsed:
        tg_send(chat_id, "Не понял строку.\nНужно минимум 8 полей через ;\nПример:\nОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-01; ИВАНОВ И.И.; жильё")
        return "ok", 200

    object_, type_, article, amount, pay_type, vat, period, employee, comment = parsed
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ВАЖНО: MessageID УБРАЛИ ПОЛНОСТЬЮ. Статус пустой.
    row = [
        now,           # A DateTime
        object_,       # B Объект
        type_,         # C Тип
        article,       # D Статья
        amount,        # E СуммаБаза
        pay_type,      # F СпособОплаты
        vat,           # G НДС
        "",            # H Категория
        period,        # I ПЕРИОД
        employee,      # J Сотрудник
        "",            # K Статус (ПУСТО)
        "TELEGRAM",    # L Источник
    ]

    try:
        append_row_to_sheet(row)
        tg_send(chat_id, f"Записал: {object_} / {type_} / {article} / {amount}")
    except Exception as e:
        print("append error:", repr(e))
        tg_send(chat_id, f"Ошибка записи в таблицу: {e}")

    return "ok", 200
