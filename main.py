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
SHEET_OPS = os.environ.get("SHEET_OPS", "").strip()  # например: ОПЕРАЦИИ

# Секрет из Secret Manager, "exposed as environment variable"
# В Cloud Run это должна быть переменная окружения GOOGLE_SA_JSON, содержащая ВЕСЬ JSON сервис-аккаунта.
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


@app.get("/")
def index():
    return "OK", 200


def send_message(chat_id: int, text: str):
    if not TOKEN:
        print("ERROR: TELEGRAM_TOKEN is not set")
        return
    r = requests.post(
        f"{TG_API}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=15
    )
    print("sendMessage:", r.status_code, r.text)


def get_sheets_service():
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is not set (Secret Manager env var is missing)")
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is not set")
    if not SHEET_OPS:
        raise RuntimeError("SHEET_OPS is not set (sheet name)")

    info = json.loads(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def append_row(row: list):
    service = get_sheets_service()

    # ✅ САМЫЙ СТАБИЛЬНЫЙ ВАРИАНТ: range = только имя листа (без "!A")
    res = service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_OPS,  # например "ОПЕРАЦИИ"
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]}
    ).execute()

    updates = res.get("updates", {})
    return updates.get("updatedRows", 1)


def parse_line(text: str):
    """
    Формат:
    ОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-01; ИВАНОВ И.И.; жильё

    Разделитель: ;
    Пустые поля допускаются.
    """
    parts = [p.strip() for p in text.split(";")]

    # добьём до 9 полей (comment может отсутствовать)
    while len(parts) < 9:
        parts.append("")

    object_ = parts[0]
    type_ = parts[1]
    article = parts[2]
    amount_raw = parts[3]
    pay_type = parts[4]
    vat = parts[5]
    period = parts[6]
    employee = parts[7]
    comment = parts[8]

    if not object_ or not type_ or not article or not amount_raw:
        raise ValueError("Не хватает обязательных полей: объект; тип; статья; сумма")

    # сумма: допускаем "10 000", "10000", "10000,50"
    amt = amount_raw.replace(" ", "").replace(",", ".")
    amount = float(amt)

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
    message_id = str(msg.get("message_id", ""))  # ✅ MessageID (колонка M)

    # /start (или "/start что-то") — только подсказка, НЕ пишем в таблицу
    if text.startswith("/start"):
        send_message(
            chat_id,
            "Привет! Я на связи.\n\nФормат:\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-01; ИВАНОВ И.И.; жильё"
        )
        return "ok", 200

    try:
        object_, type_, article, amount, pay_type, vat, period, employee, comment = parse_line(text)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ✅ Порядок колонок как ты писал:
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
        row = [
            now,            # A DateTime
            object_,        # B Объект
            type_,          # C Тип
            article,        # D Статья
            amount,         # E СуммаБаза
            pay_type,       # F СпособОплаты
            vat,            # G НДС
            "",             # H Категория
            period,         # I ПЕРИОД
            employee,       # J Сотрудник
            "",             # K Статус
            "TELEGRAM",     # L Источник
            message_id,     # M MessageID
        ]

        updated_rows = append_row(row)
        send_message(chat_id, f"Записал строк: {updated_rows}")

    except Exception as e:
        send_message(chat_id, f"Ошибка записи в таблицу: {e}")
        print("ERROR:", repr(e))

    return "ok", 200
