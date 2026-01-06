from flask import Flask, request
import os
import json
import requests
from datetime import datetime

from google.oauth2 import service_account
from google.auth.transport.requests import Request as GoogleAuthRequest

app = Flask(__name__)

# --- ENV ---
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_API = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else ""

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ").strip()

# Secret with service account JSON (text content)
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


@app.get("/")
def index():
    return "OK", 200


def send_message(chat_id: int, text: str):
    if not TOKEN:
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
        print("sendMessage EX:", str(e))


def get_sheets_access_token() -> str:
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is not set")

    info = json.loads(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    creds.refresh(GoogleAuthRequest())
    return creds.token


def append_row_to_sheet(row: list):
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is not set")

    token = get_sheets_access_token()
    sheet_encoded = requests.utils.quote(SHEET_OPS, safe="")

    url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}"
        f"/values/{sheet_encoded}!A:append"
        f"?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS"
    )

    headers = {"Authorization": f"Bearer {token}"}
    body = {"values": [row]}

    r = requests.post(url, headers=headers, json=body, timeout=15)
    print("append:", r.status_code, r.text)

    if r.status_code >= 300:
        raise RuntimeError(f"Sheets append failed: {r.status_code} {r.text}")


def parse_line(text: str):
    # Ожидаем: 0 Объект; 1 Тип; 2 Статья; 3 Сумма; 4 Оплата; 5 НДС; 6 Период; 7 Сотрудник; 8 Коммент (опц)
    parts = [p.strip() for p in text.split(";")]
    parts = [p for p in parts if p != ""]  # убираем пустые куски от ";;"

    if len(parts) < 8:
        return None, "Нужно минимум 8 полей через ';'. Пример:\nОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-01; ИВАНОВ И.И.; жильё"

    object_ = parts[0]
    type_ = parts[1]
    article = parts[2]

    # сумма
    raw_amount = parts[3].replace(" ", "").replace(",", ".")
    try:
        amount = float(raw_amount)
    except:
        return None, "Сумма должна быть числом. Пример: 10000"

    pay_type = parts[4]
    vat = parts[5]
    period = parts[6]
    employee = parts[7]
    comment = parts[8] if len(parts) >= 9 else ""

    return {
        "object_": object_,
        "type_": type_,
        "article": article,
        "amount": amount,
        "pay_type": pay_type,
        "vat": vat,
        "period": period,
        "employee": employee,
        "comment": comment,
    }, None


@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    print("update:", json.dumps(data, ensure_ascii=False))

    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return "ok", 200

    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()

    # /start (даже если дальше текст на новой строке)
    if text.startswith("/start"):
        send_message(chat_id, "Привет! Я на связи.\nФормат:\nОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-01; ИВАНОВ И.И.; жильё")
        return "ok", 200

    # парсим строку
    parsed, err = parse_line(text)
    if err:
        send_message(chat_id, err)
        return "ok", 200

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ВАЖНО: статус пустой, message_id НЕ ПИШЕМ
    # A..L (12 колонок). Если у тебя есть M (MessageID) — она останется пустой автоматически.
    row = [
        now,                    # A DateTime
        parsed["object_"],       # B Объект
        parsed["type_"],         # C Тип
        parsed["article"],       # D Статья
        parsed["amount"],        # E СуммаБаза
        parsed["pay_type"],      # F СпособОплаты
        parsed["vat"],           # G НДС
        "",                      # H Категория (пусто)
        parsed["period"],        # I ПЕРИОД
        parsed["employee"],      # J Сотрудник
        "",                      # K Статус (пусто)
        "TELEGRAM",              # L Источник
        # M MessageID — НЕ передаем
    ]

    try:
        append_row_to_sheet(row)
        send_message(chat_id, f"Записал: {parsed['object_']} / {parsed['type_']} / {parsed['article']} / {parsed['amount']}")
    except Exception as e:
        print("WRITE EX:", str(e))
        send_message(chat_id, f"Ошибка записи в таблицу: {e}")

    return "ok", 200
