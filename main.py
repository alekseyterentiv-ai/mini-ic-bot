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
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ").strip()

# В Cloud Run это Secret, "exposed as environment variable"
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
            timeout=15
        )
        print("sendMessage:", r.status_code, r.text)
    except Exception as e:
        print("sendMessage exception:", repr(e))


def get_sheets_service():
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is not set")
    info = json.loads(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def validate_and_parse(text: str):
    parts = [p.strip() for p in text.split(";")]

    if len(parts) != 9:
        return None, "❌ Ошибка формата: должно быть 9 полей через ;\nПример:\nОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-01; ИВАНОВ И.И.; жильё"

    object_, type_, article, amount, pay_type, vat, date_str, employee, comment = parts

    # Тип
    type_u = type_.upper()
    if type_u not in ("РАСХОД", "ПРИХОД"):
        return None, "❌ Тип должен быть РАСХОД или ПРИХОД"

    # Сумма
    try:
        amount_f = float(amount.replace(" ", "").replace(",", "."))
        if amount_f <= 0:
            return None, "❌ Сумма должна быть больше 0"
    except:
        return None, "❌ Сумма должна быть числом"

    # НДС
    vat_u = vat.upper()
    if vat_u not in ("ДА", "НЕТ"):
        return None, "❌ НДС только ДА или НЕТ"

    # Дата -> период
    try:
        period = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except:
        return None, "❌ Дата должна быть в формате YYYY-MM-DD"

    # Мини-проверки обязательных
    if not object_ or not article or not pay_type or not employee:
        return None, "❌ Обязательные поля не заполнены (объект/статья/оплата/сотрудник)"

    return {
        "object": object_,
        "type": type_u,
        "article": article,
        "amount": amount_f,
        "pay_type": pay_type,
        "vat": vat_u,
        "period": period,
        "employee": employee,
        "comment": comment,
    }, None


def append_row(row: list):
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is not set")
    if not SHEET_OPS:
        raise RuntimeError("SHEET_OPS is not set (sheet name)")

    service = get_sheets_service()
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_OPS,  # ВАЖНО: только имя листа, без !A
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()


@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    print("update:", json.dumps(data, ensure_ascii=False))

    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return "ok", 200

    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()

    # ВАЖНО: message_id берём из msg, иначе будет пусто
    message_id = msg.get("message_id")

    if text == "/start":
        send_message(
            chat_id,
            "Привет! Я на связи.\nФормат (9 полей через ;):\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-01; ИВАНОВ И.И.; жильё"
        )
        return "ok", 200

    parsed, err = validate_and_parse(text)
    if err:
        send_message(chat_id, err)
        return "ok", 200

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    row = [
        now,                    # A DateTime
        parsed["object"],        # B Объект
        parsed["type"],          # C Тип
        parsed["article"],       # D Статья
        parsed["amount"],        # E СуммаБаза
        parsed["pay_type"],      # F СпособОплаты
        parsed["vat"],           # G НДС
        "",                      # H Категория (пока пусто)
        parsed["period"],        # I ПЕРИОД
        parsed["employee"],      # J Сотрудник
        "",                      # K Статус (пусто)
        "TELEGRAM",              # L Источник
        message_id or "",        # M MessageID
        parsed["comment"],       # N Комментарий
    ]

    try:
        append_row(row)
        send_message(chat_id, f"✅ Записал: {parsed['object']} / {parsed['type']} / {parsed['article']} / {parsed['amount']}")
    except Exception as e:
        send_message(chat_id, f"❌ Ошибка записи в таблицу: {repr(e)}")
        print("append error:", repr(e))

    return "ok", 200
