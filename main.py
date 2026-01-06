from flask import Flask, request
import os
import json
import requests
import re
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# --- ENV ---
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_API = f"https://api.telegram.org/bot{TOKEN}"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ").strip()

# Secret Manager -> exposed as env var (full JSON)
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


def append_row(row: list):
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is not set")
    if not SHEET_OPS:
        raise RuntimeError("SHEET_OPS is not set (sheet name)")

    service = get_sheets_service()
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_OPS,  # IMPORTANT: only sheet name, no "!A"
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()


def validate_and_parse(text: str):
    """
    Формат: 9 полей через ;
    ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; ОПЛАТА; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТАРИЙ

    ПЕРИОД строго: YYYY-MM-1 или YYYY-MM-2
    1 = 1-15
    2 = 16-31
    """
    parts = [p.strip() for p in (text or "").split(";")]

    if len(parts) != 9:
        return None, (
            "❌ Ошибка формата: должно быть 9 полей через ;\n"
            "Пример:\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-1; ИВАНОВ И.И.; жильё"
        )

    object_, type_, article, amount, pay_type, vat, period_str, employee, comment = parts

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

    # ПЕРИОД: YYYY-MM-1 или YYYY-MM-2
    period_raw = period_str.strip()
    if not re.fullmatch(r"\d{4}-\d{2}-[12]", period_raw):
        return None, "❌ ПЕРИОД должен быть в формате YYYY-MM-1 или YYYY-MM-2 (пример: 2026-01-1)"
    y, m, half = period_raw.split("-")
    m_i = int(m)
    if not (1 <= m_i <= 12):
        return None, "❌ Месяц должен быть 01..12"
    period = period_raw  # сохраняем как есть

    # Минимальные обязательные поля (можешь ослабить если нужно)
    if not object_ or not article or not pay_type:
        return None, "❌ Обязательные поля: объект, статья, оплата"

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


@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    print("update:", json.dumps(data, ensure_ascii=False))

    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return "ok", 200

    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()
    message_id = msg.get("message_id", "")

    if text == "/start":
        send_message(
            chat_id,
            "Привет! Формат (9 полей через ;):\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-1; ИВАНОВ И.И.; жильё\n\n"
            "ПЕРИОД: YYYY-MM-1 (1–15) или YYYY-MM-2 (16–31)"
        )
        return "ok", 200

    parsed, err = validate_and_parse(text)
    if err:
        send_message(chat_id, err)
        return "ok", 200

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # A..N (N = Комментарий)
    row = [
        now,                     # A DateTime
        parsed["object"],         # B Объект
        parsed["type"],           # C Тип
        parsed["article"],        # D Статья
        parsed["amount"],         # E СуммаБаза
        parsed["pay_type"],       # F СпособОплаты
        parsed["vat"],            # G НДС
        "",                       # H Категория (пока пусто)
        parsed["period"],         # I ПЕРИОД (YYYY-MM-1/2)
        parsed["employee"],       # J Сотрудник
        "",                       # K Статус (пусто)
        "TELEGRAM",               # L Источник
        str(message_id),          # M MessageID
        parsed["comment"],        # N Комментарий
    ]

    try:
        append_row(row)
        send_message(chat_id, "✅ Записал")
    except Exception as e:
        send_message(chat_id, f"❌ Ошибка записи в таблицу: {repr(e)}")
        print("append error:", repr(e))

    return "ok", 200
