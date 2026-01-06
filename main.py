import os
import json
import requests
from datetime import datetime

from flask import Flask, request

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

app = Flask(__name__)

# ===== ENV =====
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ").strip()

# GOOGLE_SA_JSON должен быть именно JSON-строкой сервисного аккаунта (секрет, выведенный как env var)
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()

TG_API = f"https://api.telegram.org/bot{TOKEN}"


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
        print("sendMessage exception:", str(e))


def get_sheets_service():
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is not set")

    info = json.loads(GOOGLE_SA_JSON)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def append_row(row: list):
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID is not set")
    if not SHEET_OPS:
        raise RuntimeError("SHEET_OPS is not set")

    service = get_sheets_service()

    # КЛЮЧЕВОЕ: range = ТОЛЬКО имя листа, без !A
    res = service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=SHEET_OPS,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()

    updates = (res.get("updates") or {}).get("updatedRows", 0)
    return updates


def parse_line(text: str):
    """
    Ожидаем 9 полей:
    object_; type_; article; amount; pay_type; vat; period; employee; comment

    Разделители: ';' или ':'
    """
    raw = (text or "").strip()

    # разрешаем и ; и :
    sep = ";" if ";" in raw else ":"
    parts = [p.strip() for p in raw.split(sep)]

    # убираем пустые хвосты (если человек наделал ";;;")
    while parts and parts[-1] == "":
        parts.pop()

    if len(parts) < 4:
        return None, "Мало полей. Формат:\nОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; СПОСОБ; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТ"

    # добиваем до 9 элементов пустыми
    while len(parts) < 9:
        parts.append("")

    object_ = parts[0]
    type_ = parts[1]
    article = parts[2]

    # сумма
    amount_raw = parts[3].replace(" ", "").replace(",", ".")
    try:
        amount = float(amount_raw)
    except Exception:
        return None, f"Сумма не число: '{parts[3]}'"

    pay_type = parts[4]
    vat = parts[5]  # как строка: "ДА/НЕТ" или что ты вводишь
    period = parts[6]
    employee = parts[7]
    comment = parts[8]

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

    # берём message из разных типов апдейтов
    msg = (
        data.get("message")
        or data.get("edited_message")
        or data.get("channel_post")
        or data.get("edited_channel_post")
    )
    if not msg:
        return "ok", 200

    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    text = (msg.get("text") or "").strip()

    # MessageID — должен приходить из Telegram
    message_id = msg.get("message_id")
    if message_id is None:
        message_id = ""  # на всякий случай, но обычно он всегда есть

    # /start (даже если человек прислал "/start\nчто-то")
    if text.startswith("/start"):
        send_message(
            chat_id,
            "Привет! Я на связи.\n\nФормат:\nОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; СПОСОБ; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТ\n\nПример:\nОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-01; ИВАНОВ И.И.; жильё"
        )
        return "ok", 200

    # парсим строку
    parsed, err = parse_line(text)
    if err:
        send_message(chat_id, f"Ошибка формата: {err}")
        return "ok", 200

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ПОРЯДОК КОЛОНОК A..M (13 штук)
    row = [
        now,                      # A DateTime
        parsed["object_"],        # B Объект
        parsed["type_"],          # C Тип
        parsed["article"],        # D Статья
        parsed["amount"],         # E СуммаБаза
        parsed["pay_type"],       # F СпособОплаты
        parsed["vat"],            # G НДС
        "",                       # H Категория (пусто)
        parsed["period"],         # I ПЕРИОД
        parsed["employee"],       # J Сотрудник
        "",                       # K Статус (пусто)
        "TELEGRAM",               # L Источник
        message_id,               # M MessageID
    ]

    try:
        updated = append_row(row)
        send_message(chat_id, f"Записал строк: {updated} (message_id={message_id})")
    except Exception as e:
        send_message(chat_id, f"Ошибка записи в таблицу: {str(e)}")
        print("append error:", str(e))

    return "ok", 200
