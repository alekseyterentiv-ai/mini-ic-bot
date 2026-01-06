from flask import Flask, request
import os, json, re
import requests
import gspread
import google.auth

app = Flask(__name__)

TOKEN = os.environ.get("TELEGRAM_TOKEN")
TG_API = f"https://api.telegram.org/bot{TOKEN}"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ")

_gc = None
_ws_ops = None

def sheets_ops():
    global _gc, _ws_ops
    if _ws_ops:
        return _ws_ops
    # Берём креды автоматически из Service Account Cloud Run
    creds, _ = google.auth.default(scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    _gc = gspread.authorize(creds)
    sh = _gc.open_by_key(SPREADSHEET_ID)
    _ws_ops = sh.worksheet(SHEET_OPS)
    return _ws_ops

@app.get("/")
def index():
    return "OK", 200

def send_message(chat_id: int, text: str):
    r = requests.post(
        f"{TG_API}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=10
    )
    print("sendMessage:", r.status_code, r.text)

def parse_semicolon(text: str):
    # ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; СПОСОБ; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТ
    parts = [p.strip() for p in text.split(";")]
    if len(parts) < 4:
        return None
    # добиваем до 9 полей
    while len(parts) < 9:
        parts.append("")
    obj, typ, article, amount_raw, pay, vat, period, employee, comment = parts[:9]

    # сумма: "10 000", "10000", "5к"
    amount_raw = amount_raw.replace("₽", "").replace(" ", "")
    m = re.match(r"^(\d+(?:[.,]\d+)?)(к)?$", amount_raw, re.IGNORECASE)
    if not m:
        return None
    amount = float(m.group(1).replace(",", "."))
    if m.group(2):
        amount *= 1000

    return {
        "object": obj.upper(),
        "type": typ.upper(),
        "article": article.upper(),
        "amount": amount,
        "pay": pay.upper() if pay else "",
        "vat": vat.upper() if vat else "",
        "period": period,
        "employee": employee,
        "comment": comment,
    }

@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return "ok", 200

    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()

    if text == "/start":
        send_message(chat_id,
            "Я на связи.\n"
            "Формат ввода:\n"
            "ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; СПОСОБ; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТ\n"
            "Пример:\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 10000; БЕЗНАЛ; ДА; 2026-01-1;; за жильё"
        )
        return "ok", 200

    p = parse_semicolon(text)
    if not p:
        send_message(chat_id, "Не понял формат. Напиши /start — покажу пример.")
        return "ok", 200

    # Пишем строку в ОПЕРАЦИИ по твоей шапке:
    # DateTime | Объект | Тип | Статья | СуммаБаза | СпособОплаты | НДС | Категория | ПЕРИОД | Сотрудник | Статус | Источник | MessageID
    ws = sheets_ops()
    ws.append_row([
        "",                 # DateTime (можно оставить пусто — таблица/формулы сами поставят, или позже проставим)
        p["object"],
        p["type"],
        p["article"],
        p["amount"],
        p["pay"],
        p["vat"],
        "",                 # Категория (пока пусто — позже подтянем из НАСТРОЙКИ)
        p["period"],
        p["employee"],
        "",                 # Статус
        "TELEGRAM",
        str(msg.get("message_id", "")),
    ], value_input_option="USER_ENTERED")

    send_message(chat_id, f"Записал: {p['object']} / {p['type']} / {p['article']} / {p['amount']}")
    return "ok", 200
