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

# окно контент-антидубля (сек). 120 = 2 минуты
DUP_WINDOW_SECONDS = int(os.environ.get("DUP_WINDOW_SECONDS", "120").strip() or "120")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# ---------------- ROUTES ----------------
@app.get("/")
def index():
    return "OK", 200


# ---------------- HELPERS ----------------
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
        range=SHEET_OPS,  # только имя листа
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()


# ---------------- VALIDATION ----------------
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
            "❌ Нужно 9 полей через ;\n"
            "Пример:\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 10000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; тест"
        )

    object_, type_, article, amount_raw, pay_type, vat, period_raw, employee, comment = parts

    type_u = type_.upper()
    if type_u not in ("РАСХОД", "ПРИХОД"):
        return None, "❌ Тип: РАСХОД или ПРИХОД"

    try:
        amount = float(amount_raw.replace(" ", "").replace(",", "."))
        if amount <= 0:
            return None, "❌ Сумма должна быть > 0"
    except:
        return None, "❌ Сумма должна быть числом"

    vat_u = vat.upper()
    if vat_u not in ("ДА", "НЕТ"):
        return None, "❌ НДС: ДА или НЕТ"

    period_s = period_raw.strip()
    if not re.fullmatch(r"\d{4}-\d{2}-[12]", period_s):
        return None, "❌ ПЕРИОД: YYYY-MM-1 или YYYY-MM-2 (пример: 2026-01-1)"

    # Минимально обязательные (если хочешь — можешь ослабить)
    if not object_ or not article or not pay_type:
        return None, "❌ Обязательные поля: объект, статья, оплата"

    return {
        "object": object_.strip(),
        "type": type_u,
        "article": article.strip(),
        "amount": amount,
        "pay_type": pay_type.strip(),
        "vat": vat_u,
        "period": period_s,
        "employee": employee.strip(),
        "comment": comment.strip(),
    }, None


# ---------------- DEDUP ----------------
def message_id_exists(message_id: str) -> bool:
    """Антидубль по message_id (технический)."""
    if not message_id:
        return False

    service = get_sheets_service()
    rng = f"'{SHEET_OPS}'!M:M"  # MessageID column
    res = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=rng
    ).execute()

    for r in res.get("values", []):
        if r and str(r[0]).strip() == str(message_id).strip():
            return True
    return False


def content_duplicate_exists(content_key: str, now_dt: datetime) -> bool:
    """
    Контент-антидубль:
    если такая же операция уже была записана недавно (в пределах DUP_WINDOW_SECONDS),
    повтор не пишем.
    """
    service = get_sheets_service()
    # Берём A..N чтобы читать DateTime (A) и поля (B..N)
    rng = f"'{SHEET_OPS}'!A:N"
    res = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=rng
    ).execute()

    rows = res.get("values", [])
    if len(rows) <= 1:
        return False

    # Берём только последние 300 строк для скорости
    data_rows = rows[1:]
    tail = data_rows[-300:] if len(data_rows) > 300 else data_rows

    # Идём с конца (самые свежие)
    for r in reversed(tail):
        # ожидаем: A..N (14 колонок). Может быть короче — безопасно
        dt_str = r[0] if len(r) > 0 else ""
        if not dt_str:
            continue

        try:
            dt = datetime.strptime(dt_str.strip(), "%Y-%m-%d %H:%M:%S")
        except:
            continue

        # если запись старая — дальше можно не смотреть (мы идём по свежим)
        if (now_dt - dt).total_seconds() > DUP_WINDOW_SECONDS:
            # можно break, потому что дальше будут ещё старее
            break

        # строим ключ так же как при записи
        # B..G: object,type,article,amount,pay_type,vat
        obj = r[1].strip() if len(r) > 1 and r[1] else ""
        typ = r[2].strip() if len(r) > 2 and r[2] else ""
        art = r[3].strip() if len(r) > 3 and r[3] else ""

        amt = r[4] if len(r) > 4 else ""
        pay = r[5].strip() if len(r) > 5 and r[5] else ""
        vat = r[6].strip() if len(r) > 6 and r[6] else ""

        period = r[8].strip() if len(r) > 8 and r[8] else ""   # I
        employee = r[9].strip() if len(r) > 9 and r[9] else "" # J
        comment = r[13].strip() if len(r) > 13 and r[13] else "" # N

        # сумма может быть записана как число/строка
        try:
            amt_f = float(str(amt).replace(" ", "").replace(",", "."))
        except:
            amt_f = str(amt).strip()

        key = f"{obj}|{typ}|{art}|{amt_f}|{pay}|{vat}|{period}|{employee}|{comment}"

        if key == content_key:
            return True

    return False


# ---------------- WEBHOOK ----------------
@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return "ok", 200

    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()
    message_id = str(msg.get("message_id", "")).strip()

    if text == "/start":
        send_message(
            chat_id,
            "Формат (9 полей через ;):\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 10000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; комментарий\n"
            "ПЕРИОД: YYYY-MM-1 (1–15) или YYYY-MM-2 (16–31)\n"
            f"Антидубль контента: окно {DUP_WINDOW_SECONDS} сек.\n"
            "Если нужно записать повтор специально — добавь в конец комментария: #force"
        )
        return "ok", 200

    parsed, err = validate_and_parse(text)
    if err:
        send_message(chat_id, err)
        return "ok", 200

    # 1) ТЕХНИЧЕСКИЙ антидубль по MessageID
    if message_id and message_id_exists(message_id):
        send_message(chat_id, "⚠️ Уже записано (антидубль по MessageID)")
        return "ok", 200

    now_dt = datetime.now()
    now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")

    # 2) КОНТЕНТ-антидубль (если случайно отправил одно и то же)
    # Можно форсировать повтор: добавить "#force" в комментарий
    force = "#force" in (parsed["comment"] or "").lower()

    # формируем ключ как у сохранённой строки
    content_key = (
        f"{parsed['object']}|{parsed['type']}|{parsed['article']}|{parsed['amount']}|"
        f"{parsed['pay_type']}|{parsed['vat']}|{parsed['period']}|{parsed['employee']}|{parsed['comment']}"
    )

    if not force and content_duplicate_exists(content_key, now_dt):
        send_message(chat_id, "⚠️ Похоже на дубль (такое уже было недавно). Если нужно повторить — добавь в комментарий #force")
        return "ok", 200

    # A..N (N = Комментарий)
    row = [
        now_str,                 # A DateTime
        parsed["object"],         # B Объект
        parsed["type"],           # C Тип
        parsed["article"],        # D Статья
        parsed["amount"],         # E СуммаБаза
        parsed["pay_type"],       # F СпособОплаты
        parsed["vat"],            # G НДС
        "",                       # H Категория (пока пусто)
        parsed["period"],         # I ПЕРИОД (YYYY-MM-1/2)
        parsed["employee"],       # J Сотрудник
        "",                       # K Статус
        "TELEGRAM",               # L Источник
        message_id,               # M MessageID
        parsed["comment"],        # N Комментарий
    ]

    try:
        append_row(row)
        send_message(chat_id, "✅ Записал")
    except Exception as e:
        send_message(chat_id, f"❌ Ошибка записи в таблицу: {repr(e)}")
        print("append error:", repr(e))

    return "ok", 200
