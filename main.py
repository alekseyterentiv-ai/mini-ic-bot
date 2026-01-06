from flask import Flask, request
import os
import json
import time
import re
import requests
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# --- ENV ---
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_API = f"https://api.telegram.org/bot{TOKEN}"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "").strip()    # ОПЕРАЦИИ
SHEET_LOGS = os.environ.get("SHEET_LOGS", "ЛОГИ").strip()

GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# --- Anti-dup settings ---
DEDUP_TTL_SECONDS = 6 * 60 * 60          # MessageID защита (6 часов)
CONTENT_DEDUP_WINDOW_SECONDS = 30         # окно антидубля по тексту (30 сек)

_seen_message_ids = {}        # message_id -> ts
_seen_content = {}            # (chat_id, normalized_text) -> ts

_svc = None                   # cached sheets service
_sheet_title_to_id = None     # cached mapping title->sheetId


def _cleanup_caches(now_ts: float) -> None:
    to_del = [k for k, ts in _seen_message_ids.items() if now_ts - ts > DEDUP_TTL_SECONDS]
    for k in to_del:
        _seen_message_ids.pop(k, None)

    to_del = [k for k, ts in _seen_content.items() if now_ts - ts > CONTENT_DEDUP_WINDOW_SECONDS]
    for k in to_del:
        _seen_content.pop(k, None)


def send_message(chat_id: int, text: str) -> None:
    try:
        requests.post(
            f"{TG_API}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=20,
        )
    except Exception as e:
        print("send_message error:", repr(e))


def quote_sheet_title(title: str) -> str:
    # безопасно для кириллицы/пробелов
    safe = title.replace("'", "''")
    return f"'{safe}'"


def get_sheets_service():
    global _svc
    if _svc is not None:
        return _svc

    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is empty")

    sa_info = json.loads(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    _svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _svc


def get_sheet_ids_map():
    global _sheet_title_to_id
    if _sheet_title_to_id is not None:
        return _sheet_title_to_id

    svc = get_sheets_service()
    meta = svc.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(properties(sheetId,title))"
    ).execute()

    m = {}
    for sh in meta.get("sheets", []):
        props = sh.get("properties", {})
        m[props.get("title")] = props.get("sheetId")
    _sheet_title_to_id = m
    return m


def append_row(sheet_name: str, row: list):
    svc = get_sheets_service()
    # ВАЖНО: range = только имя листа (без !A)
    svc.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=sheet_name,
        valueInputOption="USER_ENTERED",
        body={"values": [row]},
    ).execute()


def get_values(sheet_range: str):
    svc = get_sheets_service()
    return svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=sheet_range,
    ).execute().get("values", [])


def delete_row_by_index(sheet_title: str, row_number_1based: int):
    # row_number_1based: 1..N
    sheet_ids = get_sheet_ids_map()
    sheet_id = sheet_ids.get(sheet_title)
    if sheet_id is None:
        raise RuntimeError(f"Sheet '{sheet_title}' not found in spreadsheet")

    svc = get_sheets_service()
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "requests": [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "ROWS",
                            "startIndex": row_number_1based - 1,  # 0-based inclusive
                            "endIndex": row_number_1based,        # 0-based exclusive
                        }
                    }
                }
            ]
        },
    ).execute()


def log_event(chat_id, user_id, username, full_name, message_id, text, status, error_text, action):
    # Datetime | ChatID | UserID | Username | FullName | MessageID | Text | Status | ErrorText | Source | Action
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [
        now_str,
        str(chat_id or ""),
        str(user_id or ""),
        str(username or ""),
        str(full_name or ""),
        str(message_id or ""),
        str(text or ""),
        str(status or ""),
        str(error_text or ""),
        "TELEGRAM",
        str(action or ""),
    ]
    try:
        append_row(SHEET_LOGS, row)
    except Exception as e:
        print("log_event append error:", repr(e))


def validate_and_parse(text: str):
    # ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; СПОСОБ; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТАРИЙ
    parts = [p.strip() for p in text.split(";")]

    if len(parts) != 9:
        return None, "❌ Ошибка формата: должно быть 9 полей через ;\nПример:\nОБУХОВО; РАСХОД; КВАРТИРА; 1000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; тест"

    object_, type_, article, amount_raw, pay_type, vat, period_raw, employee, comment = parts

    if not object_ or not type_ or not article or not amount_raw:
        return None, "❌ Не хватает обязательных полей: ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА"

    # Тип
    type_up = type_.upper()

    # разрешаем: РАСХОД / ДОХОД / ЗП / АВАНС (и ПРИХОД как синоним)
    if type_up == "ПРИХОД":
        type_up = "ДОХОД"

    allowed_types = {"РАСХОД", "ДОХОД", "ЗП", "АВАНС"}
    if type_up not in allowed_types:
        return None, "❌ Тип должен быть РАСХОД / ДОХОД / ЗП / АВАНС"

    # Сумма
    try:
        amt = amount_raw.replace(" ", "").replace(",", ".")
        amount = float(amt)
        if amount <= 0:
            return None, "❌ Сумма должна быть больше 0"
    except:
        return None, "❌ Сумма должна быть числом"

    # НДС
    vat_up = vat.upper()
    if vat_up not in ("ДА", "НЕТ"):
        return None, "❌ НДС только ДА или НЕТ"

    # Период: YYYY-MM-1 или YYYY-MM-2
    if not re.match(r"^\d{4}-\d{2}-[12]$", period_raw):
        return None, "❌ Период только YYYY-MM-1 или YYYY-MM-2"

    return {
        "object": object_,
        "type": type_up,
        "article": article,
        "amount": amount,
        "pay_type": pay_type,
        "vat": vat_up,
        "period": period_raw,
        "employee": employee,
        "comment": comment,
    }, None


def find_last_ok_op_message_id(chat_id: int):
    # Ищем последнюю успешную запись операции в ЛОГАХ
    # формат логов: A..K (Action в K)
    rng = f"{quote_sheet_title(SHEET_LOGS)}!A:K"
    values = get_values(rng)
    if not values:
        return None

    # header может быть, поэтому идём с конца и проверяем по колонкам
    for row in reversed(values):
        # row indexes:
        # 0 Datetime, 1 ChatID, 2 UserID, 3 Username, 4 FullName, 5 MessageID,
        # 6 Text, 7 Status, 8 ErrorText, 9 Source, 10 Action
        if len(row) < 11:
            continue
        if str(row[1]).strip() != str(chat_id):
            continue
        if str(row[7]).strip().upper() != "OK":
            continue
        if str(row[10]).strip().upper() != "OP_WRITE":
            continue
        mid = str(row[5]).strip()
        if mid:
            return mid
    return None


def find_row_number_in_ops_by_message_id(message_id: str):
    # Ищем MessageID в колонке M листа ОПЕРАЦИИ
    # ВАЖНО: используем диапазон с кавычками
    rng = f"{quote_sheet_title(SHEET_OPS)}!M:M"
    col = get_values(rng)  # список значений в колонке M, начиная с M1

    if not col:
        return None

    # col выглядит как [[val1],[val2],...]
    for i, cell in enumerate(col, start=1):  # i = номер строки (1-based)
        if not cell:
            continue
        if str(cell[0]).strip() == str(message_id).strip():
            return i
    return None


@app.get("/")
def index():
    return "ok", 200


@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}

    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return "no message", 200

    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    if not chat_id:
        return "no chat", 200

    from_user = msg.get("from") or {}
    user_id = from_user.get("id")
    username = from_user.get("username", "")
    full_name = (from_user.get("first_name", "") + " " + from_user.get("last_name", "")).strip()

    message_id = msg.get("message_id")
    text = (msg.get("text") or "").strip()

    # /start
    if text.startswith("/start"):
        send_message(
            chat_id,
            "Привет! Я на связи.\nФормат:\nОБУХОВО; РАСХОД; КВАРТИРА; 1000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; комментарий\n\nКоманды:\n/undo — отменить последнюю запись",
        )
        log_event(chat_id, user_id, username, full_name, message_id, text, "OK", "", "START")
        return "ok", 200

    now_ts = time.time()
    _cleanup_caches(now_ts)

    # --- Anti-dup by MessageID ---
    if message_id is not None:
        if message_id in _seen_message_ids:
            log_event(chat_id, user_id, username, full_name, message_id, text, "DUP", "dup message_id", "DEDUP")
            return "dup message_id", 200
        _seen_message_ids[message_id] = now_ts

    # --- Anti-dup by content within 30s (per chat) ---
    norm_text = re.sub(r"\s+", " ", text).strip().lower()
    if norm_text:
        key = (chat_id, norm_text)
        last_ts = _seen_content.get(key)
        if last_ts and (now_ts - last_ts) <= CONTENT_DEDUP_WINDOW_SECONDS:
            log_event(chat_id, user_id, username, full_name, message_id, text, "DUP", "dup content", "DEDUP")
            return "dup content", 200
        _seen_content[key] = now_ts

    # /undo
    if text.strip().lower() == "/undo":
        try:
            target_mid = find_last_ok_op_message_id(chat_id)
            if not target_mid:
                send_message(chat_id, "⚠️ Нет последней операции для отмены (в логах не найдено).")
                log_event(chat_id, user_id, username, full_name, message_id, text, "WARN", "no last op in logs", "UNDO")
                return "ok", 200

            row_num = find_row_number_in_ops_by_message_id(target_mid)
            if not row_num:
                send_message(chat_id, "⚠️ Не нашёл строку в ОПЕРАЦИИ для отмены (MessageID не найден).")
                log_event(chat_id, user_id, username, full_name, message_id, text, "WARN", f"MessageID {target_mid} not found in OPS", "UNDO")
                return "ok", 200

            delete_row_by_index(SHEET_OPS, row_num)
            send_message(chat_id, "✅ Отменил последнюю операцию")
            log_event(chat_id, user_id, username, full_name, message_id, text, "OK", f"deleted OPS row {row_num} by mid={target_mid}", "UNDO")
            return "ok", 200

        except Exception as e:
            print("undo error:", repr(e))
            send_message(chat_id, f"Ошибка /undo: {e}")
            log_event(chat_id, user_id, username, full_name, message_id, text, "ERR", str(e), "UNDO")
            return "ok", 200

    # обычная запись
    parsed, err = validate_and_parse(text)
    if err:
        send_message(chat_id, err)
        log_event(chat_id, user_id, username, full_name, message_id, text, "BAD", err, "VALIDATE")
        return "bad format", 200

    try:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ОПЕРАЦИИ: A..M
        row = [
            now_str,                 # A DateTime
            parsed["object"],        # B Объект
            parsed["type"],          # C Тип
            parsed["article"],       # D Статья
            parsed["amount"],        # E СуммаБаза
            parsed["pay_type"],      # F СпособОплаты
            parsed["vat"],           # G НДС
            "",                      # H Категория
            parsed["period"],        # I ПЕРИОД (YYYY-MM-1/2)
            parsed["employee"],      # J Сотрудник
            "",                      # K Статус
            "TELEGRAM",              # L Источник
            str(message_id or ""),   # M MessageID
        ]

        append_row(SHEET_OPS, row)
        send_message(chat_id, "✅ Записал")
        log_event(chat_id, user_id, username, full_name, message_id, text, "OK", "", "OP_WRITE")

    except Exception as e:
        print("append error:", repr(e))
        send_message(chat_id, f"Ошибка записи в таблицу: {e}")
        log_event(chat_id, user_id, username, full_name, message_id, text, "ERR", str(e), "OP_WRITE")

    return "ok", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
