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
SHEET_OPS = os.environ.get("SHEET_OPS", "").strip()          # например: ОПЕРАЦИИ
SHEET_LOGS = os.environ.get("SHEET_LOGS", "ЛОГИ").strip()     # лист для логов

GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# --- Anti-dup settings ---
DEDUP_TTL_SECONDS = 6 * 60 * 60          # MessageID защита (6 часов)
CONTENT_DEDUP_WINDOW_SECONDS = 30        # окно антидубля по тексту

# In-memory caches
_seen_message_ids = {}        # message_id -> ts
_seen_content = {}            # (chat_id, normalized_text) -> ts


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


def build_sheets_service():
    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is empty")

    sa_info = json.loads(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def append_row_with_service(service, sheet_name: str, row: list):
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=sheet_name,  # только имя листа
        valueInputOption="USER_ENTERED",
        body={"values": [row]},
    ).execute()


def log_action(service, now_str: str, chat_id, user_id, username, full_name,
               message_id, text, status: str, error_text: str = ""):
    """
    Лист ЛОГИ (A..J):
    A Datetime
    B ChatID
    C UserID
    D Username
    E FullName
    F MessageID
    G Text
    H Status (OK/ERROR/DUPLICATE/INFO)
    I ErrorText
    J Source
    """
    try:
        row = [
            now_str,
            str(chat_id or ""),
            str(user_id or ""),
            str(username or ""),
            str(full_name or ""),
            str(message_id or ""),
            str(text or ""),
            status,
            str(error_text or ""),
            "TELEGRAM",
        ]
        append_row_with_service(service, SHEET_LOGS, row)
    except Exception as e:
        print("log_action error:", repr(e))


def validate_and_parse(text: str):
    parts = [p.strip() for p in text.split(";")]

    if len(parts) != 9:
        return None, (
            "❌ Ошибка формата: должно быть 9 полей через ;\n"
            "Пример:\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 1000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; тест"
        )

    object_, type_, article, amount_raw, pay_type, vat, period_raw, employee, comment = parts

    if not object_ or not type_ or not article or not amount_raw:
        return None, "❌ Не хватает обязательных полей: ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА"

    type_up = type_.upper()
    if type_up not in ("РАСХОД", "ПРИХОД"):
        return None, "❌ Тип должен быть РАСХОД или ПРИХОД"

    try:
        amt = amount_raw.replace(" ", "").replace(",", ".")
        amount = float(amt)
        if amount <= 0:
            return None, "❌ Сумма должна быть больше 0"
    except:
        return None, "❌ Сумма должна быть числом"

    vat_up = vat.upper()
    if vat_up not in ("ДА", "НЕТ"):
        return None, "❌ НДС только ДА или НЕТ"

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


# ---------------- /undo helpers ----------------
def get_sheet_id(service, sheet_name: str):
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    for sh in meta.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("title") == sheet_name:
            return props.get("sheetId")
    return None


def find_last_ok_message_id_in_logs(service, chat_id: int, user_id: int, max_rows=2000):
    """
    Ищем последнюю запись OK в ЛОГИ для этого chat_id + user_id,
    где Text НЕ /undo (чтобы undo не отменял сам себя).
    Возвращаем message_id (как строку) или None.
    """
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_LOGS}!A:J"
    ).execute()

    rows = resp.get("values", [])
    if not rows:
        return None

    tail = rows[-max_rows:] if len(rows) > max_rows else rows

    for r in reversed(tail):
        # ожидаем A..J
        r_chat = r[1] if len(r) > 1 else ""
        r_user = r[2] if len(r) > 2 else ""
        r_mid = r[5] if len(r) > 5 else ""
        r_text = r[6] if len(r) > 6 else ""
        r_status = r[7] if len(r) > 7 else ""

        if str(r_chat) != str(chat_id):
            continue
        if str(r_user) != str(user_id):
            continue
        if str(r_status).strip().upper() != "OK":
            continue
        if str(r_text).strip().lower().startswith("/undo"):
            continue
        if not str(r_mid).strip():
            continue

        return str(r_mid).strip()

    return None


def find_row_index_by_message_id_in_ops(service, message_id: str, max_rows=3000):
    """
    Ищем строку в ОПЕРАЦИИ по колонке M (MessageID).
    Возвращаем (rowIndex0Based) для batchUpdate deleteDimension.
    """
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_OPS}!A:M"
    ).execute()

    rows = resp.get("values", [])
    if not rows:
        return None

    # rows: list of rows, each row is list of cell values
    # В Google Sheets индексация строк в batchUpdate: 0-based, endIndex is exclusive
    # range A:M includes header row if it exists. Нам нужна реальная позиция строки.
    tail = rows[-max_rows:] if len(rows) > max_rows else rows
    base_offset = len(rows) - len(tail)  # сколько строк отрезали слева

    for i in range(len(tail) - 1, -1, -1):
        r = tail[i]
        mid = r[12] if len(r) > 12 else ""  # M column index 12
        if str(mid).strip() == str(message_id).strip():
            # row number in full "rows" list (0-based)
            full_row_idx = base_offset + i
            return full_row_idx

    return None


def delete_row(service, sheet_name: str, row_index_0based: int):
    sid = get_sheet_id(service, sheet_name)
    if sid is None:
        raise RuntimeError(f"Не найден лист: {sheet_name}")

    req = {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sid,
                        "dimension": "ROWS",
                        "startIndex": row_index_0based,
                        "endIndex": row_index_0based + 1
                    }
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body=req
    ).execute()


# ---------------- routes ----------------
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

    frm = msg.get("from") or {}
    user_id = frm.get("id")
    username = frm.get("username", "")
    full_name = (str(frm.get("first_name", "")) + " " + str(frm.get("last_name", ""))).strip()

    message_id = msg.get("message_id")
    text = (msg.get("text") or "").strip()

    now_ts = time.time()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Sheets service
    try:
        service = build_sheets_service()
    except Exception as e:
        print("build_sheets_service error:", repr(e))
        send_message(chat_id, "❌ Ошибка доступа к Google Sheets (проверь GOOGLE_SA_JSON/доступ к таблице).")
        return "ok", 200

    # ---------- /start ----------
    if text.startswith("/start"):
        send_message(
            chat_id,
            "Привет! Я на связи.\nФормат:\nОБУХОВО; РАСХОД; КВАРТИРА; 1000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; комментарий\n\nКоманды:\n/undo — отменить последнюю запись"
        )
        log_action(service, now_str, chat_id, user_id, username, full_name, message_id, text, "INFO", "/start")
        return "ok", 200

    # ---------- /undo ----------
    if text.strip().lower().startswith("/undo"):
        try:
            last_mid = find_last_ok_message_id_in_logs(service, chat_id, user_id)
            if not last_mid:
                send_message(chat_id, "⚠️ Нечего отменять: нет предыдущих успешных записей.")
                log_action(service, now_str, chat_id, user_id, username, full_name, message_id, text, "ERROR", "undo: no OK logs")
                return "ok", 200

            row_idx = find_row_index_by_message_id_in_ops(service, last_mid)
            if row_idx is None:
                send_message(chat_id, "⚠️ Не нашёл строку в ОПЕРАЦИИ для отмены (MessageID не найден).")
                log_action(service, now_str, chat_id, user_id, username, full_name, message_id, text, "ERROR", f"undo: ops row not found for mid={last_mid}")
                return "ok", 200

            delete_row(service, SHEET_OPS, row_idx)
            send_message(chat_id, "✅ Отменил последнюю запись.")
            log_action(service, now_str, chat_id, user_id, username, full_name, message_id, text, "OK", f"undo: deleted mid={last_mid}")
            return "ok", 200

        except Exception as e:
            print("undo error:", repr(e))
            send_message(chat_id, f"❌ Ошибка /undo: {e}")
            log_action(service, now_str, chat_id, user_id, username, full_name, message_id, text, "ERROR", f"undo: {e}")
            return "ok", 200

    # ---------- normal flow ----------
    _cleanup_caches(now_ts)

    # Anti-dup by MessageID
    if message_id is not None:
        if message_id in _seen_message_ids:
            send_message(chat_id, "⚠️ Дубль (message_id). Уже обработано.")
            log_action(service, now_str, chat_id, user_id, username, full_name, message_id, text, "DUPLICATE", "message_id")
            return "dup message_id", 200
        _seen_message_ids[message_id] = now_ts

    # Anti-dup by content within 30s (per chat)
    norm_text = re.sub(r"\s+", " ", text).strip().lower()
    key = (chat_id, norm_text)
    if norm_text:
        last_ts = _seen_content.get(key)
        if last_ts and (now_ts - last_ts) <= CONTENT_DEDUP_WINDOW_SECONDS:
            send_message(chat_id, "⚠️ Дубль по содержимому (30 сек). Не записываю.")
            log_action(service, now_str, chat_id, user_id, username, full_name, message_id, text, "DUPLICATE", "content_30s")
            return "dup content", 200
        _seen_content[key] = now_ts

    parsed, err = validate_and_parse(text)
    if err:
        send_message(chat_id, err)
        log_action(service, now_str, chat_id, user_id, username, full_name, message_id, text, "ERROR", err)
        return "bad format", 200

    try:
        # ОПЕРАЦИИ: A..M
        row = [
            now_str,                # A DateTime
            parsed["object"],       # B Объект
            parsed["type"],         # C Тип
            parsed["article"],      # D Статья
            parsed["amount"],       # E СуммаБаза
            parsed["pay_type"],     # F СпособОплаты
            parsed["vat"],          # G НДС
            "",                     # H Категория (пусто)
            parsed["period"],       # I ПЕРИОД (YYYY-MM-1/2)
            parsed["employee"],     # J Сотрудник
            "",                     # K Статус (пусто)
            "TELEGRAM",             # L Источник
            str(message_id or ""),  # M MessageID
        ]

        append_row_with_service(service, SHEET_OPS, row)
        send_message(chat_id, "✅ Записал")
        log_action(service, now_str, chat_id, user_id, username, full_name, message_id, text, "OK", "")
    except Exception as e:
        print("append error:", repr(e))
        send_message(chat_id, f"Ошибка записи в таблицу: {e}")
        log_action(service, now_str, chat_id, user_id, username, full_name, message_id, text, "ERROR", f"append: {e}")

    return "ok", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
