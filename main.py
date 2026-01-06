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
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ").strip()
SHEET_LOGS = os.environ.get("SHEET_LOGS", "ЛОГИ").strip()

GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# --- Anti-dup settings ---
DEDUP_TTL_SECONDS = 6 * 60 * 60            # MessageID защита (6 часов)
CONTENT_DEDUP_WINDOW_SECONDS = 30           # окно антидубля по тексту

_seen_message_ids = {}        # message_id -> ts
_seen_content = {}            # (chat_id, normalized_text) -> ts

# cache sheet title -> sheetId
_sheet_id_cache = {}          # title -> sheetId


def _cleanup_caches(now_ts: float) -> None:
    # чистим message_id
    to_del = [k for k, ts in _seen_message_ids.items() if now_ts - ts > DEDUP_TTL_SECONDS]
    for k in to_del:
        _seen_message_ids.pop(k, None)

    # чистим контент
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


def _get_sheet_id(service, title: str) -> int:
    # cache first
    if title in _sheet_id_cache:
        return _sheet_id_cache[title]

    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    for sh in meta.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("title") == title:
            sid = int(props.get("sheetId"))
            _sheet_id_cache[title] = sid
            return sid

    raise RuntimeError(f"Sheet '{title}' not found in spreadsheet")


def _parse_row_num_from_updated_range(updated_range: str) -> int | None:
    """
    updated_range like: 'ОПЕРАЦИИ!A348:M348' -> returns 348
    """
    if not updated_range:
        return None
    m = re.search(r"!A(\d+):", updated_range)
    if not m:
        m = re.search(r"!(?:[A-Z]+)(\d+):", updated_range)
    if not m:
        return None
    return int(m.group(1))


def append_row(sheet_name: str, row: list) -> tuple[int | None, str | None]:
    """
    Returns: (row_number, updated_range)
    """
    service = build_sheets_service()
    resp = service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=sheet_name,                # ТОЛЬКО имя листа
        valueInputOption="USER_ENTERED",
        body={"values": [row]},          # ВАЖНО: двойные скобки, иначе поедет "вбок"
    ).execute()

    updated_range = (resp.get("updates") or {}).get("updatedRange")
    row_num = _parse_row_num_from_updated_range(updated_range or "")
    return row_num, updated_range


def delete_row(sheet_title: str, row_number_1_based: int) -> None:
    """
    Deletes a row by 1-based row number in the given sheet.
    Google API uses 0-based indexes in deleteDimension: startIndex inclusive, endIndex exclusive.
    """
    if row_number_1_based <= 0:
        raise ValueError("row_number must be >= 1")

    service = build_sheets_service()
    sheet_id = _get_sheet_id(service, sheet_title)

    start_index = row_number_1_based - 1
    end_index = row_number_1_based

    body = {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_index,
                        "endIndex": end_index,
                    }
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body=body
    ).execute()


def read_last_logs(limit: int = 300) -> list[list]:
    """
    Reads last N rows from LOGS by grabbing a tail range.
    Note: Sheets API doesn't have "last rows" query; we read a wide range and take tail.
    """
    service = build_sheets_service()
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_LOGS}!A:K"
    ).execute()
    values = resp.get("values", [])
    return values[-limit:]


def log_action(chat_id, user_id, username, full_name, message_id, text, status, error_text, action, ops_row_num=None):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [
        now_str,                         # A Datetime
        str(chat_id or ""),              # B ChatID
        str(user_id or ""),              # C UserID
        str(username or ""),             # D Username
        str(full_name or ""),            # E FullName
        str(message_id or ""),           # F MessageID
        str(text or ""),                 # G Text
        str(status or ""),               # H Status
        str(error_text or ""),           # I ErrorText
        "TELEGRAM",                      # J Source
        str(ops_row_num or ""),          # K OpsRow (номер строки в ОПЕРАЦИИ)
    ]
    try:
        append_row(SHEET_LOGS, row)
    except Exception as e:
        print("log_action error:", repr(e))


def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def validate_and_parse(text: str):
    # ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; СПОСОБ; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТАРИЙ
    parts = [p.strip() for p in (text or "").split(";")]
    if len(parts) != 9:
        return None, (
            "❌ Ошибка формата: должно быть 9 полей через ;\n"
            "Пример:\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 1000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; тест"
        )

    object_, type_, article, amount_raw, pay_type, vat, period_raw, employee, comment = parts

    if not object_ or not type_ or not article or not amount_raw:
        return None, "❌ Не хватает обязательных полей: ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА"

    # Типы: РАСХОД, ЗП, АВАНС, ДОХОД
    type_up = type_.strip().upper()
    allowed_types = {"РАСХОД", "ЗП", "АВАНС", "ДОХОД"}
    if type_up not in allowed_types:
        return None, "❌ Тип должен быть: РАСХОД / ЗП / АВАНС / ДОХОД"

    # Сумма
    try:
        amt = amount_raw.replace(" ", "").replace(",", ".")
        amount = float(amt)
        if amount <= 0:
            return None, "❌ Сумма должна быть больше 0"
    except:
        return None, "❌ Сумма должна быть числом"

    # НДС
    vat_up = vat.strip().upper()
    if vat_up not in ("ДА", "НЕТ"):
        return None, "❌ НДС только ДА или НЕТ"

    # Период: YYYY-MM-1 или YYYY-MM-2 (это НЕ дата)
    if not re.match(r"^\d{4}-\d{2}-[12]$", period_raw):
        return None, "❌ Период только YYYY-MM-1 или YYYY-MM-2"
    # доп. проверка месяца 01..12
    try:
        mm = int(period_raw[5:7])
        if mm < 1 or mm > 12:
            return None, "❌ Месяц в периоде должен быть 01..12"
    except:
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


def find_last_ok_op_for_chat(chat_id: int) -> dict | None:
    """
    Finds last LOGS row where:
    - ChatID matches
    - Status == OK
    - Text is not command
    - OpsRow exists
    Returns dict with ops_row, message_id
    """
    rows = read_last_logs(limit=400)

    # Expected LOGS columns:
    # A Datetime | B ChatID | C UserID | D Username | E FullName | F MessageID | G Text | H Status | I ErrorText | J Source | K OpsRow
    for r in reversed(rows):
        try:
            if len(r) < 11:
                continue
            r_chat = r[1]
            r_msgid = r[5] if len(r) > 5 else ""
            r_text = r[6] if len(r) > 6 else ""
            r_status = r[7] if len(r) > 7 else ""
            r_opsrow = r[10] if len(r) > 10 else ""

            if str(r_chat) != str(chat_id):
                continue
            if (r_status or "").upper() != "OK":
                continue
            if (r_text or "").strip().startswith("/"):
                continue
            if not str(r_opsrow).strip():
                continue

            return {
                "ops_row": int(str(r_opsrow).strip()),
                "message_id": str(r_msgid or "").strip(),
                "text": str(r_text or "").strip(),
            }
        except Exception:
            continue

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
    from_user = msg.get("from") or {}

    chat_id = chat.get("id")
    if not chat_id:
        return "no chat", 200

    user_id = from_user.get("id")
    username = from_user.get("username", "")
    full_name = " ".join([from_user.get("first_name", ""), from_user.get("last_name", "")]).strip()

    message_id = msg.get("message_id")
    text = (msg.get("text") or "").strip()

    # /start
    if text.startswith("/start"):
        send_message(
            chat_id,
            "Привет! Я на связи.\nФормат:\nОБУХОВО; РАСХОД; КВАРТИРА; 1000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; комментарий\n\n"
            "Типы: РАСХОД / ЗП / АВАНС / ДОХОД\n"
            "Команды: /undo",
        )
        log_action(chat_id, user_id, username, full_name, message_id, text, "OK", "", "START")
        return "ok", 200

    now_ts = time.time()
    _cleanup_caches(now_ts)

    # --- Anti-dup by MessageID ---
    if message_id is not None:
        if message_id in _seen_message_ids:
            send_message(chat_id, "⚠️ Повтор (MessageID). Не записал.")
            log_action(chat_id, user_id, username, full_name, message_id, text, "DUP_ID", "", "DEDUP")
            return "dup message_id", 200
        _seen_message_ids[message_id] = now_ts

    # --- Anti-dup by content within window (per chat) ---
    norm_text = normalize_text(text)
    if norm_text:
        key = (chat_id, norm_text)
        last_ts = _seen_content.get(key)
        if last_ts and (now_ts - last_ts) <= CONTENT_DEDUP_WINDOW_SECONDS:
            send_message(chat_id, "⚠️ Повтор (текст). Не записал.")
            log_action(chat_id, user_id, username, full_name, message_id, text, "DUP_TEXT", "", "DEDUP")
            return "dup content", 200
        _seen_content[key] = now_ts

    # /undo
    if norm_text == "/undo":
        try:
            last = find_last_ok_op_for_chat(chat_id)
            if not last:
                send_message(chat_id, "⚠️ Нет последней операции для отмены (в логах не найдено).")
                log_action(chat_id, user_id, username, full_name, message_id, text, "WARN", "no last op", "UNDO")
                return "ok", 200

            ops_row = last["ops_row"]
            delete_row(SHEET_OPS, ops_row)

            send_message(chat_id, f"✅ Отменил последнюю операцию (удалил строку {ops_row}).")
            log_action(chat_id, user_id, username, full_name, message_id, text, "OK", "", "UNDO", ops_row_num=ops_row)
            return "ok", 200

        except Exception as e:
            print("undo error:", repr(e))
            send_message(chat_id, f"❌ Ошибка /undo: {e}")
            log_action(chat_id, user_id, username, full_name, message_id, text, "BAD", str(e), "UNDO")
            return "ok", 200

    parsed, err = validate_and_parse(text)
    if err:
        send_message(chat_id, err)
        log_action(chat_id, user_id, username, full_name, message_id, text, "BAD", err, "VALIDATE")
        return "bad format", 200

    try:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        row = [
            now_str,                 # A DateTime
            parsed["object"],        # B Объект
            parsed["type"],          # C Тип (РАСХОД/ЗП/АВАНС/ДОХОД)
            parsed["article"],       # D Статья
            parsed["amount"],        # E СуммаБаза
            parsed["pay_type"],      # F СпособОплаты
            parsed["vat"],           # G НДС
            "",                      # H Категория (пусто)
            parsed["period"],        # I ПЕРИОД (YYYY-MM-1/2)
            parsed["employee"],      # J Сотрудник
            "",                      # K Статус (пусто)
            "TELEGRAM",              # L Источник
            str(message_id or ""),   # M MessageID
            parsed["comment"],       # N Комментарий (если у вас есть этот столбец; если нет — убери строку)
        ]

        ops_row_num, updated_range = append_row(SHEET_OPS, row)

        send_message(chat_id, "✅ Записал")
        log_action(
            chat_id, user_id, username, full_name, message_id, text,
            "OK", "", "OP_WRITE",
            ops_row_num=ops_row_num
        )

    except Exception as e:
        print("append error:", repr(e))
        send_message(chat_id, f"❌ Ошибка записи в таблицу: {e}")
        log_action(chat_id, user_id, username, full_name, message_id, text, "BAD", str(e), "OP_WRITE")

    return "ok", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
