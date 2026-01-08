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

# =========================
# ENV
# =========================
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_API = f"https://api.telegram.org/bot{TOKEN}"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ").strip()
SHEET_LOGS = os.environ.get("SHEET_LOGS", "ЛОГИ").strip()

GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Webhook security (Telegram secret token header)
TELEGRAM_SECRET_TOKEN = os.environ.get("TELEGRAM_SECRET_TOKEN", "").strip()

# =========================
# СПРАВОЧНИКИ
# =========================
OBJECTS = [
    "ОКТЯБРЬСКИЙ",
    "ОБУХОВО",
    "ОДИНЦОВО",
    "ЭКИПАЖ",
    "24 СКЛАД",
    "ЯРЦЕВО",
    "ОБЩЕХОЗ",
]

# Типы как ты сказал: расход, зп, аванс, доход
TYPES = ["РАСХОД", "ЗП", "АВАНС", "ДОХОД"]

ARTICLES = [
    "ОФИС",
    "КАНЦТОВАРЫ",
    "СВЯЗЬ/ИНТЕРНЕТ",
    "1С",
    "КВАРТИРА",
    "ХОСТЕЛ",
    "ЗП НАЛ",
    "ЗП ОФИЦ",
    "СИЗ",
    "МЕД КИЖКИ",
    "ОБУЧЕНИЕ/ИНСТРУКТАЖИ",
    "ТАКСИ",
    "БИЛЕТЫ",
    "БЕНЗИН",
    "РЕМОНТ АВТО",
    "ИНСТРУМЕНТ",
    "РЕМОНТ И ОБСЛУЖИВАНИЕ",
    "УСЛУГИ СТОРОННИЕ",
    "ШТРАФЫ/ПЕНИ",
    "МАРКЕТИНГ/ПРЕДСТАВИТЕЛЬСКИЕ",
    "ПОДАРКИ",
    "СКУДЫ",
    "КРЕДИТ",
    "% ПО КРЕДИТУ",
    "КОМИССИИ БАНКА",
]

PAY_TYPES = ["НАЛ", "БЕЗНАЛ", "ЗП_ОФИЦ", "АВАНС", "ПРЕДОПЛАТА"]
VAT_VALUES = ["ДА", "НЕТ"]

# =========================
# НАСТРОЙКИ / КЕШИ
# =========================
DEDUP_TTL_SECONDS = 6 * 60 * 60
CONTENT_DEDUP_WINDOW_SECONDS = 30

_seen_message_ids = {}   # message_id -> ts
_seen_content = {}       # (chat_id, norm_text) -> ts

# /new flow state
_new_flow = {}           # chat_id -> {"step": int, "data": dict, "ts": float}
NEW_FLOW_TTL = 30 * 60   # 30 минут

_sheets_service = None
_sheet_id_cache = {}     # title -> sheetId

# =========================
# TELEGRAM HELPERS
# =========================
def kb(rows):
    return {
        "keyboard": [[{"text": x} for x in r] for r in rows],
        "resize_keyboard": True,
        "one_time_keyboard": True
    }

def send_message(chat_id: int, text: str, reply_markup=None) -> None:
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        requests.post(f"{TG_API}/sendMessage", json=payload, timeout=20)
    except Exception as e:
        print("send_message error:", repr(e))

def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()

def _cleanup_caches(now_ts: float) -> None:
    to_del = [k for k, ts in _seen_message_ids.items() if now_ts - ts > DEDUP_TTL_SECONDS]
    for k in to_del:
        _seen_message_ids.pop(k, None)

    to_del = [k for k, ts in _seen_content.items() if now_ts - ts > CONTENT_DEDUP_WINDOW_SECONDS]
    for k in to_del:
        _seen_content.pop(k, None)

def quick_help_text():
    return (
        "⚡ Быстрый ввод (/quick)\n\n"
        "Формат (9 полей через ;):\n"
        "ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; СПОСОБ; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТ\n\n"
        "Пример:\n"
        "ОБУХОВО; РАСХОД; КВАРТИРА; 35000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; январь\n\n"
        "Период: YYYY-MM-1 или YYYY-MM-2\n"
        "1=1–15, 2=16–31"
    )

# =========================
# GOOGLE SHEETS HELPERS
# =========================
def build_sheets_service():
    global _sheets_service
    if _sheets_service is not None:
        return _sheets_service

    if not GOOGLE_SA_JSON:
        raise RuntimeError("GOOGLE_SA_JSON is empty")

    sa_info = json.loads(GOOGLE_SA_JSON)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    _sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _sheets_service

def _get_sheet_id(service, title: str) -> int:
    if title in _sheet_id_cache:
        return _sheet_id_cache[title]

    meta = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(properties(sheetId,title))"
    ).execute()

    for sh in meta.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("title") == title:
            sid = int(props.get("sheetId"))
            _sheet_id_cache[title] = sid
            return sid

    raise RuntimeError(f"Sheet '{title}' not found")

def append_row(sheet_name: str, row: list):
    svc = build_sheets_service()
    svc.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=sheet_name,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"majorDimension": "ROWS", "values": [row]},
    ).execute()

def read_sheet_rows(sheet_name: str, rng: str):
    svc = build_sheets_service()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!{rng}",
        majorDimension="ROWS"
    ).execute()
    return resp.get("values", [])

def read_column(sheet_name: str, col: str):
    svc = build_sheets_service()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{sheet_name}!{col}",
        majorDimension="COLUMNS"
    ).execute()
    cols = resp.get("values", [])
    return cols[0] if cols and cols[0] else []

def delete_row(sheet_name: str, row_number_1based: int):
    svc = build_sheets_service()
    sid = _get_sheet_id(svc, sheet_name)
    start = row_number_1based - 1
    end = row_number_1based
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "requests": [
                {
                    "deleteDimension": {
                        "range": {
                            "sheetId": sid,
                            "dimension": "ROWS",
                            "startIndex": start,
                            "endIndex": end,
                        }
                    }
                }
            ]
        }
    ).execute()

# =========================
# LOGS
# =========================
def log_event(chat_id, user_id, username, full_name, message_id, text, status, error_text=""):
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
    ]
    try:
        append_row(SHEET_LOGS, row)
    except Exception as e:
        print("log_event error:", repr(e))

def get_last_written_message_id_from_logs(chat_id: int):
    rows = read_sheet_rows(SHEET_LOGS, "A:J")
    if not rows:
        return None
    for r in reversed(rows):
        try:
            r_chat = str(r[1]).strip() if len(r) > 1 else ""
            r_mid = str(r[5]).strip() if len(r) > 5 else ""
            r_status = str(r[7]).strip() if len(r) > 7 else ""
            if r_chat == str(chat_id) and r_mid and r_status == "OP_WRITE OK":
                return r_mid
        except:
            continue
    return None

def find_row_by_message_id_in_ops(target_message_id: str):
    if not target_message_id:
        return None
    col_m = read_column(SHEET_OPS, "M:M")  # MessageID column
    if not col_m:
        return None
    for idx in range(len(col_m) - 1, -1, -1):
        if str(col_m[idx]).strip() == str(target_message_id).strip():
            return idx + 1
    return None

# =========================
# VALIDATION (быстрый ввод через ;)
# =========================
def validate_and_parse(text: str):
    parts = [p.strip() for p in (text or "").split(";")]
    if len(parts) != 9:
        return None, (
            "❌ Формат: 9 полей через ;\n"
            "Пример:\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 1000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; комментарий"
        )

    object_, type_, article, amount_raw, pay_type, vat, period_raw, employee, comment = parts

    if object_ not in OBJECTS:
        return None, "❌ Объект только из списка. Используй /new (кнопки) или напиши как в справочнике."

    type_up = type_.upper()
    if type_up not in TYPES:
        return None, "❌ Тип только: РАСХОД / ЗП / АВАНС / ДОХОД"

    if article not in ARTICLES:
        return None, "❌ Статья только из списка. Используй /new (кнопки) или напиши как в справочнике."

    try:
        amt = amount_raw.replace(" ", "").replace(",", ".")
        amount = float(amt)
        if amount <= 0:
            return None, "❌ Сумма должна быть > 0"
    except:
        return None, "❌ Сумма должна быть числом"

    if pay_type not in PAY_TYPES:
        return None, "❌ Способ оплаты только: НАЛ / БЕЗНАЛ / ЗП_ОФИЦ / АВАНС / ПРЕДОПЛАТА"

    vat_up = vat.upper()
    if vat_up not in VAT_VALUES:
        return None, "❌ НДС только ДА или НЕТ"

    period_raw = period_raw.strip()
    if not re.match(r"^\d{4}-\d{2}-[12]$", period_raw):
        return None, "❌ Период только YYYY-MM-1 или YYYY-MM-2 (пример: 2026-01-1)"

    if not employee.strip():
        return None, "❌ Сотрудник не должен быть пустым"

    return {
        "object": object_,
        "type": type_up,
        "article": article,
        "amount": amount,
        "pay_type": pay_type,
        "vat": vat_up,
        "period": period_raw,
        "employee": employee.strip(),
        "comment": (comment or "").strip(),
    }, None

# =========================
# /new FLOW
# =========================
def _newflow_get(chat_id: int):
    st = _new_flow.get(chat_id)
    if not st:
        return None
    if time.time() - st.get("ts", 0) > NEW_FLOW_TTL:
        _new_flow.pop(chat_id, None)
        return None
    return st

def _newflow_set(chat_id: int, step: int, data: dict):
    _new_flow[chat_id] = {"step": step, "data": data, "ts": time.time()}

def _newflow_clear(chat_id: int):
    _new_flow.pop(chat_id, None)

def _ask_step(chat_id: int, step: int):
    if step == 1:
        send_message(chat_id, "Шаг 1/9: Выбери объект:", kb([
            ["ОКТЯБРЬСКИЙ", "ОБУХОВО", "ОДИНЦОВО"],
            ["ЭКИПАЖ", "24 СКЛАД", "ЯРЦЕВО"],
            ["ОБЩЕХОЗ"],
            ["/cancel"]
        ]))
    elif step == 2:
        send_message(chat_id, "Шаг 2/9: Выбери тип:", kb([
            ["РАСХОД", "ЗП"],
            ["АВАНС", "ДОХОД"],
            ["/back", "/cancel"]
        ]))
    elif step == 3:
        rows = []
        row = []
        for a in ARTICLES:
            row.append(a)
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append(["/back", "/cancel"])
        send_message(chat_id, "Шаг 3/9: Выбери статью:", kb(rows))
    elif step == 4:
        send_message(chat_id, "Шаг 4/9: Введи сумму (пример: 1000 или 10 000 или 1000,50):", kb([["/back", "/cancel"]]))
    elif step == 5:
        send_message(chat_id, "Шаг 5/9: Выбери способ оплаты:", kb([
            ["НАЛ", "БЕЗНАЛ"],
            ["ЗП_ОФИЦ", "АВАНС"],
            ["ПРЕДОПЛАТА"],
            ["/back", "/cancel"]
        ]))
    elif step == 6:
        send_message(chat_id, "Шаг 6/9: НДС?", kb([["ДА", "НЕТ"], ["/back", "/cancel"]]))
    elif step == 7:
        send_message(
            chat_id,
            "Шаг 7/9: Период (YYYY-MM-1 или YYYY-MM-2)\n1=1–15, 2=16–31\nПример: 2026-01-1",
            kb([["2026-01-1", "2026-01-2"], ["/back", "/cancel"]])
        )
    elif step == 8:
        send_message(chat_id, "Шаг 8/9: Введи сотрудника (например: ИВАНОВ):", kb([["/back", "/cancel"]]))
    elif step == 9:
        send_message(chat_id, "Шаг 9/9: Комментарий (можно “-”):", kb([["/back", "/cancel"]]))

def _write_operation(parsed: dict, message_id):
    # ОПЕРАЦИИ: A..N (N = Комментарий)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [
        now_str,                 # A DateTime
        parsed["object"],        # B Объект
        parsed["type"],          # C Тип
        parsed["article"],       # D Статья
        parsed["amount"],        # E СуммаБаза
        parsed["pay_type"],      # F СпособОплаты
        parsed["vat"],           # G НДС
        "",                      # H Категория
        parsed["period"],        # I ПЕРИОД
        parsed["employee"],      # J Сотрудник
        "",                      # K Статус
        "TELEGRAM",              # L Источник
        str(message_id or ""),   # M MessageID
        parsed["comment"],       # N Комментарий
    ]
    append_row(SHEET_OPS, row)

# =========================
# ROUTES
# =========================
@app.get("/")
def index():
    return "ok", 200

@app.post("/webhook")
def webhook():
    # --- Webhook security ---
    # Telegram sends header "X-Telegram-Bot-Api-Secret-Token" if you set secret_token in setWebhook
    if TELEGRAM_SECRET_TOKEN:
        got = (request.headers.get("X-Telegram-Bot-Api-Secret-Token") or "").strip()
        if got != TELEGRAM_SECRET_TOKEN:
            return "forbidden", 403

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
    full_name = (" ".join([from_user.get("first_name", ""), from_user.get("last_name", "")])).strip()

    message_id = msg.get("message_id")
    text = (msg.get("text") or "").strip()

    # ---------- /start ----------
    if text.startswith("/start"):
        send_message(
            chat_id,
            "Команды:\n"
            "/new — пошаговый ввод\n"
            "/quick — быстрый ввод (формат)\n"
            "/undo — отмена последней операции\n"
            "/cancel — отмена пошагового ввода\n"
            "/back — шаг назад (в /new)\n\n"
            + quick_help_text()
        )
        log_event(chat_id, user_id, username, full_name, message_id, text, "START OK")
        return "ok", 200

    # ---------- /quick ----------
    if text.strip().lower() == "/quick":
        send_message(chat_id, quick_help_text())
        log_event(chat_id, user_id, username, full_name, message_id, text, "QUICK OK")
        return "ok", 200

    # ---------- /new /cancel ----------
    if text.strip() == "/new":
        _newflow_set(chat_id, 1, {})
        send_message(chat_id, "🧾 Пошаговый ввод. Отвечай по шагам. /cancel — отмена.", kb([["/cancel"]]))
        _ask_step(chat_id, 1)
        log_event(chat_id, user_id, username, full_name, message_id, text, "NEW START")
        return "ok", 200

    if text.strip() == "/cancel":
        _newflow_clear(chat_id)
        send_message(chat_id, "❎ Ок, отменил ввод.", kb([["/new", "/quick"], ["/undo"]]))
        log_event(chat_id, user_id, username, full_name, message_id, text, "NEW CANCEL")
        return "ok", 200

    # ---------- /undo ----------
    if text.strip().lower() == "/undo":
        try:
            target_mid = get_last_written_message_id_from_logs(chat_id)
            if not target_mid:
                send_message(chat_id, "⚠️ Нечего отменять (в логах нет последней операции).")
                log_event(chat_id, user_id, username, full_name, message_id, text, "UNDO WARN", "no last op")
                return "ok", 200

            row_num = find_row_by_message_id_in_ops(target_mid)
            if not row_num:
                send_message(chat_id, "⚠️ Не нашёл строку в ОПЕРАЦИИ для отмены (MessageID не найден).")
                log_event(chat_id, user_id, username, full_name, message_id, text, "UNDO WARN", f"mid not found: {target_mid}")
                return "ok", 200

            delete_row(SHEET_OPS, row_num)
            send_message(chat_id, f"✅ Отменил последнюю операцию (удалил строку {row_num}).")
            log_event(chat_id, user_id, username, full_name, message_id, text, "UNDO OK", f"deleted row {row_num} mid={target_mid}")
            return "ok", 200

        except Exception as e:
            print("UNDO error:", repr(e))
            send_message(chat_id, f"❌ Ошибка /undo: {e}")
            log_event(chat_id, user_id, username, full_name, message_id, text, "UNDO ERR", str(e))
            return "ok", 200

    # ---------- Anti-dup ----------
    now_ts = time.time()
    _cleanup_caches(now_ts)

    # MessageID dedup (молча)
    if message_id is not None:
        if message_id in _seen_message_ids:
            log_event(chat_id, user_id, username, full_name, message_id, text, "DEDUP MESSAGE_ID")
            return "dup message_id", 200
        _seen_message_ids[message_id] = now_ts

    # Content dedup (с уведомлением)
    norm_text = normalize_text(text)
    if norm_text:
        key = (chat_id, norm_text)
        last_ts = _seen_content.get(key)
        if last_ts and (now_ts - last_ts) <= CONTENT_DEDUP_WINDOW_SECONDS:
            send_message(chat_id, "⚠️ Повтор (текст). Не записал.")
            log_event(chat_id, user_id, username, full_name, message_id, text, "DEDUP TEXT")
            return "dup content", 200
        _seen_content[key] = now_ts

    # ---------- /new flow processing ----------
    st = _newflow_get(chat_id)
    if st:
        step = st["step"]
        data_nf = st["data"]

        if text.strip() == "/back":
            step = max(1, step - 1)
            _newflow_set(chat_id, step, data_nf)
            _ask_step(chat_id, step)
            return "ok", 200

        if step == 1:
            if text not in OBJECTS:
                send_message(chat_id, "❌ Выбери объект кнопкой.")
                _ask_step(chat_id, 1)
                return "ok", 200
            data_nf["object"] = text
            _newflow_set(chat_id, 2, data_nf)
            _ask_step(chat_id, 2)
            return "ok", 200

        if step == 2:
            if text not in TYPES:
                send_message(chat_id, "❌ Выбери тип кнопкой.")
                _ask_step(chat_id, 2)
                return "ok", 200
            data_nf["type"] = text
            _newflow_set(chat_id, 3, data_nf)
            _ask_step(chat_id, 3)
            return "ok", 200

        if step == 3:
            if text not in ARTICLES:
                send_message(chat_id, "❌ Выбери статью кнопкой.")
                _ask_step(chat_id, 3)
                return "ok", 200
            data_nf["article"] = text
            _newflow_set(chat_id, 4, data_nf)
            _ask_step(chat_id, 4)
            return "ok", 200

        if step == 4:
            try:
                amt = text.replace(" ", "").replace(",", ".")
                amount = float(amt)
                if amount <= 0:
                    raise ValueError()
            except:
                send_message(chat_id, "❌ Сумма должна быть числом > 0. Пример: 1000 или 1000,50")
                _ask_step(chat_id, 4)
                return "ok", 200
            data_nf["amount"] = amount
            _newflow_set(chat_id, 5, data_nf)
            _ask_step(chat_id, 5)
            return "ok", 200

        if step == 5:
            if text not in PAY_TYPES:
                send_message(chat_id, "❌ Выбери способ оплаты кнопкой.")
                _ask_step(chat_id, 5)
                return "ok", 200
            data_nf["pay_type"] = text
            _newflow_set(chat_id, 6, data_nf)
            _ask_step(chat_id, 6)
            return "ok", 200

        if step == 6:
            if text not in VAT_VALUES:
                send_message(chat_id, "❌ НДС только ДА или НЕТ.")
                _ask_step(chat_id, 6)
                return "ok", 200
            data_nf["vat"] = text
            _newflow_set(chat_id, 7, data_nf)
            _ask_step(chat_id, 7)
            return "ok", 200

        if step == 7:
            if not re.match(r"^\d{4}-\d{2}-[12]$", text.strip()):
                send_message(chat_id, "❌ Период только YYYY-MM-1 или YYYY-MM-2 (пример: 2026-01-1)")
                _ask_step(chat_id, 7)
                return "ok", 200
            data_nf["period"] = text.strip()
            _newflow_set(chat_id, 8, data_nf)
            _ask_step(chat_id, 8)
            return "ok", 200

        if step == 8:
            if not text.strip():
                send_message(chat_id, "❌ Сотрудник не должен быть пустым.")
                _ask_step(chat_id, 8)
                return "ok", 200
            data_nf["employee"] = text.strip()
            _newflow_set(chat_id, 9, data_nf)
            _ask_step(chat_id, 9)
            return "ok", 200

        if step == 9:
            data_nf["comment"] = text.strip() if text.strip() else "-"

            try:
                parsed = {
                    "object": data_nf["object"],
                    "type": data_nf["type"],
                    "article": data_nf["article"],
                    "amount": data_nf["amount"],
                    "pay_type": data_nf["pay_type"],
                    "vat": data_nf["vat"],
                    "period": data_nf["period"],
                    "employee": data_nf["employee"],
                    "comment": data_nf["comment"],
                }
                _write_operation(parsed, message_id)
                send_message(chat_id, "✅ Записал")
                log_event(chat_id, user_id, username, full_name, message_id, f"/new {parsed}", "OP_WRITE OK")
            except Exception as e:
                print("append error:", repr(e))
                send_message(chat_id, f"❌ Ошибка записи: {e}")
                log_event(chat_id, user_id, username, full_name, message_id, text, "OP_WRITE ERR", str(e))

            _newflow_clear(chat_id)
            return "ok", 200

    # ---------- fast input (;) ----------
    parsed, err = validate_and_parse(text)
    if err:
        send_message(chat_id, err)
        log_event(chat_id, user_id, username, full_name, message_id, text, "VALIDATE BAD", err)
        return "bad format", 200

    try:
        _write_operation(parsed, message_id)
        send_message(chat_id, "✅ Записал")
        log_event(chat_id, user_id, username, full_name, message_id, text, "OP_WRITE OK")
    except Exception as e:
        print("append error:", repr(e))
        send_message(chat_id, f"❌ Ошибка записи: {e}")
        log_event(chat_id, user_id, username, full_name, message_id, text, "OP_WRITE ERR", str(e))

    return "ok", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)

