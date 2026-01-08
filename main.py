from flask import Flask, request
import os, json, time, re, requests
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = Flask(__name__)

# ========= ENV =========
TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TG_API = f"https://api.telegram.org/bot{TOKEN}"

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
SHEET_OPS = os.environ.get("SHEET_OPS", "ОПЕРАЦИИ").strip()
SHEET_LOGS = os.environ.get("SHEET_LOGS", "ЛОГИ").strip()

GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "").strip()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ========= СПРАВОЧНИКИ =========
OBJECTS = ["ОКТЯБРЬСКИЙ","ОБУХОВО","ОДИНЦОВО","ЭКИПАЖ","24 СКЛАД","ЯРЦЕВО","ОБЩЕХОЗ"]
TYPES = ["РАСХОД","ЗП","АВАНС","ДОХОД"]
ARTICLES = [
    "ОФИС","КАНЦТОВАРЫ","СВЯЗЬ/ИНТЕРНЕТ","1С","КВАРТИРА","ХОСТЕЛ",
    "ЗП НАЛ","ЗП ОФИЦ","СИЗ","МЕД КИЖКИ","ОБУЧЕНИЕ/ИНСТРУКТАЖИ",
    "ТАКСИ","БИЛЕТЫ","БЕНЗИН","РЕМОНТ АВТО","ИНСТРУМЕНТ",
    "РЕМОНТ И ОБСЛУЖИВАНИЕ","УСЛУГИ СТОРОННИЕ","ШТРАФЫ/ПЕНИ",
    "МАРКЕТИНГ/ПРЕДСТАВИТЕЛЬСКИЕ","ПОДАРКИ","СКУДЫ","КРЕДИТ",
    "% ПО КРЕДИТУ","КОМИССИИ БАНКА"
]
PAY_TYPES = ["НАЛ","БЕЗНАЛ","ЗП_ОФИЦ","АВАНС","ПРЕДОПЛАТА"]
VAT_VALUES = ["ДА","НЕТ"]

# ========= КЭШИ =========
_seen_message_ids = {}
_seen_content = {}

_new_flow = {}
NEW_FLOW_TTL = 1800

_svc = None
_sheet_ids = {}

# ========= HELPERS =========
def send(chat_id, text, kb=None):
    data = {"chat_id": chat_id, "text": text}
    if kb:
        data["reply_markup"] = kb
    requests.post(f"{TG_API}/sendMessage", json=data, timeout=15)

def keyboard(rows):
    return {"keyboard":[[{"text":x} for x in r] for r in rows], "resize_keyboard":True}

def sheets():
    global _svc
    if _svc: return _svc
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_SA_JSON), scopes=SCOPES
    )
    _svc = build("sheets","v4",credentials=creds,cache_discovery=False)
    return _svc

def append(sheet, row):
    sheets().spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=sheet,
        valueInputOption="USER_ENTERED",
        body={"values":[row]}
    ).execute()

def log(chat_id, mid, text, status, err=""):
    append(SHEET_LOGS, [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        chat_id,
        mid,
        text,
        status,
        err,
        "TELEGRAM"
    ])

# ========= VALIDATE QUICK =========
def parse_quick(text):
    p = [x.strip() for x in text.split(";")]
    if len(p)!=9:
        return None,"❌ Нужно 9 полей через ;"
    o,t,a,s,pt,v,per,emp,c = p
    if o not in OBJECTS: return None,"❌ Объект не найден"
    if t not in TYPES: return None,"❌ Тип неверный"
    if a not in ARTICLES: return None,"❌ Статья неверная"
    try:
        s=float(s.replace(",",".")); assert s>0
    except: return None,"❌ Сумма ошибка"
    if pt not in PAY_TYPES: return None,"❌ Способ оплаты ошибка"
    if v not in VAT_VALUES: return None,"❌ НДС только ДА/НЕТ"
    if not re.match(r"^\d{4}-\d{2}-[12]$",per): return None,"❌ Период формат"
    if not emp: return None,"❌ Сотрудник пуст"
    return {
        "object":o,"type":t,"article":a,"amount":s,"pay_type":pt,
        "vat":v,"period":per,"employee":emp,"comment":c
    },None

def write_op(p, mid):
    append(SHEET_OPS, [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        p["object"],p["type"],p["article"],p["amount"],
        p["pay_type"],p["vat"],"",p["period"],p["employee"],
        "","TELEGRAM",mid or "",p["comment"]
    ])

# ========= ROUTES =========
@app.post("/webhook")
def webhook():
    msg = (request.get_json() or {}).get("message")
    if not msg: return "ok",200

    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()
    mid = msg.get("message_id")

    # dedup
    if mid in _seen_message_ids:
        return "ok",200
    _seen_message_ids[mid]=time.time()

    if text=="/start":
        send(chat_id,
            "Команды:\n"
            "/new — пошаговый ввод\n"
            "/quick — быстрый ввод\n"
            "/undo — отмена"
        )
        return "ok",200

    if text=="/quick":
        send(chat_id,
            "⚡ Быстрый ввод\n"
            "Формат:\n"
            "ОБЪЕКТ; ТИП; СТАТЬЯ; СУММА; СПОСОБ; НДС; ПЕРИОД; СОТРУДНИК; КОММЕНТ\n\n"
            "Пример:\n"
            "ОБУХОВО; РАСХОД; КВАРТИРА; 35000; НАЛ; НЕТ; 2026-01-1; ИВАНОВ; январь"
        )
        return "ok",200

    if text.startswith("/"):
        send(chat_id,"❌ Неизвестная команда")
        return "ok",200

    parsed,err=parse_quick(text)
    if err:
        send(chat_id,err)
        log(chat_id,mid,text,"BAD",err)
        return "ok",200

    write_op(parsed,mid)
    send(chat_id,"✅ Записал (quick)")
    log(chat_id,mid,text,"OK")
    return "ok",200

@app.get("/")
def index(): return "ok",200
