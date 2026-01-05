from flask import Flask, request
import os
import json
import requests

app = Flask(__name__)

TOKEN = os.environ.get("TELEGRAM_TOKEN")
TG_API = f"https://api.telegram.org/bot{TOKEN}"

@app.get("/")
def index():
    return "OK", 200

def send_message(chat_id: int, text: str):
    if not TOKEN:
        print("ERROR: TELEGRAM_TOKEN is not set")
        return
    r = requests.post(
        f"{TG_API}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=10
    )
    print("sendMessage:", r.status_code, r.text)

@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True) or {}
    print("update:", json.dumps(data, ensure_ascii=False))

    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return "ok", 200

    chat_id = msg["chat"]["id"]
    text = (msg.get("text") or "").strip()

    if text == "/start":
        send_message(chat_id, "Привет! Я на связи. Напиши: приход 1000 или расход 500")
    else:
        send_message(chat_id, f"Принял: {text}")

    return "ok", 200
