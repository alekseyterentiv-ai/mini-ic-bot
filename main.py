from flask import Flask, request
import os
import json
import requests

app = Flask(__name__)

TOKEN = os.environ.get("TELEGRAM_TOKEN")
TG_API = f"https://api.telegram.org/bot{TOKEN}"
if not TOKEN:
    print("ERROR: TELEGRAM_TOKEN is not set")

@app.get("/")
def index():
    return "OK", 200


def send_message(chat_id, text, keyboard=None):
    payload = {
        "chat_id": chat_id,
        "text": text
    }
    if keyboard:
        payload["reply_markup"] = json.dumps(keyboard)

    requests.post(f"{TG_API}/sendMessage", json=payload)


@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True)

    if not data:
        return "no data", 200

    message = data.get("message")
    if not message:
        return "no message", 200

    chat_id = message["chat"]["id"]

    keyboard = {
        "keyboard": [
            [{"text": "➕ Приход"}, {"text": "➖ Расход"}],
            [{"text": "📦 Остаток"}]
        ],
        "resize_keyboard": True
    }

    send_message(chat_id, "Принял", keyboard)
    return "ok", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
