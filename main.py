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


def send_message(chat_id, text):
    payload = {
        "chat_id": chat_id,
        "text": text
    }
    requests.post(f"{TG_API}/sendMessage", json=payload)


@app.post("/webhook")
def webhook():
    data = request.get_json()
    if not data:
        return "ok", 200

    message = data.get("message")
    if not message:
        return "ok", 200

    chat_id = message["chat"]["id"]
    send_message(chat_id, "Я жив")

    return "ok", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
