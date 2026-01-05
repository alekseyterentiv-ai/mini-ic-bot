from flask import Flask, request
import json

app = Flask(__name__)

@app.get("/")
def index():
    return "OK", 200

@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True)
    print(json.dumps(data, ensure_ascii=False))
    return "ok", 200
