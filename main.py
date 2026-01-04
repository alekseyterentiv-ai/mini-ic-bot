import os
import json
from flask import Flask, request

app = Flask(name)

@app.get("/")
def index():
    return "OK", 200

@app.post("/webhook")
def webhook():
    data = request.get_json(silent=True)
    print(json.dumps(data, ensure_ascii=False))
    return "ok", 200

if name == "main":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
