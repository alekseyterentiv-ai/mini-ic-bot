import os
import json
from flask import Flask, request

app = Flask(name)

@app.route("/", methods=["GET"])
def index():
    return "OK", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True)
    print(json.dumps(data, ensure_ascii=False))
    return "ok", 200

if name == "main":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
