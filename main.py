import os
import json
from flask import Flask, request

app = Flask(name)

@app.route("/", methods=["GET"])
def index():
    return "OK", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print(json.dumps(data, ensure_ascii=False))
    return "ok", 200
