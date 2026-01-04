import os
from flask import Flask, request

app = Flask(name)

@app.route("/", methods=["GET"])
def index():
    return "OK", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print(data)
    return "ok", 200

if name == "main":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
