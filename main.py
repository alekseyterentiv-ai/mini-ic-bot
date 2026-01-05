app = Flask(__name__)

TOKEN = os.environ.get("TELEGRAM_TOKEN")   # только get
TG_API = f"https://api.telegram.org/bot{TOKEN}"

@app.get("/")
def index():
    return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
