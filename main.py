app = Flask(__name__)

TOKEN = os.environ.get("TELEGRAM_TOKEN")   # только get
TG_API = f"https://api.telegram.org/bot{TOKEN}"

@app.get("/")
def index():
    return "OK", 200

