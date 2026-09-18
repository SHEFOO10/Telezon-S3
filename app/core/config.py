import os

import uvicorn
from dotenv import load_dotenv

load_dotenv()

logger = uvicorn.logging.logging.getLogger("uvicorn")


PROJECT_NAME = os.getenv("PROJECT_NAME", "Telezon-S3")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 8000))

SECRET_KEY = os.getenv("SECRET_KEY", "default_secret_key")

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")

DATABASE_NAME = os.getenv("DATABASE_NAME")

if MONGO_USER and MONGO_PASSWORD:
    AUTH_URL = f"{MONGO_USER}:{MONGO_PASSWORD}@"
else:
    AUTH_URL = ""

DATABASE_URL = (
    os.getenv("DATABASE_URL") or f"mongodb://{AUTH_URL}{MONGO_HOST}:{MONGO_PORT}"
)

API_KEY = os.getenv("API_KEY")

TOKEN = os.getenv("BOT_TOKEN")
CID = os.getenv("CID")
if CID:
    CID = CID.strip().strip("'\"")

TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
if TELEGRAM_API_ID:
    TELEGRAM_API_ID = TELEGRAM_API_ID.strip().strip("'\"")

TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
if TELEGRAM_API_HASH:
    TELEGRAM_API_HASH = TELEGRAM_API_HASH.strip().strip("'\"")

SESSION_STRING = os.getenv("SESSION_STRING")
if SESSION_STRING:
    SESSION_STRING = SESSION_STRING.strip().strip("'\"")

INITIAL_ADMIN_USER = os.getenv("INITIAL_ADMIN_USER")
INITIAL_ADMIN_PASSWORD = os.getenv("INITIAL_ADMIN_PASSWORD")

# Multipart storage mode: "diskless" (default, direct Telegram part streaming) or "assembled" (temp disk assembly)
MULTIPART_MODE = os.getenv("MULTIPART_MODE", "diskless").strip().lower()

