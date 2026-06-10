import telebot
from telebot import types
import json
import os
import random
from flask import Flask, Response
import threading
import queue
import requests
import re
import html
import phonenumbers
import pycountry
import time
import hashlib
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
from contextlib import contextmanager

# ---------------- CONFIG / LOGGING ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Environment Variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = 8195360535
PANEL_USERNAME = os.getenv("PANEL_USERNAME") 
PANEL_PASSWORD = os.getenv("PANEL_PASSWORD")

bot = telebot.TeleBot(BOT_TOKEN)

DATA_FILE = "bot_data.json"
NUMBERS_DIR = "numbers"
DB_FILE = "otp_data.db"

os.makedirs(NUMBERS_DIR, exist_ok=True)

# API Config
LOGIN_URL = "https://flysms.net/signin"
XHR_URL = "https://flysms.net/client/res/data_smscdr.php?fdate1=2026-06-10%2000:00:00&fdate2=2027-06-10%2023:59:59&frange=&fnum=&fcli=&fgdate=&fgmonth=&fgrange=&fgnumber=&fgcli=&fg=0&sEcho=1&iColumns=7&sColumns=%2C%2C%2C%2C%2C%2C&iDisplayStart=0&iDisplayLength=25&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&mDataProp_2=2&sSearch_2=&bRegex_2=false&bSearchable_2=true&bSortable_2=true&mDataProp_3=3&sSearch_3=&bRegex_3=false&bSearchable_3=true&bSortable_3=true&mDataProp_4=4&sSearch_4=&bRegex_4=false&bSearchable_4=true&bSortable_4=true&mDataProp_5=5&sSearch_5=&bRegex_5=false&bSearchable_5=true&bSortable_5=true&mDataProp_6=6&sSearch_6=&bRegex_6=false&bSearchable_6=true&bSortable_6=true&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=desc&iSortingCols=1&_=1781106704547" 

OTP_GROUP_IDS = ["-1003462043194"]
AUTO_DELETE_MINUTES = 0  # 0 means disabled

CHANNEL_LINK = "https://whatsapp.com/channel/0029Va5XJaU6xCSHlSwIXH1P"
BACKUP = "https://t.me/VASUHUB"
DEVELOPER_ID = "@uxotpbot"
CODE_GROUP = "https://t.me/+SDPuI2Ud62RkN2Jl"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://flysms.net/login"
}
AJAX_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://flysms.net/client/SMSDashboard"
}

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

# ---------------- DATA STORAGE ----------------
data = {}
numbers_by_country = {}
current_country = None
user_messages = {}
user_current_country = {}
temp_uploads = {}
user_numbers = {}

MAX_SEEN = 200000
seen_messages = set()
seen_order = deque()

# Separate queues for different operations
group_message_queue = queue.Queue()
personal_message_queue = queue.Queue()
otp_processing_queue = queue.Queue()

# ThreadPool configs
MAX_WORKERS_GROUP = 8
MAX_WORKERS_PERSONAL = 10
SEND_TIMEOUT = 8

active_users = set()
REQUIRED_CHANNELS = ["@ddxotp", "@vasuhub", "@Nokosxotps", "@Uxotp"]

# Service name mappings
SERVICE_CODES = {
    "whatsapp": "WA", "WhatsApp": "WA", "WHATSAPP": "WA",
    "telegram": "TG", "Telegram": "TG", "TELEGRAM": "TG",
    "instagram": "IG", "Instagram": "IG", "INSTAGRAM": "IG",
    "facebook": "FB", "Facebook": "FB", "FACEBOOK": "FB",
    "twitter": "TW", "Twitter": "TW", "TWITTER": "TW",
    "google": "GO", "Google": "GO", "GOOGLE": "GO",
    "amazon": "AZ", "Amazon": "AZ", "AMAZON": "AZ",
    "snapchat": "SC", "Snapchat": "SC", "SNAPCHAT": "SC",
    "tiktok": "TT", "TikTok": "TT", "TIKTOK": "TT",
    "linkedin": "LI", "LinkedIn": "LI", "LINKEDIN": "LI",
    "uber": "UB", "Uber": "UB", "UBER": "UB",
    "paypal": "PP", "PayPal": "PP", "PAYPAL": "PP",
}

# ---------------- SQLITE DATABASE ----------------
def init_database():
    """Initialize SQLite database with required tables"""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    
    # OTP records table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS otp_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash_id TEXT UNIQUE NOT NULL,
            number TEXT NOT NULL,
            sender TEXT,
            message TEXT,
            otp_code TEXT,
            country TEXT,
            timestamp TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_number ON otp_records(number)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON otp_records(timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash ON otp_records(hash_id)')
    
    # User assignments table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            number TEXT NOT NULL,
            country TEXT,
            assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_chat ON user_assignments(chat_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_number_assign ON user_assignments(number)')
    
    # Active users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS active_users (
            chat_id INTEGER PRIMARY KEY,
            username TEXT,
            first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_active DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("✅ Database initialized")

@contextmanager
def get_db():
    """Context manager for database connections"""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def save_otp_to_db(record, hash_id):
    """Save OTP record to database"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO otp_records 
                (hash_id, number, sender, message, otp_code, country, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                hash_id,
                record.get("num", ""),
                record.get("cli", ""),
                record.get("message", ""),
                record.get("otp", ""),
                record.get("country", ""),
                record.get("dt", "")
            ))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to save OTP to DB: {e}")
        return False

def get_past_otps(number, limit=10):
    """Get past OTP records for a number"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM otp_records 
                WHERE number = ? 
                ORDER BY created_at DESC 
                LIMIT ?
            ''', (number, limit))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Failed to fetch past OTPs: {e}")
        return []

def save_user_assignment(chat_id, numbers, country):
    """Save user number assignments - supports multiple numbers"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM user_assignments WHERE chat_id = ?', (chat_id,))
            for number in numbers:
                cursor.execute('''
                    INSERT INTO user_assignments (chat_id, number, country)
                    VALUES (?, ?, ?)
                ''', (chat_id, number, country))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to save user assignment: {e}")

def get_user_numbers(chat_id):
    """Get all numbers assigned to a user"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT number FROM user_assignments WHERE chat_id = ?', (chat_id,))
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Failed to get user numbers: {e}")
        return []

def update_active_user(chat_id, username=None):
    """Update active user record"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO active_users (chat_id, username, last_active)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(chat_id) DO UPDATE SET
                    username = excluded.username,
                    last_active = CURRENT_TIMESTAMP
            ''', (chat_id, username))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to update active user: {e}")

def get_active_user_count():
    """Get count of active users"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM active_users')
            return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Failed to get user count: {e}")
        return 0

def get_all_active_users():
    """Get all active user chat IDs"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT chat_id FROM active_users')
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Failed to get active users: {e}")
        return []

# Initialize database
init_database()

# ---------------- DATA FUNCTIONS ----------------
def load_data():
    global data, numbers_by_country, current_country, OTP_GROUP_IDS, AUTO_DELETE_MINUTES
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                numbers_by_country = data.get("numbers_by_country", {}) or {}
                current_country = data.get("current_country")
                OTP_GROUP_IDS = data.get("otp_groups", ["-1003672667505"])
                AUTO_DELETE_MINUTES = data.get("auto_delete_minutes", 0)
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            data = {"numbers_by_country": {}, "current_country": None, "otp_groups": ["-1003672667505"], "auto_delete_minutes": 0}
            numbers_by_country = {}
            current_country = None
            OTP_GROUP_IDS = ["-1003672667505"]
            AUTO_DELETE_MINUTES = 0
    else:
        data = {"numbers_by_country": {}, "current_country": None, "otp_groups": ["-1003672667505"], "auto_delete_minutes": 0}
        numbers_by_country = {}
        current_country = None
        OTP_GROUP_IDS = ["-1003672667505"]
        AUTO_DELETE_MINUTES = 0

def save_data():
    data["numbers_by_country"] = numbers_by_country
    data["current_country"] = current_country
    data["otp_groups"] = OTP_GROUP_IDS
    data["auto_delete_minutes"] = AUTO_DELETE_MINUTES
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save data: {e}")

load_data()

# ---------------- FLASK ----------------
app = Flask(__name__)

@app.route("/")
def index():
    return "Bot is running"

@app.route("/health")
def health():
    return Response("OK", status=200)

@app.route("/stats")
def stats():
    user_count = get_active_user_count()
    return Response(f"Active Users: {user_count}", status=200)

def run_flask():
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

# ---------------- TELEGRAM SENDER (PARALLEL) ----------------
def _send_single(chat_id, payload):
    payload_local = payload.copy()
    payload_local["chat_id"] = chat_id
    try:
        r = session.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", 
                        data=payload_local, timeout=SEND_TIMEOUT)
        return chat_id, r.status_code
    except Exception as e:
        logger.debug(f"Error sending to {chat_id}: {e}")
        return chat_id, None

def send_to_telegram(msg, chat_ids, kb=None):
    payload = {
        "text": msg[:3900],
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    if kb:
        try:
            payload["reply_markup"] = json.dumps(kb.to_dict())
        except Exception:
            pass

    results = {}
    if not chat_ids:
        return results
    
    workers = min(MAX_WORKERS_GROUP, max(1, len(chat_ids)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_send_single, cid, payload): cid for cid in chat_ids}
        for fut in as_completed(futures):
            cid = futures[fut]
            try:
                _, status = fut.result()
                results[cid] = status
            except Exception as e:
                logger.debug(f"Send exception for {cid}: {e}")
                results[cid] = None
    return results

# ---------------- MESSAGE WORKERS ----------------
def group_sender_worker():
    """Dedicated worker for group messages"""
    logger.info("🚀 Group sender worker started")
    while True:
        try:
            item = group_message_queue.get()
            msg, chat_ids, kb = item
            send_to_telegram(msg, chat_ids, kb)
        except Exception as e:
            logger.error(f"Group sender error: {e}")
        finally:
            group_message_queue.task_done()
        time.sleep(0.03)

def personal_sender_worker():
    """Dedicated worker for personal messages"""
    logger.info("🚀 Personal sender worker started")
    while True:
        try:
            item = personal_message_queue.get()
            msg, chat_id = item
            send_to_telegram(msg, [chat_id])
        except Exception as e:
            logger.error(f"Personal sender error: {e}")
        finally:
            personal_message_queue.task_done()
        time.sleep(0.02)

def delete_message_safe(chat_id, message_id):
    """Safely delete a message"""
    try:
        bot.delete_message(chat_id, message_id)
        logger.info(f"🗑️ Auto-deleted message {message_id} from {chat_id}")
    except Exception as e:
        logger.debug(f"Failed to delete message {message_id}: {e}")

def otp_processor_worker():
    """Dedicated worker for processing OTP records"""
    logger.info("🚀 OTP processor worker started")
    while True:
        try:
            record = otp_processing_queue.get()

            hash_id = record.get("hash_id")
            save_otp_to_db(record, hash_id)

            msg_group, number = format_message(record, personal=False)
            otp = record.get("otp") or extract_otp(record.get("message", ""))

            # Premium emoji keyboard
            keyboard = {
                "inline_keyboard": [
                    *([[{"text": f"🔑 {otp}", "callback_data": f"copy_{otp}", "icon_custom_emoji_id": "5382357040008021292"}]] if otp else []),
                    [{"text": "View Full", "callback_data": f"fullsms_{hash_id}", "icon_custom_emoji_id": "6125390694363175728"}],
                    [
                        {"text": "Panel", "url": f"https://t.me/{DEVELOPER_ID.lstrip('@')}", "icon_custom_emoji_id": "5330237710655306682"},
                        {"text": "Channel", "url": CHANNEL_LINK, "icon_custom_emoji_id": "6125390694363175728"}
                    ]
                ]
            }

            if OTP_GROUP_IDS:
                for group_id in OTP_GROUP_IDS:
                    try:
                        sent_msg = bot.send_message(
                            group_id,
                            msg_group,
                            reply_markup=json.dumps(keyboard),
                            parse_mode="HTML"
                        )
                        if AUTO_DELETE_MINUTES > 0:
                            threading.Timer(
                                AUTO_DELETE_MINUTES * 60,
                                delete_message_safe,
                                args=(group_id, sent_msg.message_id)
                            ).start()
                    except Exception as e:
                        logger.error(f"Failed to send to group {group_id}: {e}")

            assigned_users = []
            try:
                with get_db() as conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT DISTINCT chat_id FROM user_assignments WHERE number = ?', (number,))
                    assigned_users = [row[0] for row in cursor.fetchall()]
            except Exception as e:
                logger.error(f"Failed to get assigned users: {e}")

            for chat_id in assigned_users:
                msg_personal, _ = format_message(record, personal=True)
                personal_message_queue.put((msg_personal, chat_id))

        except Exception as e:
            logger.error(f"OTP processor error: {e}")
        finally:
            otp_processing_queue.task_done()
        time.sleep(0.01)

        
# ---------------- HELPER FUNCTIONS ----------------
EXTRA_CODES = {"Kosovo": "XK"}

def country_to_flag(country_name: str) -> str:
    code = EXTRA_CODES.get(country_name)
    if not code:
        try:
            country = pycountry.countries.lookup(country_name)
            code = country.alpha_2
        except LookupError:
            return ""
    return "".join(chr(127397 + ord(c)) for c in code.upper())

def get_country_code(country_name: str) -> str:
    """Get 2-letter country code"""
    code = EXTRA_CODES.get(country_name)
    if not code:
        try:
            country = pycountry.countries.lookup(country_name)
            code = country.alpha_2
        except LookupError:
            return country_name[:2].upper()
    return code.upper()

def get_service_code(sender: str) -> str:
    """Convert service name to short code"""
    for service, code in SERVICE_CODES.items():
        if service.lower() in sender.lower():
            return code
    return sender[:2].upper() if len(sender) >= 2 else sender.upper()

def extract_otp(message: str) -> str | None:
    text = message.strip()
    m = re.search(r"(?:otp|code|pin|password|verification|verif)[^\d]{0,8}([0-9][0-9\-\s]{2,10}[0-9])", text, re.I)
    if m:
        cand = re.sub(r"\D", "", m.group(1))
        if 3 <= len(cand) <= 8 and not (1900 <= int(cand) <= 2099):
            return cand

    m2 = re.search(r"([0-9][0-9\-\s]{2,10}[0-9])[^\w]{0,8}(?:otp|code|pin|password|verification|verif)", text, re.I)
    if m2:
        cand = re.sub(r"\D", "", m2.group(1))
        if 3 <= len(cand) <= 8 and not (1900 <= int(cand) <= 2099):
            return cand

    generic = re.findall(r"\b[0-9][0-9\-\s]{2,7}[0-9]\b", text)
    for g in generic:
        cand = re.sub(r"\D", "", g)
        if 3 <= len(cand) <= 8 and not (1900 <= int(cand) <= 2099):
            return cand

    return None

def mask_number(number: str) -> str:
    """Mask number to look less like a phone number"""
    if len(number) <= 4:
        return number
    return f"{number[:2]}DDX{number[-4:]}"

# ---------------- FLAG OVERRIDE SYSTEM ----------------
flag_overrides = {}

def load_flag_overrides():
    global flag_overrides
    flag_overrides = data.get("flag_overrides", {})

def save_flag_overrides():
    data["flag_overrides"] = flag_overrides
    save_data()

load_flag_overrides()

def get_flag(country_name: str) -> str:
    code = EXTRA_CODES.get(country_name)
    if not code:
        try:
            country = pycountry.countries.lookup(country_name)
            code = country.alpha_2
        except LookupError:
            return ""
    code = code.upper()
    regular_flag = "".join(chr(127397 + ord(c)) for c in code)
    emoji_id = flag_overrides.get(code)
    if emoji_id:
        return f'<tg-emoji emoji-id="{emoji_id}">{regular_flag}</tg-emoji>'
    return regular_flag

@bot.message_handler(commands=["addflag"])
def add_flag(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    parts = message.text.strip().split()
    if len(parts) != 3:
        return bot.reply_to(message, "❌ Usage: <code>/addflag IN 5222300011366200403</code>", parse_mode="HTML")
    _, code, emoji_id = parts
    code = code.upper()
    try:
        if code not in EXTRA_CODES.values():
            pycountry.countries.lookup(code)
    except LookupError:
        return bot.reply_to(message, f"❌ Unknown country code: <code>{code}</code>", parse_mode="HTML")
    flag_overrides[code] = emoji_id
    save_flag_overrides()
    regular_flag = "".join(chr(127397 + ord(c)) for c in code)
    preview = f'<tg-emoji emoji-id="{emoji_id}">{regular_flag}</tg-emoji>'
    bot.reply_to(message, f"✅ <b>Flag set!</b>\n{preview} <b>{code}</b> → <code>{emoji_id}</code>", parse_mode="HTML")

@bot.message_handler(commands=["removeflag"])
def remove_flag(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    parts = message.text.strip().split()
    if len(parts) != 2:
        return bot.reply_to(message, "❌ Usage: <code>/removeflag IN</code>", parse_mode="HTML")
    code = parts[1].upper()
    if code in flag_overrides:
        del flag_overrides[code]
        save_flag_overrides()
        bot.reply_to(message, f"✅ Removed premium flag for <b>{code}</b>.", parse_mode="HTML")
    else:
        bot.reply_to(message, f"❌ No override for <b>{code}</b>.", parse_mode="HTML")

@bot.message_handler(commands=["listflags"])
def list_flags(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    if not flag_overrides:
        return bot.reply_to(message, "📭 No premium flags set.")
    text = "🏳 <b>Premium Flag Overrides:</b>\n\n"
    for code, emoji_id in sorted(flag_overrides.items()):
        regular_flag = "".join(chr(127397 + ord(c)) for c in code)
        preview = f'<tg-emoji emoji-id="{emoji_id}">{regular_flag}</tg-emoji>'
        text += f"{preview} <b>{code}</b> → <code>{emoji_id}</code>\n"
    bot.reply_to(message, text, parse_mode="HTML")

def format_message(record, personal=False):
    number = record.get("num") or "Unknown"
    sender = record.get("cli") or "Unknown"
    message = record.get("message") or ""
    country = record.get("country") or "Unknown"

    flag = get_flag(country)
    country_code = get_country_code(country)
    service_code = get_service_code(sender)
    masked = mask_number(number)
    otp = record.get("otp") or extract_otp(message) or "❓"

    sender_lower = sender.lower()
    if "whatsapp" in sender_lower:
        service_emoji = '<tg-emoji emoji-id="5334998226636390258">📱</tg-emoji>'
    elif "telegram" in sender_lower:
        service_emoji = '<tg-emoji emoji-id="5330237710655306682">✈️</tg-emoji>'
    elif "instagram" in sender_lower:
        service_emoji = '<tg-emoji emoji-id="5319160079465857105">📸</tg-emoji>'
    elif "facebook" in sender_lower:
        service_emoji = '<tg-emoji emoji-id="5323261730283863478">👤</tg-emoji>'
    else:
        service_emoji = '<tg-emoji emoji-id="6125390694363175728">🌐</tg-emoji>'

    if personal:
        formatted = (
            f'<tg-emoji emoji-id="5382357040008021292">⚡</tg-emoji> <b>OTP RECEIVED!</b>\n'
            f'━━━━━━━━━━━━━━━\n'
            f'{flag} <b>Country:</b> {html.escape(country)}\n'
            f'{service_emoji} <b>Service:</b> {html.escape(sender)}\n'
            f'📞 <b>Number:</b> <code>{html.escape(number)}</code>\n'
            f'━━━━━━━━━━━━━━━\n'
            f'🔑 <b>OTP Code:</b> <code>{otp}</code>\n'
            f'━━━━━━━━━━━━━━━\n'
            f'💬 <b>Message:</b>\n<code>{html.escape(message[:300])}</code>'
        )
    else:
        formatted = (
            f'<tg-emoji emoji-id="5382357040008021292">⚡</tg-emoji> '
            f'{flag} <b>{country_code}</b> | <code>{masked}</code> | '
            f'{service_emoji} <b>{service_code}</b> | '
            f'🔑 <code>{otp}</code>'
        )

    return formatted, number

# ---------------- OTP FETCHER ----------------
def login():
    try:
        res = session.get("https://flysms.net/login", headers=HEADERS, timeout=15)
    except Exception as e:
        logger.error(f"Login page request failed: {e}")
        return False

    soup = BeautifulSoup(res.text, "html.parser")
    captcha_text = None
    for string in soup.stripped_strings:
        if "What is" in string and "+" in string:
            captcha_text = string.strip()
            break

    match = re.search(r"What is\s*(\d+)\s*\+\s*(\d+)", captcha_text or "")
    if not match:
        logger.error("❌ Captcha not found.")
        return False

    a, b = int(match.group(1)), int(match.group(2))
    captcha_answer = str(a + b)
    logger.info(f"✅ Captcha solved: {a} + {b} = {captcha_answer}")

    payload = {
    "username": PANEL_USERNAME,
    "password": PANEL_PASSWORD,
    "capt": captcha_answer
}

    try:
        res = session.post(LOGIN_URL, data=payload, headers=HEADERS, timeout=15)
    except Exception as e:
        logger.error(f"Login POST failed: {e}")
        return False

    if "SMSCDRStats" not in res.text:
        logger.error("❌ Login failed.")
        return False

    logger.info("✅ Logged in successfully.")
    return True

def main_loop():
    """Main OTP fetching loop"""
    logger.info("🚀 OTP Monitor Started...")
    if not login():
        logger.error("❌ Initial login failed. Exiting OTP loop.")
        return

    while True:
        try:
            res = session.get(XHR_URL, headers=AJAX_HEADERS, timeout=15)
            try:
                data = res.json()
            except Exception as e:
                logger.debug(f"Invalid JSON from XHR: {e}")
                time.sleep(1.5)
                continue

            otps = data.get("aaData", [])
            otps = [row for row in otps if isinstance(row[0], str) and ":" in row[0]]

            for row in otps:
                try:
                    time_ = row[0]
                    country = row[1].split()[0]
                    number = row[2]
                    sender = row[3]
                    message = row[5]

                    hash_id = hashlib.md5((str(number) + str(time_) + str(message)).encode()).hexdigest()
                    if hash_id in seen_messages:
                        continue

                    seen_messages.add(hash_id)
                    seen_order.append(hash_id)
                    if len(seen_order) > MAX_SEEN:
                        old = seen_order.popleft()
                        seen_messages.discard(old)

                    otp_code = extract_otp(message)
                    record = {
                        "hash_id": hash_id,
                        "dt": time_,
                        "country": country,
                        "num": number,
                        "cli": sender,
                        "message": message,
                        "otp": otp_code
                    }

                    otp_processing_queue.put(record)
                    logger.info(f"📱 New OTP: {number} | {sender} | {otp_code or 'N/A'}")

                except Exception as e:
                    logger.debug(f"Row parse error: {e}")

        except Exception as e:
            logger.error(f"❌ Error fetching OTPs: {e}")
            try:
                if res is not None and getattr(res, 'status_code', None) == 401:
                    logger.info("Attempting to re-login...")
                    if not login():
                        logger.error("❌ Re-login failed.")
            except Exception:
                pass

        time.sleep(1.0)

# ---------------- USER BOT FUNCTIONS ----------------
def send_random_numbers(chat_id, country=None, edit=False):
    """Assign 5 random numbers to user"""
    if country is None:
        country = user_current_country.get(chat_id)
        if not country:
            bot.send_message(chat_id, "❌ No country selected.")
            return
    
    numbers = numbers_by_country.get(country, [])
    if not numbers:
        bot.send_message(chat_id, f"❌ No numbers for {country}.")
        return
    
    if len(numbers) < 5:
        selected_numbers = numbers
    else:
        selected_numbers = random.sample(numbers, 5)
    
    user_current_country[chat_id] = country
    save_user_assignment(chat_id, selected_numbers, country)
    
    flag = country_to_flag(country)
    country_code = get_country_code(country)
    
    text = f"{flag} <b>{country}</b> Numbers:\n\n"
    for i, num in enumerate(selected_numbers, 1):
        text += f"{i}. <code>{num}</code>\n"
    
    text += f"\n⏳ Waiting for OTPs on any number...\n🔔 Instant notifications enabled!"
  
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("🔄 New Numbers", callback_data="change_number"),
        types.InlineKeyboardButton("🌎 Change Country", callback_data="change_country")
    )
    markup.row(
        types.InlineKeyboardButton("📱 Code Group", url=CODE_GROUP)
    )

    if chat_id in user_messages and edit:
        try:
            bot.edit_message_text(text, chat_id, user_messages[chat_id].message_id, 
                                reply_markup=markup, parse_mode="HTML")
        except Exception:
            msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")
            user_messages[chat_id] = msg
    else:
        msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")
        user_messages[chat_id] = msg

@bot.message_handler(commands=["start"])
def start(message):
    chat_id = message.chat.id
    username = message.from_user.username or message.from_user.first_name
    
    update_active_user(chat_id, username)

    if message.from_user.id == ADMIN_ID:
        bot.send_message(chat_id, "👋 Welcome Admin!\nUse /adminhelp for commands.")
        return

    active_users.add(chat_id)

    not_joined = []
    for channel in REQUIRED_CHANNELS:
        try:
            member = bot.get_chat_member(channel, chat_id)
            if member.status not in ["member", "creator", "administrator"]:
                not_joined.append(channel)
        except Exception:
            not_joined.append(channel)

    if not_joined:
        markup = types.InlineKeyboardMarkup()
        for ch in not_joined:
            markup.add(types.InlineKeyboardButton(f"🚀 Join {ch}", url=f"https://t.me/{ch[1:]}"))
        bot.send_message(chat_id, "❌ You must join all required channels to use the bot.", 
                        reply_markup=markup)
        return

    if not numbers_by_country:
        bot.send_message(chat_id, "❌ No countries available yet.")
        return

    markup = types.InlineKeyboardMarkup()
    for country in sorted(numbers_by_country.keys()):
        markup.add(types.InlineKeyboardButton(country, callback_data=f"user_select_{country}"))
    msg = bot.send_message(chat_id, "🌎 Choose a country:", reply_markup=markup)
    user_messages[chat_id] = msg

# ---------------- CALLBACK HANDLERS ----------------

@bot.callback_query_handler(func=lambda call: call.data.startswith("copy_"))
def handle_copy_otp(call):
    """Handle OTP copy button"""
    otp = call.data[5:]
    try:
        bot.answer_callback_query(call.id, f"✅ OTP: {otp}\nClick to dismiss!", show_alert=True)
    except Exception as e:
        logger.error(f"Failed to show OTP: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("fullsms_"))
def handle_full_sms(call):
    """Handle full SMS view"""
    hash_id = call.data[8:]
    
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT message FROM otp_records WHERE hash_id = ?', (hash_id,))
            row = cursor.fetchone()
            
            if row:
                message = row[0] or 'No message'
                bot.answer_callback_query(call.id, message, show_alert=True)
            else:
                bot.answer_callback_query(call.id, "❌ Message not found", show_alert=True)
    except Exception as e:
        logger.error(f"Failed to fetch full SMS: {e}")
        bot.answer_callback_query(call.id, "❌ Error loading message", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("addto_"))
def callback_addto(call):
    """Handle admin country selection"""
    if call.from_user.id != ADMIN_ID:
        return bot.answer_callback_query(call.id, "❌ Not authorized")
    
    numbers = temp_uploads.get(call.from_user.id, [])
    if not numbers:
        return bot.answer_callback_query(call.id, "❌ No uploaded numbers found")

    choice = call.data[6:]
    
    if choice == "new":
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "✏️ Send new country name:")
        bot.register_next_step_handler(call.message, save_new_country, numbers)
    else:
        existing = numbers_by_country.get(choice, [])
        merged = list(dict.fromkeys(existing + numbers))
        numbers_by_country[choice] = merged
        save_data()
        
        file_path = os.path.join(NUMBERS_DIR, f"{choice}.txt")
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(merged))
            logger.info(f"✅ Saved {len(merged)} numbers to {choice}")
        except Exception as e:
            logger.error(f"Failed to write numbers file: {e}")

        try:
            bot.answer_callback_query(call.id, f"✅ Added {len(numbers)} numbers!")
            bot.edit_message_text(
                f"✅ Successfully added {len(numbers)} numbers to *{choice}*\n"
                f"Total numbers in {choice}: {len(merged)}",
                call.message.chat.id, 
                call.message.message_id, 
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.debug(f"edit_message_text failed: {e}")
            bot.send_message(call.message.chat.id, 
                f"✅ Added {len(numbers)} numbers to *{choice}*\n"
                f"Total: {len(merged)}", 
                parse_mode="Markdown")

        temp_uploads.pop(call.from_user.id, None)

@bot.callback_query_handler(func=lambda call: call.data.startswith("user_select_"))
def handle_country_selection(call):
    """Handle user country selection"""
    chat_id = call.message.chat.id
    if call.from_user.id != ADMIN_ID:
        active_users.add(chat_id)
        update_active_user(chat_id, call.from_user.username)
    
    country = call.data[12:]
    user_current_country[chat_id] = country
    try:
        bot.answer_callback_query(call.id, f"Selected {country}")
    except Exception as e:
        print("Callback expired:", e)
    send_random_numbers(chat_id, country, edit=True)

@bot.callback_query_handler(func=lambda call: call.data in ["change_number", "change_country"])
def handle_change_actions(call):
    """Handle change number and change country"""
    chat_id = call.message.chat.id
    if call.from_user.id != ADMIN_ID:
        active_users.add(chat_id)
        update_active_user(chat_id, call.from_user.username)
    
    if call.data == "change_number":
        bot.answer_callback_query(call.id, "🔄 Getting new numbers...")
        send_random_numbers(chat_id, user_current_country.get(chat_id), edit=True)
        
    elif call.data == "change_country":
        bot.answer_callback_query(call.id)
        markup = types.InlineKeyboardMarkup()
        for country in sorted(numbers_by_country.keys()):
            markup.add(types.InlineKeyboardButton(country, callback_data=f"user_select_{country}"))
        
        if chat_id in user_messages:
            try:
                bot.edit_message_text(
                    "🌎 Select a country:", 
                    chat_id, 
                    user_messages[chat_id].message_id, 
                    reply_markup=markup
                )
            except Exception as e:
                logger.debug(f"Failed to edit message: {e}")
                msg = bot.send_message(chat_id, "🌎 Select a country:", reply_markup=markup)
                user_messages[chat_id] = msg
        else:
            msg = bot.send_message(chat_id, "🌎 Select a country:", reply_markup=markup)
            user_messages[chat_id] = msg

# ---------------- BROADCAST ----------------
def broadcast_message(message):
    text = message.text
    success_count = 0
    fail_count = 0

    all_users = get_all_active_users()
    
    for user_id in all_users:
        try:
            bot.send_message(user_id, f"📢 <b>Broadcast Message:</b>\n\n{html.escape(text)}", parse_mode="HTML")
            success_count += 1
        except Exception:
            fail_count += 1
        time.sleep(0.05)

    bot.reply_to(message, f"✅ Broadcast sent!\n✅ Success: {success_count}\n❌ Failed: {fail_count}")

@bot.message_handler(commands=["broadcast"])
def broadcast_start(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    
    msg = bot.reply_to(message, "✉️ Send the message you want to broadcast to all users:")
    bot.register_next_step_handler(msg, broadcast_message)

@bot.message_handler(commands=["usercount"])
def user_count(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    count = get_active_user_count()
    bot.reply_to(message, f"👥 Total active users: {count}")

@bot.message_handler(commands=["stats"])
def show_stats(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM otp_records')
            total_otps = cursor.fetchone()[0]
            
            cursor.execute('''
                SELECT COUNT(*) FROM otp_records 
                WHERE DATE(created_at) = DATE('now')
            ''')
            otps_today = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM active_users')
            active_count = cursor.fetchone()[0]
            
            group_queue_size = group_message_queue.qsize()
            personal_queue_size = personal_message_queue.qsize()
            processing_queue_size = otp_processing_queue.qsize()
            
            stats_text = (
                f"📊 <b>Bot Statistics</b>\n\n"
                f"📱 <b>OTPs:</b>\n"
                f"   • Total: {total_otps}\n"
                f"   • Today: {otps_today}\n\n"
                f"👥 <b>Users:</b>\n"
                f"   • Active: {active_count}\n\n"
                f"⚙️ <b>Queue Status:</b>\n"
                f"   • Group Queue: {group_queue_size}\n"
                f"   • Personal Queue: {personal_queue_size}\n"
                f"   • Processing Queue: {processing_queue_size}\n\n"
                f"🌍 <b>Countries:</b> {len(numbers_by_country)}\n"
                f"📞 <b>Total Numbers:</b> {sum(len(v) for v in numbers_by_country.values())}\n\n"
                f"🗑️ <b>Auto-Delete:</b> {'Enabled (' + str(AUTO_DELETE_MINUTES) + ' min)' if AUTO_DELETE_MINUTES > 0 else 'Disabled'}\n"
                f"📡 <b>OTP Groups:</b> {len(OTP_GROUP_IDS)}"
            )
            
            bot.reply_to(message, stats_text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Stats error: {e}")
        bot.reply_to(message, "❌ Failed to fetch statistics")

# ---------------- ADMIN FILE UPLOAD ----------------
@bot.message_handler(content_types=["document"])
def handle_document(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    if not message.document.file_name.endswith(".txt"):
        return bot.reply_to(message, "❌ Please upload a .txt file.")

    file_info = bot.get_file(message.document.file_id)
    downloaded_file = bot.download_file(file_info.file_path)
    try:
        numbers = [line.strip() for line in downloaded_file.decode("utf-8").splitlines() if line.strip()]
    except Exception:
        return bot.reply_to(message, "❌ Failed to decode uploaded file. Ensure it's UTF-8 plain text.")

    if not numbers:
        return bot.reply_to(message, "❌ File is empty.")

    temp_uploads[message.from_user.id] = numbers

    markup = types.InlineKeyboardMarkup()
    for country in sorted(numbers_by_country.keys()):
        markup.add(types.InlineKeyboardButton(country, callback_data=f"addto_{country}"))
    markup.add(types.InlineKeyboardButton("➕ New Country", callback_data="addto_new"))

    bot.reply_to(message, "📂 File received. Select country to add numbers:", reply_markup=markup)

def save_new_country(message, numbers):
    country = message.text.strip()
    if not country:
        return bot.reply_to(message, "❌ Invalid country name.")
    
    numbers_clean = [n.strip() for n in numbers if n.strip()]
    numbers_by_country[country] = list(dict.fromkeys(numbers_clean))
    save_data()
    
    file_path = os.path.join(NUMBERS_DIR, f"{country}.txt")
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(numbers_by_country[country]))
    except Exception as e:
        logger.error(f"Failed to write new country file: {e}")

    try:
        bot.reply_to(message, f"✅ Saved {len(numbers_by_country[country])} numbers under *{country}*", 
                    parse_mode="Markdown")
    except Exception:
        try:
            bot.send_message(ADMIN_ID, f"✅ Saved {len(numbers_by_country[country])} numbers under {country}")
        except Exception as e:
            logger.error(f"Failed to confirm saved country: {e}")

    temp_uploads.pop(message.from_user.id, None)

# ---------------- ADMIN COMMANDS ----------------
@bot.message_handler(commands=["setcountry"])
def set_country(message):
    global current_country
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    if len(message.text.split()) > 1:
        current_country = " ".join(message.text.split()[1:]).strip()
        if current_country not in numbers_by_country:
            numbers_by_country[current_country] = []
        save_data()
        bot.reply_to(message, f"✅ Current country set to: {current_country}")
    else:
        bot.reply_to(message, "Usage: /setcountry <country name>")

@bot.message_handler(commands=["deletecountry"])
def delete_country(message):
    global current_country
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    if len(message.text.split()) > 1:
        country = " ".join(message.text.split()[1:]).strip()
        if country in numbers_by_country:
            del numbers_by_country[country]
            if current_country == country:
                current_country = None
            file_path = os.path.join(NUMBERS_DIR, f"{country}.txt")
            if os.path.exists(file_path):
                os.remove(file_path)
            save_data()
            bot.reply_to(message, f"✅ Deleted country: {country}")
        else:
            bot.reply_to(message, f"❌ Country '{country}' not found.")
    else:
        bot.reply_to(message, "Usage: /deletecountry <country name>")

@bot.message_handler(commands=["cleannumbers"])
def clear_numbers(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    if len(message.text.split()) > 1:
        country = " ".join(message.text.split()[1:]).strip()
        if country in numbers_by_country:
            numbers_by_country[country] = []
            file_path = os.path.join(NUMBERS_DIR, f"{country}.txt")
            open(file_path, "w").close()
            save_data()
            bot.reply_to(message, f"✅ Cleared numbers for {country}.")
        else:
            bot.reply_to(message, f"❌ Country '{country}' not found.")
    else:
        bot.reply_to(message, "Usage: /cleannumbers <country name>")

@bot.message_handler(commands=["listcountries"])
def list_countries(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    if not numbers_by_country:
        return bot.reply_to(message, "❌ No countries available.")
    text = "🌍 Available countries and number counts:\n\n"
    for country, nums in sorted(numbers_by_country.items()):
        text += f"• {country}: {len(nums)} numbers\n"
    bot.reply_to(message, text)

@bot.message_handler(commands=["addchat"])
def add_chat(message):
    """Add current chat/group to OTP group list"""
    global OTP_GROUP_IDS
    
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    
    chat_id = str(message.chat.id)
    chat_type = message.chat.type
    chat_title = message.chat.title or "Private Chat"
    
    if chat_type == "private":
        return bot.reply_to(message, "❌ This command should be used in a group/channel.")
    
    old_groups = OTP_GROUP_IDS.copy()
    OTP_GROUP_IDS = [chat_id]
    
    data["otp_groups"] = OTP_GROUP_IDS
    save_data()
    
    response = f"✅ <b>OTP Group Updated!</b>\n\n"
    response += f"📱 <b>New Group:</b> {html.escape(chat_title)}\n"
    response += f"🆔 <b>Chat ID:</b> <code>{chat_id}</code>\n\n"
    
    if old_groups:
        response += f"🗑️ <b>Removed Groups:</b> {len(old_groups)}\n"
    
    response += "\n✅ All future OTPs will be sent to this group only!"
    
    bot.reply_to(message, response, parse_mode="HTML")
    logger.info(f"✅ OTP group updated: {chat_title} ({chat_id})")

@bot.message_handler(commands=["autodelete"])
def set_autodelete(message):
    """Set auto-delete timer for group messages"""
    global AUTO_DELETE_MINUTES
    
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    
    args = message.text.split()
    if len(args) < 2:
        status = "✅ Enabled" if AUTO_DELETE_MINUTES > 0 else "❌ Disabled"
        current = f"{AUTO_DELETE_MINUTES} minutes" if AUTO_DELETE_MINUTES > 0 else "Disabled"
        return bot.reply_to(
            message,
            f"🗑️ <b>Auto-Delete Status:</b> {status}\n"
            f"⏱️ <b>Current Timer:</b> {current}\n\n"
            f"<b>Usage:</b> /autodelete &lt;minutes&gt;\n"
            f"<b>Example:</b> /autodelete 2\n"
            f"<b>To disable:</b> /autodelete 0",
            parse_mode="HTML"
        )
    
    try:
        minutes = int(args[1])
        if minutes < 0:
            return bot.reply_to(message, "❌ Minutes must be 0 or positive.")
        
        AUTO_DELETE_MINUTES = minutes
        data["auto_delete_minutes"] = AUTO_DELETE_MINUTES
        save_data()
        
        if minutes == 0:
            response = "✅ <b>Auto-Delete Disabled</b>\n\n"
            response += "Group messages will no longer be auto-deleted."
        else:
            response = f"✅ <b>Auto-Delete Enabled</b>\n\n"
            response += f"⏱️ Group messages will be deleted after <b>{minutes} minute(s)</b>"
        
        bot.reply_to(message, response, parse_mode="HTML")
        logger.info(f"✅ Auto-delete set to {minutes} minutes")
        
    except ValueError:
        bot.reply_to(message, "❌ Invalid number. Use: /autodelete &lt;minutes&gt;", parse_mode="HTML")

@bot.message_handler(commands=["adminhelp"])
def admin_help(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    help_text = """
🔧 <b>Admin Commands:</b>

📁 <b>File Management:</b>
• Upload .txt file - Add numbers to a country
• /setcountry &lt;country&gt; - Set current country
• /deletecountry &lt;country&gt; - Delete a country
• /cleannumbers &lt;country&gt; - Clear numbers for country
• /listcountries - View all countries

📊 <b>Statistics:</b>
• /stats - View detailed bot statistics
• /usercount - Get active user count

📢 <b>Communication:</b>
• /broadcast - Send message to all users

🔧 <b>Group Management:</b>
• /addchat - Add current chat as OTP group
• /autodelete &lt;minutes&gt; - Set auto-delete timer (0 to disable)

❓ /adminhelp - Show this help menu
"""
    bot.reply_to(message, help_text, parse_mode="HTML")

# ---------------- DATABASE CLEANUP ----------------
def cleanup_old_otps():
    """Periodically clean up old OTP records"""
    while True:
        try:
            time.sleep(3600)
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    DELETE FROM otp_records 
                    WHERE created_at < datetime('now', '-30 days')
                ''')
                deleted = cursor.rowcount
                conn.commit()
                if deleted > 0:
                    logger.info(f"🗑️ Cleaned up {deleted} old OTP records")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

# ---------------- START EVERYTHING ----------------
def run_bot():
    logger.info("🤖 Starting bot polling...")
    bot.infinity_polling()

if __name__ == "__main__":
    logger.info("🚀 Starting all services...")
    
    threading.Thread(target=run_flask, daemon=True, name="Flask").start()
    threading.Thread(target=group_sender_worker, daemon=True, name="GroupSender").start()
    threading.Thread(target=personal_sender_worker, daemon=True, name="PersonalSender").start()
    threading.Thread(target=otp_processor_worker, daemon=True, name="OTPProcessor").start()
    threading.Thread(target=main_loop, daemon=True, name="OTPFetcher").start()
    threading.Thread(target=cleanup_old_otps, daemon=True, name="Cleanup").start()
    threading.Thread(target=run_bot, daemon=True, name="BotPoller").start()
    
    logger.info("✅ All services started successfully!")
    
    while True:
        time.sleep(60)
