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
from datetime import datetime, timedelta, timezone
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
from contextlib import contextmanager

# ---------------- CONFIG / LOGGING ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Environment Variables
BOT_TOKEN   = os.getenv("BOT_TOKEN")
ADMIN_ID    = 8195360535
# ── Grand Panel ──────────────────────────────────
GRANDPANEL_API_TOKEN = os.getenv("GRANDPANEL_API_TOKEN", "YOUR_TOKEN_HERE")
GRANDPANEL_BASE_URL  = os.getenv("GRANDPANEL_BASE_URL",  "https://panel.grand-panel.com")
FETCH_DELAY          = 3.0
# ─────────────────────────────────────────────────

bot = telebot.TeleBot(BOT_TOKEN)

DATA_FILE   = "bot_data.json"
NUMBERS_DIR = "numbers"
DB_FILE     = "otp_data.db"

os.makedirs(NUMBERS_DIR, exist_ok=True)

OTP_GROUP_IDS       = ["-1003672667505"]
AUTO_DELETE_MINUTES = 0

CHANNEL_LINK  = "https://whatsapp.com/channel/0029Va5XJaU6xCSHlSwIXH1P"
BACKUP        = "https://t.me/VASUHUB"
DEVELOPER_ID  = "@UXOTPBOT"
CODE_GROUP    = "https://t.me/+SDPuI2Ud62RkN2Jl"

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

# ---------------- DATA STORAGE ----------------
data               = {}
numbers_by_country = {}
current_country    = None
user_messages      = {}
user_current_country = {}
temp_uploads       = {}

MAX_SEEN      = 200000
seen_messages = set()
seen_order    = deque()

# Numbers that have already received an OTP — never suggest again
used_numbers: set = set()

group_message_queue    = queue.Queue()
personal_message_queue = queue.Queue()
otp_processing_queue   = queue.Queue()

MAX_WORKERS_GROUP    = 8
MAX_WORKERS_PERSONAL = 10
SEND_TIMEOUT         = 8

active_users      = set()
REQUIRED_CHANNELS = ["@uxotp", "@Vasuhub"]

COUNTRY_MAP = {
    "91": "India", "92": "Pakistan", "880": "Bangladesh", "94": "Sri Lanka",
    "62": "Indonesia", "63": "Philippines", "60": "Malaysia", "66": "Thailand",
    "84": "Vietnam", "971": "UAE", "966": "Saudi Arabia", "965": "Kuwait",
    "968": "Oman", "974": "Qatar", "973": "Bahrain", "972": "Israel", "90": "Turkey",
    "358": "Finland", "46": "Sweden", "47": "Norway", "45": "Denmark",
    "49": "Germany", "33": "France", "39": "Italy", "34": "Spain","967": "Yemen",
    "44": "UK", "48": "Poland", "31": "Netherlands", "32": "Belgium",
    "351": "Portugal", "420": "Czech Republic", "36": "Hungary", "40": "Romania",
    "30": "Greece", "380": "Ukraine", "7": "Russia", "20": "Egypt",
    "212": "Morocco", "213": "Algeria", "216": "Tunisia", "218": "Libya",
    "27": "South Africa", "234": "Nigeria", "233": "Ghana", "225": "Ivory Coast",
    "254": "Kenya", "255": "Tanzania", "256": "Uganda", "257": "Burundi",
    "258": "Mozambique", "244": "Angola", "221": "Senegal", "220": "Gambia",
    "231": "Liberia", "251": "Ethiopia", "1": "USA / Canada", "52": "Mexico",
    "55": "Brazil", "54": "Argentina", "57": "Colombia", "56": "Chile",
    "58": "Venezuela", "51": "Peru",
}


def get_country_from_num(num: str) -> str:
    for code in sorted(COUNTRY_MAP.keys(), key=len, reverse=True):
        if num.startswith(code):
            return COUNTRY_MAP[code]
    return "Unknown"

SERVICE_CODES = {
    "whatsapp": "WA", "telegram": "TG", "instagram": "IG", "facebook": "FB",
    "twitter": "TW", "google": "GO", "amazon": "AZ", "snapchat": "SC",
    "tiktok": "TT", "linkedin": "LI", "uber": "UB", "paypal": "PP",
}

# ---------------- SQLITE DATABASE ----------------
def init_database():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()

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
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_number    ON otp_records(number)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON otp_records(timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash      ON otp_records(hash_id)')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            number TEXT NOT NULL,
            country TEXT,
            assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_chat          ON user_assignments(chat_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_number_assign ON user_assignments(number)')

    # Persists used numbers across restarts
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS used_numbers (
            number TEXT PRIMARY KEY,
            marked_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

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
    logger.info("✅ SQLite database initialized")

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

# ── Mark number as permanently used — remove from pool forever ───────────
def mark_number_used(number: str):
    """
    Called as soon as an OTP is received on a number.
    1. Add to in-memory set
    2. Persist to DB
    3. Remove from every country pool in memory
    4. Remove from user_assignments (no one needs it anymore)
    5. Save updated pool to disk
    """
    if number in used_numbers:
        return  # already done

    used_numbers.add(number)

    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT OR IGNORE INTO used_numbers (number) VALUES (?)', (number,))
            cursor.execute('DELETE FROM user_assignments WHERE number = ?', (number,))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to mark number used in DB: {e}")

    # Remove from in-memory pool
    for country in list(numbers_by_country.keys()):
        if number in numbers_by_country[country]:
            numbers_by_country[country].remove(number)
            logger.info(f"🗑️ Removed used number {number} from [{country}] pool")

    save_data()


def _preload_used_numbers():
    """On startup: load used_numbers from DB, clean them out of the pool."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT number FROM used_numbers')
            for row in cursor.fetchall():
                n = row[0]
                used_numbers.add(n)
                for country in list(numbers_by_country.keys()):
                    if n in numbers_by_country[country]:
                        numbers_by_country[country].remove(n)
        logger.info(f"♻️ Pre-loaded {len(used_numbers)} used numbers (removed from pool)")
    except Exception as e:
        logger.error(f"Failed to preload used numbers: {e}")

# ── Standard DB helpers ──────────────────────────────────────────────────
def save_otp_to_db(record, hash_id):
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
    except Exception as e:
        logger.error(f"Failed to save OTP to DB: {e}")

def get_past_otps(number, limit=10):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM otp_records WHERE number = ?
                ORDER BY created_at DESC LIMIT ?
            ''', (number, limit))
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Failed to fetch past OTPs: {e}")
        return []

def save_user_assignment(chat_id, numbers, country):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM user_assignments WHERE chat_id = ?', (chat_id,))
            for number in numbers:
                cursor.execute(
                    'INSERT INTO user_assignments (chat_id, number, country) VALUES (?, ?, ?)',
                    (chat_id, number, country)
                )
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to save user assignment: {e}")

def get_user_numbers(chat_id):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT number FROM user_assignments WHERE chat_id = ?', (chat_id,))
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        return []

def update_active_user(chat_id, username=None):
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
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM active_users')
            return cursor.fetchone()[0]
    except Exception:
        return 0

def get_all_active_users():
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT chat_id FROM active_users')
            return [row[0] for row in cursor.fetchall()]
    except Exception:
        return []

init_database()

# ---------------- DATA FUNCTIONS ----------------
def load_data():
    global data, numbers_by_country, current_country, OTP_GROUP_IDS, AUTO_DELETE_MINUTES
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                numbers_by_country  = data.get("numbers_by_country", {}) or {}
                current_country     = data.get("current_country")
                OTP_GROUP_IDS       = data.get("otp_groups", ["-1003672667505"])
                AUTO_DELETE_MINUTES = data.get("auto_delete_minutes", 0)
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            data = {"numbers_by_country": {}, "current_country": None,
                    "otp_groups": ["-1003672667505"], "auto_delete_minutes": 0}
            numbers_by_country = {}
            current_country    = None
    else:
        data = {"numbers_by_country": {}, "current_country": None,
                "otp_groups": ["-1003672667505"], "auto_delete_minutes": 0}
        numbers_by_country = {}
        current_country    = None

def save_data():
    data["numbers_by_country"]  = numbers_by_country
    data["current_country"]     = current_country
    data["otp_groups"]          = OTP_GROUP_IDS
    data["auto_delete_minutes"] = AUTO_DELETE_MINUTES
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Failed to save data: {e}")

load_data()
_preload_used_numbers()   # must be AFTER load_data

# ---------------- FLASK ----------------
app = Flask(__name__)

@app.route("/")
def index():
    return "Bot is running"

@app.route("/health")
def health():
    return Response("OK", status=200)

@app.route("/stats")
def stats_route():
    return Response(f"Active Users: {get_active_user_count()}", status=200)

def run_flask():
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

# ---------------- TELEGRAM SENDER (PARALLEL) ----------------
def _send_single(chat_id, payload):
    payload_local = payload.copy()
    payload_local["chat_id"] = chat_id
    try:
        r = session.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data=payload_local, timeout=SEND_TIMEOUT
        )
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
    logger.info("🚀 Group sender worker started")
    while True:
        try:
            msg, chat_ids, kb = group_message_queue.get()
            for chat_id in chat_ids:
                try:
                    sent_msg = bot.send_message(
                        chat_id, msg,
                        reply_markup=kb, parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                    if AUTO_DELETE_MINUTES > 0:
                        threading.Timer(
                            AUTO_DELETE_MINUTES * 60,
                            delete_message_safe,
                            args=(chat_id, sent_msg.message_id)
                        ).start()
                    logger.info(f"✅ Sent to group {chat_id}")
                except Exception as e:
                    logger.error(f"❌ Failed group {chat_id}: {e}")
        except Exception as e:
            logger.error(f"Group sender error: {e}")
        finally:
            group_message_queue.task_done()
        time.sleep(0.05)

def personal_sender_worker():
    logger.info("🚀 Personal sender worker started")
    while True:
        try:
            msg, chat_id = personal_message_queue.get()
            send_to_telegram(msg, [chat_id])
        except Exception as e:
            logger.error(f"Personal sender error: {e}")
        finally:
            personal_message_queue.task_done()
        time.sleep(0.02)

def delete_message_safe(chat_id, message_id):
    try:
        bot.delete_message(chat_id, message_id)
    except Exception as e:
        logger.debug(f"Failed to delete message {message_id}: {e}")

def otp_processor_worker():
    logger.info("🚀 OTP processor worker started")
    while True:
        try:
            record  = otp_processing_queue.get()
            hash_id = record.get("hash_id")
            number  = record.get("num", "")

            # Save to DB first
            save_otp_to_db(record, hash_id)

            otp = record.get("otp") or extract_otp(record.get("message", ""))

            # Who is assigned this number?
            assigned_users = []
            try:
                with get_db() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        'SELECT DISTINCT chat_id FROM user_assignments WHERE number = ?',
                        (number,)
                    )
                    assigned_users = [row[0] for row in cursor.fetchall()]
            except Exception as e:
                logger.error(f"Failed to get assigned users: {e}")

            # ✅ PERMANENTLY remove this number from pool right now
            mark_number_used(number)

            # Build keyboard
            keyboard = types.InlineKeyboardMarkup()
            if otp:
                keyboard.add(
                    types.InlineKeyboardButton(f"🔑 {otp}", callback_data=f"copy_{otp}")
                )
            keyboard.add(
                types.InlineKeyboardButton("📨 View Full", callback_data=f"fullsms_{hash_id}")
            )
            keyboard.row(
                types.InlineKeyboardButton("🚀 Panel", url=f"https://t.me/{DEVELOPER_ID.lstrip('@')}"),
                types.InlineKeyboardButton("📱 Channel", url=CHANNEL_LINK)
            )

            # ── Always send to group via queue ──────────────────────
            msg_group, _ = format_message(record, personal=False)
            group_message_queue.put((msg_group, list(OTP_GROUP_IDS), keyboard))
            logger.info(f"📤 Queued to group(s): {OTP_GROUP_IDS}")

            # ── Also send personal message if user has this number ──
            for chat_id in assigned_users:
                msg_personal, _ = format_message(record, personal=True)
                personal_kb = types.InlineKeyboardMarkup()
                if otp:
                    personal_kb.add(
                        types.InlineKeyboardButton(f"🔑 {otp}", callback_data=f"copy_{otp}")
                    )
                personal_kb.add(
                    types.InlineKeyboardButton("📨 Full SMS", callback_data=f"fullsms_{hash_id}")
                )
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
    code = EXTRA_CODES.get(country_name)
    if not code:
        try:
            country = pycountry.countries.lookup(country_name)
            code = country.alpha_2
        except LookupError:
            return country_name[:2].upper()
    return code.upper()

def get_service_code(sender: str) -> str:
    for service, code in SERVICE_CODES.items():
        if service.lower() in sender.lower():
            return code
    return sender[:2].upper() if len(sender) >= 2 else sender.upper()

def extract_otp(message: str):
    text = message.strip()
    m = re.search(
        r"(?:otp|code|pin|password|verification|verif)[^\d]{0,8}([0-9][0-9\-\s]{2,10}[0-9])",
        text, re.I
    )
    if m:
        cand = re.sub(r"\D", "", m.group(1))
        if 3 <= len(cand) <= 8 and not (1900 <= int(cand) <= 2099):
            return cand
    m2 = re.search(
        r"([0-9][0-9\-\s]{2,10}[0-9])[^\w]{0,8}(?:otp|code|pin|password|verification|verif)",
        text, re.I
    )
    if m2:
        cand = re.sub(r"\D", "", m2.group(1))
        if 3 <= len(cand) <= 8 and not (1900 <= int(cand) <= 2099):
            return cand
    for g in re.findall(r"\b[0-9][0-9\-\s]{2,7}[0-9]\b", text):
        cand = re.sub(r"\D", "", g)
        if 3 <= len(cand) <= 8 and not (1900 <= int(cand) <= 2099):
            return cand
    return None

def mask_number(number: str) -> str:
    if len(number) <= 4:
        return number
    return f"{number[:2]}••{number[-4:]}"

def format_message(record, personal=False):
    number  = record.get("num")     or "Unknown"
    sender  = record.get("cli")     or "Unknown"
    message = record.get("message") or ""
    country = record.get("country") or "Unknown"

    flag         = country_to_flag(country)
    country_code = get_country_code(country)
    service_code = get_service_code(sender)
    masked       = mask_number(number)

    if personal:
        formatted = (
            f"{flag} {country_code} | {masked} | {service_code}\n\n"
            f"<b>Full Number:</b> <code>{html.escape(number)}</code>\n"
            f"<b>Service:</b> {html.escape(sender)}\n\n"
            f"<b>Message:</b>\n<code>{html.escape(message[:200])}</code>"
        )
    else:
        formatted = f"{flag} {country_code} | {masked} | {service_code}"

    return formatted, number

# ---------------- GRAND PANEL FETCHER ----------------
def _fetch_grand_date(date_str: str, headers: dict):
    try:
        response = requests.get(
            f"{GRANDPANEL_BASE_URL}/api/v1/messages",
            headers=headers,
            params={"date": date_str},
            timeout=8
        )
        logger.debug(
            f"[grandpanel] HTTP {response.status_code} date={date_str} | {response.text[:150]}"
        )

        if response.status_code == 401:
            logger.warning("[grandpanel] ⚠️ 401 Unauthorized — check GRANDPANEL_API_TOKEN")
            return None
        if response.status_code == 404:
            logger.warning("[grandpanel] ⚠️ 404 — check GRANDPANEL_BASE_URL")
            return None
        if response.status_code != 200:
            logger.warning(f"[grandpanel] ⚠️ HTTP {response.status_code}")
            return []
        if not response.text.strip():
            return []
        try:
            data = response.json()
        except Exception:
            logger.warning(f"[grandpanel] ⚠️ Non-JSON: {response.text[:100]}")
            return []

        messages = data.get("messages", [])
        logger.info(
            f"[grandpanel] 📅 date={date_str} → {len(messages)} msg(s) "
            f"| total={data.get('total','?')} | range={data.get('range','?')}"
        )
        return messages

    except requests.exceptions.Timeout:
        logger.warning("[grandpanel] ⚠️ Timeout")
        return []
    except requests.exceptions.ConnectionError:
        logger.warning("[grandpanel] ⚠️ Connection error")
        return []
    except Exception as e:
        logger.error(f"[grandpanel] ❌ {e}")
        return []


def main_loop():
    logger.info("🚀 Grand Panel OTP Monitor Started...")
    logger.info(
        f"[grandpanel] 🔑 Token: {GRANDPANEL_API_TOKEN[:10]}...  | URL: {GRANDPANEL_BASE_URL}"
    )

    headers     = {"Authorization": f"Bearer {GRANDPANEL_API_TOKEN}"}
    auth_failed = False

    while True:
        if auth_failed:
            logger.error("[grandpanel] 🔴 Auth failed — fix token and restart.")
            time.sleep(60)
            continue

        now_utc        = datetime.now(timezone.utc)
        dates_to_fetch = [
            now_utc.strftime("%Y-%m-%d"),
            (now_utc - timedelta(days=1)).strftime("%Y-%m-%d"),
        ]

        for date_str in dates_to_fetch:
            records = _fetch_grand_date(date_str, headers)

            if records is None:
                auth_failed = True
                break

            for record in records:
                raw_num = str(record.get("destination", "")).strip()
                num     = raw_num.lstrip("0").lstrip("+")
                sender  = str(record.get("source")  or "Unknown").strip()
                message = str(record.get("content") or "").strip()
                time_   = str(record.get("date")    or "").strip()

                if not num or not message:
                    continue

                hash_id = hashlib.md5(f"{time_}{num}{message[:50]}".encode()).hexdigest()
                if hash_id in seen_messages:
                    continue

                seen_messages.add(hash_id)
                seen_order.append(hash_id)
                if len(seen_order) > MAX_SEEN:
                    old = seen_order.popleft()
                    seen_messages.discard(old)

                country  = get_country_from_num(num)
                otp_code = extract_otp(message)

                otp_processing_queue.put({
                    "hash_id": hash_id,
                    "dt":      time_,
                    "country": country,
                    "num":     num,
                    "cli":     sender,
                    "message": message,
                    "otp":     otp_code
                })
                logger.info(f"[grandpanel] 📱 {num} | {sender} | OTP: {otp_code or 'N/A'}")

        time.sleep(FETCH_DELAY)

# ---------------- USER BOT: send_random_numbers ----------------
def send_random_numbers(chat_id, country=None, edit=False):
    if country is None:
        country = user_current_country.get(chat_id)
        if not country:
            bot.send_message(chat_id, "❌ No country selected.")
            return

    all_numbers = numbers_by_country.get(country, [])

    # Only suggest numbers that have NEVER received an OTP
    available = [n for n in all_numbers if n not in used_numbers]

    if not available:
        bot.send_message(
            chat_id,
            f"⚠️ No fresh numbers available for <b>{country}</b> right now.\n"
            f"Please try another country or check back later.",
            parse_mode="HTML"
        )
        return

    selected_numbers = random.sample(available, min(5, len(available)))

    user_current_country[chat_id] = country
    save_user_assignment(chat_id, selected_numbers, country)

    flag         = country_to_flag(country)
    country_code = get_country_code(country)

    text = f"{flag} <b>{country}</b> Numbers:\n\n"
    for i, num in enumerate(selected_numbers, 1):
        text += f"{i}. <code>{num}</code>\n"
    text += (
        f"\n⏳ Waiting for OTPs...\n"
        f"🔔 Instant notification on SMS arrival!"
    )

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
            bot.edit_message_text(
                text, chat_id, user_messages[chat_id].message_id,
                reply_markup=markup, parse_mode="HTML"
            )
            return
        except Exception:
            pass
    msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")
    user_messages[chat_id] = msg

# ---------------- BOT HANDLERS ----------------
@bot.message_handler(commands=["start"])
def start_cmd(message):
    chat_id  = message.chat.id
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
            markup.add(
                types.InlineKeyboardButton(f"🚀 Join {ch}", url=f"https://t.me/{ch[1:]}")
            )
        bot.send_message(
            chat_id,
            "❌ You must join all required channels to use the bot.",
            reply_markup=markup
        )
        return

    if not numbers_by_country:
        bot.send_message(chat_id, "❌ No countries available yet.")
        return

    markup = types.InlineKeyboardMarkup()
    for country in sorted(numbers_by_country.keys()):
        markup.add(
            types.InlineKeyboardButton(country, callback_data=f"user_select_{country}")
        )
    msg = bot.send_message(chat_id, "🌎 Choose a country:", reply_markup=markup)
    user_messages[chat_id] = msg

# ---------------- CALLBACK HANDLERS ----------------
@bot.callback_query_handler(func=lambda call: call.data.startswith("copy_"))
def handle_copy_otp(call):
    otp = call.data[5:]
    try:
        bot.answer_callback_query(call.id, f"✅ OTP: {otp}", show_alert=True)
    except Exception as e:
        logger.error(f"Failed to show OTP: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("fullsms_"))
def handle_full_sms(call):
    hash_id = call.data[8:]
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT message FROM otp_records WHERE hash_id = ?', (hash_id,))
            row = cursor.fetchone()
            if row:
                bot.answer_callback_query(call.id, row[0] or 'No message', show_alert=True)
            else:
                bot.answer_callback_query(call.id, "❌ Message not found", show_alert=True)
    except Exception as e:
        logger.error(f"Failed to fetch full SMS: {e}")
        bot.answer_callback_query(call.id, "❌ Error loading message", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("addto_"))
def callback_addto(call):
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
        # Skip already-used numbers when adding
        new_nums = [n for n in numbers if n not in used_numbers]
        merged   = list(dict.fromkeys(existing + new_nums))
        numbers_by_country[choice] = merged
        save_data()

        file_path = os.path.join(NUMBERS_DIR, f"{choice}.txt")
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(merged))
        except Exception as e:
            logger.error(f"Failed to write numbers file: {e}")

        skipped  = len(numbers) - len(new_nums)
        msg_text = f"✅ Added {len(new_nums)} numbers to *{choice}*\nTotal: {len(merged)}"
        if skipped:
            msg_text += f"\n⚠️ Skipped {skipped} already-used numbers"

        try:
            bot.answer_callback_query(call.id, f"✅ Added {len(new_nums)} numbers!")
            bot.edit_message_text(
                msg_text, call.message.chat.id,
                call.message.message_id, parse_mode="Markdown"
            )
        except Exception:
            bot.send_message(call.message.chat.id, msg_text, parse_mode="Markdown")

        temp_uploads.pop(call.from_user.id, None)

@bot.callback_query_handler(func=lambda call: call.data.startswith("user_select_"))
def handle_country_selection(call):
    chat_id = call.message.chat.id
    if call.from_user.id != ADMIN_ID:
        active_users.add(chat_id)
        update_active_user(chat_id, call.from_user.username)

    country = call.data[12:]
    user_current_country[chat_id] = country
    try:
        bot.answer_callback_query(call.id, f"Selected {country}")
    except Exception:
        pass
    send_random_numbers(chat_id, country, edit=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("view_past_"))
def handle_view_past(call):
    chat_id   = call.message.chat.id
    number    = call.data[10:]
    past_otps = get_past_otps(number, limit=10)

    if not past_otps:
        bot.answer_callback_query(call.id, "❌ No past OTPs found", show_alert=True)
        return

    text = f"📜 <b>Past OTPs for {mask_number(number)}</b>\n\n"
    for i, r in enumerate(past_otps[:10], 1):
        flag = country_to_flag(r.get('country') or '')
        text += (
            f"{i}. {flag} <b>{r.get('sender','Unknown')}</b>\n"
            f"   🔢 OTP: <code>{r.get('otp_code','N/A')}</code>\n"
            f"   🕐 Time: {r.get('timestamp','Unknown')}\n\n"
        )
    text += f"<i>Showing last {len(past_otps)} OTPs</i>"

    try:
        bot.send_message(chat_id, text, parse_mode="HTML")
        bot.answer_callback_query(call.id, "✅ Past OTPs sent!")
    except Exception as e:
        logger.error(f"Failed to send past OTPs: {e}")
        bot.answer_callback_query(call.id, "❌ Error", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data in ["change_number", "change_country"])
def handle_change_actions(call):
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
            markup.add(
                types.InlineKeyboardButton(country, callback_data=f"user_select_{country}")
            )
        if chat_id in user_messages:
            try:
                bot.edit_message_text(
                    "🌎 Select a country:", chat_id,
                    user_messages[chat_id].message_id, reply_markup=markup
                )
                return
            except Exception:
                pass
        msg = bot.send_message(chat_id, "🌎 Select a country:", reply_markup=markup)
        user_messages[chat_id] = msg

# ---------------- BROADCAST ----------------
def broadcast_message(message):
    text = message.text
    success_count = fail_count = 0
    for user_id in get_all_active_users():
        try:
            bot.send_message(
                user_id,
                f"📢 <b>Broadcast Message:</b>\n\n{html.escape(text)}",
                parse_mode="HTML"
            )
            success_count += 1
        except Exception:
            fail_count += 1
        time.sleep(0.05)
    bot.reply_to(
        message,
        f"✅ Broadcast sent!\n✅ Success: {success_count}\n❌ Failed: {fail_count}"
    )

@bot.message_handler(commands=["broadcast"])
def broadcast_start(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    msg = bot.reply_to(message, "✉️ Send the message to broadcast:")
    bot.register_next_step_handler(msg, broadcast_message)

@bot.message_handler(commands=["usercount"])
def user_count(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    bot.reply_to(message, f"👥 Total active users: {get_active_user_count()}")

@bot.message_handler(commands=["stats"])
def show_stats(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM otp_records')
            total_otps = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM otp_records WHERE DATE(created_at)=DATE('now')")
            otps_today = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(*) FROM active_users')
            active_count = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(*) FROM used_numbers')
            used_count = cursor.fetchone()[0]

        fresh_total = sum(
            len([n for n in v if n not in used_numbers])
            for v in numbers_by_country.values()
        )

        stats_text = (
            f"📊 <b>Bot Statistics</b>\n\n"
            f"📱 <b>OTPs:</b>\n"
            f"   • Total: {total_otps}\n"
            f"   • Today: {otps_today}\n\n"
            f"👥 <b>Users:</b> {active_count}\n\n"
            f"🗑️ <b>Used Numbers:</b> {used_count} (removed forever)\n"
            f"📞 <b>Fresh Numbers Left:</b> {fresh_total}\n\n"
            f"⚙️ <b>Queues:</b>\n"
            f"   • Group: {group_message_queue.qsize()}\n"
            f"   • Personal: {personal_message_queue.qsize()}\n"
            f"   • Processing: {otp_processing_queue.qsize()}\n\n"
            f"🌍 <b>Countries:</b> {len(numbers_by_country)}\n"
            f"🗑️ <b>Auto-Delete:</b> "
            f"{'Enabled (' + str(AUTO_DELETE_MINUTES) + ' min)' if AUTO_DELETE_MINUTES > 0 else 'Disabled'}\n"
            f"📡 <b>OTP Groups:</b> {len(OTP_GROUP_IDS)}\n"
            f"🌐 <b>Panel:</b> Grand Panel"
        )
        bot.reply_to(message, stats_text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Stats error: {e}")
        bot.reply_to(message, "❌ Failed to fetch statistics")

# ---------------- ADMIN FILE UPLOAD ----------------
@bot.message_handler(commands=["importusers"])
def import_users_cmd(message):
    if message.from_user.id != ADMIN_ID:
        return
    bot.reply_to(message, "📂 Send users.txt (numeric IDs, one per line):")
    bot.register_next_step_handler(message, _wait_for_users_file)

def _wait_for_users_file(message):
    if message.from_user.id != ADMIN_ID:
        return
    if not message.document or not message.document.file_name.endswith(".txt"):
        bot.reply_to(message, "❌ .txt file required")
        return
    info  = bot.get_file(message.document.file_id)
    raw   = bot.download_file(info.file_path)
    lines = [l.strip() for l in raw.decode("utf-8").splitlines() if l.strip().isdigit()]
    if not lines:
        bot.reply_to(message, "❌ File empty or IDs invalid")
        return
    for uid in lines:
        active_users.add(int(uid))
    bot.reply_to(message, f"✅ {len(lines)} user IDs loaded!")

@bot.message_handler(content_types=["document"])
def handle_document(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    if not message.document.file_name.endswith(".txt"):
        return bot.reply_to(message, "❌ Please upload a .txt file.")

    file_info       = bot.get_file(message.document.file_id)
    downloaded_file = bot.download_file(file_info.file_path)
    try:
        numbers = [
            line.strip()
            for line in downloaded_file.decode("utf-8").splitlines()
            if line.strip()
        ]
    except Exception:
        return bot.reply_to(message, "❌ Failed to decode file.")

    if not numbers:
        return bot.reply_to(message, "❌ File is empty.")

    temp_uploads[message.from_user.id] = numbers

    markup = types.InlineKeyboardMarkup()
    for country in sorted(numbers_by_country.keys()):
        markup.add(
            types.InlineKeyboardButton(country, callback_data=f"addto_{country}")
        )
    markup.add(types.InlineKeyboardButton("➕ New Country", callback_data="addto_new"))
    bot.reply_to(message, "📂 File received. Select country:", reply_markup=markup)

def save_new_country(message, numbers):
    country = message.text.strip()
    if not country:
        return bot.reply_to(message, "❌ Invalid country name.")

    # Don't add already-used numbers
    numbers_clean               = [n.strip() for n in numbers if n.strip() and n not in used_numbers]
    numbers_by_country[country] = list(dict.fromkeys(numbers_clean))
    save_data()

    file_path = os.path.join(NUMBERS_DIR, f"{country}.txt")
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(numbers_by_country[country]))
    except Exception as e:
        logger.error(f"Failed to write country file: {e}")

    try:
        bot.reply_to(
            message,
            f"✅ Saved {len(numbers_by_country[country])} numbers under *{country}*",
            parse_mode="Markdown"
        )
    except Exception:
        bot.send_message(ADMIN_ID, f"✅ Saved numbers under {country}")

    temp_uploads.pop(message.from_user.id, None)

# ---------------- ADMIN COMMANDS ----------------
@bot.message_handler(commands=["exportusers"])
def export_users(message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id FROM active_users")
            db_users = [str(row[0]) for row in cursor.fetchall()]

        all_ids = set(db_users) | {str(uid) for uid in active_users}
        if not all_ids:
            return bot.reply_to(message, "❌ No users found.")

        content = "\n".join(sorted(all_ids)).encode("utf-8")
        bot.send_document(
            message.chat.id,
            ("users.txt", content, "text/plain"),
            caption=f"✅ Total Users: {len(all_ids)}"
        )
    except Exception as e:
        logger.error(f"Export error: {e}")
        bot.reply_to(message, "❌ Export failed")

@bot.message_handler(commands=["setcountry"])
def set_country(message):
    global current_country
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    if len(message.text.split()) > 1:
        current_country = " ".join(message.text.split()[1:]).strip()
        if current_country not in numbers_by_country:
            numbers_by_country[current_country] = []
        save_data()
        bot.reply_to(message, f"✅ Country set to: {current_country}")
    else:
        bot.reply_to(message, "Usage: /setcountry <country>")

@bot.message_handler(commands=["deletecountry"])
def delete_country(message):
    global current_country
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    if len(message.text.split()) > 1:
        country = " ".join(message.text.split()[1:]).strip()
        if country in numbers_by_country:
            del numbers_by_country[country]
            if current_country == country:
                current_country = None
            fp = os.path.join(NUMBERS_DIR, f"{country}.txt")
            if os.path.exists(fp):
                os.remove(fp)
            save_data()
            bot.reply_to(message, f"✅ Deleted: {country}")
        else:
            bot.reply_to(message, f"❌ '{country}' not found.")
    else:
        bot.reply_to(message, "Usage: /deletecountry <country>")

@bot.message_handler(commands=["cleannumbers"])
def clear_numbers(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    if len(message.text.split()) > 1:
        country = " ".join(message.text.split()[1:]).strip()
        if country in numbers_by_country:
            numbers_by_country[country] = []
            open(os.path.join(NUMBERS_DIR, f"{country}.txt"), "w").close()
            save_data()
            bot.reply_to(message, f"✅ Cleared numbers for {country}.")
        else:
            bot.reply_to(message, f"❌ '{country}' not found.")
    else:
        bot.reply_to(message, "Usage: /cleannumbers <country>")

@bot.message_handler(commands=["listcountries"])
def list_countries(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    if not numbers_by_country:
        return bot.reply_to(message, "❌ No countries available.")
    text = "🌍 Countries — fresh vs total:\n\n"
    for country, nums in sorted(numbers_by_country.items()):
        fresh = len([n for n in nums if n not in used_numbers])
        text += f"• {country}: {fresh} fresh / {len(nums)} total\n"
    bot.reply_to(message, text)

@bot.message_handler(commands=["addchat"])
def add_chat(message):
    global OTP_GROUP_IDS
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    if message.chat.type == "private":
        return bot.reply_to(message, "❌ Use in a group/channel.")

    chat_id    = str(message.chat.id)
    chat_title = message.chat.title or "Unknown"
    old_groups = OTP_GROUP_IDS.copy()
    OTP_GROUP_IDS = [chat_id]
    save_data()

    response = (
        f"✅ <b>OTP Group Updated!</b>\n\n"
        f"📱 <b>Group:</b> {html.escape(chat_title)}\n"
        f"🆔 <b>ID:</b> <code>{chat_id}</code>\n"
    )
    if old_groups:
        response += f"🗑️ Removed {len(old_groups)} old group(s)"
    bot.reply_to(message, response, parse_mode="HTML")

@bot.message_handler(commands=["autodelete"])
def set_autodelete(message):
    global AUTO_DELETE_MINUTES
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")

    args = message.text.split()
    if len(args) < 2:
        status = "✅ Enabled" if AUTO_DELETE_MINUTES > 0 else "❌ Disabled"
        return bot.reply_to(
            message,
            f"🗑️ Auto-Delete: {status}\n"
            f"Usage: /autodelete &lt;minutes&gt; | 0 to disable",
            parse_mode="HTML"
        )
    try:
        minutes = int(args[1])
        if minutes < 0:
            return bot.reply_to(message, "❌ Must be 0 or positive.")
        AUTO_DELETE_MINUTES = minutes
        save_data()
        msg = (
            "✅ Auto-Delete Disabled"
            if minutes == 0 else
            f"✅ Auto-Delete: messages deleted after <b>{minutes} min</b>"
        )
        bot.reply_to(message, msg, parse_mode="HTML")
    except ValueError:
        bot.reply_to(message, "❌ Invalid number.")

@bot.message_handler(commands=["adminhelp"])
def admin_help(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    help_text = """
🔧 <b>Admin Commands:</b>

📁 <b>Numbers:</b>
• Upload .txt — Add numbers to a country
• /setcountry &lt;country&gt;
• /deletecountry &lt;country&gt;
• /cleannumbers &lt;country&gt;
• /listcountries — fresh vs total counts

📊 <b>Stats:</b>
• /stats — Full statistics
• /usercount

📢 <b>Users:</b>
• /broadcast
• /exportusers | /importusers

🔧 <b>Groups:</b>
• /addchat — Set OTP group
• /autodelete &lt;minutes&gt;

🌐 Panel: Grand Panel
♻️ Numbers are permanently removed once an OTP is received
"""
    bot.reply_to(message, help_text, parse_mode="HTML")

# ---------------- DATABASE CLEANUP ----------------
def cleanup_old_otps():
    while True:
        try:
            time.sleep(3600)
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM otp_records WHERE created_at < datetime('now', '-30 days')"
                )
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
    logger.info("🚀 Starting OTP Bot — Grand Panel | Permanent number removal...")

    threading.Thread(target=run_flask,              daemon=True, name="Flask").start()
    threading.Thread(target=group_sender_worker,    daemon=True, name="GroupSender").start()
    threading.Thread(target=personal_sender_worker, daemon=True, name="PersonalSender").start()
    threading.Thread(target=otp_processor_worker,   daemon=True, name="OTPProcessor").start()
    threading.Thread(target=main_loop,              daemon=True, name="GrandPanelFetcher").start()
    threading.Thread(target=cleanup_old_otps,       daemon=True, name="Cleanup").start()
    threading.Thread(target=run_bot,                daemon=True, name="BotPoller").start()

    logger.info("✅ All services started | Panel: Grand Panel | Used-number removal: ✅")

    while True:
        time.sleep(60)
