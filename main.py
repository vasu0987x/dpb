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

import os
import telebot

# ✅ Environment Variables से वैल्यू लेंगे
BOT_TOKEN = os.getenv("BOT_TOKEN")  # Example: 8452xxxx:ABCxxx
ADMIN_ID = int(os.getenv("ADMIN_ID", "8195360535"))
USERNAME = os.getenv("USERNAME", "Vasu")
PASSWORD = os.getenv("PASSWORD", "Vasu")

bot = telebot.TeleBot(BOT_TOKEN)

DATA_FILE = "bot_data.json"
NUMBERS_DIR = "numbers"
DB_FILE = "otp_data.db"

os.makedirs(NUMBERS_DIR, exist_ok=True)

# API Config
LOGIN_URL = "http://139.99.63.204/ints/signin"
XHR_URL = "http://139.99.63.204/ints/agent/res/data_smscdr.php?fdate1=2025-11-20%2000:00:00&fdate2=2026-11-20%2023:59:59&frange=&fclient=&fnum=&fcli=&fgdate=&fgmonth=&fgrange=&fgclient=&fgnumber=&fgcli=&fg=0&sEcho=1&iColumns=9&sColumns=%2C%2C%2C%2C%2C%2C%2C%2C&iDisplayStart=0&iDisplayLength=25&mDataProp_0=0&sSearch_0=&bRegex_0=false&bSearchable_0=true&bSortable_0=true&mDataProp_1=1&sSearch_1=&bRegex_1=false&bSearchable_1=true&bSortable_1=true&mDataProp_2=2&sSearch_2=&bRegex_2=false&bSearchable_2=true&bSortable_2=true&mDataProp_3=3&sSearch_3=&bRegex_3=false&bSearchable_3=true&bSortable_3=true&mDataProp_4=4&sSearch_4=&bRegex_4=false&bSearchable_4=true&bSortable_4=true&mDataProp_5=5&sSearch_5=&bRegex_5=false&bSearchable_5=true&bSortable_5=true&mDataProp_6=6&sSearch_6=&bRegex_6=false&bSearchable_6=true&bSortable_6=true&mDataProp_7=7&sSearch_7=&bRegex_7=false&bSearchable_7=true&bSortable_7=true&mDataProp_8=8&sSearch_8=&bRegex_8=false&bSearchable_8=true&bSortable_8=false&sSearch=&bRegex=false&iSortCol_0=0&sSortDir_0=desc&iSortingCols=1&_=1763656395192" 

OTP_GROUP_IDS = ["-1002129550700"]

CHANNEL_LINK = "https://whatsapp.com/channel/0029Va5XJaU6xCSHlSwIXH1P"
BACKUP = "https://t.me/UXOTP"
DEVELOPER_ID = "@UXOTPBOT"
CODE_GROUP = "https://t.me/+9oYo5yC5HwYzZGI9"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "http://139.99.63.204/ints/login"
}
AJAX_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "http://139.99.63.204/ints/agent/SMSDashboard"
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
REQUIRED_CHANNELS = ["@ddxotp", "@vasuhub", "@Nokosxotp", "@Uxotp"]

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

def save_user_assignment(chat_id, number, country):
    """Save user number assignment"""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            # Remove old assignments for this chat_id
            cursor.execute('DELETE FROM user_assignments WHERE chat_id = ?', (chat_id,))
            # Add new assignment
            cursor.execute('''
                INSERT INTO user_assignments (chat_id, number, country)
                VALUES (?, ?, ?)
            ''', (chat_id, number, country))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to save user assignment: {e}")

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
    global data, numbers_by_country, current_country
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                numbers_by_country = data.get("numbers_by_country", {}) or {}
                current_country = data.get("current_country")
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            data = {"numbers_by_country": {}, "current_country": None}
            numbers_by_country = {}
            current_country = None
    else:
        data = {"numbers_by_country": {}, "current_country": None}
        numbers_by_country = {}
        current_country = None

def save_data():
    data["numbers_by_country"] = numbers_by_country
    data["current_country"] = current_country
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
    port = int(os.getenv("PORT", 5000))  # Render देगा PORT=10000 जैसा नंबर
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

def otp_processor_worker():
    """Dedicated worker for processing OTP records"""
    logger.info("🚀 OTP processor worker started")
    while True:
        try:
            record = otp_processing_queue.get()
            
            # Save to database
            hash_id = record.get("hash_id")
            save_otp_to_db(record, hash_id)
            
            # Format and queue group message
            msg_group, number = format_message(record, personal=False)
            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(types.InlineKeyboardButton("📱 Channel", url=CHANNEL_LINK))
            keyboard.add(types.InlineKeyboardButton("🚀 Panel", url=f"https://t.me/{DEVELOPER_ID.lstrip('@')}"))
            group_message_queue.put((msg_group, OTP_GROUP_IDS, keyboard))
            
            # Check for personal assignment
            chat_id = user_numbers.get(number)
            if chat_id:
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

def extract_otp(message: str) -> str | None:
    text = message.strip()
    # Look for explicit keywords near digits
    m = re.search(r"(?:otp|code|pin|password|verification|verif)[^\d]{0,8}([0-9][0-9\-\s]{2,10}[0-9])", text, re.I)
    if m:
        cand = re.sub(r"\D", "", m.group(1))
        if 3 <= len(cand) <= 8 and not (1900 <= int(cand) <= 2099):
            return cand

    # Reverse pattern
    m2 = re.search(r"([0-9][0-9\-\s]{2,10}[0-9])[^\w]{0,8}(?:otp|code|pin|password|verification|verif)", text, re.I)
    if m2:
        cand = re.sub(r"\D", "", m2.group(1))
        if 3 <= len(cand) <= 8 and not (1900 <= int(cand) <= 2099):
            return cand

    # Generic
    generic = re.findall(r"\b[0-9][0-9\-\s]{2,7}[0-9]\b", text)
    for g in generic:
        cand = re.sub(r"\D", "", g)
        if 3 <= len(cand) <= 8 and not (1900 <= int(cand) <= 2099):
            return cand

    return None

def mask_number(number: str) -> str:
    if len(number) <= 6:
        return number
    mid = len(number) // 2
    return number[:mid-1] + "***" + number[mid+2:]

def format_message(record, personal=False):
    number = record.get("num") or "Unknown"
    sender = record.get("cli") or "Unknown"
    message = record.get("message") or ""
    dt = record.get("dt") or ""
    country = record.get("country") or "Unknown"
    flag = country_to_flag(country)
    otp = record.get("otp") or extract_otp(message)
    otp_line = f"<b>OTP:</b> <code>{html.escape(otp)}</code>\n" if otp else ""
    
    if personal:
        formatted = (
            f"{flag} New {country} {sender} OTP Received \n\n"
            f"<blockquote>🕰 <b>Time:</b> <b>{html.escape(str(dt))}</b></blockquote>\n"
            f"<blockquote>🌎 <b>Country:</b> <b>{html.escape(country)} {flag}</b></blockquote>\n"
            f"<blockquote>📱 <b>Service:</b> <b>{html.escape(sender)}</b></blockquote>\n"
            f"<blockquote>📞 <b>Number:</b> <b>{html.escape(number)}</b></blockquote>\n"
            f"<blockquote>{otp_line}</blockquote>"
            f"<blockquote>✉️ <b>Full Message:</b></blockquote>\n"
            f"<blockquote><code>{html.escape(message)}</code></blockquote>\n\n"
            f"<blockquote>💥 <b>Powered By: @VASUHUB </b></blockquote>\n"
        )
    else:
        formatted = (
            f"{flag} New {country} {sender} OTP Received \n\n"
            f"<blockquote>🕰 <b>Time:</b> <b>{html.escape(str(dt))}</b></blockquote>\n"
            f"<blockquote>🌎 <b>Country:</b> <b>{html.escape(country)} {flag}</b></blockquote>\n"
            f"<blockquote>📱 <b>Service:</b> <b>{html.escape(sender)}</b></blockquote>\n"
            f"<blockquote>📞 <b>Number:</b> <b>{html.escape(mask_number(number))}</b></blockquote>\n"
            f"<blockquote>{otp_line}</blockquote>"
            f"<blockquote>✉️ <b>Full Message:</b></blockquote>\n"
            f"<blockquote><code>{html.escape(message)}</code></blockquote>\n\n"
        )
    return formatted, number

# ---------------- OTP FETCHER ----------------
def login():
    try:
        res = session.get("http://139.99.63.204/ints/login", headers=HEADERS, timeout=15)
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
        "username": USERNAME,
        "password": PASSWORD,
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
                    country = row[1].split("-")[0]
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

                    # Queue for processing
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
def send_random_number(chat_id, country=None, edit=False):
    if country is None:
        country = user_current_country.get(chat_id)
        if not country:
            bot.send_message(chat_id, "❌ No country selected.")
            return
    
    numbers = numbers_by_country.get(country, [])
    if not numbers:
        bot.send_message(chat_id, f"❌ No numbers for {country}.")
        return
    
    number = random.choice(numbers)
    user_current_country[chat_id] = country
    user_numbers[number] = chat_id
    
    # Save assignment to database
    save_user_assignment(chat_id, number, country)

    text = f"📞 Number for *{country}*:\n`{number}`\n\n⏳ Waiting for OTP...\n🔔 You'll get notified instantly!"
  
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("🔄 Change Number", callback_data="change_number"),
        types.InlineKeyboardButton("🌎 Change Country", callback_data="change_country")
    )
    markup.row(
        types.InlineKeyboardButton("📜 View Past OTPs", callback_data=f"view_past_{number}"),
        )
    markup.row(
        
        types.InlineKeyboardButton("📱 Code Group", url=CODE_GROUP)
    )

    if chat_id in user_messages and edit:
        try:
            bot.edit_message_text(text, chat_id, user_messages[chat_id].message_id, 
                                reply_markup=markup, parse_mode="Markdown")
        except Exception:
            msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode="Markdown")
            user_messages[chat_id] = msg
    else:
        msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode="Markdown")
        user_messages[chat_id] = msg

@bot.message_handler(commands=["start"])
def start(message):
    chat_id = message.chat.id
    username = message.from_user.username or message.from_user.first_name
    
    # Update active user in database
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

# Replace your callback handlers section with this fixed version

# ---------------- CALLBACK HANDLERS (ORDER MATTERS!) ----------------

# IMPORTANT: Register specific handlers BEFORE general handlers

@bot.callback_query_handler(func=lambda call: call.data.startswith("addto_"))
def callback_addto(call):
    """Handle admin country selection for number uploads"""
    if call.from_user.id != ADMIN_ID:
        return bot.answer_callback_query(call.id, "❌ Not authorized")
    
    numbers = temp_uploads.get(call.from_user.id, [])
    if not numbers:
        return bot.answer_callback_query(call.id, "❌ No uploaded numbers found")

    choice = call.data[6:]  # Remove "addto_" prefix
    
    if choice == "new":
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "✏️ Send new country name:")
        bot.register_next_step_handler(call.message, save_new_country, numbers)
    else:
        # Add to existing country
        existing = numbers_by_country.get(choice, [])
        merged = list(dict.fromkeys(existing + numbers))
        numbers_by_country[choice] = merged
        save_data()
        
        # Save to file
        file_path = os.path.join(NUMBERS_DIR, f"{choice}.txt")
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(merged))
            logger.info(f"✅ Saved {len(merged)} numbers to {choice}")
        except Exception as e:
            logger.error(f"Failed to write numbers file: {e}")

        # Notify admin
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

        # Clear temp storage
        temp_uploads.pop(call.from_user.id, None)


@bot.callback_query_handler(func=lambda call: call.data.startswith("user_select_"))
def handle_country_selection(call):
    """Handle user country selection"""
    chat_id = call.message.chat.id
    if call.from_user.id != ADMIN_ID:
        active_users.add(chat_id)
        update_active_user(chat_id, call.from_user.username)
    
    country = call.data[12:]  # Remove "user_select_" prefix
    user_current_country[chat_id] = country
    bot.answer_callback_query(call.id, f"Selected {country}")
    send_random_number(chat_id, country, edit=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith("view_past_"))
def handle_view_past(call):
    """Handle viewing past OTPs"""
    chat_id = call.message.chat.id
    number = call.data[10:]  # Remove "view_past_" prefix
    
    past_otps = get_past_otps(number, limit=10)
    
    if not past_otps:
        bot.answer_callback_query(call.id, "❌ No past OTPs found for this number", show_alert=True)
        return
    
    text = f"📜 <b>Past OTPs for {mask_number(number)}</b>\n\n"
    
    for i, otp_record in enumerate(past_otps[:10], 1):
        otp_code = otp_record.get('otp_code') or 'N/A'
        sender = otp_record.get('sender') or 'Unknown'
        timestamp = otp_record.get('timestamp') or 'Unknown'
        country = otp_record.get('country') or 'Unknown'
        flag = country_to_flag(country)
        
        text += (
            f"{i}. {flag} <b>{sender}</b>\n"
            f"   🔢 OTP: <code>{otp_code}</code>\n"
            f"   🕐 Time: {timestamp}\n\n"
        )
    
    text += f"<i>Showing last {len(past_otps)} OTPs</i>"
    
    try:
        bot.send_message(chat_id, text, parse_mode="HTML")
        bot.answer_callback_query(call.id, "✅ Past OTPs sent!")
    except Exception as e:
        logger.error(f"Failed to send past OTPs: {e}")
        bot.answer_callback_query(call.id, "❌ Error fetching OTPs", show_alert=True)


@bot.callback_query_handler(func=lambda call: call.data in ["change_number", "change_country"])
def handle_change_actions(call):
    """Handle change number and change country actions"""
    chat_id = call.message.chat.id
    if call.from_user.id != ADMIN_ID:
        active_users.add(chat_id)
        update_active_user(chat_id, call.from_user.username)
    
    if call.data == "change_number":
        bot.answer_callback_query(call.id, "🔄 Getting new number...")
        send_random_number(chat_id, user_current_country.get(chat_id), edit=True)
        
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
            
            # Total OTPs
            cursor.execute('SELECT COUNT(*) FROM otp_records')
            total_otps = cursor.fetchone()[0]
            
            # OTPs today
            cursor.execute('''
                SELECT COUNT(*) FROM otp_records 
                WHERE DATE(created_at) = DATE('now')
            ''')
            otps_today = cursor.fetchone()[0]
            
            # Active users
            cursor.execute('SELECT COUNT(*) FROM active_users')
            active_count = cursor.fetchone()[0]
            
            # Queue sizes
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
                f"📞 <b>Total Numbers:</b> {sum(len(v) for v in numbers_by_country.values())}"
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

@bot.callback_query_handler(func=lambda call: call.data.startswith("addto_"))
def callback_addto(call):
    if call.from_user.id != ADMIN_ID:
        return bot.answer_callback_query(call.id, "❌ Not authorized")
    numbers = temp_uploads.get(call.from_user.id, [])
    if not numbers:
        return bot.answer_callback_query(call.id, "❌ No uploaded numbers found")

    choice = call.data[6:]
    if choice == "new":
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
        except Exception as e:
            logger.error(f"Failed to write numbers file: {e}")

        try:
            bot.edit_message_text(f"✅ Added {len(numbers)} numbers to *{choice}*",
                                call.message.chat.id, call.message.message_id, parse_mode="Markdown")
        except Exception as e:
            logger.debug(f"edit_message_text failed: {e}")
            try:
                bot.send_message(ADMIN_ID, f"✅ Added {len(numbers)} numbers to {choice}")
            except Exception as ex:
                logger.error(f"Failed to send admin confirmation: {ex}")

        temp_uploads.pop(call.from_user.id, None)

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

❓ /adminhelp - Show this help menu
"""
    bot.reply_to(message, help_text, parse_mode="HTML")

# ---------------- DATABASE CLEANUP ----------------
def cleanup_old_otps():
    """Periodically clean up old OTP records to keep database size manageable"""
    while True:
        try:
            time.sleep(3600)  # Run every hour
            with get_db() as conn:
                cursor = conn.cursor()
                # Keep only last 30 days of OTPs
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
    
    # Start Flask
    threading.Thread(target=run_flask, daemon=True, name="Flask").start()
    
    # Start message workers
    threading.Thread(target=group_sender_worker, daemon=True, name="GroupSender").start()
    threading.Thread(target=personal_sender_worker, daemon=True, name="PersonalSender").start()
    threading.Thread(target=otp_processor_worker, daemon=True, name="OTPProcessor").start()
    
    # Start OTP fetcher
    threading.Thread(target=main_loop, daemon=True, name="OTPFetcher").start()
    
    # Start cleanup worker
    threading.Thread(target=cleanup_old_otps, daemon=True, name="Cleanup").start()
    
    # Start bot
    threading.Thread(target=run_bot, daemon=True, name="BotPoller").start()
    
    logger.info("✅ All services started successfully!")
    
    # Keep main thread alive
    while True:

        time.sleep(60)

