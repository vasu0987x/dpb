import telebot
from telebot import types
import json
import os
import random
from flask import Flask, Response
import threading
import requests
import re
import html
import phonenumbers
import pycountry
import time
import sqlite3
from queue import Queue
from datetime import datetime, timedelta
from collections import deque

# ==================== CONFIG ====================
BOT_TOKEN = os.getenv("BOT_TOKEN") or "8504239136:AAG1m06XgmZC_4dyvk5IMlj4GAUlv-Ae7-4"
ADMIN_ID = 8195360535
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

DATA_FILE = "bot_data.json"
NUMBERS_DIR = "numbers"
DB_FILE = "bot_database.db"
os.makedirs(NUMBERS_DIR, exist_ok=True)

# API Config
API_TOKEN = os.getenv("API_TOKEN") or "Qk5VRkdBUzR9aGKAZnBwe4WRiEpIhFR0iGJ5U1uRa11fhGpGdmhoQg=="
BASE_URL = "http://147.135.212.197/crapi/had"
OTP_GROUP_IDS = ["-1003633481131"]
AUTO_DELETE_MINUTES = 0  # 0 means disabled
BACKUP = "https://t.me/Vasuhub"
CHANNEL_LINK = "https://t.me/uxotpbot"

# ==================== QUEUES ====================
group_queue = Queue(maxsize=1000)
personal_queue = Queue(maxsize=5000)
seen_messages = deque(maxlen=50000)

# ==================== REGEX PATTERNS ====================
KEYWORD_REGEX = re.compile(r"(otp|code|pin|password|verify)[^\d]{0,10}(\d[\d\-]{3,8})", re.I)
REVERSE_REGEX = re.compile(r"(\d[\d\-]{3,8})[^\w]{0,10}(otp|code|pin|password|verify)", re.I)
GENERIC_REGEX = re.compile(r"\d{2,4}[-]?\d{2,4}")
UNICODE_CLEAN = re.compile(r"[\u200f\u200e\u202a-\u202e]")

# ==================== DATABASE SETUP ====================
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS user_numbers
                 (number TEXT PRIMARY KEY, chat_id INTEGER, country TEXT, assigned_at REAL)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS user_stats
                 (chat_id INTEGER PRIMARY KEY, total_otps INTEGER DEFAULT 0, 
                  last_otp REAL, joined_at REAL)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS message_cache
                 (msg_id TEXT PRIMARY KEY, created_at REAL)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS past_otps_cache
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  number TEXT,
                  sender TEXT,
                  message TEXT,
                  otp TEXT,
                  timestamp TEXT,
                  received_at REAL)''')
    
    c.execute('''CREATE INDEX IF NOT EXISTS idx_number ON past_otps_cache(number)''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_received_at ON past_otps_cache(received_at)''')
    
    conn.commit()
    conn.close()

init_db()

# ==================== DATA STORAGE ====================
data = {}
numbers_by_country = {}
current_country = None
user_messages = {}
user_current_country = {}
temp_uploads = {}
last_change_time = {}
active_users = set()
past_otp_fetch_cooldown = {}
REQUIRED_CHANNELS = ["@Vasuhub", "@DDxOTP", "@nokosxotps", "@DDXBACKUP"]

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

# ==================== DATA FUNCTIONS ====================
def load_data():
    global data, numbers_by_country, current_country, OTP_GROUP_IDS, AUTO_DELETE_MINUTES
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
            numbers_by_country = data.get("numbers_by_country", {})
            current_country = data.get("current_country")
            OTP_GROUP_IDS = data.get("otp_groups", ["-1003462043194"])
            AUTO_DELETE_MINUTES = data.get("auto_delete_minutes", 0)
    else:
        data = {"numbers_by_country": {}, "current_country": None, "otp_groups": ["-1003462043194"], "auto_delete_minutes": 0}
        numbers_by_country = {}
        current_country = None

def save_data():
    data["numbers_by_country"] = numbers_by_country
    data["current_country"] = current_country
    data["otp_groups"] = OTP_GROUP_IDS
    data["auto_delete_minutes"] = AUTO_DELETE_MINUTES
    with open(DATA_FILE, "w") as f:
        json.dump(data, f)

load_data()

# ==================== DATABASE HELPERS ====================
def get_chat_by_number(number):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT chat_id FROM user_numbers WHERE number=?", (number,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

def get_number_by_chat(chat_id):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT number FROM user_numbers WHERE chat_id=? ORDER BY assigned_at DESC LIMIT 1", (chat_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

def assign_number(number, chat_id, country):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO user_numbers VALUES (?, ?, ?, ?)",
              (number, chat_id, country, time.time()))
    conn.commit()
    conn.close()

def increment_user_stats(chat_id):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("""INSERT INTO user_stats (chat_id, total_otps, last_otp, joined_at) 
                 VALUES (?, 1, ?, ?) 
                 ON CONFLICT(chat_id) DO UPDATE SET 
                 total_otps = total_otps + 1, last_otp = ?""",
              (chat_id, time.time(), time.time(), time.time()))
    conn.commit()
    conn.close()

def cache_past_otp(number, sender, message, otp, timestamp):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute("""INSERT INTO past_otps_cache 
                     (number, sender, message, otp, timestamp, received_at)
                     VALUES (?, ?, ?, ?, ?, ?)""",
                  (number, sender, message, otp, timestamp, time.time()))
        conn.commit()
    except:
        pass
    conn.close()

def get_cached_past_otps(number, limit=50):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("""SELECT sender, message, otp, timestamp 
                 FROM past_otps_cache 
                 WHERE number=? 
                 ORDER BY received_at DESC 
                 LIMIT ?""", (number, limit))
    results = c.fetchall()
    conn.close()
    return results

def is_message_seen(msg_id):
    if msg_id in seen_messages:
        return True
    seen_messages.append(msg_id)
    return False

def clean_old_cache():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    cutoff = time.time() - 86400
    c.execute("DELETE FROM message_cache WHERE created_at < ?", (cutoff,))
    otp_cutoff = time.time() - (7 * 86400)
    c.execute("DELETE FROM past_otps_cache WHERE received_at < ?", (otp_cutoff,))
    conn.commit()
    conn.close()

# ==================== FLASK ====================
app = Flask(__name__)

@app.route("/")
def index():
    return "🚀 OTP Bot v2.0 Running"

@app.route("/health")
def health():
    return Response(f"OK - Queue: G={group_queue.qsize()} P={personal_queue.qsize()}", status=200)

# ==================== HELPER FUNCTIONS ====================
def extract_otp(message: str) -> str | None:
    message = UNICODE_CLEAN.sub("", message)
    
    match = KEYWORD_REGEX.search(message)
    if match:
        return re.sub(r"\D", "", match.group(2))
    
    match = REVERSE_REGEX.search(message)
    if match:
        return re.sub(r"\D", "", match.group(1))
    
    match = GENERIC_REGEX.findall(message)
    if match:
        return re.sub(r"\D", "", match[0])
    
    return None

def mask_number(number: str) -> str:
    number = number.strip()
    if len(number) <= 4:
        return number
    return f"{number[:2]}DDX{number[-4:]}"

def country_from_number(number: str) -> tuple[str, str]:
    try:
        parsed = phonenumbers.parse("+" + number)
        region = phonenumbers.region_code_for_number(parsed)
        if not region:
            return "Unknown", "🌍"
        country_obj = pycountry.countries.get(alpha_2=region)
        if not country_obj:
            return "Unknown", "🌍"
        flag = "".join([chr(127397 + ord(c)) for c in region])
        return country_obj.name, flag
    except:
        return "Unknown", "🌍"

def get_country_code(country_name: str) -> str:
    try:
        country = pycountry.countries.lookup(country_name)
        return country.alpha_2.upper()
    except:
        return country_name[:2].upper()

def get_service_code(sender: str) -> str:
    for service, code in SERVICE_CODES.items():
        if service.lower() in sender.lower():
            return code
    return sender[:2].upper() if len(sender) >= 2 else sender.upper()

def delete_message_safe(chat_id, message_id):
    try:
        bot.delete_message(chat_id, message_id)
        print(f"🗑️ Auto-deleted message {message_id} from {chat_id}", flush=True)
    except Exception as e:
        print(f"Failed to delete message: {e}", flush=True)

# ==================== MESSAGE FORMATTERS ====================
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
    try:
        country = pycountry.countries.lookup(country_name)
        code = country.alpha_2.upper()
    except:
        return "🌍"
    regular_flag = "".join(chr(127397 + ord(c)) for c in code)
    emoji_id = flag_overrides.get(code)
    if emoji_id:
        return f'<tg-emoji emoji-id="{emoji_id}">{regular_flag}</tg-emoji>'
    return regular_flag

def get_service_emoji(sender: str) -> str:
    s = sender.lower()
    if "whatsapp" in s:
        return '<tg-emoji emoji-id="5334998226636390258">📱</tg-emoji>'
    elif "telegram" in s:
        return '<tg-emoji emoji-id="5330237710655306682">✈️</tg-emoji>'
    elif "instagram" in s:
        return '<tg-emoji emoji-id="5319160079465857105">📸</tg-emoji>'
    elif "facebook" in s:
        return '<tg-emoji emoji-id="5323261730283863478">👤</tg-emoji>'
    return '<tg-emoji emoji-id="6125390694363175728">🌐</tg-emoji>'

def format_group_message(record):
    number = record.get("num") or "Unknown"
    sender = record.get("cli") or "Unknown"
    message = record.get("message") or ""

    country, _ = country_from_number(number)
    country_code = get_country_code(country)
    flag = get_flag(country)
    service_code = get_service_code(sender)
    service_emoji = get_service_emoji(sender)
    masked = mask_number(number)
    otp = extract_otp(message)

    formatted = (
        f'<tg-emoji emoji-id="5382357040008021292">⚡</tg-emoji> '
        f'{flag} <b>{country_code}</b> | <code>{masked}</code> | '
        f'{service_emoji} <b>{service_code}</b> '
        
    )

    msg_hash = hash(f"{number}{message}{time.time()}")
    cache_full_message(msg_hash, number, sender, message)

    keyboard = {
        "inline_keyboard": [
            *([[{"text": f"{otp}", "callback_data": f"copy_{otp}", "icon_custom_emoji_id": "5443038326535759644"}]] if otp else []),
            [{"text": "View Full", "callback_data": f"fullsms_{msg_hash}", "icon_custom_emoji_id": "5253742260054409879"}],
            [
                {"text": "Panel", "url": CHANNEL_LINK, "icon_custom_emoji_id": "5330237710655306682"},
                {"text": "Channel", "url": BACKUP, "icon_custom_emoji_id": "6125390694363175728"}
            ]
        ]
    }

    return formatted, keyboard

def format_personal_message(record):
    number = record.get("num") or "Unknown"
    sender = record.get("cli") or "Unknown"
    message = record.get("message") or ""

    country, _ = country_from_number(number)
    country_code = get_country_code(country)
    flag = get_flag(country)
    service_emoji = get_service_emoji(sender)
    service_code = get_service_code(sender)
    masked = mask_number(number)
    otp = extract_otp(message) or "N/A"

    return (
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


def cache_full_message(msg_hash, number, sender, message):
    """Cache full message for view full button"""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS full_messages
                     (msg_hash INTEGER PRIMARY KEY, number TEXT, sender TEXT, 
                      message TEXT, created_at REAL)""")
        c.execute("""INSERT OR REPLACE INTO full_messages VALUES (?, ?, ?, ?, ?)""",
                  (msg_hash, number, sender, message, time.time()))
        conn.commit()
    except:
        pass
    conn.close()

def get_full_message(msg_hash):
    """Get full message from cache"""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    try:
        c.execute("SELECT message FROM full_messages WHERE msg_hash=?", (msg_hash,))
        result = c.fetchone()
        conn.close()
        return result[0] if result else None
    except:
        conn.close()
        return None

def format_personal_message(record):
    """Format message for personal DM"""
    number = record.get("num") or "Unknown"
    sender = record.get("cli") or "Unknown"
    message = record.get("message") or ""
    
    country, flag = country_from_number(number)
    country_code = get_country_code(country)
    service_code = get_service_code(sender)
    masked = mask_number(number)
    
    otp = extract_otp(message)
    
    formatted = (
        f"{flag} {country_code} | {masked} | {service_code}\n\n"
        f"<b>Full Number:</b> <code>{html.escape(number)}</code>\n"
        f"<b>Service:</b> {html.escape(sender)}\n\n"
        f"<b>Message:</b>\n<code>{html.escape(message[:200])}</code>"
    )
    
    return formatted

# ==================== THREAD 1: OTP SCRAPER ====================
def otp_scraper_thread():
    print("🟢 OTP Scraper Started", flush=True)
    
    while True:
        try:
            response = requests.get(
                f"{BASE_URL}/viewstats",
                params={
                    "token": API_TOKEN,
                    "dt1": "1970-01-01 00:00:00",
                    "dt2": "2099-12-31 23:59:59",
                    "records": 10
                },
                timeout=8
            )
            
            # Empty response check
            if not response.text.strip():
                print("⚠️ API returned empty response - token invalid ya server down", flush=True)
                time.sleep(5)
                continue
            
            try:
                stats = response.json()
            except Exception:
                print(f"⚠️ API non-JSON response: {response.text[:100]}", flush=True)
                time.sleep(5)
                continue
            
            if stats.get("status") == "error":
                print(f"⚠️ API error: {stats.get('msg', 'Unknown')}", flush=True)
                time.sleep(10)
                continue
            
            if stats.get("status") == "success":
                for record in stats["data"]:
                    msg_id = f"{record.get('dt')}_{record.get('num')}_{record.get('message', '')[:50]}"
                    
                    if is_message_seen(msg_id):
                        continue
                    
                    number = str(record.get("num", "")).lstrip("0").lstrip("+")
                    sender = record.get("cli", "Unknown")
                    message = record.get("message", "")
                    timestamp = record.get("dt", "")
                    otp = extract_otp(message)
                    
                    cache_past_otp(number, sender, message, otp, timestamp)
                    
                    try:
                        group_queue.put_nowait((record, time.time()))
                        print(f"📤 Queued: {number} | {sender} | OTP: {otp or 'N/A'}", flush=True)
                    except:
                        print("⚠️ Group queue full!", flush=True)
                    
                    chat_id = get_chat_by_number(number)
                    if chat_id:
                        try:
                            personal_queue.put_nowait((record, chat_id, time.time()))
                        except:
                            print(f"⚠️ Personal queue full for {chat_id}!", flush=True)
            
            time.sleep(3)
            
        except requests.exceptions.Timeout:
            print("⚠️ API timeout - retrying...", flush=True)
            time.sleep(3)
        except requests.exceptions.ConnectionError:
            print("⚠️ API connection error - retrying...", flush=True)
            time.sleep(5)
        except Exception as e:
            print(f"❌ Scraper error: {e}", flush=True)
            time.sleep(2)


# ==================== THREAD 2: GROUP SENDER ====================
def group_sender_thread():
    print("🟢 Group Sender Started", flush=True)
    
    while True:
        try:
            record, fetch_time = group_queue.get()
            
            msg, kb = format_group_message(record)
            
            # Send to all configured groups
            for group_id in OTP_GROUP_IDS:
                payload = {
                    "chat_id": group_id,
                    "text": msg[:4000],
                    "parse_mode": "HTML",
                    "reply_markup": json.dumps(kb)
                }
                
                response = requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json=payload,
                    timeout=5
                )
                
                if response.status_code == 200:
                    delay = time.time() - fetch_time
                    print(f"✅ Group sent (delay: {delay:.2f}s)", flush=True)
                    
                    # Schedule auto-delete if enabled
                    if AUTO_DELETE_MINUTES > 0:
                        result = response.json()
                        if result.get("ok"):
                            message_id = result["result"]["message_id"]
                            threading.Timer(
                                AUTO_DELETE_MINUTES * 60,
                                delete_message_safe,
                                args=(group_id, message_id)
                            ).start()
                            
                elif response.status_code == 429:
                    retry_after = response.json().get("parameters", {}).get("retry_after", 2)
                    print(f"⏳ Rate limited, waiting {retry_after}s", flush=True)
                    time.sleep(retry_after)
                    group_queue.put((record, fetch_time))
                else:
                    print(f"❌ Group send failed: {response.status_code}", flush=True)
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Group sender error: {e}", flush=True)
            time.sleep(1)

# ==================== THREAD 3: PERSONAL DM SENDER ====================
def personal_sender_thread():
    print("🟢 Personal Sender Started", flush=True)
    
    while True:
        try:
            record, chat_id, fetch_time = personal_queue.get()
            
            msg = format_personal_message(record)
            
            payload = {
                "chat_id": chat_id,
                "text": msg[:4000],
                "parse_mode": "HTML"
            }
            
            response = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json=payload,
                timeout=5
            )
            
            if response.status_code == 200:
                increment_user_stats(chat_id)
                delay = time.time() - fetch_time
                print(f"✅ DM sent to {chat_id} (delay: {delay:.2f}s)", flush=True)
            elif response.status_code == 429:
                retry_after = response.json().get("parameters", {}).get("retry_after", 1)
                time.sleep(retry_after)
                personal_queue.put((record, chat_id, fetch_time))
            else:
                print(f"❌ DM failed for {chat_id}: {response.status_code}", flush=True)
            
            time.sleep(0.2)
            
        except Exception as e:
            print(f"❌ Personal sender error: {e}", flush=True)
            time.sleep(1)

# ==================== CALLBACK HANDLERS ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("copy_"))
def handle_copy_otp(call):
    otp = call.data[5:]
    try:
        bot.answer_callback_query(call.id, f"✅ OTP: {otp}\nClick to dismiss!", show_alert=True)
    except Exception as e:
        print(f"Failed to show OTP: {e}", flush=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("fullsms_"))
def handle_full_sms(call):
    try:
        msg_hash = int(call.data[8:])
        message = get_full_message(msg_hash)
        
        if message:
            bot.answer_callback_query(call.id, message[:200], show_alert=True)
        else:
            bot.answer_callback_query(call.id, "❌ Message not found", show_alert=True)
    except Exception as e:
        print(f"Failed to fetch full SMS: {e}", flush=True)
        bot.answer_callback_query(call.id, "❌ Error loading message", show_alert=True)

# ==================== ADMIN COMMANDS ====================


@bot.message_handler(commands=["addflag"])
def add_flag(message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.strip().split()
    if len(parts) != 3:
        return bot.reply_to(message, "❌ Usage: <code>/addflag IN 5222300011366200403</code>", parse_mode="HTML")
    _, code, emoji_id = parts
    code = code.upper()
    flag_overrides[code] = emoji_id
    save_flag_overrides()
    regular_flag = "".join(chr(127397 + ord(c)) for c in code)
    preview = f'<tg-emoji emoji-id="{emoji_id}">{regular_flag}</tg-emoji>'
    bot.reply_to(message, f"✅ Flag set!\n{preview} <b>{code}</b>", parse_mode="HTML")

@bot.message_handler(commands=["removeflag"])
def remove_flag(message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.strip().split()
    if len(parts) != 2:
        return bot.reply_to(message, "❌ Usage: <code>/removeflag IN</code>", parse_mode="HTML")
    code = parts[1].upper()
    if code in flag_overrides:
        del flag_overrides[code]
        save_flag_overrides()
        bot.reply_to(message, f"✅ Removed flag for <b>{code}</b>", parse_mode="HTML")
    else:
        bot.reply_to(message, f"❌ No override for <b>{code}</b>", parse_mode="HTML")

@bot.message_handler(commands=["listflags"])
def list_flags(message):
    if message.from_user.id != ADMIN_ID:
        return
    if not flag_overrides:
        return bot.reply_to(message, "📭 No premium flags set.")
    text = "🏳 <b>Premium Flags:</b>\n\n"
    for code, emoji_id in sorted(flag_overrides.items()):
        regular_flag = "".join(chr(127397 + ord(c)) for c in code)
        preview = f'<tg-emoji emoji-id="{emoji_id}">{regular_flag}</tg-emoji>'
        text += f"{preview} <b>{code}</b> → <code>{emoji_id}</code>\n"
    bot.reply_to(message, text, parse_mode="HTML")


@bot.message_handler(content_types=["document"])
def handle_document(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized")
    
    if not message.document.file_name.endswith(".txt"):
        return bot.reply_to(message, "❌ Upload .txt file only")
    
    file_info = bot.get_file(message.document.file_id)
    downloaded_file = bot.download_file(file_info.file_path)
    numbers = [line.strip().lstrip("0").lstrip("+") 
               for line in downloaded_file.decode("utf-8").splitlines() if line.strip()]
    
    if not numbers:
        return bot.reply_to(message, "❌ File is empty")
    
    temp_uploads[message.from_user.id] = numbers
    
    markup = types.InlineKeyboardMarkup()
    for country in sorted(numbers_by_country.keys()):
        markup.add(types.InlineKeyboardButton(country, callback_data=f"addto_{country}"))
    markup.add(types.InlineKeyboardButton("➕ New Country", callback_data="addto_new"))
    
    bot.reply_to(message, f"📂 Received {len(numbers)} numbers. Select country:", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("addto_"))
def callback_addto(call):
    if call.from_user.id != ADMIN_ID:
        return bot.answer_callback_query(call.id, "❌ Not authorized")
    
    numbers = temp_uploads.get(call.from_user.id, [])
    if not numbers:
        return bot.answer_callback_query(call.id, "❌ No numbers found")
    
    choice = call.data[6:]
    
    if choice == "new":
        bot.send_message(call.message.chat.id, "✏️ Send new country name:")
        bot.register_next_step_handler(call.message, save_new_country, numbers)
    else:
        existing = numbers_by_country.get(choice, [])
        merged = list(set(existing + numbers))
        numbers_by_country[choice] = merged
        save_data()
        
        file_path = os.path.join(NUMBERS_DIR, f"{choice}.txt")
        with open(file_path, "w") as f:
            f.write("\n".join(merged))
        
        bot.edit_message_text(
            f"✅ Added {len(numbers)} numbers to <b>{choice}</b>",
            call.message.chat.id,
            call.message.message_id
        )
        temp_uploads.pop(call.from_user.id, None)

def save_new_country(message, numbers):
    country = message.text.strip()
    if not country:
        return bot.reply_to(message, "❌ Invalid country name")
    
    numbers_by_country[country] = numbers
    save_data()
    
    file_path = os.path.join(NUMBERS_DIR, f"{country}.txt")
    with open(file_path, "w") as f:
        f.write("\n".join(numbers))
    
    bot.reply_to(message, f"✅ Saved {len(numbers)} numbers under <b>{country}</b>")
    temp_uploads.pop(message.from_user.id, None)

@bot.message_handler(commands=["addchat"])
def add_chat(message):
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
    save_data()
    
    response = f"✅ <b>OTP Group Updated!</b>\n\n"
    response += f"📱 <b>New Group:</b> {html.escape(chat_title)}\n"
    response += f"🆔 <b>Chat ID:</b> <code>{chat_id}</code>\n\n"
    
    if old_groups:
        response += f"🗑️ <b>Removed Groups:</b> {len(old_groups)}\n"
    
    response += "\n✅ All future OTPs will be sent to this group only!"
    
    bot.reply_to(message, response)
    print(f"✅ OTP group updated: {chat_title} ({chat_id})", flush=True)

@bot.message_handler(commands=["autodelete"])
def set_autodelete(message):
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
            f"<b>To disable:</b> /autodelete 0"
        )
    
    try:
        minutes = int(args[1])
        if minutes < 0:
            return bot.reply_to(message, "❌ Minutes must be 0 or positive.")
        
        AUTO_DELETE_MINUTES = minutes
        save_data()
        
        if minutes == 0:
            response = "✅ <b>Auto-Delete Disabled</b>\n\n"
            response += "Group messages will no longer be auto-deleted."
        else:
            response = f"✅ <b>Auto-Delete Enabled</b>\n\n"
            response += f"⏱️ Group messages will be deleted after <b>{minutes} minute(s)</b>"
        
        bot.reply_to(message, response)
        print(f"✅ Auto-delete set to {minutes} minutes", flush=True)
        
    except ValueError:
        bot.reply_to(message, "❌ Invalid number. Use: /autodelete &lt;minutes&gt;")

@bot.message_handler(commands=["adminhelp"])
def admin_help(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    help_text = """
🔧 <b>Admin Commands:</b>

📁 <b>File Management:</b>
• Upload .txt file - Add numbers
• /setcountry &lt;name&gt; - Set current country
• /deletecountry &lt;name&gt; - Delete country
• /cleannumbers &lt;name&gt; - Clear numbers
• /listcountries - View all countries

📊 <b>Statistics:</b>
• /stats - Bot statistics
• /usercount - Total users

📢 <b>Communication:</b>
• /broadcast - Send message to all users

🔧 <b>Group Management:</b>
• /addchat - Add current chat as OTP group
• /autodelete &lt;minutes&gt; - Set auto-delete timer (0 to disable)

🧹 <b>Maintenance:</b>
• /clearcache - Clear past OTP cache
"""
    bot.reply_to(message, help_text)

@bot.message_handler(commands=["stats"])
def bot_stats(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM past_otps_cache")
    cache_count = c.fetchone()[0]
    conn.close()
    
    stats_text = f"""
📊 <b>Bot Statistics:</b>

👥 Active Users: {len(active_users)}
📥 Group Queue: {group_queue.qsize()}
📨 Personal Queue: {personal_queue.qsize()}
💾 Cached Messages: {len(seen_messages)}
💿 Past OTPs Cache: {cache_count}
🌍 Countries: {len(numbers_by_country)}
📞 Total Numbers: {sum(len(v) for v in numbers_by_country.values())}
🗑️ Auto-Delete: {'Enabled (' + str(AUTO_DELETE_MINUTES) + ' min)' if AUTO_DELETE_MINUTES > 0 else 'Disabled'}
📡 OTP Groups: {len(OTP_GROUP_IDS)}
"""
    bot.reply_to(message, stats_text)

@bot.message_handler(commands=["broadcast"])
def broadcast_start(message):
    if message.from_user.id != ADMIN_ID:
        return
    msg = bot.reply_to(message, "✉️ Send broadcast message:")
    bot.register_next_step_handler(msg, broadcast_message)

def broadcast_message(message):
    text = message.text
    success = fail = 0
    
    for user_id in active_users:
        try:
            bot.send_message(user_id, f"📢 <b>Broadcast:</b>\n\n{text}")
            success += 1
            time.sleep(0.05)
        except:
            fail += 1
    
    bot.reply_to(message, f"✅ Sent: {success}\n❌ Failed: {fail}")

@bot.message_handler(commands=["clearcache"])
def clear_cache(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("DELETE FROM past_otps_cache")
    deleted = c.rowcount
    conn.commit()
    conn.close()
    
    bot.reply_to(message, f"✅ Cleared {deleted} cached OTPs")

# ==================== USER COMMANDS ====================
@bot.message_handler(commands=["start"])
def start(message):
    chat_id = message.chat.id
    
    if message.from_user.id == ADMIN_ID:
        bot.send_message(chat_id, "👋 Welcome Admin! Use /adminhelp")
        return
    
    active_users.add(chat_id)
    
    not_joined = []
    for channel in REQUIRED_CHANNELS:
        try:
            member = bot.get_chat_member(channel, chat_id)
            if member.status not in ["member", "creator", "administrator"]:
                not_joined.append(channel)
        except:
            not_joined.append(channel)
    
    if not_joined:
        markup = types.InlineKeyboardMarkup()
        for ch in not_joined:
            markup.add(types.InlineKeyboardButton(f"Join {ch}", url=f"https://t.me/{ch[1:]}"))
        markup.add(types.InlineKeyboardButton("✅ Verify", callback_data="verify_join"))
        bot.send_message(chat_id, "❌ Join required channels first:", reply_markup=markup)
        return
    
    if not numbers_by_country:
        bot.send_message(chat_id, "❌ No countries available")
        return
    
    markup = types.InlineKeyboardMarkup()
    for country in sorted(numbers_by_country.keys()):
        count = len(numbers_by_country[country])
        markup.add(types.InlineKeyboardButton(
            f"{country} ({count} numbers)", 
            callback_data=f"user_select_{country}"
        ))
    
    msg = bot.send_message(
        chat_id,
        "🌍 <b>Select Country:</b>\n\n"
        "⚡️ Fast delivery\n"
        "🔒 Secure numbers\n"
        "♻️ Change anytime",
        reply_markup=markup
    )
    user_messages[chat_id] = msg

@bot.message_handler(commands=["mystats"])
def my_stats(message):
    chat_id = message.chat.id
    
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT total_otps, last_otp FROM user_stats WHERE chat_id=?", (chat_id,))
    result = c.fetchone()
    conn.close()
    
    if result:
        total, last = result
        last_time = datetime.fromtimestamp(last).strftime("%Y-%m-%d %H:%M:%S")
        stats_text = f"""
📊 <b>Your Statistics:</b>

📩 Total OTPs: {total}
🕐 Last OTP: {last_time}
⚡️ Status: Active
"""
    else:
        stats_text = "📊 No OTPs received yet!"
    
    bot.reply_to(message, stats_text)

@bot.message_handler(commands=["help"])
def help_command(message):
    help_text = """
📚 <b>Bot Commands:</b>

/start - Get a new number
/mystats - View your statistics
/help - Show this help message

<b>Features:</b>
• Instant OTP delivery
• View past OTPs
• Change number anytime
• Multiple countries
"""
    bot.reply_to(message, help_text)

def send_random_number(chat_id, country=None, edit=False):
    """Assign random number to user"""
    now = time.time()
    
    if chat_id in last_change_time and now - last_change_time[chat_id] < 10:
        wait = 10 - int(now - last_change_time[chat_id])
        bot.send_message(chat_id, f"⏳ Wait {wait}s before changing number")
        return
    
    last_change_time[chat_id] = now
    
    if country is None:
        country = user_current_country.get(chat_id)
        if not country:
            bot.send_message(chat_id, "❌ No country selected")
            return
    
    numbers = numbers_by_country.get(country, [])
    if not numbers:
        bot.send_message(chat_id, f"❌ No numbers for {country}")
        return
    
    number = random.choice(numbers).lstrip("0").lstrip("+")
    user_current_country[chat_id] = country
    assign_number(number, chat_id, country)
    
    country_info, flag = country_from_number(number)
    
    text = f"""
{flag} <b>Your Number ({country}):</b>

📞 <code>{number}</code>

⏳ <b>Waiting for OTP...</b>
🔔 You'll get notified instantly!
"""
    
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("🔄 Change Number", callback_data="change_number"),
        types.InlineKeyboardButton("🌍 Change Country", callback_data="change_country")
    )
    markup.row(
        types.InlineKeyboardButton("📜 View Past OTPs", callback_data=f"view_past_{number}")
    )
    markup.row(
        types.InlineKeyboardButton("📢 OTP Group", url=f"https://t.me/+aXIW_HL5DCdhNzhl")
    )

    if chat_id in user_messages and edit:
        try:
            bot.edit_message_text(
                text,
                chat_id,
                user_messages[chat_id].message_id,
                reply_markup=markup
            )
        except:
            msg = bot.send_message(chat_id, text, reply_markup=markup)
            user_messages[chat_id] = msg
    else:
        msg = bot.send_message(chat_id, text, reply_markup=markup)
        user_messages[chat_id] = msg

def fetch_past_otps(chat_id, number):
    """Fetch and display past OTPs"""
    now = time.time()
    if chat_id in past_otp_fetch_cooldown:
        time_passed = now - past_otp_fetch_cooldown[chat_id]
        if time_passed < 3:
            wait_time = int(3 - time_passed)
            bot.send_message(chat_id, f"⏳ Please wait {wait_time}s before fetching past OTPs again.")
            return
    
    past_otp_fetch_cooldown[chat_id] = now
    
    try:
        loading_msg = bot.send_message(chat_id, "⏳ <b>Fetching past OTPs...</b>\n\nThis may take a few seconds.")
        
        cached_otps = get_cached_past_otps(number, 50)
        
        response = requests.get(
            f"{BASE_URL}/viewstats",
            params={
                "token": API_TOKEN,
                "dt1": "1970-01-01 00:00:00",
                "dt2": "2099-12-31 23:59:59",
                "records": 2000
            },
            timeout=15
        )
        
        bot.delete_message(chat_id, loading_msg.message_id)
        
        if response.status_code != 200:
            bot.send_message(chat_id, "❌ Failed to fetch past OTPs. Try again later.")
            return
        
        data = response.json()
        
        if data.get("status") != "success":
            bot.send_message(chat_id, "❌ No past OTPs found in API response.")
            return
        
        user_messages_list = []
        for record in data.get("data", []):
            record_number = str(record.get("num", "")).lstrip("0").lstrip("+")
            if record_number == number:
                user_messages_list.append(record)
        
        if not user_messages_list and not cached_otps:
            bot.send_message(chat_id, f"📭 <b>No past OTPs found for:</b>\n<code>{number}</code>")
            return
        
        country_info, flag = country_from_number(number)
        
        msg_text = f"{flag} <b>Past OTPs for {number}</b>\n"
        msg_text += f"<b>Country:</b> {country_info}\n"
        msg_text += f"<b>Total Messages Found:</b> {len(user_messages_list)}\n"
        msg_text += "━━━━━━━━━━━━━━━━━━\n\n"
        
        display_count = min(50, len(user_messages_list))
        
        if display_count == 0 and cached_otps:
            msg_text += "<i>📦 Showing cached data:</i>\n\n"
            for i, (sender, message, otp, timestamp) in enumerate(cached_otps[:30], 1):
                otp_display = f"🎯 <code>{html.escape(otp)}</code>" if otp else "❌ No OTP"
                
                msg_text += f"<b>{i}. {html.escape(sender)}</b>\n"
                msg_text += f"   {otp_display}\n"
                msg_text += f"   🕐 {html.escape(timestamp)}\n"
                msg_text += f"   📩 {html.escape(message[:80])}\n\n"
                
                if len(msg_text) > 3500:
                    bot.send_message(chat_id, msg_text, disable_web_page_preview=True)
                    msg_text = ""
        else:
            for i, record in enumerate(user_messages_list[:display_count], 1):
                sender = record.get("cli", "Unknown")
                message = record.get("message", "")
                dt = record.get("dt", "")
                
                otp = extract_otp(message)
                otp_display = f"🎯 <code>{html.escape(otp)}</code>" if otp else "❌ No OTP"
                
                msg_text += f"<b>{i}. {html.escape(sender)}</b>\n"
                msg_text += f"   {otp_display}\n"
                msg_text += f"   🕐 {html.escape(str(dt))}\n"
                msg_text += f"   📩 {html.escape(message[:100])}\n\n"
                
                if len(msg_text) > 3500:
                    bot.send_message(chat_id, msg_text, disable_web_page_preview=True)
                    msg_text = ""
        
        if msg_text:
            if len(user_messages_list) > display_count:
                msg_text += f"\n<i>Showing {display_count} of {len(user_messages_list)} messages</i>"
            bot.send_message(chat_id, msg_text, disable_web_page_preview=True)
        
        summary = f"""
📊 <b>Summary:</b>

✅ Found {len(user_messages_list)} messages
📱 Service providers: {len(set(r.get('cli', 'Unknown') for r in user_messages_list))}
🔑 OTPs extracted: {sum(1 for r in user_messages_list if extract_otp(r.get('message', '')))}
"""
        bot.send_message(chat_id, summary)
        
    except Exception as e:
        print(f"❌ Error fetching past OTPs: {e}", flush=True)
        bot.send_message(chat_id, "❌ Error fetching past OTPs. Please try again later.")

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    chat_id = call.message.chat.id
    
    if call.from_user.id != ADMIN_ID:
        active_users.add(chat_id)
    
    if call.data.startswith("user_select_"):
        country = call.data[12:]
        user_current_country[chat_id] = country
        send_random_number(chat_id, country, edit=True)
    
    elif call.data == "change_number":
        send_random_number(chat_id, user_current_country.get(chat_id), edit=True)
    
    elif call.data == "change_country":
        markup = types.InlineKeyboardMarkup()
        for country in sorted(numbers_by_country.keys()):
            markup.add(types.InlineKeyboardButton(
                country, 
                callback_data=f"user_select_{country}"
            ))
        bot.edit_message_text(
            "🌍 Select Country:",
            chat_id,
            user_messages[chat_id].message_id,
            reply_markup=markup
        )
    
    elif call.data.startswith("view_past_"):
        number = call.data[10:]
        assigned_number = get_number_by_chat(chat_id)
        if assigned_number != number:
            bot.answer_callback_query(call.id, "❌ This is not your current number!")
            return
        
        bot.answer_callback_query(call.id, "⏳ Fetching past OTPs...")
        fetch_past_otps(chat_id, number)
    
    elif call.data == "verify_join":
        not_joined = []
        for channel in REQUIRED_CHANNELS:
            try:
                member = bot.get_chat_member(channel, chat_id)
                if member.status not in ["member", "creator", "administrator"]:
                    not_joined.append(channel)
            except:
                not_joined.append(channel)
        
        if not_joined:
            bot.answer_callback_query(call.id, "❌ Still not joined all channels!")
        else:
            bot.answer_callback_query(call.id, "✅ Verified!")
            start(call.message)

@bot.message_handler(commands=["setcountry", "deletecountry", "cleannumbers", "listcountries", "usercount"])
def other_admin_commands(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    cmd = message.text.split()[0][1:]
    
    if cmd == "listcountries":
        if not numbers_by_country:
            return bot.reply_to(message, "❌ No countries")
        text = "🌍 <b>Countries:</b>\n\n"
        for country, nums in sorted(numbers_by_country.items()):
            text += f"• {country}: {len(nums)} numbers\n"
        bot.reply_to(message, text)
    
    elif cmd == "usercount":
        bot.reply_to(message, f"👥 Active users: {len(active_users)}")
    
    elif cmd == "setcountry":
        global current_country
        if len(message.text.split()) > 1:
            current_country = " ".join(message.text.split()[1:])
            save_data()
            bot.reply_to(message, f"✅ Current country: {current_country}")
        else:
            bot.reply_to(message, "Usage: /setcountry <name>")
    
    elif cmd == "deletecountry":
        if len(message.text.split()) > 1:
            country = " ".join(message.text.split()[1:])
            if country in numbers_by_country:
                del numbers_by_country[country]
                save_data()
                bot.reply_to(message, f"✅ Deleted {country}")
            else:
                bot.reply_to(message, "❌ Country not found")
        else:
            bot.reply_to(message, "Usage: /deletecountry <name>")
    
    elif cmd == "cleannumbers":
        if len(message.text.split()) > 1:
            country = " ".join(message.text.split()[1:])
            if country in numbers_by_country:
                numbers_by_country[country] = []
                save_data()
                bot.reply_to(message, f"✅ Cleared {country}")
            else:
                bot.reply_to(message, "❌ Country not found")
        else:
            bot.reply_to(message, "Usage: /cleannumbers <name>")

# ==================== CLEANUP THREAD ====================
def cleanup_thread():
    while True:
        time.sleep(3600)
        try:
            clean_old_cache()
            print("🧹 Cleaned old message cache", flush=True)
        except Exception as e:
            print(f"❌ Cleanup error: {e}", flush=True)

# ==================== BOT POLLING ====================
def run_bot():
    while True:
        try:
            print("🤖 Bot polling started...", flush=True)
            bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception as e:
            print(f"❌ Polling error: {e}", flush=True)
            time.sleep(5)

# ==================== MAIN ====================
if __name__ == "__main__":
    print(f"🚀 OTP Bot v2.0 Starting at {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"📊 Initial stats: {len(numbers_by_country)} countries loaded", flush=True)
    
    threading.Thread(target=run_bot, daemon=True, name="BotPoller").start()
    threading.Thread(target=otp_scraper_thread, daemon=True, name="OTPScraper").start()
    threading.Thread(target=group_sender_thread, daemon=True, name="GroupSender").start()
    threading.Thread(target=personal_sender_thread, daemon=True, name="PersonalSender").start()
    threading.Thread(target=cleanup_thread, daemon=True, name="Cleaner").start()
    
    print("✅ All threads started successfully!", flush=True)
    
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
