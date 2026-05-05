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
from urllib.parse import urlencode

# ==================== CONFIG ====================
BOT_TOKEN  = os.getenv("BOT_TOKEN")
ADMIN_ID = 6102951142
API_TOKEN  = os.getenv("API_TOKEN")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

DATA_FILE   = "bot_data.json"
NUMBERS_DIR = "numbers"
DB_FILE     = "bot_database.db"
os.makedirs(NUMBERS_DIR, exist_ok=True)

# ==================== ZONESMS API CONFIG ====================
ZONE_API_BASE     = "http://137.74.1.203/zonecr/reseller/mdr.php"
ZONE_API_RECORDS  = 200                      # max 200 per request
ZONE_POLL_INTERVAL = 5                       # seconds between polls
ZONE_FROM_DATE    = "2026-02-20 00:00:00"    # fixed start — fetch everything from here
ZONE_TO_DATE      = "2028-12-31 23:59:59"    # fixed end — covers future too

def zone_build_url(fromdate=None, todate=None, searchnumber="", searchcli=""):
    params = {
        "token":        API_TOKEN,
        "fromdate":     fromdate or ZONE_FROM_DATE,
        "todate":       todate   or ZONE_TO_DATE,
        "records":      str(ZONE_API_RECORDS),
        "searchnumber": searchnumber,
        "searchcli":    searchcli,
    }
    return ZONE_API_BASE + "?" + urlencode(params)

def zone_fetch(fromdate=None, todate=None, searchnumber="", searchcli=""):
    """Fetch records from ZoneSMS API with retry logic."""
    url      = zone_build_url(fromdate, todate, searchnumber, searchcli)
    last_err = None
    for attempt in range(1, 4):
        try:
            resp    = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            payload = resp.json()
            status  = payload.get("status", "")
            if status == "Success":
                records = payload.get("data", [])
                print(f"✅ ZoneAPI: {len(records)} record(s)", flush=True)
                return records
            else:
                print(f"⚠️ ZoneAPI: {status} — {payload.get('description','')}", flush=True)
                return []
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError) as e:
            last_err = e
            print(f"⚠️ ZoneAPI connection error (attempt {attempt}/3): {e}", flush=True)
            time.sleep(attempt * 3)
        except Exception as e:
            print(f"❌ ZoneAPI unexpected error: {e}", flush=True)
            return []
    print(f"❌ ZoneAPI failed after 3 attempts: {last_err}", flush=True)
    return []

def zone_clean_message(text: str) -> str:
    """Unescape HTML entities and fix literal \\n sequences in API messages."""
    text = text.replace("\\n", "\n").replace("\\\\n", "\n")
    text = html.unescape(text)
    return text.strip()

def zone_parse_row(row: dict):
    """
    Convert a ZoneSMS API row into internal record format.
    API row: { "datetime": "...", "number": "...", "cli": "...", "message": "..." }
    Internal: { "dt": ..., "num": ..., "cli": ..., "message": ... }
    """
    try:
        number  = str(row.get("number",   "") or "").strip().lstrip("+").lstrip("0")
        sender  = str(row.get("cli",      "") or "").strip()
        message = zone_clean_message(str(row.get("message", "") or ""))
        dt      = str(row.get("datetime", "") or "").strip()
        if not number or not message:
            return None
        return {
            "dt":      dt,
            "num":     number,
            "cli":     sender,
            "message": message,
        }
    except Exception as e:
        print(f"zone_parse_row error: {e}", flush=True)
        return None

# ==================== QUEUES ====================
group_queue    = Queue(maxsize=1000)
personal_queue = Queue(maxsize=5000)
seen_messages  = deque(maxlen=50000)   # dedup by msg_id string

OTP_GROUP_IDS       = ["-1003633481131"]
AUTO_DELETE_MINUTES = 0
BACKUP       = "https://t.me/nomorgo"
CHANNEL_LINK = "https://t.me/nomorxbot"

# ==================== REGEX PATTERNS ====================
KEYWORD_REGEX = re.compile(r"(otp|code|codigo|pin|password|verify)[^\d]{0,10}(\d[\d\-\s]{2,8}\d)", re.I)
REVERSE_REGEX = re.compile(r"(\d[\d\-\s]{2,8}\d)[^\w]{0,10}(otp|code|codigo|pin|password|verify)", re.I)
WHATSAPP_REGEX = re.compile(r"\b(\d{3})-(\d{3})\b")   # 772-853 style
GENERIC_REGEX  = re.compile(r"\b\d{4,8}\b")
UNICODE_CLEAN  = re.compile(r"[\u200f\u200e\u202a-\u202e]")

# ==================== DATABASE ====================
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS user_numbers
                 (number TEXT PRIMARY KEY, chat_id INTEGER, country TEXT, assigned_at REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_stats
                 (chat_id INTEGER PRIMARY KEY, total_otps INTEGER DEFAULT 0,
                  last_otp REAL, joined_at REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS past_otps_cache
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  number TEXT, sender TEXT, message TEXT,
                  otp TEXT, timestamp TEXT, received_at REAL)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_number     ON past_otps_cache(number)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_received   ON past_otps_cache(received_at)')
    c.execute('''CREATE TABLE IF NOT EXISTS full_messages
                 (msg_hash INTEGER PRIMARY KEY, number TEXT, sender TEXT,
                  message TEXT, created_at REAL)''')
    conn.commit(); conn.close()

init_db()

# ==================== DATA STORAGE ====================
data               = {}
numbers_by_country = {}
current_country    = None
user_messages      = {}
user_current_country = {}
temp_uploads       = {}
last_change_time   = {}
active_users       = set()
past_otp_cooldown  = {}
flag_overrides     = {}
REQUIRED_CHANNELS = ["@NomorGo","@nomorgoextra","@sunilhubbackup"]

SERVICE_CODES = {
    "whatsapp": "WA", "telegram": "TG", "instagram": "IG", "facebook": "FB",
    "twitter": "TW", "google": "GO", "amazon": "AZ", "snapchat": "SC",
    "tiktok": "TT", "linkedin": "LI", "uber": "UB", "paypal": "PP",
    "microsoft": "MS", "apple": "AP", "netflix": "NF", "smsinfo": "SI",
}

# ==================== DATA FUNCTIONS ====================
def load_data():
    global data, numbers_by_country, current_country, OTP_GROUP_IDS, AUTO_DELETE_MINUTES, flag_overrides
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
        numbers_by_country  = data.get("numbers_by_country", {})
        current_country     = data.get("current_country")
        OTP_GROUP_IDS       = data.get("otp_groups", ["-1003633481131"])
        AUTO_DELETE_MINUTES = data.get("auto_delete_minutes", 0)
        flag_overrides      = data.get("flag_overrides", {})
    else:
        data = {"numbers_by_country": {}, "current_country": None,
                "otp_groups": ["-1003633481131"], "auto_delete_minutes": 0, "flag_overrides": {}}
        numbers_by_country = {}; current_country = None

def save_data():
    data["numbers_by_country"]  = numbers_by_country
    data["current_country"]     = current_country
    data["otp_groups"]          = OTP_GROUP_IDS
    data["auto_delete_minutes"] = AUTO_DELETE_MINUTES
    data["flag_overrides"]      = flag_overrides
    with open(DATA_FILE, "w") as f:
        json.dump(data, f)

load_data()

# ==================== DB HELPERS ====================
def get_chat_by_number(number):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT chat_id FROM user_numbers WHERE number=?", (number,))
    r = c.fetchone(); conn.close()
    return r[0] if r else None

def get_number_by_chat(chat_id):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT number FROM user_numbers WHERE chat_id=? ORDER BY assigned_at DESC LIMIT 1", (chat_id,))
    r = c.fetchone(); conn.close()
    return r[0] if r else None

def assign_number(number, chat_id, country):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("INSERT OR REPLACE INTO user_numbers VALUES (?,?,?,?)",
                 (number, chat_id, country, time.time()))
    conn.commit(); conn.close()

def increment_user_stats(chat_id):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("""INSERT INTO user_stats(chat_id,total_otps,last_otp,joined_at) VALUES(?,1,?,?)
                    ON CONFLICT(chat_id) DO UPDATE SET total_otps=total_otps+1, last_otp=?""",
                 (chat_id, time.time(), time.time(), time.time()))
    conn.commit(); conn.close()

def cache_past_otp(number, sender, message, otp, timestamp):
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.execute("INSERT INTO past_otps_cache(number,sender,message,otp,timestamp,received_at) VALUES(?,?,?,?,?,?)",
                     (number, sender, message, otp, timestamp, time.time()))
        conn.commit(); conn.close()
    except: pass

def get_cached_past_otps(number, limit=50):
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    c = conn.cursor()
    c.execute("SELECT sender,message,otp,timestamp FROM past_otps_cache WHERE number=? ORDER BY received_at DESC LIMIT ?",
              (number, limit))
    r = c.fetchall(); conn.close()
    return r

def cache_full_message(msg_hash, number, sender, message):
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        conn.execute("INSERT OR REPLACE INTO full_messages VALUES(?,?,?,?,?)",
                     (msg_hash, number, sender, message, time.time()))
        conn.commit(); conn.close()
    except: pass

def get_full_message(msg_hash):
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        c = conn.cursor()
        c.execute("SELECT message FROM full_messages WHERE msg_hash=?", (msg_hash,))
        r = c.fetchone(); conn.close()
        return r[0] if r else None
    except: return None

def is_message_seen(msg_id):
    if msg_id in seen_messages: return True
    seen_messages.append(msg_id)
    return False

def clean_old_cache():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("DELETE FROM past_otps_cache WHERE received_at < ?", (time.time() - 7*86400,))
    conn.execute("DELETE FROM full_messages WHERE created_at < ?",    (time.time() - 86400,))
    conn.commit(); conn.close()

# ==================== FLASK ====================
app = Flask(__name__)

@app.route("/")
def index(): return "🚀 OTP Bot v2.0 Running"

@app.route("/health")
def health(): return Response(f"OK - G={group_queue.qsize()} P={personal_queue.qsize()}", status=200)

# ==================== HELPERS ====================
def extract_otp(message: str):
    message = UNICODE_CLEAN.sub("", message)

    # keyword then code
    m = KEYWORD_REGEX.search(message)
    if m:
        cand = re.sub(r"\D", "", m.group(2))
        if 4 <= len(cand) <= 8 and not (1900 <= int(cand) <= 2099):
            return cand

    # code then keyword
    m = REVERSE_REGEX.search(message)
    if m:
        cand = re.sub(r"\D", "", m.group(1))
        if 4 <= len(cand) <= 8 and not (1900 <= int(cand) <= 2099):
            return cand

    # WhatsApp NNN-NNN style
    m = WHATSAPP_REGEX.search(message)
    if m:
        return m.group(1) + m.group(2)

    # generic 4-8 digit number
    for g in GENERIC_REGEX.findall(message):
        if not (1900 <= int(g) <= 2099):
            return g

    return None

def mask_number(number: str) -> str:
    number = number.strip()
    if len(number) <= 4: return number
    return f"{number[:2]}DDX{number[-4:]}"

def country_from_number(number: str):
    try:
        parsed = phonenumbers.parse("+" + number)
        region = phonenumbers.region_code_for_number(parsed)
        if not region: return "Unknown", "🌍"
        c = pycountry.countries.get(alpha_2=region)
        if not c: return "Unknown", "🌍"
        flag = "".join(chr(127397 + ord(x)) for x in region)
        return c.name, flag
    except: return "Unknown", "🌍"

def get_country_code(country_name: str) -> str:
    try: return pycountry.countries.lookup(country_name).alpha_2.upper()
    except: return country_name[:2].upper()

def get_service_code(sender: str) -> str:
    for svc, code in SERVICE_CODES.items():
        if svc.lower() in sender.lower(): return code
    return sender[:2].upper() if len(sender) >= 2 else sender.upper()

def delete_message_safe(chat_id, message_id):
    try: bot.delete_message(chat_id, message_id)
    except Exception as e: print(f"delete_message_safe: {e}", flush=True)

def get_flag(country_name: str) -> str:
    try:
        code = pycountry.countries.lookup(country_name).alpha_2.upper()
    except: return "🌍"
    regular = "".join(chr(127397 + ord(c)) for c in code)
    eid = flag_overrides.get(code)
    if eid: return f'<tg-emoji emoji-id="{eid}">{regular}</tg-emoji>'
    return regular

def get_service_emoji(sender: str) -> str:
    s = sender.lower()
    if "whatsapp"  in s: return '<tg-emoji emoji-id="5334998226636390258">📱</tg-emoji>'
    if "telegram"  in s: return '<tg-emoji emoji-id="5330237710655306682">✈️</tg-emoji>'
    if "instagram" in s: return '<tg-emoji emoji-id="5319160079465857105">📸</tg-emoji>'
    if "facebook"  in s: return '<tg-emoji emoji-id="5323261730283863478">👤</tg-emoji>'
    return '<tg-emoji emoji-id="6131716438160842744">🌐</tg-emoji>'

# ==================== MESSAGE FORMATTERS ====================
def format_group_message(record):
    number  = record.get("num")     or "Unknown"
    sender  = record.get("cli")     or "Unknown"
    message = record.get("message") or ""

    country, _   = country_from_number(number)
    country_code = get_country_code(country)
    flag         = get_flag(country)
    service_code = get_service_code(sender)
    service_emoji= get_service_emoji(sender)
    masked       = mask_number(number)
    otp          = extract_otp(message)

    msg_hash = hash(f"{number}{message}{record.get('dt','')}")
    cache_full_message(msg_hash, number, sender, message)

    formatted = (
        f'<tg-emoji emoji-id="5382357040008021292">⚡</tg-emoji> '
        f'{flag} <b>{country_code}</b> | <code>{masked}</code> | '
        f'{service_emoji} <b>{service_code}</b>'
    )

    keyboard = {
        "inline_keyboard": [
            *([[{"text": f"🔑 {otp}", "callback_data": f"copy_{otp}",
                 "icon_custom_emoji_id": "5443038326535759644"}]] if otp else []),
            [{"text": " View Full SMS", "callback_data": f"fullsms_{msg_hash}",
              "icon_custom_emoji_id": "5253742260054409879"}],
            [
                {"text": " Panel",   "url": CHANNEL_LINK,
                 "icon_custom_emoji_id": "5330237710655306682"},
                {"text": " Channel", "url": BACKUP,
                 "icon_custom_emoji_id": "6131716438160842744"}
            ]
        ]
    }
    return formatted, keyboard

def format_personal_message(record):
    number  = record.get("num")     or "Unknown"
    sender  = record.get("cli")     or "Unknown"
    message = record.get("message") or ""
    country, _ = country_from_number(number)
    otp = extract_otp(message) or "N/A"

    return (
        f'<tg-emoji emoji-id="5382357040008021292">⚡</tg-emoji> <b>OTP RECEIVED!</b>\n'
        f'━━━━━━━━━━━━━━━\n'
        f'{get_flag(country)} <b>Country:</b> {html.escape(country)}\n'
        f'{get_service_emoji(sender)} <b>Service:</b> {html.escape(sender)}\n'
        f'📞 <b>Number:</b> <code>{html.escape(number)}</code>\n'
        f'━━━━━━━━━━━━━━━\n'
        f'🔑 <b>OTP Code:</b> <code>{otp}</code>\n'
        f'━━━━━━━━━━━━━━━\n'
        f'💬 <b>Message:</b>\n<code>{html.escape(message[:300])}</code>'
    )

# ==================== THREAD 1: OTP SCRAPER (ZoneSMS) ====================
def otp_scraper_thread():
    print("🟢 OTP Scraper Started (ZoneSMS API)", flush=True)

    # ── Startup: fetch full history, send last 5 as "old", mark rest seen ──
    print("📦 Loading full OTP history from API...", flush=True)
    all_rows    = zone_fetch()
    all_records = []
    for raw in all_rows:
        rec = zone_parse_row(raw)
        if not rec: continue
        msg_id = f"{rec['dt']}_{rec['num']}_{rec['message'][:50]}"
        seen_messages.append(msg_id)   # mark all as seen
        all_records.append(rec)

    # sort oldest → newest by datetime string
    all_records.sort(key=lambda r: r.get("dt", ""))
    print(f"✅ History loaded: {len(all_records)} records", flush=True)

    # Send last 5 to group with "🕐 Old" label
    last5 = all_records[-5:] if len(all_records) >= 5 else all_records
    for rec in last5:
        try:
            _direct_send_to_group(rec, old=True)
            time.sleep(0.4)
        except Exception as e:
            print(f"History send error: {e}", flush=True)

    print("🔄 Starting live polling...", flush=True)
    empty_count = 0

    # ── Live polling loop ──
    while True:
        try:
            rows = zone_fetch()   # same wide date range, dedup handles rest
            new_count = 0

            for raw in rows:
                rec = zone_parse_row(raw)
                if not rec: continue

                msg_id = f"{rec['dt']}_{rec['num']}_{rec['message'][:50]}"
                if is_message_seen(msg_id): continue

                # NEW record
                number = rec["num"]
                sender = rec["cli"]
                message = rec["message"]
                dt     = rec["dt"]
                otp    = extract_otp(message)

                cache_past_otp(number, sender, message, otp, dt)

                try:
                    group_queue.put_nowait((rec, time.time()))
                    print(f"📤 Queued: {number} | {sender} | OTP: {otp or 'N/A'}", flush=True)
                except:
                    print("⚠️ Group queue full!", flush=True)

                chat_id = get_chat_by_number(number)
                if chat_id:
                    try: personal_queue.put_nowait((rec, chat_id, time.time()))
                    except: print(f"⚠️ Personal queue full for {chat_id}!", flush=True)

                new_count += 1

            if new_count:
                empty_count = 0
            else:
                empty_count += 1
                if empty_count % 60 == 0:
                    print(f"💤 No new OTPs for ~{empty_count * ZONE_POLL_INTERVAL // 60} min", flush=True)

        except Exception as e:
            print(f"❌ Scraper error: {e}", flush=True)

        time.sleep(ZONE_POLL_INTERVAL)


def _direct_send_to_group(record, old=False):
    """Send record directly to group (used for startup history)."""
    number  = record.get("num",     "Unknown")
    sender  = record.get("cli",     "Unknown")
    message = record.get("message", "")
    dt      = record.get("dt",      "")

    country, _   = country_from_number(number)
    country_code = get_country_code(country)
    flag         = get_flag(country)
    service_code = get_service_code(sender)
    service_emoji= get_service_emoji(sender)
    masked       = mask_number(number)
    otp          = extract_otp(message)

    label    = "🕐" if old else "📩"
    msg_hash = hash(f"{number}{message}{dt}")
    cache_full_message(msg_hash, number, sender, message)
    cache_past_otp(number, sender, message, otp, dt)

    text = (
        f'{label} <tg-emoji emoji-id="5382357040008021292">⚡</tg-emoji> '
        f'{flag} <b>{country_code}</b> | <code>{masked}</code> | '
        f'{service_emoji} <b>{service_code}</b>'
    )

    keyboard = {
        "inline_keyboard": [
            *([[{"text": f" {otp}", "callback_data": f"copy_{otp}",
                 "icon_custom_emoji_id": "5443038326535759644"}]] if otp else []),
            [{"text": " View Full SMS", "callback_data": f"fullsms_{msg_hash}",
              "icon_custom_emoji_id": "5253742260054409879"}],
            [
                {"text": " Panel",   "url": CHANNEL_LINK,
                 "icon_custom_emoji_id": "5330237710655306682"},
                {"text": " Channel", "url": BACKUP,
                 "icon_custom_emoji_id": "6131716438160842744"}
            ]
        ]
    }

    for group_id in OTP_GROUP_IDS:
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": group_id, "text": text[:4000],
                      "parse_mode": "HTML", "reply_markup": json.dumps(keyboard)},
                timeout=8
            )
            if AUTO_DELETE_MINUTES > 0 and resp.status_code == 200:
                result = resp.json()
                if result.get("ok"):
                    mid = result["result"]["message_id"]
                    threading.Timer(AUTO_DELETE_MINUTES * 60, delete_message_safe,
                                    args=(group_id, mid)).start()
        except Exception as e:
            print(f"_direct_send_to_group {group_id}: {e}", flush=True)

# ==================== THREAD 2: GROUP SENDER ====================
def group_sender_thread():
    print("🟢 Group Sender Started", flush=True)
    while True:
        try:
            record, fetch_time = group_queue.get()
            msg, kb = format_group_message(record)

            for group_id in OTP_GROUP_IDS:
                resp = requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={"chat_id": group_id, "text": msg[:4000],
                          "parse_mode": "HTML", "reply_markup": json.dumps(kb)},
                    timeout=5
                )
                if resp.status_code == 200:
                    delay = time.time() - fetch_time
                    print(f"✅ Group sent (delay: {delay:.2f}s)", flush=True)
                    if AUTO_DELETE_MINUTES > 0:
                        r = resp.json()
                        if r.get("ok"):
                            mid = r["result"]["message_id"]
                            threading.Timer(AUTO_DELETE_MINUTES * 60, delete_message_safe,
                                            args=(group_id, mid)).start()
                elif resp.status_code == 429:
                    wait = resp.json().get("parameters", {}).get("retry_after", 2)
                    time.sleep(wait)
                    group_queue.put((record, fetch_time))
                else:
                    print(f"❌ Group send failed: {resp.status_code}", flush=True)

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
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": msg[:4000], "parse_mode": "HTML"},
                timeout=5
            )
            if resp.status_code == 200:
                increment_user_stats(chat_id)
                print(f"✅ DM sent to {chat_id} (delay: {time.time()-fetch_time:.2f}s)", flush=True)
            elif resp.status_code == 429:
                wait = resp.json().get("parameters", {}).get("retry_after", 1)
                time.sleep(wait)
                personal_queue.put((record, chat_id, fetch_time))
            else:
                print(f"❌ DM failed {chat_id}: {resp.status_code}", flush=True)
            time.sleep(0.2)
        except Exception as e:
            print(f"❌ Personal sender error: {e}", flush=True)
            time.sleep(1)

# ==================== CALLBACK HANDLERS ====================
@bot.callback_query_handler(func=lambda c: c.data.startswith("copy_"))
def handle_copy_otp(call):
    otp = call.data[5:]
    try: bot.answer_callback_query(call.id, f"✅ OTP: {otp}", show_alert=True)
    except Exception as e: print(f"copy_otp: {e}", flush=True)

@bot.callback_query_handler(func=lambda c: c.data.startswith("fullsms_"))
def handle_full_sms(call):
    try:
        msg_hash = int(call.data[8:])
        message  = get_full_message(msg_hash)
        if message: bot.answer_callback_query(call.id, message[:200], show_alert=True)
        else:       bot.answer_callback_query(call.id, "❌ Message not found", show_alert=True)
    except Exception as e:
        print(f"fullsms: {e}", flush=True)
        bot.answer_callback_query(call.id, "❌ Error loading message", show_alert=True)

# ==================== ADMIN COMMANDS ====================
@bot.message_handler(commands=["addflag"])
def add_flag(message):
    if message.from_user.id != ADMIN_ID: return
    parts = message.text.strip().split()
    if len(parts) != 3:
        return bot.reply_to(message, "Usage: /addflag IN 5222300011366200403")
    _, code, eid = parts; code = code.upper()
    flag_overrides[code] = eid; save_data()
    regular = "".join(chr(127397 + ord(c)) for c in code)
    bot.reply_to(message, f'✅ Flag set!\n<tg-emoji emoji-id="{eid}">{regular}</tg-emoji> <b>{code}</b>')

@bot.message_handler(commands=["removeflag"])
def remove_flag(message):
    if message.from_user.id != ADMIN_ID: return
    parts = message.text.strip().split()
    if len(parts) != 2: return bot.reply_to(message, "Usage: /removeflag IN")
    code = parts[1].upper()
    if code in flag_overrides:
        del flag_overrides[code]; save_data()
        bot.reply_to(message, f"✅ Removed flag for <b>{code}</b>")
    else:
        bot.reply_to(message, f"❌ No override for <b>{code}</b>")

@bot.message_handler(commands=["listflags"])
def list_flags(message):
    if message.from_user.id != ADMIN_ID: return
    if not flag_overrides: return bot.reply_to(message, "📭 No premium flags set.")
    text = "🏳 <b>Premium Flags:</b>\n\n"
    for code, eid in sorted(flag_overrides.items()):
        reg = "".join(chr(127397 + ord(c)) for c in code)
        text += f'<tg-emoji emoji-id="{eid}">{reg}</tg-emoji> <b>{code}</b> → <code>{eid}</code>\n'
    bot.reply_to(message, text)

@bot.message_handler(content_types=["document"])
def handle_document(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized")
    if not message.document.file_name.endswith(".txt"):
        return bot.reply_to(message, "❌ Upload .txt file only")
    info = bot.get_file(message.document.file_id)
    raw  = bot.download_file(info.file_path)
    numbers = [l.strip().lstrip("+").lstrip("0") for l in raw.decode("utf-8").splitlines() if l.strip()]
    if not numbers: return bot.reply_to(message, "❌ File is empty")
    temp_uploads[message.from_user.id] = numbers
    mk = types.InlineKeyboardMarkup()
    for c in sorted(numbers_by_country.keys()):
        mk.add(types.InlineKeyboardButton(c, callback_data=f"addto_{c}"))
    mk.add(types.InlineKeyboardButton("➕ New Country", callback_data="addto_new"))
    bot.reply_to(message, f"📂 {len(numbers)} numbers received. Select country:", reply_markup=mk)

@bot.callback_query_handler(func=lambda c: c.data.startswith("addto_"))
def callback_addto(call):
    if call.from_user.id != ADMIN_ID:
        return bot.answer_callback_query(call.id, "❌ Not authorized")
    numbers = temp_uploads.get(call.from_user.id, [])
    if not numbers: return bot.answer_callback_query(call.id, "❌ No numbers found")
    choice = call.data[6:]
    if choice == "new":
        bot.send_message(call.message.chat.id, "✏️ Send new country name:")
        bot.register_next_step_handler(call.message, save_new_country, numbers)
    else:
        merged = list(set(numbers_by_country.get(choice, []) + numbers))
        numbers_by_country[choice] = merged; save_data()
        with open(os.path.join(NUMBERS_DIR, f"{choice}.txt"), "w") as f:
            f.write("\n".join(merged))
        bot.edit_message_text(f"✅ Added {len(numbers)} numbers to <b>{choice}</b>",
                              call.message.chat.id, call.message.message_id)
        temp_uploads.pop(call.from_user.id, None)

def save_new_country(message, numbers):
    country = message.text.strip()
    if not country: return bot.reply_to(message, "❌ Invalid country name")
    numbers_by_country[country] = numbers; save_data()
    with open(os.path.join(NUMBERS_DIR, f"{country}.txt"), "w") as f:
        f.write("\n".join(numbers))
    bot.reply_to(message, f"✅ Saved {len(numbers)} numbers under <b>{country}</b>")
    temp_uploads.pop(message.from_user.id, None)

@bot.message_handler(commands=["addchat"])
def add_chat(message):
    global OTP_GROUP_IDS
    if message.from_user.id != ADMIN_ID: return bot.reply_to(message, "❌ Not admin.")
    if message.chat.type == "private": return bot.reply_to(message, "❌ Use in group/channel.")
    OTP_GROUP_IDS = [str(message.chat.id)]; save_data()
    bot.reply_to(message,
        f"✅ <b>OTP Group set!</b>\n📱 {html.escape(message.chat.title or 'Chat')}\n"
        f"🆔 <code>{message.chat.id}</code>")

@bot.message_handler(commands=["autodelete"])
def set_autodelete(message):
    global AUTO_DELETE_MINUTES
    if message.from_user.id != ADMIN_ID: return bot.reply_to(message, "❌ Not admin.")
    args = message.text.split()
    if len(args) < 2:
        return bot.reply_to(message,
            f"🗑️ Auto-Delete: {'ON ('+str(AUTO_DELETE_MINUTES)+' min)' if AUTO_DELETE_MINUTES else 'OFF'}\n"
            f"Usage: /autodelete &lt;minutes&gt; (0 to disable)")
    try:
        m = int(args[1])
        if m < 0: return bot.reply_to(message, "❌ Use 0 or positive.")
        AUTO_DELETE_MINUTES = m; save_data()
        bot.reply_to(message, f"✅ Auto-Delete {'disabled' if m==0 else f'set to {m} min'}")
    except ValueError:
        bot.reply_to(message, "❌ Invalid number.")

@bot.message_handler(commands=["stats"])
def bot_stats(message):
    if message.from_user.id != ADMIN_ID: return
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cache_c = conn.execute("SELECT COUNT(*) FROM past_otps_cache").fetchone()[0]
    conn.close()
    bot.reply_to(message,
        f"📊 <b>Bot Stats</b>\n\n"
        f"👥 Active Users: {len(active_users)}\n"
        f"📥 Group Queue: {group_queue.qsize()}\n"
        f"📨 Personal Queue: {personal_queue.qsize()}\n"
        f"💾 Seen Cache: {len(seen_messages)}\n"
        f"💿 Past OTPs: {cache_c}\n"
        f"🌍 Countries: {len(numbers_by_country)}\n"
        f"📞 Total Numbers: {sum(len(v) for v in numbers_by_country.values())}\n"
        f"🗑️ Auto-Delete: {'ON ('+str(AUTO_DELETE_MINUTES)+' min)' if AUTO_DELETE_MINUTES else 'OFF'}\n"
        f"📡 OTP Groups: {len(OTP_GROUP_IDS)}")

@bot.message_handler(commands=["broadcast"])
def broadcast_start(message):
    if message.from_user.id != ADMIN_ID: return
    msg = bot.reply_to(message, "✉️ Send broadcast message:")
    bot.register_next_step_handler(msg, do_broadcast)

def do_broadcast(message):
    ok = fail = 0
    for uid in active_users:
        try:
            bot.send_message(uid, f"📢 <b>Broadcast:</b>\n\n{message.text}")
            ok += 1; time.sleep(0.05)
        except: fail += 1
    bot.reply_to(message, f"✅ Sent: {ok} | ❌ Failed: {fail}")

@bot.message_handler(commands=["clearcache"])
def clear_cache(message):
    if message.from_user.id != ADMIN_ID: return
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("DELETE FROM past_otps_cache")
    d = conn.total_changes; conn.commit(); conn.close()
    bot.reply_to(message, f"✅ Cleared {d} cached OTPs")

@bot.message_handler(commands=["adminhelp"])
def admin_help(message):
    if message.from_user.id != ADMIN_ID: return
    bot.reply_to(message, """🔧 <b>Admin Commands</b>

📁 Numbers: Upload .txt | /setcountry | /deletecountry | /cleannumbers | /listcountries
📊 Stats: /stats | /usercount
📢 /broadcast | /clearcache
🔧 /addchat | /autodelete &lt;min&gt;
🏳 /addflag | /removeflag | /listflags""")

@bot.message_handler(commands=["setcountry", "deletecountry", "cleannumbers", "listcountries", "usercount"])
def other_admin_commands(message):
    if message.from_user.id != ADMIN_ID: return
    cmd = message.text.split()[0][1:]
    if cmd == "listcountries":
        if not numbers_by_country: return bot.reply_to(message, "❌ No countries")
        text = "🌍 <b>Countries:</b>\n\n"
        for c, nums in sorted(numbers_by_country.items()):
            text += f"• {c}: {len(nums)} numbers\n"
        bot.reply_to(message, text)
    elif cmd == "usercount":
        bot.reply_to(message, f"👥 Active users: {len(active_users)}")
    elif cmd == "setcountry":
        global current_country
        parts = message.text.split(None, 1)
        if len(parts) < 2: return bot.reply_to(message, "Usage: /setcountry <name>")
        current_country = parts[1].strip()
        numbers_by_country.setdefault(current_country, []); save_data()
        bot.reply_to(message, f"✅ Country: {current_country}")
    elif cmd == "deletecountry":
        parts = message.text.split(None, 1)
        if len(parts) < 2: return bot.reply_to(message, "Usage: /deletecountry <name>")
        c = parts[1].strip()
        if c not in numbers_by_country: return bot.reply_to(message, "❌ Not found")
        del numbers_by_country[c]; save_data()
        fp = os.path.join(NUMBERS_DIR, f"{c}.txt")
        if os.path.exists(fp): os.remove(fp)
        bot.reply_to(message, f"✅ Deleted {c}")
    elif cmd == "cleannumbers":
        parts = message.text.split(None, 1)
        if len(parts) < 2: return bot.reply_to(message, "Usage: /cleannumbers <name>")
        c = parts[1].strip()
        if c not in numbers_by_country: return bot.reply_to(message, "❌ Not found")
        numbers_by_country[c] = []; save_data()
        bot.reply_to(message, f"✅ Cleared {c}")

# ==================== USER COMMANDS ====================
@bot.message_handler(commands=["start"])
def start(message):
    chat_id  = message.chat.id
    if message.from_user.id == ADMIN_ID:
        bot.send_message(chat_id, "👋 Welcome Admin! Use /adminhelp"); return
    active_users.add(chat_id)
    not_joined = []
    for ch in REQUIRED_CHANNELS:
        try:
            m = bot.get_chat_member(ch, chat_id)
            if m.status not in ["member","creator","administrator"]: not_joined.append(ch)
        except: not_joined.append(ch)
    if not_joined:
        mk = types.InlineKeyboardMarkup()
        for ch in not_joined: mk.add(types.InlineKeyboardButton(f"Join {ch}", url=f"https://t.me/{ch[1:]}"))
        mk.add(types.InlineKeyboardButton("✅ Verify", callback_data="verify_join"))
        bot.send_message(chat_id, "❌ Join required channels first:", reply_markup=mk); return
    if not numbers_by_country:
        bot.send_message(chat_id, "❌ No countries available"); return
    mk = types.InlineKeyboardMarkup()
    for c in sorted(numbers_by_country.keys()):
        mk.add(types.InlineKeyboardButton(f"{c} ({len(numbers_by_country[c])} numbers)",
                                          callback_data=f"user_select_{c}"))
    msg = bot.send_message(chat_id,
        "🌍 <b>Select Country:</b>\n\n⚡️ Fast delivery\n🔒 Secure numbers\n♻️ Change anytime",
        reply_markup=mk)
    user_messages[chat_id] = msg

@bot.message_handler(commands=["mystats"])
def my_stats(message):
    chat_id = message.chat.id
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    r = conn.execute("SELECT total_otps, last_otp FROM user_stats WHERE chat_id=?", (chat_id,)).fetchone()
    conn.close()
    if r:
        bot.reply_to(message,
            f"📊 <b>Your Stats</b>\n\n"
            f"📩 Total OTPs: {r[0]}\n"
            f"🕐 Last OTP: {datetime.fromtimestamp(r[1]).strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        bot.reply_to(message, "📊 No OTPs received yet!")

@bot.message_handler(commands=["help"])
def help_command(message):
    bot.reply_to(message, "📚 <b>Commands:</b>\n/start - Get number\n/mystats - Your stats\n/help - Help")

def send_random_number(chat_id, country=None, edit=False):
    now = time.time()
    if chat_id in last_change_time and now - last_change_time[chat_id] < 10:
        wait = 10 - int(now - last_change_time[chat_id])
        bot.send_message(chat_id, f"⏳ Wait {wait}s before changing number"); return
    last_change_time[chat_id] = now
    if country is None:
        country = user_current_country.get(chat_id)
        if not country:
            bot.send_message(chat_id, "❌ No country selected"); return
    numbers = numbers_by_country.get(country, [])
    if not numbers:
        bot.send_message(chat_id, f"❌ No numbers for {country}"); return
    number = random.choice(numbers).lstrip("+").lstrip("0")
    user_current_country[chat_id] = country
    assign_number(number, chat_id, country)
    _, flag = country_from_number(number)
    text = (
        f"{flag} <b>Your Number ({country}):</b>\n\n"
        f"📞 <code>{number}</code>\n\n"
        f"⏳ <b>Waiting for OTP...</b>\n🔔 You'll get notified instantly!"
    )
    mk = types.InlineKeyboardMarkup()
    mk.row(
        types.InlineKeyboardButton("🔄 Change Number",  callback_data="change_number"),
        types.InlineKeyboardButton("🌍 Change Country", callback_data="change_country")
    )
    mk.row(types.InlineKeyboardButton("📜 View Past OTPs", callback_data=f"view_past_{number}"))
    mk.row(types.InlineKeyboardButton("📢 OTP Group", url="https://t.me/nomorlinks"))
    if chat_id in user_messages and edit:
        try:
            bot.edit_message_text(text, chat_id, user_messages[chat_id].message_id, reply_markup=mk)
            return
        except: pass
    msg = bot.send_message(chat_id, text, reply_markup=mk)
    user_messages[chat_id] = msg

def fetch_past_otps(chat_id, number):
    now = time.time()
    if chat_id in past_otp_cooldown and now - past_otp_cooldown[chat_id] < 3:
        bot.send_message(chat_id, f"⏳ Wait {3 - int(now-past_otp_cooldown[chat_id])}s"); return
    past_otp_cooldown[chat_id] = now
    loading = bot.send_message(chat_id, "⏳ <b>Fetching past OTPs...</b>")
    try:
        # Use full ZoneSMS API fetch — filter by number
        rows = zone_fetch(searchnumber=number)
        bot.delete_message(chat_id, loading.message_id)

        # Also check local cache
        cached = get_cached_past_otps(number, 50)
        api_records = [zone_parse_row(r) for r in rows if zone_parse_row(r) and
                       zone_parse_row(r).get("num","").lstrip("0") == number.lstrip("0")]

        if not api_records and not cached:
            bot.send_message(chat_id, f"📭 <b>No past OTPs for</b> <code>{number}</code>"); return

        _, flag = country_from_number(number)
        text = f"{flag} <b>Past OTPs for {number}</b>\n━━━━━━━━━━━━━━━━\n\n"

        source = api_records if api_records else []
        for i, rec in enumerate(source[:30], 1):
            otp   = rec.get("otp") or extract_otp(rec.get("message",""))
            otp_d = f"🔑 <code>{html.escape(otp)}</code>" if otp else "❌ No OTP"
            text += (f"<b>{i}. {html.escape(rec.get('cli','?'))}</b>\n"
                     f"   {otp_d}\n"
                     f"   🕐 {html.escape(rec.get('dt',''))}\n"
                     f"   📩 {html.escape(rec.get('message','')[:80])}\n\n")
            if len(text) > 3500:
                bot.send_message(chat_id, text, disable_web_page_preview=True); text = ""

        if not source and cached:
            text += "<i>📦 From local cache:</i>\n\n"
            for i, (sender, message, otp, timestamp) in enumerate(cached[:20], 1):
                otp_d = f"🔑 <code>{html.escape(otp)}</code>" if otp else "❌ No OTP"
                text += (f"<b>{i}. {html.escape(sender)}</b>\n"
                         f"   {otp_d}\n"
                         f"   🕐 {html.escape(timestamp)}\n"
                         f"   📩 {html.escape(message[:80])}\n\n")
                if len(text) > 3500:
                    bot.send_message(chat_id, text, disable_web_page_preview=True); text = ""

        if text: bot.send_message(chat_id, text, disable_web_page_preview=True)
        found = len(source) or len(cached)
        bot.send_message(chat_id, f"📊 Found {found} message(s) for this number.")

    except Exception as e:
        print(f"fetch_past_otps error: {e}", flush=True)
        try: bot.delete_message(chat_id, loading.message_id)
        except: pass
        bot.send_message(chat_id, "❌ Error fetching past OTPs. Try again.")

@bot.callback_query_handler(func=lambda c: True)
def handle_callbacks(call):
    chat_id = call.message.chat.id
    if call.from_user.id != ADMIN_ID: active_users.add(chat_id)

    if call.data.startswith("user_select_"):
        country = call.data[12:]
        user_current_country[chat_id] = country
        send_random_number(chat_id, country, edit=True)

    elif call.data == "change_number":
        send_random_number(chat_id, user_current_country.get(chat_id), edit=True)

    elif call.data == "change_country":
        mk = types.InlineKeyboardMarkup()
        for c in sorted(numbers_by_country.keys()):
            mk.add(types.InlineKeyboardButton(c, callback_data=f"user_select_{c}"))
        try:
            bot.edit_message_text("🌍 Select Country:", chat_id,
                                  user_messages[chat_id].message_id, reply_markup=mk)
        except: pass

    elif call.data.startswith("view_past_"):
        number = call.data[10:]
        if get_number_by_chat(chat_id) != number:
            bot.answer_callback_query(call.id, "❌ Not your current number!"); return
        bot.answer_callback_query(call.id, "⏳ Fetching...")
        fetch_past_otps(chat_id, number)

    elif call.data == "verify_join":
        not_joined = []
        for ch in REQUIRED_CHANNELS:
            try:
                m = bot.get_chat_member(ch, chat_id)
                if m.status not in ["member","creator","administrator"]: not_joined.append(ch)
            except: not_joined.append(ch)
        if not_joined: bot.answer_callback_query(call.id, "❌ Still not joined all channels!")
        else:
            bot.answer_callback_query(call.id, "✅ Verified!")
            start(call.message)

# ==================== CLEANUP THREAD ====================
def cleanup_thread():
    while True:
        time.sleep(3600)
        try: clean_old_cache(); print("🧹 Cache cleaned", flush=True)
        except Exception as e: print(f"Cleanup error: {e}", flush=True)

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
    print(f"🚀 OTP Bot v2.0 Starting — ZoneSMS API", flush=True)
    print(f"📊 Countries loaded: {len(numbers_by_country)}", flush=True)

    threading.Thread(target=run_bot,              daemon=True, name="BotPoller").start()
    threading.Thread(target=otp_scraper_thread,   daemon=True, name="OTPScraper").start()
    threading.Thread(target=group_sender_thread,  daemon=True, name="GroupSender").start()
    threading.Thread(target=personal_sender_thread,daemon=True, name="PersonalSender").start()
    threading.Thread(target=cleanup_thread,       daemon=True, name="Cleaner").start()

    print("✅ All threads started!", flush=True)
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
