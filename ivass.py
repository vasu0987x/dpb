import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import time
import hashlib
import json
import os
import threading
from queue import Queue, Empty
import random
import html as html_module
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, Response

# ╔══════════════════════════════════════════════════════════════╗
# ║                    BOT CONFIGURATION                         ║
# ╚══════════════════════════════════════════════════════════════╝
LOGIN_URL       = "https://ivas.tempnum.qzz.io/login"
PORTAL_URL      = "https://ivas.tempnum.qzz.io/portal"
NUMBERS_API_URL = "https://ivas.tempnum.qzz.io/portal/numbers"
GETSMS_URL      = "https://ivas.tempnum.qzz.io/portal/sms/received/getsms"
NUMBERS_URL     = "https://ivas.tempnum.qzz.io/portal/sms/received/getsms/number"
SMS_URL         = "https://ivas.tempnum.qzz.io/portal/sms/received/getsms/number/sms"

EMAIL    = os.getenv("EMAIL", "Coldflyteam")
PASSWORD = os.getenv("PASSWORD", "Coldflyteam")

# ╔══════════════════════════════════════════════════════════════╗
# ║                  TELEGRAM CONFIGURATION                      ║
# ╚══════════════════════════════════════════════════════════════╝
TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN", "token")
TELEGRAM_CHAT_ID   = "-1003633481131"
ADMIN_USER_IDS     = [8195360535]

# ╔══════════════════════════════════════════════════════════════╗
# ║                   INLINE BUTTON LINKS                        ║
# ╚══════════════════════════════════════════════════════════════╝
PANEL_LINK     = "https://t.me/DDXOTPBOT"
CHANNEL_LINK   = "https://t.me/ddxotp"
OTP_GROUP_LINK = "https://t.me/+SDPuI2Ud62RkN2Jl"

# ╔══════════════════════════════════════════════════════════════╗
# ║               FORCE SUBSCRIBE CONFIGURATION                  ║
# ╚══════════════════════════════════════════════════════════════╝
FORCE_SUB_CHANNELS = [
    {"channel_id": "@DDXOTP",  "channel_name": "DDXOTP",  "link": "https://t.me/ddxotp"},
    {"channel_id": "@VASUHUB", "channel_name": "VASUHUB", "link": "https://t.me/vasuhub"},
]
FORCE_SUB_ENABLED = True

# ╔══════════════════════════════════════════════════════════════╗
# ║                     TIMING CONFIG  ⚡ OPTIMIZED              ║
# ╚══════════════════════════════════════════════════════════════╝
def get_today():
    return datetime.now().strftime("%Y-%m-%d")

OTP_POLL_INTERVAL   = 0.3    # 0.5 → 0.3
CMD_POLL_INTERVAL   = 0.0    # 0.1 → 0 (tight loop)
NUM_SMS_THREADS     = 30     # 20 → 30
NUM_MSG_SENDERS     = 8      # 3 → 8
CSRF_REFRESH_EVERY  = 100
HTTP_TIMEOUT        = 8      # 5 → 8 (Koyeb network)
TG_API_TIMEOUT      = 10     # 8 → 10
TG_RATE_LIMIT_DELAY = 0.02   # 0.05 → 0.02

# ╔══════════════════════════════════════════════════════════════╗
# ║                    GLOBAL VARIABLES                          ║
# ╚══════════════════════════════════════════════════════════════╝
SEEN_SMS       = set()
LAST_UPDATE_ID = 0
BOT_RUNNING    = True
SMS_LOCK       = threading.Lock()
UPDATE_LOCK    = threading.Lock()
NUMBERS_CACHE  = {}
CACHE_LOCK     = threading.Lock()
MESSAGE_QUEUE  = Queue()
SMS_TEXT_CACHE = {}

USER_DB_FILE     = "user_numbers.json"
NUMBERS_TXT_FILE = "numbers_cache.txt"
CACHE_INFO_FILE  = "cache_info.json"
SETTINGS_FILE    = "bot_settings.json"
COOKIES_FILE     = "cookies.json"

# ── Session expire tracking ────────────────────────────────────
SESSION_EXPIRED = False
SESSION_LOCK    = threading.Lock()

SMS_EXECUTOR = ThreadPoolExecutor(max_workers=NUM_SMS_THREADS, thread_name_prefix="SMS")

# ╔══════════════════════════════════════════════════════════════╗
# ║                    SETTINGS                                  ║
# ╚══════════════════════════════════════════════════════════════╝
EXTRA_CHATS_FILE = "extra_chats.json"

def load_extra_chats():
    if os.path.exists(EXTRA_CHATS_FILE):
        try:
            with open(EXTRA_CHATS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return []

def save_extra_chats(chats):
    with open(EXTRA_CHATS_FILE, "w") as f:
        json.dump(chats, f, indent=2)

EXTRA_CHATS = load_extra_chats()


def handle_addchat_command(chat_id, user_id, message_text):
    global EXTRA_CHATS
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    parts = message_text.strip().split()
    if len(parts) < 2:
        send_telegram_message(chat_id,
            "❌ <b>Usage:</b> <code>/addchat -1001234567890</code>\n\n"
            "Bot ko us group ka admin banana zaroori hai pehle.")
        return
    try:
        new_chat_id = int(parts[1])
    except ValueError:
        send_telegram_message(chat_id, "❌ Valid chat ID do (e.g. -1001234567890)")
        return
    if new_chat_id in EXTRA_CHATS:
        send_telegram_message(chat_id, f"⚠️ Chat <code>{new_chat_id}</code> already added hai.")
        return
    EXTRA_CHATS.append(new_chat_id)
    save_extra_chats(EXTRA_CHATS)
    send_telegram_message(chat_id,
        f"✅ <b>Chat Added!</b>\n\n"
        f"📢 Chat ID: <code>{new_chat_id}</code>\n"
        f"Ab is group mein bhi SMS forward honge.\n\n"
        f"Total extra chats: <b>{len(EXTRA_CHATS)}</b>")

def handle_removechat_command(chat_id, user_id, message_text):
    global EXTRA_CHATS
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    parts = message_text.strip().split()
    if len(parts) < 2:
        send_telegram_message(chat_id,
            "❌ <b>Usage:</b> <code>/removechat -1001234567890</code>")
        return
    try:
        rem_chat_id = int(parts[1])
    except ValueError:
        send_telegram_message(chat_id, "❌ Valid chat ID do.")
        return
    if rem_chat_id not in EXTRA_CHATS:
        send_telegram_message(chat_id, f"❌ Chat <code>{rem_chat_id}</code> list mein nahi hai.")
        return
    EXTRA_CHATS.remove(rem_chat_id)
    save_extra_chats(EXTRA_CHATS)
    send_telegram_message(chat_id,
        f"✅ <b>Chat Removed!</b>\n\nChat ID: <code>{rem_chat_id}</code>")

def handle_listchats_command(chat_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    if not EXTRA_CHATS:
        send_telegram_message(chat_id, "📭 Koi extra chat add nahi hai.\n\nUse: /addchat -100xxxxxxxxx")
        return
    text = f"📢 <b>Extra Chats ({len(EXTRA_CHATS)}):</b>\n\n"
    for i, cid in enumerate(EXTRA_CHATS, 1):
        text += f"{i}. <code>{cid}</code>\n"
    text += "\nRemove: <code>/removechat chat_id</code>"
    send_telegram_message(chat_id, text)


def load_settings():
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"auto_delete_minutes": 0, "flag_overrides": {}}

def save_settings(settings):
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=2)

_settings           = load_settings()
AUTO_DELETE_MINUTES = _settings.get("auto_delete_minutes", 0)
FLAG_OVERRIDES      = _settings.get("flag_overrides", {})

# ╔══════════════════════════════════════════════════════════════╗
# ║                    COUNTRY FLAGS                             ║
# ╚══════════════════════════════════════════════════════════════╝
COUNTRY_FLAGS = {
    "PAKISTAN": "🇵🇰", "INDIA": "🇮🇳", "USA": "🇺🇸", "UK": "🇬🇧",
    "CANADA": "🇨🇦", "AUSTRALIA": "🇦🇺", "BANGLADESH": "🇧🇩", "UAE": "🇦🇪",
    "SAUDI": "🇸🇦", "KUWAIT": "🇰🇼", "QATAR": "🇶🇦", "OMAN": "🇴🇲",
    "BAHRAIN": "🇧🇭", "TURKEY": "🇹🇷", "GERMANY": "🇩🇪", "FRANCE": "🇫🇷",
    "ITALY": "🇮🇹", "SPAIN": "🇪🇸", "RUSSIA": "🇷🇺", "CHINA": "🇨🇳",
    "JAPAN": "🇯🇵", "KOREA": "🇰🇷", "SINGAPORE": "🇸🇬", "MALAYSIA": "🇲🇾",
    "INDONESIA": "🇮🇩", "THAILAND": "🇹🇭", "VIETNAM": "🇻🇳", "PHILIPPINES": "🇵🇭",
    "NETHERLANDS": "🇳🇱", "BELGIUM": "🇧🇪", "SWITZERLAND": "🇨🇭", "SWEDEN": "🇸🇪",
    "NORWAY": "🇳🇴", "DENMARK": "🇩🇰", "POLAND": "🇵🇱", "BRAZIL": "🇧🇷",
    "MEXICO": "🇲🇽", "ARGENTINA": "🇦🇷", "SOUTH AFRICA": "🇿🇦", "EGYPT": "🇪🇬",
    "NIGERIA": "🇳🇬", "KENYA": "🇰🇪", "MADAGASCAR": "🇲🇬", "AFGHANISTAN": "🇦🇫",
    "IVORY": "🇨🇮", "GHANA": "🇬🇭", "ETHIOPIA": "🇪🇹", "TANZANIA": "🇹🇿",
}

# ╔══════════════════════════════════════════════════════════════╗
# ║                 SERVICE CODES                                ║
# ╚══════════════════════════════════════════════════════════════╝
SERVICE_CODES = {
    "whatsapp": "WA", "telegram": "TG", "instagram": "IG",
    "facebook": "FB", "twitter": "TW", "google": "GO",
    "amazon": "AZ", "snapchat": "SC", "tiktok": "TT",
    "linkedin": "LI", "uber": "UB", "paypal": "PP",
}

def get_service_code(sender: str) -> str:
    s = sender.lower()
    for key, code in SERVICE_CODES.items():
        if key in s:
            return code
    return sender[:2].upper() if len(sender) >= 2 else sender.upper()

# ╔══════════════════════════════════════════════════════════════╗
# ║                   DATABASE FUNCTIONS                         ║
# ╚══════════════════════════════════════════════════════════════╝
_user_db_lock = threading.Lock()

def load_user_db():
    if os.path.exists(USER_DB_FILE):
        with _user_db_lock:
            with open(USER_DB_FILE, "r") as f:
                return json.load(f)
    return {}

def save_user_db(db):
    with _user_db_lock:
        with open(USER_DB_FILE, "w") as f:
            json.dump(db, f, indent=2)

def load_numbers_from_txt():
    global NUMBERS_CACHE
    if not os.path.exists(NUMBERS_TXT_FILE):
        return False
    try:
        with CACHE_LOCK:
            NUMBERS_CACHE = {}
            with open(NUMBERS_TXT_FILE, "r", encoding="utf-8") as f:
                current_country = None
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith("#"):
                        current_country = line[1:].strip()
                        NUMBERS_CACHE[current_country] = []
                    elif "|" in line and current_country:
                        range_name, number = line.split("|", 1)
                        NUMBERS_CACHE[current_country].append({
                            "range": range_name, "number": number, "country": current_country
                        })
        total = sum(len(v) for v in NUMBERS_CACHE.values())
        print(f"📦 Loaded {total} numbers from cache")
        return True
    except Exception as e:
        print(f"❌ Error loading cache: {e}")
        return False

def save_numbers_to_txt():
    try:
        with CACHE_LOCK:
            with open(NUMBERS_TXT_FILE, "w", encoding="utf-8") as f:
                for country in sorted(NUMBERS_CACHE.keys()):
                    f.write(f"# {country}\n")
                    for num_data in NUMBERS_CACHE[country]:
                        f.write(f"{num_data['range']}|{num_data['number']}\n")
                    f.write("\n")
            with open(CACHE_INFO_FILE, "w") as f:
                json.dump({
                    "timestamp": datetime.now().isoformat(),
                    "total_numbers": sum(len(v) for v in NUMBERS_CACHE.values()),
                    "total_countries": len(NUMBERS_CACHE)
                }, f, indent=2)
        total = sum(len(v) for v in NUMBERS_CACHE.values())
        print(f"💾 Saved {total} numbers to cache")
        return True
    except Exception as e:
        print(f"❌ Error saving cache: {e}")
        return False

def delete_old_cache():
    for file in [NUMBERS_TXT_FILE, CACHE_INFO_FILE]:
        if os.path.exists(file):
            os.remove(file)
            print(f"🗑️ Deleted {file}")

# ╔══════════════════════════════════════════════════════════════╗
# ║                  COOKIE MANAGEMENT                           ║
# ╚══════════════════════════════════════════════════════════════╝
def load_cookies():
    if os.path.exists(COOKIES_FILE):
        try:
            with open(COOKIES_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_cookies_to_file(cookies_dict):
    with open(COOKIES_FILE, "w") as f:
        json.dump(cookies_dict, f, indent=2)
    print("💾 Cookies saved!")

def inject_cookies(session):
    cookies = load_cookies()
    if not cookies:
        return False
    for name, value in cookies.items():
        session.cookies.set(name, value, domain="ivas.tempnum.qzz.io")
    print(f"🍪 Injected {len(cookies)} cookies")
    return True

def notify_admins_session_expired():
    msg = (
        "⚠️ <b>Session Expired!</b>\n\n"
        "Bot ka login expire ho gaya hai.\n\n"
        "📋 <b>Steps to fix:</b>\n"
        "1. Browser mein <code>ivas.tempnum.qzz.io/login</code> open karo\n"
        "2. Manually login karo\n"
        "3. <b>F12 → Application → Cookies</b>\n"
        "4. <code>ivas_sms_session</code> copy karo\n"
        "5. <code>XSRF-TOKEN</code> copy karo\n\n"
        "Phir bot mein yeh command use karo:\n"
        "<code>/setcookies SESSION_VALUE XSRF_VALUE</code>"
    )
    for admin_id in ADMIN_USER_IDS:
        try:
            send_telegram_message(admin_id, msg)
            print(f"📢 Notified admin {admin_id} about session expiry")
        except Exception as e:
            print(f"⚠️ Could not notify admin {admin_id}: {e}")

# ╔══════════════════════════════════════════════════════════════╗
# ║               FORCE SUBSCRIBE SYSTEM                         ║
# ╚══════════════════════════════════════════════════════════════╝
def check_force_subscribe(user_id):
    if not FORCE_SUB_ENABLED or not FORCE_SUB_CHANNELS:
        return True, []
    not_joined = []
    for channel in FORCE_SUB_CHANNELS:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getChatMember"
            response = requests.post(url, json={
                "chat_id": channel["channel_id"],
                "user_id": user_id
            }, timeout=TG_API_TIMEOUT)
            data = response.json()
            if not data.get("ok"):
                continue
            status = data.get("result", {}).get("status", "")
            if status not in ["member", "administrator", "creator"]:
                not_joined.append(channel)
        except Exception as e:
            print(f"⚠️ Force sub check error: {e}")
    return len(not_joined) == 0, not_joined

def send_force_subscribe_message(chat_id, not_joined_channels):
    msg = "🔒 <b>Access Required!</b>\n\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "🚫 Must Join These Channels\n"
    msg += "To Use This Bot:\n\n"
    buttons = []
    for i, channel in enumerate(not_joined_channels, 1):
        msg += f"{'🔴' if i == 1 else '🟠' if i == 2 else '🟡'} <b>Channel {i}:</b> {channel['channel_name']}\n"
        buttons.append([{"text": f"Join {channel['channel_name']}", "url": channel["link"]}])
    msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "⚡ After join send <b>/start</b> again"
    buttons.append([{"text": "✅ JOINED!", "callback_data": "check_subscription"}])
    send_telegram_message(chat_id, msg, {"inline_keyboard": buttons})

# ╔══════════════════════════════════════════════════════════════╗
# ║                      HELPER FUNCTIONS                        ║
# ╚══════════════════════════════════════════════════════════════╝
def create_session():
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=30,   # 20 → 30
        pool_maxsize=30,       # 20 → 30
        max_retries=1
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*"
    })
    return s

def extract_csrf(html):
    soup = BeautifulSoup(html, "html.parser")
    token = soup.find("input", {"name": "_token"})
    if token:
        return token.get("value")
    meta = soup.find("meta", {"name": "csrf-token"})
    if meta:
        return meta.get("content")
    return None

def extract_otp(text):
    if not text or len(text) < 4:
        return None
    space_pattern = re.findall(r'\b(\d{3})\s+(\d{3})\b', text)
    if space_pattern:
        return space_pattern[0][0] + space_pattern[0][1]
    hyphen_pattern = re.findall(r'\b(\d{3})-(\d{3})\b', text)
    if hyphen_pattern:
        return hyphen_pattern[0][0] + hyphen_pattern[0][1]
    clean_text = re.sub(r'[^0-9]', ' ', text)
    matches = re.findall(r'\b(\d{4,8})\b', clean_text)
    if matches:
        for match in matches:
            if len(match) == 6:
                return match
        for match in matches:
            if len(match) == 4:
                return match
        for match in matches:
            if 4 <= len(match) <= 8:
                return match
    return None

def sms_hash(range_name, number, text):
    raw = f"{range_name}|{number}|{text}"
    return hashlib.md5(raw.encode()).hexdigest()

def extract_country_name(range_name):
    parts = range_name.split()
    return parts[0].upper()

def get_country_flag(country_name):
    code = country_name.upper()
    if code in FLAG_OVERRIDES:
        regular = COUNTRY_FLAGS.get(code, "🌍")
        emoji_id = FLAG_OVERRIDES[code]
        return f'<tg-emoji emoji-id="{emoji_id}">{regular}</tg-emoji>'
    return COUNTRY_FLAGS.get(code, "🌍")

def get_plain_flag(country_name):
    return COUNTRY_FLAGS.get(country_name.upper(), "🌍")

def mask_number(number):
    if len(number) <= 4:
        return number
    return f"{number[:2]}DDX{number[-4:]}"

# ╔══════════════════════════════════════════════════════════════╗
# ║                       LOGIN SYSTEM                           ║
# ╚══════════════════════════════════════════════════════════════╝
def login(session):
    if inject_cookies(session):
        test = session.get(PORTAL_URL, timeout=HTTP_TIMEOUT, allow_redirects=True)
        if "login" not in test.url.lower():
            print("✅ Logged in via cookies!")
            csrf = extract_csrf(test.text)
            if not csrf:
                r = session.get(LOGIN_URL.replace("/login", "/portal/sms/received"), timeout=HTTP_TIMEOUT)
                csrf = extract_csrf(r.text)
            return csrf
        else:
            print("⚠️ Cookies expired, trying credentials...")

    r = session.get(LOGIN_URL, timeout=HTTP_TIMEOUT)
    csrf = extract_csrf(r.text)
    if not csrf:
        raise Exception("CSRF NOT FOUND")
    session.post(LOGIN_URL, data={
        "email": EMAIL,
        "password": PASSWORD,
        "_token": csrf,
        "submit": "register"
    }, timeout=HTTP_TIMEOUT)
    test = session.get(PORTAL_URL, timeout=HTTP_TIMEOUT)
    if "login" in test.url.lower():
        raise Exception("SESSION_EXPIRED")
    return csrf

# ╔══════════════════════════════════════════════════════════════╗
# ║                    NUMBER MANAGEMENT                         ║
# ╚══════════════════════════════════════════════════════════════╝
def fetch_all_numbers_from_api():
    print("🔄 Fetching fresh numbers from API...")
    try:
        session = create_session()
        csrf = login(session)
        params = {
            'draw': '1',
            'columns[0][data]': 'number_id', 'columns[0][name]': 'id',
            'columns[0][orderable]': 'false',
            'columns[1][data]': 'Number', 'columns[2][data]': 'range',
            'columns[3][data]': 'A2P', 'columns[4][data]': 'P2P',
            'columns[5][data]': 'LimitA2P', 'columns[6][data]': 'limit_cli_a2p',
            'columns[7][data]': 'limit_did_a2p', 'columns[8][data]': 'limit_cli_did_a2p',
            'columns[9][data]': 'LimitP2P', 'columns[10][data]': 'limit_cli_p2p',
            'columns[11][data]': 'limit_did_p2p', 'columns[12][data]': 'limit_cli_did_p2p',
            'columns[13][data]': 'action', 'columns[13][searchable]': 'false',
            'columns[13][orderable]': 'false',
            'order[0][column]': '1', 'order[0][dir]': 'desc',
            'start': '0', 'length': '10000', 'search[value]': '',
        }
        headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'X-CSRF-TOKEN': csrf,
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Referer': PORTAL_URL + '/numbers'
        }
        r = session.get(NUMBERS_API_URL, params=params, headers=headers, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            print(f"❌ API request failed: {r.status_code}")
            return {}
        data = r.json()
        numbers_data = data.get('data', [])
        country_dict = {}
        for item in numbers_data:
            number = str(item.get('Number', ''))
            range_name = item.get('range', '')
            if not number or not range_name:
                continue
            country = extract_country_name(range_name)
            if country not in country_dict:
                country_dict[country] = []
            country_dict[country].append({
                "range": range_name, "number": number, "country": country
            })
        print(f"✅ Fetched {len(numbers_data)} numbers from {len(country_dict)} countries")
        return country_dict
    except Exception as e:
        print(f"❌ Error fetching numbers: {e}")
        return {}

def refresh_numbers_cache():
    global NUMBERS_CACHE
    print("🗑️ Deleting old cache...")
    delete_old_cache()
    new_cache = fetch_all_numbers_from_api()
    if not new_cache:
        print("❌ Failed to fetch numbers")
        return False
    with CACHE_LOCK:
        NUMBERS_CACHE = new_cache
    save_numbers_to_txt()
    return True

def get_available_countries():
    with CACHE_LOCK:
        return list(NUMBERS_CACHE.keys())

def get_number_from_country(country, exclude_numbers=None):
    with CACHE_LOCK:
        if country not in NUMBERS_CACHE or not NUMBERS_CACHE[country]:
            return None
        user_db = load_user_db()
        taken_numbers = {data["number"] for data in user_db.values()}
        if exclude_numbers:
            taken_numbers.update(exclude_numbers)
        available = [n for n in NUMBERS_CACHE[country] if n["number"] not in taken_numbers]
        if not available:
            return None
        return random.choice(available)

# ╔══════════════════════════════════════════════════════════════╗
# ║     TELEGRAM API — POOLED SESSION ⚡ OPTIMIZED              ║
# ╚══════════════════════════════════════════════════════════════╝
_tg_session = requests.Session()
_tg_adapter = requests.adapters.HTTPAdapter(
    pool_connections=20,   # 10 → 20
    pool_maxsize=50,       # 20 → 50
    max_retries=1
)
_tg_session.mount("https://", _tg_adapter)
_tg_lock = threading.Lock()

def _tg_post(endpoint, payload):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{endpoint}"
    try:
        r = _tg_session.post(url, json=payload, timeout=TG_API_TIMEOUT)
        return r.json() if r.status_code == 200 else None
    except Exception as e:
        print(f"⚠️ TG {endpoint} error: {e}")
        return None

def _tg_get(endpoint, params=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{endpoint}"
    try:
        r = _tg_session.get(url, params=params, timeout=TG_API_TIMEOUT)
        return r.json() if r.status_code == 200 else None
    except Exception as e:
        print(f"⚠️ TG {endpoint} error: {e}")
        return None

def send_telegram_message(chat_id, message, reply_markup=None):
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return _tg_post("sendMessage", payload)

def edit_telegram_message(chat_id, message_id, message, reply_markup=None):
    payload = {
        "chat_id": chat_id, "message_id": message_id,
        "text": message, "parse_mode": "HTML"
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    result = _tg_post("editMessageText", payload)
    return result is not None

def delete_telegram_message(chat_id, message_id):
    _tg_post("deleteMessage", {"chat_id": chat_id, "message_id": message_id})

def get_telegram_updates():
    global LAST_UPDATE_ID
    with UPDATE_LOCK:
        offset = LAST_UPDATE_ID + 1
    # ⚡ timeout=0 = instant response, no waiting
    data = _tg_get("getUpdates", {"offset": offset, "timeout": 0, "limit": 100})
    if data and data.get("ok"):
        updates = data.get("result", [])
        if updates:
            with UPDATE_LOCK:
                LAST_UPDATE_ID = updates[-1].get("update_id", LAST_UPDATE_ID)
        return updates
    return []

def answer_callback_query(query_id, text=None, show_alert=False):
    payload = {"callback_query_id": query_id, "show_alert": show_alert}
    if text:
        payload["text"] = text
    _tg_post("answerCallbackQuery", payload)

# ╔══════════════════════════════════════════════════════════════╗
# ║                     KEYBOARD BUILDERS                        ║
# ╚══════════════════════════════════════════════════════════════╝
def create_country_keyboard():
    countries = sorted(get_available_countries())
    buttons = []
    row = []
    for country in countries:
        flag = get_plain_flag(country)
        row.append({"text": f"{flag} {country}", "callback_data": f"country:{country}"})
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return {"inline_keyboard": buttons}

def create_number_keyboard():
    return {"inline_keyboard": [
        [{"text": "🔄 Change Number", "callback_data": "change_same"},
         {"text": "🌍 Change Country", "callback_data": "change_country"}],
        [{"text": "📢 Join OTP Group", "url": OTP_GROUP_LINK}]
    ]}

def create_admin_keyboard():
    return {"inline_keyboard": [
        [
            {"text": "📊 Stats", "callback_data": "admin:stats"},
            {"text": "👥 Users", "callback_data": "admin:usercount"}
        ],
        [
            {"text": "🔄 Refresh Numbers", "callback_data": "admin:fresh"},
            {"text": "📱 Seen SMS", "callback_data": "admin:seensms"}
        ],
        [
            {"text": "🗑️ Clear SMS Cache", "callback_data": "admin:clearseen"},
            {"text": "🔔 Broadcast", "callback_data": "admin:broadcast_help"}
        ],
        [
            {"text": "🍪 Session Status", "callback_data": "admin:sessionstatus"}
        ],
        [
            {"text": "📋 All Commands", "callback_data": "admin:help"}
        ]
    ]}

# ╔══════════════════════════════════════════════════════════════╗
# ║                    USER MANAGEMENT                           ║
# ╚══════════════════════════════════════════════════════════════╝
def assign_number_to_user(user_id, username, country, exclude_numbers=None):
    user_db = load_user_db()
    num_data = get_number_from_country(country, exclude_numbers)
    if not num_data:
        return None
    user_db[str(user_id)] = {
        "user_id": user_id, "username": username,
        "number": num_data["number"], "range": num_data["range"],
        "country": num_data["country"],
        "assigned_at": datetime.now().isoformat()
    }
    save_user_db(user_db)
    return user_db[str(user_id)]

def get_user_number(user_id):
    return load_user_db().get(str(user_id))

def change_user_number(user_id, country):
    user_db = load_user_db()
    old_username = "Unknown"
    exclude_set = set()
    if str(user_id) in user_db:
        old_data = user_db[str(user_id)]
        old_username = old_data.get("username", "Unknown")
        old_number = old_data.get("number", "")
        if old_number:
            exclude_set.add(old_number)
        del user_db[str(user_id)]
        save_user_db(user_db)
    return assign_number_to_user(user_id, old_username, country, exclude_set)

def get_user_id_by_number(number):
    user_db = load_user_db()
    number_str = str(number)
    for user_id, data in user_db.items():
        if str(data.get("number", "")) == number_str:
            return int(user_id)
    return None

# ╔══════════════════════════════════════════════════════════════╗
# ║               MESSAGE FORMATTER                              ║
# ╚══════════════════════════════════════════════════════════════╝
def get_service_emoji(sender):
    sender_lower = sender.lower() if sender else ""
    if "whatsapp" in sender_lower:
        return '<tg-emoji emoji-id="5334998226636390258">📱</tg-emoji>'
    elif "telegram" in sender_lower:
        return '<tg-emoji emoji-id="5330237710655306682">✈️</tg-emoji>'
    elif "instagram" in sender_lower:
        return '<tg-emoji emoji-id="5319160079465857105">📸</tg-emoji>'
    elif "facebook" in sender_lower:
        return '<tg-emoji emoji-id="5323261730283863478">👤</tg-emoji>'
    else:
        return '<tg-emoji emoji-id="6125390694363175728">🌐</tg-emoji>'

def format_message_group(range_name, number, sms_text, otp, sender=""):
    country      = extract_country_name(range_name)
    flag         = get_country_flag(country)
    masked_num   = mask_number(number)
    service_emoji = get_service_emoji(sender)
    service_code  = get_service_code(sender) if sender else "SMS"

    sms_escaped = (sms_text
                   .replace('&', '&amp;')
                   .replace('<', '&lt;')
                   .replace('>', '&gt;'))

    msg = (
        f'<tg-emoji emoji-id="5382357040008021292">⚡</tg-emoji> '
        f'{flag} <b>{country}</b> | <code>{masked_num}</code> | '
        f'{service_emoji} <b>{service_code}</b>\n'
    )

    return msg

def format_message_personal(range_name, number, sms_text, otp, sender=""):
    country    = extract_country_name(range_name)
    flag       = get_country_flag(country)
    service_emoji = get_service_emoji(sender)
    sender_str = sender if sender else "Unknown"
    otp_str    = otp if otp else "❓ Not detected"

    sms_escaped = (sms_text
                   .replace('&', '&amp;')
                   .replace('<', '&lt;')
                   .replace('>', '&gt;'))

    return (
        f'<tg-emoji emoji-id="5382357040008021292">⚡</tg-emoji> <b>NEW MESSAGE!</b>\n'
        f'━━━━━━━━━━━━━━━\n'
        f'{flag} <b>Country:</b> {html_module.escape(country)}\n'
        f'{service_emoji} <b>Service:</b> {html_module.escape(sender_str)}\n'
        f'📞 <b>Number:</b> <code>{html_module.escape(number)}</code>\n'
        f'━━━━━━━━━━━━━━━\n'
        f'🔑 <b>OTP Code:</b> <code>{otp_str}</code>\n'
        f'━━━━━━━━━━━━━━━\n'
        f'💬 <b>Full Message:</b>\n<code>{sms_escaped[:500]}</code>'
    )

# ╔══════════════════════════════════════════════════════════════╗
# ║                    CALLBACK HANDLERS                         ║
# ╚══════════════════════════════════════════════════════════════╝
def handle_callback_query(callback_query):
    query_id   = callback_query.get("id")
    user       = callback_query.get("from", {})
    user_id    = user.get("id")
    username   = user.get("username", "Unknown")
    data       = callback_query.get("data", "")
    message    = callback_query.get("message", {})
    chat_id    = message.get("chat", {}).get("id")
    message_id = message.get("message_id")

    answer_callback_query(query_id)

    if data == "check_subscription":
        is_subbed, not_joined = check_force_subscribe(user_id)
        if is_subbed:
            answer_callback_query(query_id, "✅ Verified! Access granted.", show_alert=True)
            show_country_selection(chat_id, message_id)
        else:
            answer_callback_query(query_id, "❌ Abhi bhi join nahi kiya!", show_alert=True)
            send_force_subscribe_message(chat_id, not_joined)
        return

    if data.startswith("otp_copy_"):
        otp_val = data[9:]
        answer_callback_query(query_id, f"✅ OTP: {otp_val}", show_alert=True)
        return

    if data.startswith("fullsms_"):
        hash_id = data[8:]
        with SMS_LOCK:
            sms_text = SMS_TEXT_CACHE.get(hash_id)
        if sms_text:
            answer_callback_query(query_id, sms_text[:200], show_alert=True)
        else:
            answer_callback_query(query_id, "❌ SMS expired or not found.", show_alert=True)
        return

    if not data.startswith("admin:"):
        is_subbed, not_joined = check_force_subscribe(user_id)
        if not is_subbed:
            answer_callback_query(query_id, "⛔ Pehle channel join karo!", show_alert=True)
            send_force_subscribe_message(chat_id, not_joined)
            return

    if data == "change_same":
        handle_change_same_country(chat_id, message_id, user_id, username)
    elif data == "change_country":
        show_country_selection(chat_id, message_id)
    elif data.startswith("country:"):
        country = data.split(":", 1)[1]
        handle_country_selection(chat_id, message_id, user_id, username, country)
    elif data == "admin:stats":
        _admin_cb_stats(chat_id, message_id, user_id)
    elif data == "admin:usercount":
        _admin_cb_usercount(chat_id, message_id, user_id)
    elif data == "admin:fresh":
        _admin_cb_fresh(chat_id, message_id, user_id)
    elif data == "admin:seensms":
        _admin_cb_seensms(chat_id, message_id, user_id)
    elif data == "admin:clearseen":
        _admin_cb_clearseen(chat_id, message_id, user_id)
    elif data == "admin:broadcast_help":
        _admin_cb_broadcast_help(chat_id, message_id, user_id)
    elif data == "admin:help":
        _admin_cb_help(chat_id, message_id, user_id)
    elif data == "admin:menu":
        _show_admin_panel(chat_id, message_id, user_id)
    elif data == "admin:sessionstatus":
        _admin_cb_sessionstatus(chat_id, message_id, user_id)

def show_country_selection(chat_id, message_id):
    msg = (
        "🌍 <b>Select Your Country</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "📱 You will receive a phone number\n"
        "to get OTP messages.\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👇 <b>Choose your country below:</b>"
    )
    edit_telegram_message(chat_id, message_id, msg, create_country_keyboard())

def handle_country_selection(chat_id, message_id, user_id, username, country):
    user_data = get_user_number(user_id)
    if user_data:
        new_data = change_user_number(user_id, country)
    else:
        new_data = assign_number_to_user(user_id, username, country)

    if new_data:
        flag   = get_country_flag(country)
        number = new_data["number"]
        CHECK_ID = "6217462750101113987"
        GLOBE_ID = "6221806022894294249"
        PHONE_ID = "6217716058682299470"
        BOLT_ID  = "6120713986078937328"
        JOIN_ID  = "6219498740693078802"

        msg = (
            f"<tg-emoji emoji-id='{CHECK_ID}'>✅</tg-emoji> <b>Number Assigned!</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<tg-emoji emoji-id='{GLOBE_ID}'>{flag}</tg-emoji> <b>Country:</b> {country}\n"
            f"<tg-emoji emoji-id='{PHONE_ID}'>📱</tg-emoji> <b>Number:</b> <code>{number}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<tg-emoji emoji-id='{BOLT_ID}'>⚡</tg-emoji> All messages received on this\n"
            f"number will be delivered to you.\n\n"
            f"<tg-emoji emoji-id='{JOIN_ID}'>📢</tg-emoji> Join the OTP group below 👇"
        )

        kb = {
            "inline_keyboard": [
                [
                    {"text": "Change Number", "callback_data": "change_same",
                     "icon_custom_emoji_id": "5258500400918587241"},
                    {"text": "Change Country", "callback_data": "change_country",
                     "icon_custom_emoji_id": "5257980374868311346"}
                ],
                [
                    {"text": "Join OTP Group", "url": OTP_GROUP_LINK,
                     "icon_custom_emoji_id": "6219641556945606133"}
                ]
            ]
        }
        edit_telegram_message(chat_id, message_id, msg, kb)
    else:
        msg = (
            f"❌ <b>No Numbers Available</b>\n\n"
            f"😔 {country} mein koi number\n"
            f"available nahi hai abhi.\n\n"
            f"👇 Koi aur country choose karo:"
        )
        edit_telegram_message(chat_id, message_id, msg, create_country_keyboard())

def handle_change_same_country(chat_id, message_id, user_id, username):
    user_data = get_user_number(user_id)
    if not user_data:
        show_country_selection(chat_id, message_id)
        return
    current_country = user_data["country"]
    new_data = change_user_number(user_id, current_country)
    if new_data:
        flag   = get_country_flag(current_country)
        number = new_data["number"]
        CHANGE_ID = "6222195469053859346"
        FLAG_ID   = "6221806022894294249"
        PHONE_ID  = "6217716058682299470"
        BOLT_ID   = "6120713986078937328"
        JOIN_ID   = "6219498740693078802"

        msg = (
            f"<tg-emoji emoji-id='{CHANGE_ID}'>🔄</tg-emoji> <b>Number Changed!</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"<tg-emoji emoji-id='{FLAG_ID}'>{flag}</tg-emoji> <b>Country:</b> {current_country}\n"
            f"<tg-emoji emoji-id='{PHONE_ID}'>📱</tg-emoji> <b>New Number:</b> <code>{number}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<tg-emoji emoji-id='{BOLT_ID}'>⚡</tg-emoji> New number assigned!\n\n"
            f"<tg-emoji emoji-id='{JOIN_ID}'>📢</tg-emoji> Join otp group below 👇"
        )

        kb = {
            "inline_keyboard": [
                [
                    {"text": "Change Number", "callback_data": "change_same",
                     "icon_custom_emoji_id": "5258500400918587241"},
                    {"text": "Change Country", "callback_data": "change_country",
                     "icon_custom_emoji_id": "5257980374868311346"}
                ],
                [
                    {"text": "Join OTP Group", "url": OTP_GROUP_LINK,
                     "icon_custom_emoji_id": "6219641556945606133"}
                ]
            ]
        }
        edit_telegram_message(chat_id, message_id, msg, kb)
    else:
        msg = (
            f"❌ <b>No More Numbers</b>\n\n"
            f"😔 {current_country} mein koi\n"
            f"aur number nahi hai.\n\n"
            f"👇 Country change karo:"
        )
        edit_telegram_message(chat_id, message_id, msg, create_number_keyboard())

# ╔══════════════════════════════════════════════════════════════╗
# ║               ADMIN PANEL INLINE HANDLERS                    ║
# ╚══════════════════════════════════════════════════════════════╝
def _back_btn():
    return [{"text": "🔙 Admin Panel", "callback_data": "admin:menu"}]

def _show_admin_panel(chat_id, message_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        return
    with CACHE_LOCK:
        total = sum(len(v) for v in NUMBERS_CACHE.values())
        countries = len(NUMBERS_CACHE)
    user_db = load_user_db()
    with SESSION_LOCK:
        session_status = "❌ Expired" if SESSION_EXPIRED else "✅ Active"
    msg = (
        "👑 <b>Admin Control Panel</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 Users: <b>{len(user_db)}</b>\n"
        f"📱 Numbers: <b>{total}</b>\n"
        f"🌍 Countries: <b>{countries}</b>\n"
        f"🔌 Session: <b>{session_status}</b>\n"
        f"🗑️ Auto-Delete: <b>{str(AUTO_DELETE_MINUTES) + ' min' if AUTO_DELETE_MINUTES > 0 else 'Disabled'}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "👇 Select an option:"
    )
    edit_telegram_message(chat_id, message_id, msg, create_admin_keyboard())

def _admin_cb_stats(chat_id, message_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        return
    user_db = load_user_db()
    with CACHE_LOCK:
        total_numbers = sum(len(v) for v in NUMBERS_CACHE.values())
        total_countries = len(NUMBERS_CACHE)
    with SESSION_LOCK:
        session_status = "❌ Expired" if SESSION_EXPIRED else "✅ Active"
    msg = (
        "📊 <b>Bot Statistics</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 Total Users: <b>{len(user_db)}</b>\n"
        f"📱 Total Numbers: <b>{total_numbers}</b>\n"
        f"🌍 Countries: <b>{total_countries}</b>\n"
        f"🔌 Session: <b>{session_status}</b>\n"
        f"🗑️ Auto-Delete: <b>{str(AUTO_DELETE_MINUTES) + ' min' if AUTO_DELETE_MINUTES > 0 else 'Disabled'}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>📊 Numbers by Country:</b>\n"
    )
    with CACHE_LOCK:
        for country in sorted(NUMBERS_CACHE.keys()):
            flag = get_country_flag(country)
            count = len(NUMBERS_CACHE[country])
            msg += f"{flag} {country}: <b>{count}</b>\n"
    edit_telegram_message(chat_id, message_id, msg, {"inline_keyboard": [_back_btn()]})

def _admin_cb_usercount(chat_id, message_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        return
    user_db = load_user_db()
    country_usage = {}
    for data in user_db.values():
        c = data.get("country", "Unknown")
        country_usage[c] = country_usage.get(c, 0) + 1
    msg = (
        "👥 <b>User Statistics</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Total Users: <b>{len(user_db)}</b>\n"
        f"📱 Numbers Assigned: <b>{len(user_db)}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>Users by Country:</b>\n"
    )
    if country_usage:
        for country, count in sorted(country_usage.items(), key=lambda x: x[1], reverse=True):
            flag = get_country_flag(country)
            msg += f"{flag} {country}: <b>{count}</b>\n"
    else:
        msg += "No users yet.\n"
    edit_telegram_message(chat_id, message_id, msg, {"inline_keyboard": [_back_btn()]})

def _admin_cb_fresh(chat_id, message_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        return
    msg = "⏳ <b>Refreshing Numbers...</b>\n\n<i>Yeh thoda time le sakta hai</i>"
    edit_telegram_message(chat_id, message_id, msg)

    def do_refresh():
        success = refresh_numbers_cache()
        if success:
            with CACHE_LOCK:
                total = sum(len(nums) for nums in NUMBERS_CACHE.values())
                countries_count = len(NUMBERS_CACHE)
            result_msg = (
                "✅ <b>Refresh Complete!</b>\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📱 Total Numbers: <b>{total}</b>\n"
                f"🌍 Countries: <b>{countries_count}</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "<b>By Country:</b>\n"
            )
            with CACHE_LOCK:
                for country in sorted(NUMBERS_CACHE.keys()):
                    flag = get_country_flag(country)
                    count = len(NUMBERS_CACHE[country])
                    result_msg += f"{flag} {country}: <b>{count}</b>\n"
        else:
            result_msg = "❌ <b>Refresh Failed!</b>\n\nLogs check karo."
        send_telegram_message(chat_id, result_msg, {"inline_keyboard": [_back_btn()]})

    threading.Thread(target=do_refresh, daemon=True).start()

def _admin_cb_seensms(chat_id, message_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        return
    with SMS_LOCK:
        seen_count = len(SEEN_SMS)
    with SESSION_LOCK:
        session_status = "❌ Expired" if SESSION_EXPIRED else "✅ Active"
    msg = (
        "📊 <b>OTP Worker Status</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Bot: <b>{'Running' if BOT_RUNNING else 'Stopped'}</b>\n"
        f"🔌 Session: <b>{session_status}</b>\n"
        f"💾 Seen SMS: <b>{seen_count}</b>\n"
        f"⏱️ Poll Interval: <b>{OTP_POLL_INTERVAL}s</b>\n"
        f"🧵 SMS Threads: <b>{NUM_SMS_THREADS}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    )
    edit_telegram_message(chat_id, message_id, msg, {"inline_keyboard": [_back_btn()]})

def _admin_cb_clearseen(chat_id, message_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        return
    global SEEN_SMS
    with SMS_LOCK:
        old_count = len(SEEN_SMS)
        SEEN_SMS.clear()
    with _NUMBER_INIT_LOCK:
        _NUMBER_INITIALIZED.clear()
        _NUMBER_SMS_COUNT.clear()
    msg = (
        "🗑️ <b>SMS Cache Cleared!</b>\n\n"
        f"✅ Removed <b>{old_count}</b> seen messages\n"
        "🔄 Next poll will apply startup limit"
    )
    edit_telegram_message(chat_id, message_id, msg, {"inline_keyboard": [_back_btn()]})

def _admin_cb_broadcast_help(chat_id, message_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        return
    msg = (
        "📢 <b>Broadcast Message</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "Sabhi users ko message bhejne\n"
        "ke liye yeh command use karo:\n\n"
        "<code>/broadcast Aapka message yahan</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    )
    edit_telegram_message(chat_id, message_id, msg, {"inline_keyboard": [_back_btn()]})

def _admin_cb_sessionstatus(chat_id, message_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        return
    with SESSION_LOCK:
        expired = SESSION_EXPIRED
    if expired:
        msg = (
            "⚠️ <b>Session Status: EXPIRED</b>\n\n"
            "Bot ka session expire ho gaya hai.\n\n"
            "📋 <b>Fix karo:</b>\n"
            "1. Browser mein login karo\n"
            "2. F12 → Application → Cookies\n"
            "3. <code>ivas_sms_session</code> copy karo\n"
            "4. <code>XSRF-TOKEN</code> copy karo\n\n"
            "<code>/setcookies SESSION_VALUE XSRF_VALUE</code>"
        )
    else:
        msg = "✅ <b>Session Status: ACTIVE</b>\n\nBot properly logged in hai."
    edit_telegram_message(chat_id, message_id, msg, {"inline_keyboard": [_back_btn()]})

def _admin_cb_help(chat_id, message_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        return
    msg = (
        "📋 <b>All Admin Commands</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🔢 <b>Number Management:</b>\n"
        "• /fresh — Numbers refresh karo\n\n"
        "📊 <b>Statistics:</b>\n"
        "• /stats — Detailed statistics\n"
        "• /usercount — Total users\n"
        "• /seensms — OTP worker status\n\n"
        "🛠️ <b>Tools:</b>\n"
        "• /testotp &lt;text&gt; — OTP extraction test\n"
        "• /clearseen — Clear SMS cache\n\n"
        "🍪 <b>Session Management:</b>\n"
        "• /setcookies &lt;session&gt; &lt;xsrf&gt; — Update cookies\n"
        "• /sessionstatus — Session check karo\n\n"
        "🗑️ <b>Auto-Delete:</b>\n"
        "• /autodelete &lt;min&gt; — Group messages auto-delete (0=off)\n\n"
        "🏳 <b>Flag Overrides:</b>\n"
        "• /addflag IN &lt;emoji_id&gt; — Premium flag set karo\n"
        "• /removeflag IN — Flag remove karo\n"
        "• /listflags — Saare flags dekho\n\n"
        "📢 <b>Communication:</b>\n"
        "• /broadcast &lt;msg&gt; — Broadcast to all\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "• /adminhelp — Yeh message"
    )
    edit_telegram_message(chat_id, message_id, msg, {"inline_keyboard": [_back_btn()]})

# ╔══════════════════════════════════════════════════════════════╗
# ║                   COMMAND HANDLERS                           ║
# ╚══════════════════════════════════════════════════════════════╝
def handle_start_command(chat_id, user_id, username):
    loading = send_telegram_message(chat_id, "⏳ Loading...")
    loading_msg_id = None
    if loading and isinstance(loading, dict):
        loading_msg_id = loading.get("result", {}).get("message_id")

    is_subbed, not_joined = check_force_subscribe(user_id)
    if not is_subbed:
        if loading_msg_id:
            delete_telegram_message(chat_id, loading_msg_id)
        send_force_subscribe_message(chat_id, not_joined)
        return

    if user_id in ADMIN_USER_IDS:
        with CACHE_LOCK:
            total = sum(len(v) for v in NUMBERS_CACHE.values())
            countries = len(NUMBERS_CACHE)
        user_db = load_user_db()
        with SESSION_LOCK:
            session_status = "❌ Expired" if SESSION_EXPIRED else "✅ Active"
        msg = (
            f"👑 <b>Welcome Admin!</b>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Users: <b>{len(user_db)}</b>\n"
            f"📱 Numbers: <b>{total}</b>\n"
            f"🌍 Countries: <b>{countries}</b>\n"
            f"🔌 Session: <b>{session_status}</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "👇 Admin panel:"
        )
        if loading_msg_id:
            edit_telegram_message(chat_id, loading_msg_id, msg, create_admin_keyboard())
        else:
            send_telegram_message(chat_id, msg, create_admin_keyboard())
        return

    msg = (
        f"👋 <b>Welcome to the Bot!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🔐 <b>What you get here:</b>\n"
        "• Real phone numbers\n"
        "• Live OTP messages\n"
        "• Multiple countries\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🌍 <b>Select your country:</b>"
    )
    if loading_msg_id:
        edit_telegram_message(chat_id, loading_msg_id, msg, create_country_keyboard())
    else:
        send_telegram_message(chat_id, msg, create_country_keyboard())

def handle_setcookies_command(chat_id, user_id, message_text):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    parts = message_text.strip().split()
    if len(parts) < 3:
        msg = (
            "❌ <b>Usage:</b>\n"
            "<code>/setcookies SESSION_VALUE XSRF_VALUE</code>\n\n"
            "<b>Steps:</b>\n"
            "1. Browser mein login karo\n"
            "2. F12 → Application → Cookies\n"
            "3. <code>ivas_sms_session</code> copy karo\n"
            "4. <code>XSRF-TOKEN</code> copy karo\n\n"
            "<b>Example:</b>\n"
            "<code>/setcookies eyJpdiI6... eyJpdiI6...</code>"
        )
        send_telegram_message(chat_id, msg)
        return

    session_value = parts[1]
    xsrf_value    = parts[2]
    new_cookies = {
        "ivas_sms_session": session_value,
        "XSRF-TOKEN": xsrf_value
    }
    save_cookies_to_file(new_cookies)

    test_session = create_session()
    try:
        csrf = login(test_session)
        global SESSION_EXPIRED
        with SESSION_LOCK:
            SESSION_EXPIRED = False
        msg = (
            "✅ <b>Cookies updated successfully!</b>\n\n"
            "Bot ab kaam karega.\n"
            "OTP monitoring resume ho gayi."
        )
        send_telegram_message(chat_id, msg)
        print(f"✅ Admin {user_id} updated cookies successfully")
    except Exception as e:
        msg = (
            f"❌ <b>Cookies invalid!</b>\n\n"
            f"Error: <code>{str(e)}</code>\n\n"
            "Dobara try karo — sahi cookies copy karo."
        )
        send_telegram_message(chat_id, msg)
        print(f"❌ Admin {user_id} provided invalid cookies: {e}")

def handle_sessionstatus_command(chat_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    with SESSION_LOCK:
        expired = SESSION_EXPIRED
    if expired:
        msg = (
            "⚠️ <b>Session: EXPIRED</b>\n\n"
            "Use /setcookies to restore:\n"
            "<code>/setcookies SESSION_VALUE XSRF_VALUE</code>"
        )
    else:
        msg = "✅ <b>Session: ACTIVE</b>\n\nBot properly logged in hai."
    send_telegram_message(chat_id, msg)

def handle_fresh_command(chat_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    send_telegram_message(chat_id, "⏳ <b>Refreshing Numbers...</b>")

    def do_refresh():
        success = refresh_numbers_cache()
        if success:
            with CACHE_LOCK:
                total = sum(len(nums) for nums in NUMBERS_CACHE.values())
                ccount = len(NUMBERS_CACHE)
            msg = f"✅ <b>Done!</b>\n📱 Numbers: <b>{total}</b>\n🌍 Countries: <b>{ccount}</b>"
        else:
            msg = "❌ Refresh failed. Logs check karo."
        send_telegram_message(chat_id, msg)

    threading.Thread(target=do_refresh, daemon=True).start()

def handle_stats_command(chat_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    user_db = load_user_db()
    with CACHE_LOCK:
        total_numbers = sum(len(v) for v in NUMBERS_CACHE.values())
        total_countries = len(NUMBERS_CACHE)
    with SESSION_LOCK:
        session_status = "❌ Expired" if SESSION_EXPIRED else "✅ Active"
    msg = (
        "📊 <b>Bot Statistics</b>\n\n"
        f"👥 Users: <b>{len(user_db)}</b>\n"
        f"📱 Numbers: <b>{total_numbers}</b>\n"
        f"🌍 Countries: <b>{total_countries}</b>\n"
        f"🔌 Session: <b>{session_status}</b>\n"
        f"🗑️ Auto-Delete: <b>{str(AUTO_DELETE_MINUTES) + ' min' if AUTO_DELETE_MINUTES > 0 else 'Disabled'}</b>\n"
        f"🧵 SMS Threads: <b>{NUM_SMS_THREADS}</b>"
    )
    send_telegram_message(chat_id, msg)

def handle_usercount_command(chat_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    user_db = load_user_db()
    send_telegram_message(chat_id,
        f"👥 <b>Total Users:</b> {len(user_db)}\n📱 <b>Numbers Assigned:</b> {len(user_db)}")

def handle_broadcast_command(chat_id, user_id, message_text):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    parts = message_text.split(maxsplit=1)
    if len(parts) < 2:
        send_telegram_message(chat_id, "❌ <b>Usage:</b> /broadcast &lt;message&gt;")
        return
    broadcast_msg = parts[1]
    user_db = load_user_db()
    if not user_db:
        send_telegram_message(chat_id, "❌ No users to broadcast to.")
        return
    send_telegram_message(chat_id, f"📢 <b>Broadcasting to {len(user_db)} users...</b>")

    def send_one(uid_str):
        formatted = f"📢 <b>Announcement</b>\n\n{broadcast_msg}"
        return send_telegram_message(int(uid_str), formatted) is not None

    success_count = 0
    fail_count = 0
    with ThreadPoolExecutor(max_workers=10) as ex:  # 5 → 10
        futures = {ex.submit(send_one, uid): uid for uid in user_db.keys()}
        for future in as_completed(futures):
            if future.result():
                success_count += 1
            else:
                fail_count += 1

    send_telegram_message(chat_id,
        f"✅ <b>Broadcast Done!</b>\n\n"
        f"✅ Success: <b>{success_count}</b>\n"
        f"❌ Failed: <b>{fail_count}</b>")

def handle_seensms_command(chat_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    with SMS_LOCK:
        seen_count = len(SEEN_SMS)
    with SESSION_LOCK:
        session_status = "❌ Expired" if SESSION_EXPIRED else "✅ Active"
    send_telegram_message(chat_id,
        f"📊 <b>OTP Worker Status</b>\n\n"
        f"✅ Status: <b>{'Running' if BOT_RUNNING else 'Stopped'}</b>\n"
        f"🔌 Session: <b>{session_status}</b>\n"
        f"💾 Seen SMS: <b>{seen_count}</b>\n"
        f"⏱️ Poll: <b>{OTP_POLL_INTERVAL}s</b>\n"
        f"🧵 SMS Threads: <b>{NUM_SMS_THREADS}</b>")

def handle_testotp_command(chat_id, user_id, message_text):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    parts = message_text.split(maxsplit=1)
    if len(parts) < 2:
        send_telegram_message(chat_id, "❌ <b>Usage:</b> /testotp &lt;sms text&gt;")
        return
    otp = extract_otp(parts[1])
    msg = f"✅ <b>OTP Found:</b> <code>{otp}</code>" if otp else "❌ <b>No OTP found</b> (message will still be sent!)"
    send_telegram_message(chat_id, msg)

def handle_clearseen_command(chat_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    global SEEN_SMS
    with SMS_LOCK:
        old_count = len(SEEN_SMS)
        SEEN_SMS.clear()
    with _NUMBER_INIT_LOCK:
        _NUMBER_INITIALIZED.clear()
        _NUMBER_SMS_COUNT.clear()
    send_telegram_message(chat_id, f"🗑️ <b>Cleared {old_count} messages from cache</b>")

def handle_autodelete_command(chat_id, user_id, message_text):
    global AUTO_DELETE_MINUTES
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    parts = message_text.split()
    if len(parts) < 2:
        status = (f"✅ <b>{AUTO_DELETE_MINUTES} min</b>" if AUTO_DELETE_MINUTES > 0 else "❌ <b>Disabled</b>")
        send_telegram_message(chat_id,
            f"🗑️ <b>Auto-Delete Status:</b> {status}\n\n"
            f"<b>Usage:</b> /autodelete &lt;minutes&gt;\n"
            f"<b>Disable:</b> /autodelete 0")
        return
    try:
        minutes = int(parts[1])
        if minutes < 0:
            raise ValueError
    except ValueError:
        send_telegram_message(chat_id, "❌ Valid number do.")
        return
    AUTO_DELETE_MINUTES = minutes
    _settings["auto_delete_minutes"] = minutes
    save_settings(_settings)
    if minutes > 0:
        send_telegram_message(chat_id, f"✅ <b>Auto-Delete set to {minutes} min!</b>")
    else:
        send_telegram_message(chat_id, "✅ <b>Auto-Delete disabled.</b>")

def handle_addflag_command(chat_id, user_id, message_text):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    parts = message_text.strip().split()
    if len(parts) != 3:
        send_telegram_message(chat_id,
            "❌ <b>Usage:</b> <code>/addflag IN 5222300011366200403</code>")
        return
    code = parts[1].upper()
    emoji_id = parts[2]
    FLAG_OVERRIDES[code] = emoji_id
    _settings["flag_overrides"] = FLAG_OVERRIDES
    save_settings(_settings)
    send_telegram_message(chat_id, f"✅ <b>Flag set for {code}!</b>")

def handle_removeflag_command(chat_id, user_id, message_text):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    parts = message_text.strip().split()
    if len(parts) != 2:
        send_telegram_message(chat_id, "❌ <b>Usage:</b> <code>/removeflag IN</code>")
        return
    code = parts[1].upper()
    if code in FLAG_OVERRIDES:
        del FLAG_OVERRIDES[code]
        _settings["flag_overrides"] = FLAG_OVERRIDES
        save_settings(_settings)
        send_telegram_message(chat_id, f"✅ <b>Flag removed for {code}.</b>")
    else:
        send_telegram_message(chat_id, f"❌ <b>{code}</b> ka koi override nahi tha.")

def handle_listflags_command(chat_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    if not FLAG_OVERRIDES:
        send_telegram_message(chat_id, "📭 Koi premium flag set nahi hai.")
        return
    text = "🏳 <b>Premium Flag Overrides:</b>\n\n"
    for code, emoji_id in sorted(FLAG_OVERRIDES.items()):
        text += f"<b>{code}</b> → <code>{emoji_id}</code>\n"
    send_telegram_message(chat_id, text)

def handle_adminhelp_command(chat_id, user_id):
    if user_id not in ADMIN_USER_IDS:
        send_telegram_message(chat_id, "❌ Sirf admins ke liye.")
        return
    msg = (
        "👑 <b>Admin Commands</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "🔢 /fresh — Numbers refresh\n"
        "📊 /stats — Statistics\n"
        "👥 /usercount — Total users\n"
        "📱 /seensms — OTP worker status\n"
        "🛠️ /testotp &lt;text&gt; — Test OTP\n"
        "🗑️ /clearseen — Clear SMS cache\n"
        "🍪 /setcookies &lt;session&gt; &lt;xsrf&gt; — Update cookies\n"
        "🔌 /sessionstatus — Session check\n"
        "⏱️ /autodelete &lt;min&gt; — Auto-delete\n"
        "📢 /addchat &lt;id&gt; — Extra group add karo\n"
        "📢 /removechat &lt;id&gt; — Group remove karo\n"
        "📢 /listchats — Extra groups list\n"
        "🏳 /addflag IN &lt;id&gt; — Premium flag\n"
        "🏳 /removeflag IN — Remove flag\n"
        "🏳 /listflags — List flags\n"
        "📢 /broadcast &lt;msg&gt; — Broadcast\n"
        "━━━━━━━━━━━━━━━━━━━━━━"
    )
    send_telegram_message(chat_id, msg, create_admin_keyboard())

# ╔══════════════════════════════════════════════════════════════╗
# ║                  OTP HELPER FUNCTIONS                        ║
# ╚══════════════════════════════════════════════════════════════╝
def get_csrf_for_sms(session):
    r = session.get("https://ivas.tempnum.qzz.io/portal/sms/received", timeout=HTTP_TIMEOUT)
    return extract_csrf(r.text)

def trigger_getsms(csrf, session):
    today = get_today()
    return session.post(GETSMS_URL,
                        data={"_token": csrf, "start": today, "end": today},
                        timeout=HTTP_TIMEOUT).text

def parse_ranges(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    return list({span.get_text(strip=True)
                 for span in soup.find_all("span", class_="rname")
                 if span.get_text(strip=True)})

def get_numbers_from_range(csrf, range_name, session):
    today = get_today()
    r = session.post(NUMBERS_URL,
                     data={"_token": csrf, "start": today, "end": today, "range": range_name},
                     timeout=HTTP_TIMEOUT)
    soup = BeautifulSoup(r.text, "html.parser")
    nums = []
    for div in soup.find_all("div", onclick=True):
        m = re.search(r"toggleNum\w+\('(\d+)'", div.get("onclick", ""))
        if m:
            nums.append(m.group(1))
    return list(set(nums))

def get_sms(csrf, number, range_name, session):
    today = get_today()
    try:
        r = session.post(SMS_URL, data={
            "_token": csrf, "start": today, "end": today,
            "Number": number, "Range": range_name
        }, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        results = []

        rows = soup.find_all("tr")
        for row in rows:
            cli_tag = row.find("span", class_="cli-tag")
            msg_div = row.find("div", class_="msg-text")
            if msg_div:
                text   = msg_div.get_text(strip=True)
                sender = cli_tag.get_text(strip=True) if cli_tag else ""
                if text and len(text) > 1:
                    results.append((sender, text))

        if not results:
            for div in soup.find_all("div", class_="msg-text"):
                text = div.get_text(strip=True)
                if text and len(text) > 1:
                    parent_row = div.find_parent("tr")
                    cli_tag = parent_row.find("span", class_="cli-tag") if parent_row else None
                    sender  = cli_tag.get_text(strip=True) if cli_tag else ""
                    results.append((sender, text))

        if not results:
            for p in soup.find_all("p"):
                text = p.get_text(strip=True)
                if text and len(text) > 1:
                    results.append(("", text))

        return results
    except Exception as e:
        print(f"⚠️ SMS fetch error for {number}: {e}")
        return []

# ╔══════════════════════════════════════════════════════════════╗
# ║          SMART SMS COUNT TRACKING                            ║
# ╚══════════════════════════════════════════════════════════════╝
_NUMBER_SMS_COUNT   = {}
_NUMBER_INIT_LOCK   = threading.Lock()
_NUMBER_INITIALIZED = set()

# ╔══════════════════════════════════════════════════════════════╗
# ║     WORKER 1 — COMMAND PROCESSOR  ⚡ TIGHT LOOP             ║
# ╚══════════════════════════════════════════════════════════════╝
_cmd_executor = ThreadPoolExecutor(max_workers=12, thread_name_prefix="CmdHandler")  # 6 → 12

def dispatch_update(update):
    try:
        callback_query = update.get("callback_query")
        if callback_query:
            handle_callback_query(callback_query)
            return
        message = update.get("message")
        if not message:
            return
        text     = message.get("text", "").strip()
        user     = message.get("from", {})
        user_id  = user.get("id")
        username = user.get("username", "Unknown")
        chat_id  = message.get("chat", {}).get("id")
        if not text.startswith("/"):
            return
        command = text.split()[0].lower()
        if   command == "/start":          handle_start_command(chat_id, user_id, username)
        elif command == "/fresh":          handle_fresh_command(chat_id, user_id)
        elif command == "/stats":          handle_stats_command(chat_id, user_id)
        elif command == "/usercount":      handle_usercount_command(chat_id, user_id)
        elif command == "/seensms":        handle_seensms_command(chat_id, user_id)
        elif command == "/testotp":        handle_testotp_command(chat_id, user_id, text)
        elif command == "/clearseen":      handle_clearseen_command(chat_id, user_id)
        elif command == "/broadcast":      handle_broadcast_command(chat_id, user_id, text)
        elif command == "/autodelete":     handle_autodelete_command(chat_id, user_id, text)
        elif command == "/addchat":        handle_addchat_command(chat_id, user_id, text)
        elif command == "/removechat":     handle_removechat_command(chat_id, user_id, text)
        elif command == "/listchats":      handle_listchats_command(chat_id, user_id)
        elif command == "/addflag":        handle_addflag_command(chat_id, user_id, text)
        elif command == "/removeflag":     handle_removeflag_command(chat_id, user_id, text)
        elif command == "/listflags":      handle_listflags_command(chat_id, user_id)
        elif command == "/setcookies":     handle_setcookies_command(chat_id, user_id, text)
        elif command == "/sessionstatus":  handle_sessionstatus_command(chat_id, user_id)
        elif command == "/adminhelp":      handle_adminhelp_command(chat_id, user_id)
    except Exception as e:
        print(f"⚠️ dispatch_update error: {e}")

def command_worker():
    """⚡ Tight loop — no sleep, maximum responsiveness"""
    print("🎮 Command Worker Started (Tight Loop)")
    while BOT_RUNNING:
        try:
            updates = get_telegram_updates()
            for update in updates:
                _cmd_executor.submit(dispatch_update, update)
            # ⚡ NO sleep here — instant polling
        except Exception as e:
            print(f"⚠️ Command Worker Error: {e}")
            time.sleep(1)  # only sleep on error

# ╔══════════════════════════════════════════════════════════════╗
# ║        WORKER 2 — OTP MONITOR  ⚡ OPTIMIZED                 ║
# ╚══════════════════════════════════════════════════════════════╝
def otp_worker(worker_id=0):
    global SESSION_EXPIRED
    print("📱 OTP Worker Started")
    session = create_session()
    csrf    = None

    for attempt in range(1, 4):
        try:
            print(f"🔑 Login attempt {attempt}/3...")
            csrf = login(session)
            csrf = get_csrf_for_sms(session)
            print("✅ OTP Worker logged in")
            with SESSION_LOCK:
                SESSION_EXPIRED = False
            break
        except Exception as e:
            err = str(e)
            print(f"❌ Login failed ({attempt}): {e}")
            if "SESSION_EXPIRED" in err or "CSRF NOT FOUND" in err:
                with SESSION_LOCK:
                    SESSION_EXPIRED = True
                notify_admins_session_expired()
                time.sleep(30)
                session = create_session()
            elif attempt >= 3:
                print("❌ Max attempts reached.")
                return
            else:
                time.sleep(5)

    if csrf is None:
        return

    csrf_counter = 0
    iteration    = 0

    while BOT_RUNNING:
        with SESSION_LOCK:
            if SESSION_EXPIRED:
                print("⏸️ Waiting for admin to update cookies...")
                time.sleep(10)
                try:
                    new_session = create_session()
                    new_csrf = login(new_session)
                    new_csrf = get_csrf_for_sms(new_session)
                    session = new_session
                    csrf = new_csrf
                    SESSION_EXPIRED = False
                    print("✅ Session restored!")
                    for admin_id in ADMIN_USER_IDS:
                        send_telegram_message(admin_id,
                            "✅ <b>Session Restored!</b>\n\nOTP monitoring resume ho gayi.")
                except Exception:
                    pass
                continue

        try:
            iteration += 1
            if iteration % 60 == 0:
                print(f"💓 OTP Worker alive — iter {iteration}")

            if csrf_counter >= CSRF_REFRESH_EVERY:
                try:
                    csrf = get_csrf_for_sms(session)
                    csrf_counter = 0
                except Exception:
                    try:
                        csrf = login(session)
                        csrf = get_csrf_for_sms(session)
                        csrf_counter = 0
                    except Exception as e:
                        print(f"❌ Re-auth failed: {e}")
                        if "SESSION_EXPIRED" in str(e):
                            with SESSION_LOCK:
                                SESSION_EXPIRED = True
                            notify_admins_session_expired()
                        time.sleep(10)
                        continue
            csrf_counter += 1

            try:
                html_text = trigger_getsms(csrf, session)
                ranges    = parse_ranges(html_text)
            except Exception as e:
                print(f"⚠️ getsms error: {e}")
                time.sleep(OTP_POLL_INTERVAL)
                continue

            if not ranges:
                time.sleep(OTP_POLL_INTERVAL)
                continue

            active_pairs = []
            lock = threading.Lock()

            def _fetch_range(rname):
                try:
                    nums = get_numbers_from_range(csrf, rname, session)
                    with lock:
                        for n in nums:
                            active_pairs.append((n, rname))
                except Exception as e:
                    print(f"⚠️ Range error {rname}: {e}")

            rfuts = [SMS_EXECUTOR.submit(_fetch_range, r) for r in ranges]
            for f in rfuts:
                try: f.result(timeout=HTTP_TIMEOUT)
                except Exception: pass

            if not active_pairs:
                time.sleep(OTP_POLL_INTERVAL)
                continue

            user_db          = load_user_db()
            assigned_numbers = {str(v["number"]): int(uid) for uid, v in user_db.items()}

            def _fetch_sms(num, rname):
                try:
                    sms_list = get_sms(csrf, num, rname, session)
                    total    = len(sms_list)

                    with _NUMBER_INIT_LOCK:
                        is_first   = num not in _NUMBER_INITIALIZED
                        last_count = _NUMBER_SMS_COUNT.get(num, 0)

                    if is_first:
                        with _NUMBER_INIT_LOCK:
                            _NUMBER_INITIALIZED.add(num)
                            _NUMBER_SMS_COUNT[num] = total

                        country = extract_country_name(rname)
                        flag    = get_plain_flag(country)
                        sent = 0
                        for sender, sms in sms_list:
                            h = sms_hash(rname, num, sms)
                            with SMS_LOCK:
                                already = h in SEEN_SMS
                                if not already:
                                    SEEN_SMS.add(h)
                                    SMS_TEXT_CACHE[h] = sms
                            if already:
                                continue
                            otp = extract_otp(sms)
                            print(f"🆕 NEW SMS (first) | {flag} {country} | {num} | {sender or '?'} | OTP:{otp or 'none'}")
                            msg_gc = format_message_group(rname, num, sms, otp, sender)
                            MESSAGE_QUEUE.put(("group", TELEGRAM_CHAT_ID, msg_gc, otp, h))
                            print(f"📤 Queued → group")
                            assigned_uid = assigned_numbers.get(str(num))
                            if assigned_uid:
                                msg_dm = format_message_personal(rname, num, sms, otp, sender)
                                MESSAGE_QUEUE.put(("user", assigned_uid, msg_dm, otp, h))
                                print(f"📤 Queued → user {assigned_uid}")
                            sent += 1
                        skipped = total - sent
                        if skipped > 0:
                            print(f"📋 Init {num}: {skipped} old SMS skipped, {sent} new sent")
                        return

                    if total <= last_count:
                        return

                    diff     = total - last_count
                    new_smss = sms_list[-diff:]

                    with _NUMBER_INIT_LOCK:
                        _NUMBER_SMS_COUNT[num] = total

                    country = extract_country_name(rname)
                    flag    = get_plain_flag(country)
                    print(f"📬 {num}: {last_count}→{total} (+{diff} new)")

                    for sender, sms in new_smss:
                        with SMS_LOCK:
                            h = sms_hash(rname, num, sms)
                            if h in SEEN_SMS:
                                continue
                            SEEN_SMS.add(h)
                            SMS_TEXT_CACHE[h] = sms

                        otp = extract_otp(sms)
                        print(f"🆕 NEW SMS | {flag} {country} | {num} | {sender or '?'} | OTP:{otp or 'none'}")

                        msg_gc = format_message_group(rname, num, sms, otp, sender)
                        MESSAGE_QUEUE.put(("group", TELEGRAM_CHAT_ID, msg_gc, otp, h))
                        print(f"📤 Queued → group")

                        assigned_uid = assigned_numbers.get(str(num))
                        if assigned_uid:
                            msg_dm = format_message_personal(rname, num, sms, otp, sender)
                            MESSAGE_QUEUE.put(("user", assigned_uid, msg_dm, otp, h))
                            print(f"📤 Queued → user {assigned_uid}")
                        else:
                            print(f"ℹ️ {num} not assigned to any user")

                except Exception as e:
                    print(f"⚠️ SMS fetch error {num}: {e}")

            sfuts = [SMS_EXECUTOR.submit(_fetch_sms, num, rname) for num, rname in active_pairs]
            for f in sfuts:
                try: f.result(timeout=HTTP_TIMEOUT)
                except Exception: pass

            time.sleep(OTP_POLL_INTERVAL)

        except Exception as e:
            err = str(e)
            print(f"⚠️ OTP Worker Error: {e}")
            if "SESSION_EXPIRED" in err or "login" in err.lower():
                with SESSION_LOCK:
                    SESSION_EXPIRED = True
                notify_admins_session_expired()
            else:
                time.sleep(5)

# ╔══════════════════════════════════════════════════════════════╗
# ║     WORKER 3 — MESSAGE SENDERS  ⚡ OPTIMIZED                ║
# ╚══════════════════════════════════════════════════════════════╝
def message_sender_worker(sender_id=0):
    print(f"📤 Message Sender-{sender_id} Started")
    while BOT_RUNNING:
        try:
            try:
                item = MESSAGE_QUEUE.get(timeout=0.02)  # 0.05 → 0.02
            except Empty:
                continue

            if len(item) == 5:
                msg_type, chat_id, message, otp, hash_id = item
            else:
                msg_type, chat_id, message, otp = item
                hash_id = None

            keyboard = None
            if msg_type == "group":
                buttons = []

                if otp:
                    buttons.append([{
                        "text": f"{otp}",
                        "callback_data": f"otp_copy_{otp}",
                        "icon_custom_emoji_id": "5258500400918587241"
                    }])

                buttons.append([
                    {
                        "text": "Panel",
                        "url": PANEL_LINK,
                        "icon_custom_emoji_id": "5145427681680032825"
                    },
                    {
                        "text": "Channel",
                        "url": CHANNEL_LINK,
                        "icon_custom_emoji_id": "6219641556945606133"
                    }
                ])

                keyboard = {"inline_keyboard": buttons}

            if msg_type == "group":
                result = send_telegram_message(chat_id, message, keyboard)
                # ⚡ Extra chats parallel mein bhejo
                if EXTRA_CHATS:
                    def _send_extra(cid):
                        try:
                            send_telegram_message(cid, message, keyboard)
                        except Exception as e:
                            print(f"⚠️ Extra chat {cid} error: {e}")
                    with ThreadPoolExecutor(max_workers=len(EXTRA_CHATS)) as ex:
                        ex.map(_send_extra, EXTRA_CHATS)
            else:
                result = send_telegram_message(chat_id, message, keyboard)

            if (msg_type == "group" and AUTO_DELETE_MINUTES > 0
                    and result and isinstance(result, dict)):
                sent_msg_id = result.get("result", {}).get("message_id")
                if sent_msg_id:
                    delay = AUTO_DELETE_MINUTES * 60
                    threading.Timer(
                        delay, delete_telegram_message,
                        args=(chat_id, sent_msg_id)
                    ).start()

            if result:
                print(f"✅ Sender-{sender_id} → {'group' if msg_type == 'group' else f'user {chat_id}'}")
            else:
                print(f"❌ Sender-{sender_id} failed → {chat_id}")

            if msg_type == "group":
                time.sleep(TG_RATE_LIMIT_DELAY)

        except Exception as e:
            print(f"⚠️ Message Sender-{sender_id} Error: {e}")
            time.sleep(0.5)

# ╔══════════════════════════════════════════════════════════════╗
# ║                    FLASK WEB SERVER                          ║
# ╚══════════════════════════════════════════════════════════════╝
flask_app = Flask(__name__)

@flask_app.route("/")
def index():
    return Response("OTP Bot is running ✅", status=200)

@flask_app.route("/health")
def health():
    return Response("OK", status=200)

@flask_app.route("/ping")
def ping():
    return Response("pong", status=200)

def run_flask():
    port = int(os.getenv("PORT", 8080))
    print(f"🌐 Flask server starting on port {port}...")
    flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

# ╔══════════════════════════════════════════════════════════════╗
# ║                          MAIN                                ║
# ╚══════════════════════════════════════════════════════════════╝
def main():
    print("=" * 60)
    print("🤖  DDX OTP BOT  |  ⚡ ROCKET SPEED EDITION")
    print("=" * 60)
    print()
    print(f"⚡ SPEED: Poll={OTP_POLL_INTERVAL}s | SMS Threads={NUM_SMS_THREADS} | Senders={NUM_MSG_SENDERS}")
    print(f"🔒 Force Subscribe: {'ENABLED' if FORCE_SUB_ENABLED else 'DISABLED'}")
    print(f"🗑️ Auto-Delete: {'ENABLED — ' + str(AUTO_DELETE_MINUTES) + ' min' if AUTO_DELETE_MINUTES > 0 else 'DISABLED'}")
    print(f"🏳 Premium Flags: {len(FLAG_OVERRIDES)} loaded")
    print(f"🎮 Cmd Workers: 12 | 📤 Msg Senders: {NUM_MSG_SENDERS}")
    print()

    if not load_numbers_from_txt():
        print("📡 No cache found. Use /fresh to fetch numbers.")

    print("\n🚀 Starting Workers...\n")
    workers = []
    workers.append(threading.Thread(target=run_flask, name="FlaskServer", daemon=True))
    workers.append(threading.Thread(target=command_worker, name="CommandWorker", daemon=True))
    workers.append(threading.Thread(target=otp_worker, args=(0,), name="OTPWorker", daemon=True))
    for i in range(NUM_MSG_SENDERS):
        workers.append(threading.Thread(
            target=message_sender_worker, args=(i,),
            name=f"MsgSender-{i}", daemon=True
        ))

    for w in workers:
        w.start()
        print(f"   ✅ Started: {w.name}")

    print(f"\n✅ {len(workers)} workers running! ⚡\n")

    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
