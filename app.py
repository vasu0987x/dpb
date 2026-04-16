"""
main.py  —  Multi-Panel OTP Bot  (with Reward System)
FIXED VERSION
"""

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
import importlib
import importlib.util
import glob

# ─────────────────────────────────────────────
# CONFIG / LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID  = 8195360535

bot = telebot.TeleBot(BOT_TOKEN)

DATA_FILE   = "bot_data.json"
NUMBERS_DIR = "numbers"
DB_FILE     = "otp_data.db"

os.makedirs(NUMBERS_DIR, exist_ok=True)

OTP_GROUP_IDS       = ["-1003749252061"]
AUTO_DELETE_MINUTES = 0

CHANNEL_LINK  = "https://t.me/UXOTP"
BACKUP        = "https://t.me/VASUHUB"
DEVELOPER_ID  = "@uxotpbot"
CODE_GROUP    = "https://t.me/+SDPuI2Ud62RkN2Jl"

REQUIRED_CHANNELS = ["@uxotp","@Vasuhub","@ddxotp","@NokosX"]

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

# ─────────────────────────────────────────────
# DATA STORAGE
# ─────────────────────────────────────────────
data                = {}
numbers_by_country  = {}
current_country     = None
user_messages       = {}
user_current_country= {}
temp_uploads        = {}

MAX_SEEN      = 200000
seen_messages = set()
seen_order    = deque()

group_message_queue    = queue.Queue()
personal_message_queue = queue.Queue()
otp_processing_queue   = queue.Queue()

MAX_WORKERS_GROUP    = 8
MAX_WORKERS_PERSONAL = 10
SEND_TIMEOUT         = 8

active_users = set()

EXTRA_CODES    = {"Kosovo": "XK"}
flag_overrides = {}

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

# ─────────────────────────────────────────────
# REWARD MODULE — loaded at import time
# ─────────────────────────────────────────────
class _RewardsStub:
    """Fallback stub used if rewards.py fails to load."""
    def process_otp_reward(self, *a, **kw): return None
    def ensure_user(self, *a, **kw): pass
    def get_user(self, *a, **kw): return {}
    def get_user_count(self): return 0
    def _get_config(self): return {"min_withdrawal": 0.05, "rewards_enabled": False, "default_reward": 0.005}
    def set_wallet(self, *a, **kw): pass
    def deduct_balance(self, *a, **kw): pass
    def create_withdrawal(self, *a, **kw): return ""
    def get_pending_withdrawals(self): return []
    def resolve_withdrawal(self, *a, **kw): return None
    def register_handlers(self, *a, **kw): pass
    register_reward_commands = register_handlers

    class _FakeCol:
        def find_one(self, *a, **kw): return None
        def find(self, *a, **kw):
            class _C:
                def sort(self, *a, **kw): return self
                def limit(self, *a): return []
            return _C()

    col_withdrawals  = _FakeCol()
    col_transactions = _FakeCol()


_rewards_loaded = False
try:
    import rewards as _rewards_module
    _rewards_module._client.admin.command("ping")
    _rewards = _rewards_module
    _rewards_loaded = True
    logger.info("✅ Reward module imported + MongoDB connected")
except Exception as _rew_err:
    logger.error(f"❌ rewards.py import OR MongoDB connection failed: {_rew_err}")
    _rewards = _RewardsStub()


# ─────────────────────────────────────────────
# PANEL MANAGEMENT
# ─────────────────────────────────────────────
panel_registry    = {}
panel_statuses    = {}
panel_threads     = {}
panel_stop_events = {}


def _panel_wrapper(name, module, stop_event):
    try:
        panel_statuses[name] = "online"
        if hasattr(module, "start"):
            import inspect
            sig    = inspect.signature(module.start)
            params = list(sig.parameters.keys())
            if "stop_event" in params:
                module.start(otp_processing_queue, seen_messages, seen_order, MAX_SEEN, stop_event)
            else:
                _start_with_stop_check(name, module, stop_event)
    except Exception as e:
        logger.error(f"[{name}] Panel crashed: {e}")
        panel_statuses[name] = "error"


def _start_with_stop_check(name, module, stop_event):
    try:
        module._stop_event = stop_event
    except Exception:
        pass

    proxy_queue = queue.Queue()

    def _proxy_forwarder():
        while not stop_event.is_set():
            try:
                record = proxy_queue.get(timeout=1)
                otp_processing_queue.put(record)
            except queue.Empty:
                continue
        logger.info(f"[{name}] Proxy forwarder stopped")
        while not proxy_queue.empty():
            try:
                proxy_queue.get_nowait()
            except Exception:
                break

    inner_thread = threading.Thread(
        target=module.start,
        args=(proxy_queue, seen_messages, seen_order, MAX_SEEN),
        daemon=True, name=f"PanelInner-{name}")
    forwarder_thread = threading.Thread(
        target=_proxy_forwarder, daemon=True, name=f"PanelProxy-{name}")

    inner_thread.start()
    forwarder_thread.start()

    while not stop_event.is_set():
        if not inner_thread.is_alive():
            panel_statuses[name] = "error"
            logger.warning(f"[{name}] Panel thread died unexpectedly")
            break
        time.sleep(2)

    if stop_event.is_set():
        panel_statuses[name] = "stopped"
        logger.info(f"[{name}] Panel fully stopped")


def register_panel(name, module):
    panel_registry[name]  = module
    panel_statuses[name]  = "loaded"
    logger.info(f"✅ Panel registered: {name}")


def load_all_panels():
    panel_files = glob.glob(os.path.join(os.path.dirname(__file__), "panels", "panel_*.py"))
    for panel_file in sorted(panel_files):
        panel_name = os.path.splitext(os.path.basename(panel_file))[0]
        if panel_name == "panel_TEMPLATE":
            continue
        try:
            spec   = importlib.util.spec_from_file_location(panel_name, panel_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            register_panel(panel_name, module)
            logger.info(f"📦 Loaded panel: {panel_name}")
        except Exception as e:
            logger.error(f"❌ Failed to load panel {panel_name}: {e}")
            panel_statuses[panel_name] = "error"


def _start_single_panel(name):
    module = panel_registry.get(name)
    if not module:
        return False
    stop_event = threading.Event()
    panel_stop_events[name] = stop_event
    t = threading.Thread(
        target=_panel_wrapper, args=(name, module, stop_event),
        daemon=True, name=f"Panel-{name}")
    panel_threads[name]   = t
    panel_statuses[name]  = "starting"
    t.start()
    logger.info(f"🚀 Started panel: {name}")
    return True


def start_all_panels():
    for name in panel_registry:
        _start_single_panel(name)


def stop_panel(name) -> bool:
    if name not in panel_registry:
        return False
    stop_event = panel_stop_events.get(name)
    if stop_event:
        stop_event.set()
    panel_statuses[name] = "stopped"
    logger.info(f"Panel stop signal sent: {name}")
    return True


def start_panel(name) -> bool:
    if name not in panel_registry:
        return False
    old_event = panel_stop_events.get(name)
    if old_event:
        old_event.clear()
    return _start_single_panel(name)


def restart_panel(name) -> bool:
    if name not in panel_registry:
        return False
    stop_panel(name)
    time.sleep(2)
    return start_panel(name)

# ─────────────────────────────────────────────
# SQLITE DATABASE
# ─────────────────────────────────────────────
def init_database():
    conn   = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS otp_records (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            hash_id    TEXT UNIQUE NOT NULL,
            number     TEXT NOT NULL,
            sender     TEXT,
            message    TEXT,
            otp_code   TEXT,
            country    TEXT,
            timestamp  TEXT,
            panel_name TEXT DEFAULT 'unknown',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_number    ON otp_records(number)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON otp_records(timestamp)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_hash      ON otp_records(hash_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_panel     ON otp_records(panel_name)')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_assignments (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id     INTEGER NOT NULL,
            number      TEXT NOT NULL,
            country     TEXT,
            assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            expire_at   DATETIME
        )
    ''')
    # expire_at column add karo agar purani DB hai
    try:
        cursor.execute('ALTER TABLE user_assignments ADD COLUMN expire_at DATETIME')
        conn.commit()
    except Exception:
        pass  # column pehle se exist karta hai

    #change2
    # cursor.execute('''
    #     CREATE TABLE IF NOT EXISTS user_assignments (
    #         id          INTEGER PRIMARY KEY AUTOINCREMENT,
    #         chat_id     INTEGER NOT NULL,
    #         number      TEXT NOT NULL,
    #         country     TEXT,
    #         assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP
    #     )
    # ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_chat          ON user_assignments(chat_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_number_assign ON user_assignments(number)')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS active_users (
            chat_id    INTEGER PRIMARY KEY,
            username   TEXT,
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


def save_otp_to_db(record, hash_id):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO otp_records
                (hash_id, number, sender, message, otp_code, country, timestamp, panel_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                hash_id,
                record.get("num", ""),
                record.get("cli", ""),
                record.get("message", ""),
                record.get("otp", ""),
                record.get("country", ""),
                record.get("dt", ""),
                record.get("panel_name", "unknown")
            ))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to save OTP to DB: {e}")
        return False


def get_past_otps(number, limit=10):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM otp_records
                WHERE number = ?
                ORDER BY created_at DESC
                LIMIT ?
            ''', (number, limit))
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Failed to fetch past OTPs: {e}")
        return []

#change
def save_user_assignment(chat_id, numbers, country):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM user_assignments WHERE chat_id = ?', (chat_id,))
            expire_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            for number in numbers:
                cursor.execute(
                    'INSERT INTO user_assignments (chat_id, number, country, assigned_at) VALUES (?, ?, ?, ?)',
                    (chat_id, number, country, expire_at))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to save user assignment: {e}")

# def save_user_assignment(chat_id, numbers, country):
#     try:
#         with get_db() as conn:
#             cursor = conn.cursor()
#             cursor.execute('DELETE FROM user_assignments WHERE chat_id = ?', (chat_id,))
#             for number in numbers:
#                 cursor.execute(
#                     'INSERT INTO user_assignments (chat_id, number, country) VALUES (?, ?, ?)',
#                     (chat_id, number, country))
#             conn.commit()
#     except Exception as e:
#         logger.error(f"Failed to save user assignment: {e}")


def get_user_numbers(chat_id):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT number FROM user_assignments WHERE chat_id = ?', (chat_id,))
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Failed to get user numbers: {e}")
        return []


def update_active_user(chat_id, username=None):
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO active_users (chat_id, username, last_active)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(chat_id) DO UPDATE SET
                    username    = excluded.username,
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

# ─────────────────────────────────────────────
# DATA FUNCTIONS
# ─────────────────────────────────────────────
def load_data():
    global data, numbers_by_country, current_country, OTP_GROUP_IDS, AUTO_DELETE_MINUTES
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data                = json.load(f)
                numbers_by_country  = data.get("numbers_by_country", {}) or {}
                current_country     = data.get("current_country")
                OTP_GROUP_IDS       = data.get("otp_groups", ["-1003749252061"])
                AUTO_DELETE_MINUTES = data.get("auto_delete_minutes", 0)
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            data               = {}
            numbers_by_country = {}
    else:
        data                = {}
        numbers_by_country  = {}
        OTP_GROUP_IDS       = ["-1003749252061"]
        AUTO_DELETE_MINUTES = 0


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

# ─────────────────────────────────────────────
# FLASK
# ─────────────────────────────────────────────
app = Flask(__name__)

@app.route("/")
def index():
    return "Multi-Panel OTP Bot Running"

@app.route("/health")
def health():
    return Response("OK", status=200)

@app.route("/panels")
def panels_status():
    status_text = "\n".join([f"{n}: {s}" for n, s in panel_statuses.items()])
    return Response(status_text or "No panels loaded", status=200)

@app.route("/stats")
def stats():
    return Response(
        f"Active Users: {get_active_user_count()} | Panels: {len(panel_registry)}",
        status=200)

def run_flask():
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

# ─────────────────────────────────────────────
# TELEGRAM SENDER (PARALLEL)
# ─────────────────────────────────────────────
def _send_single(chat_id, payload):
    payload_local = payload.copy()
    payload_local["chat_id"] = chat_id
    try:
        r = session.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data=payload_local, timeout=SEND_TIMEOUT)
        return chat_id, r.status_code
    except Exception as e:
        logger.debug(f"Error sending to {chat_id}: {e}")
        return chat_id, None


def send_to_telegram(msg, chat_ids, kb=None):
    payload = {"text": msg[:3900], "parse_mode": "HTML", "disable_web_page_preview": True}
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
            except Exception:
                results[cid] = None
    return results

# ─────────────────────────────────────────────
# MESSAGE WORKERS
# ─────────────────────────────────────────────
def group_sender_worker():
    logger.info("🚀 Group sender worker started")
    while True:
        try:
            msg, chat_ids, kb = group_message_queue.get()
            send_to_telegram(msg, chat_ids, kb)
        except Exception as e:
            logger.error(f"Group sender error: {e}")
        finally:
            group_message_queue.task_done()
        time.sleep(0.03)


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
    """
    Main OTP processor.
    Sends OTPs to assigned users + grants reward per OTP.
    """
    logger.info("🚀 OTP processor worker started")
    while True:
        try:
            record     = otp_processing_queue.get()
            hash_id    = record.get("hash_id")
            panel_name = record.get("panel_name", "unknown")

            save_otp_to_db(record, hash_id)

            msg_group, number = format_message(record, personal=False)
            otp = record.get("otp") or extract_otp(record.get("message", ""))

            keyboard = {
    "inline_keyboard": [
        *([[{
            "text": f"{otp}",
            "callback_data": f"copy_{otp}",
            "icon_custom_emoji_id": "5258500400918587241"
        }]] if otp else []),
        [{
            "text": "View Full SMS",
            "callback_data": f"fullsms_{hash_id}",
            "icon_custom_emoji_id": "5257980374868311346"
        }],
        [
            {
                "text": "Panel",
                "url": f"https://t.me/{DEVELOPER_ID.lstrip('@')}",
                "icon_custom_emoji_id": "5145427681680032825"
            },
            {
                "text": "Channel",
                "url": CHANNEL_LINK,
                "icon_custom_emoji_id": "6219641556945606133"
            }
        ]
    ]
}

            if OTP_GROUP_IDS:
                for group_id in OTP_GROUP_IDS:
                    try:
                        sent_msg = bot.send_message(
                            group_id, msg_group,
                            reply_markup=json.dumps(keyboard),
                            parse_mode="HTML")
                        if AUTO_DELETE_MINUTES > 0:
                            threading.Timer(
                                AUTO_DELETE_MINUTES * 60,
                                delete_message_safe,
                                args=(group_id, sent_msg.message_id)
                            ).start()
                    except Exception as e:
                        logger.error(f"Failed to send to group {group_id}: {e}")

            # ── Find assigned users ────────────────────────────
            assigned_users = []
            try:
                with get_db() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        'SELECT DISTINCT chat_id FROM user_assignments WHERE number = ?',
                        (number,))
                    assigned_users = [row[0] for row in cursor.fetchall()]
            except Exception as e:
                logger.error(f"Failed to get assigned users: {e}")

            # ── Detect service code for reward ─────────────────
            sender       = record.get("cli", "")
            service_code = get_service_code(sender)

            for chat_id in assigned_users:
                # 1️⃣ Personal OTP message
                msg_personal, _ = format_message(record, personal=True)
                personal_message_queue.put((msg_personal, chat_id))

                # 2️⃣ REWARD — only if rewards module loaded
                if not _rewards_loaded:
                    continue
                try:
                    logger.info(f"[REWARD] chat={chat_id} service={service_code}")
                    reward_amount = _rewards.process_otp_reward(
                        chat_id=chat_id,
                        service_code=service_code,
                        username=None,
                        country=record.get("country", "")
                    )
                    if reward_amount and reward_amount > 0:
                        user_doc  = _rewards.get_user(chat_id)
                        new_bal   = user_doc.get("balance", 0.0) if user_doc else reward_amount
                        reward_msg = (
                            f"💰 <b>Reward Credited!</b>\n"
                            f"━━━━━━━━━━━━━━━\n"
                            f"✅ <b>+${reward_amount:.5f}</b> added for [{service_code}] OTP\n"
                            f"💵 <b>New Balance:</b> <code>${new_bal:.5f}</code>\n"
                            f"━━━━━━━━━━━━━━━\n"
                            f"<i>Tap 💰 Balance to see full earnings</i>"
                        )
                        personal_message_queue.put((reward_msg, chat_id))
                        logger.info(f"[REWARD] ✅ chat={chat_id} +${reward_amount:.5f}")
                    else:
                        logger.info(f"[REWARD] No reward for chat={chat_id} svc={service_code}")
                except Exception as re_err:
                    logger.error(f"[REWARD] ❌ chat={chat_id}: {re_err}", exc_info=True)

        except Exception as e:
            logger.error(f"OTP processor error: {e}")
        finally:
            otp_processing_queue.task_done()
        time.sleep(0.01)

# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────
def country_to_flag(country_name: str) -> str:
    code = EXTRA_CODES.get(country_name)
    if not code:
        try:
            country = pycountry.countries.lookup(country_name)
            code    = country.alpha_2
        except LookupError:
            return ""
    return "".join(chr(127397 + ord(c)) for c in code.upper())


def get_country_code(country_name: str) -> str:
    code = EXTRA_CODES.get(country_name)
    if not code:
        try:
            country = pycountry.countries.lookup(country_name)
            code    = country.alpha_2
        except LookupError:
            return country_name[:2].upper()
    return code.upper()


def get_service_code(sender: str) -> str:
    for service, code in SERVICE_CODES.items():
        if service.lower() in sender.lower():
            return code
    return sender[:2].upper() if len(sender) >= 2 else sender.upper()


def extract_otp(message: str) -> str | None:
    text = message.strip()
    m = re.search(
        r"(?:otp|code|pin|password|verification|verif)[^\d]{0,8}([0-9][0-9\-\s]{2,10}[0-9])",
        text, re.I)
    if m:
        cand = re.sub(r"\D", "", m.group(1))
        if 3 <= len(cand) <= 8 and not (1900 <= int(cand) <= 2099):
            return cand
    m2 = re.search(
        r"([0-9][0-9\-\s]{2,10}[0-9])[^\w]{0,8}(?:otp|code|pin|password|verification|verif)",
        text, re.I)
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
    if len(number) <= 4:
        return number
    return f"{number[:2]}DDX{number[-4:]}"


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
            code    = country.alpha_2
        except LookupError:
            return ""
    code = code.upper()
    regular_flag = "".join(chr(127397 + ord(c)) for c in code)
    emoji_id = flag_overrides.get(code)
    if emoji_id:
        return f'<tg-emoji emoji-id="{emoji_id}">{regular_flag}</tg-emoji>'
    return regular_flag


def format_message(record, personal=False):
    number    = record.get("num")     or "Unknown"
    sender    = record.get("cli")     or "Unknown"
    message   = record.get("message") or ""
    country   = record.get("country") or "Unknown"
    panel_name= record.get("panel_name", "")

    flag         = get_flag(country)
    country_code = get_country_code(country)
    service_code = get_service_code(sender)
    masked       = mask_number(number)
    otp          = record.get("otp") or extract_otp(message) or "❓"

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
            f'{service_emoji} <b>{service_code}</b>'
          
        )

    return formatted, number

# ─────────────────────────────────────────────
# FLAG COMMANDS
# ─────────────────────────────────────────────
@bot.message_handler(commands=["addflag"])
def add_flag(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    parts = message.text.strip().split()
    if len(parts) != 3:
        return bot.reply_to(message,
            "❌ Usage: <code>/addflag IN 5222300011366200403</code>", parse_mode="HTML")
    _, code, emoji_id = parts
    code = code.upper()
    flag_overrides[code] = emoji_id
    save_flag_overrides()
    bot.reply_to(message, f"✅ Flag set for <b>{code}</b>", parse_mode="HTML")


@bot.message_handler(commands=["removeflag"])
def remove_flag(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    parts = message.text.strip().split()
    if len(parts) != 2:
        return bot.reply_to(message,
            "❌ Usage: <code>/removeflag IN</code>", parse_mode="HTML")
    code = parts[1].upper()
    if code in flag_overrides:
        del flag_overrides[code]
        save_flag_overrides()
        bot.reply_to(message, f"✅ Removed flag for <b>{code}</b>.", parse_mode="HTML")
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
        text += f"<b>{code}</b> → <code>{emoji_id}</code>\n"
    bot.reply_to(message, text, parse_mode="HTML")

# ─────────────────────────────────────────────
# PANEL MANAGEMENT COMMANDS
# ─────────────────────────────────────────────
@bot.message_handler(commands=["panels"])
def list_panels(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    if not panel_registry:
        return bot.reply_to(message, "📭 No panels loaded.")
    text = "🔌 <b>Loaded Panels:</b>\n\n"
    for name, status in panel_statuses.items():
        icon = {"online": "🟢", "stopped": "🔴", "error": "⚠️", "starting": "🟡"}.get(status, "⚪")
        text += f"{icon} <code>{name}</code> — <b>{status}</b>\n"
    text += "\n<i>Use /panelstop, /panelstart, /panelrestart &lt;name&gt;</i>"
    markup = types.InlineKeyboardMarkup()
    for name, status in panel_statuses.items():
        short = name.replace("panel_", "")
        if status == "online":
            markup.row(
                types.InlineKeyboardButton(f"🔴 Stop {short}",    callback_data=f"pstop_{name}"),
                types.InlineKeyboardButton(f"🔄 Restart {short}", callback_data=f"prestart_{name}")
            )
        elif status in ("stopped", "error"):
            markup.row(
                types.InlineKeyboardButton(f"🟢 Start {short}",   callback_data=f"pstart_{name}"),
                types.InlineKeyboardButton(f"🔄 Restart {short}", callback_data=f"prestart_{name}")
            )
        else:
            markup.row(
                types.InlineKeyboardButton(f"🔄 Restart {short}", callback_data=f"prestart_{name}")
            )
    bot.reply_to(message, text, parse_mode="HTML", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith("pstop_"))
def cb_panel_stop(call):
    if call.from_user.id != ADMIN_ID:
        return bot.answer_callback_query(call.id, "❌ Not authorized")
    name = call.data[6:]
    if stop_panel(name):
        bot.answer_callback_query(call.id, f"🔴 {name} stopped!", show_alert=False)
        bot.edit_message_text(
            f"🔴 Panel <code>{name}</code> stopped.",
            call.message.chat.id, call.message.message_id, parse_mode="HTML")
    else:
        bot.answer_callback_query(call.id, "❌ Panel not found", show_alert=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith("pstart_"))
def cb_panel_start(call):
    if call.from_user.id != ADMIN_ID:
        return bot.answer_callback_query(call.id, "❌ Not authorized")
    name = call.data[7:]
    if start_panel(name):
        bot.answer_callback_query(call.id, f"🟢 {name} starting!", show_alert=False)
        bot.edit_message_text(
            f"🟢 Panel <code>{name}</code> starting...",
            call.message.chat.id, call.message.message_id, parse_mode="HTML")
    else:
        bot.answer_callback_query(call.id, "❌ Panel not found", show_alert=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith("prestart_"))
def cb_panel_restart(call):
    if call.from_user.id != ADMIN_ID:
        return bot.answer_callback_query(call.id, "❌ Not authorized")
    name = call.data[9:]
    bot.answer_callback_query(call.id, f"🔄 Restarting {name}...", show_alert=False)
    threading.Thread(target=_do_restart, args=(call, name), daemon=True).start()


def _do_restart(call, name):
    if restart_panel(name):
        try:
            bot.edit_message_text(
                f"🔄 Panel <code>{name}</code> restarted.",
                call.message.chat.id, call.message.message_id, parse_mode="HTML")
        except Exception:
            pass
    else:
        try:
            bot.send_message(call.message.chat.id,
                f"❌ Panel <code>{name}</code> not found.", parse_mode="HTML")
        except Exception:
            pass


@bot.message_handler(commands=["panelstop"])
def cmd_panel_stop(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    args = message.text.split()
    if len(args) < 2:
        return bot.reply_to(message, "Usage: /panelstop <panel_name>")
    name = args[1].strip()
    if stop_panel(name):
        bot.reply_to(message, f"🔴 Panel <code>{name}</code> stopped.", parse_mode="HTML")
    else:
        bot.reply_to(message, f"❌ Panel <code>{name}</code> not found.", parse_mode="HTML")


@bot.message_handler(commands=["panelstart"])
def cmd_panel_start(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    args = message.text.split()
    if len(args) < 2:
        return bot.reply_to(message, "Usage: /panelstart <panel_name>")
    name = args[1].strip()
    if start_panel(name):
        bot.reply_to(message, f"🟢 Panel <code>{name}</code> starting...", parse_mode="HTML")
    else:
        bot.reply_to(message, f"❌ Panel <code>{name}</code> not found.", parse_mode="HTML")


@bot.message_handler(commands=["panelrestart"])
def cmd_panel_restart(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    args = message.text.split()
    if len(args) < 2:
        return bot.reply_to(message, "Usage: /panelrestart <panel_name>")
    name = args[1].strip()
    bot.reply_to(message, f"🔄 Restarting <code>{name}</code>...", parse_mode="HTML")
    threading.Thread(
        target=lambda: (
            restart_panel(name),
            bot.send_message(message.chat.id,
                f"✅ Panel <code>{name}</code> restarted!", parse_mode="HTML")
        ), daemon=True).start()


@bot.message_handler(commands=["panelstats"])
def panel_stats(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT panel_name, COUNT(*) as total,
                       SUM(CASE WHEN DATE(created_at) = DATE('now') THEN 1 ELSE 0 END) as today
                FROM otp_records
                GROUP BY panel_name
                ORDER BY total DESC
            ''')
            rows = cursor.fetchall()
            if not rows:
                return bot.reply_to(message, "📭 No OTP records yet.")
            text = "📊 <b>Per-Panel OTP Stats:</b>\n\n"
            for row in rows:
                status = panel_statuses.get(row[0], "unknown")
                icon   = "🟢" if status == "online" else ("🔴" if status == "stopped" else "⚠️")
                text  += f"{icon} <code>{row[0]}</code>\n   Total: <b>{row[1]}</b> | Today: <b>{row[2]}</b>\n\n"
            bot.reply_to(message, text, parse_mode="HTML")
    except Exception as e:
        bot.reply_to(message, f"❌ Error: {e}")

# ─────────────────────────────────────────────
# USER BOT FUNCTIONS
# ─────────────────────────────────────────────

ASSIGNMENT_MINUTES = 90  # number expiry time

def get_available_numbers(country, chat_id, count=2):
    """
    Pehle unassigned (ya expired) numbers do.
    Agar utne available nahi hain toh already-assigned wale bhi include karo.
    """
    all_numbers = numbers_by_country.get(country, [])
    if not all_numbers:
        return []
    
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            # Numbers jo abhi kisi active user ko assigned hain (expired nahi)
            cursor.execute('''
                SELECT DISTINCT number FROM user_assignments
                WHERE chat_id != ? 
                AND (expire_at IS NULL OR expire_at > ?)
            ''', (chat_id, now_str))
            busy_numbers = {row[0] for row in cursor.fetchall()}
    except Exception as e:
        logger.error(f"get_available_numbers error: {e}")
        busy_numbers = set()
    
    free_numbers = [n for n in all_numbers if n not in busy_numbers]
    
    if len(free_numbers) >= count:
        return random.sample(free_numbers, count)
    
    # Free numbers kam hain — baaki busy wale se fill karo
    result = free_numbers[:]
    extra_needed = count - len(result)
    busy_list = [n for n in all_numbers if n in busy_numbers]
    if busy_list:
        extra = random.sample(busy_list, min(extra_needed, len(busy_list)))
        result.extend(extra)
    
    return result[:count]


def save_user_assignment(chat_id, numbers, country):
    try:
        expire_at = (
            datetime.utcnow().replace(second=0, microsecond=0)
        )
        # 90 minutes add karo
        import math
        expire_dt = datetime.utcnow()
        expire_dt = expire_dt.replace(
            minute=(expire_dt.minute + ASSIGNMENT_MINUTES) % 60,
            second=0, microsecond=0
        )
        # Proper timedelta use karo
        from datetime import timedelta
        expire_at_str = (datetime.utcnow() + timedelta(minutes=ASSIGNMENT_MINUTES)).strftime("%Y-%m-%d %H:%M:%S")
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM user_assignments WHERE chat_id = ?', (chat_id,))
            for number in numbers:
                cursor.execute(
                    'INSERT INTO user_assignments (chat_id, number, country, expire_at) VALUES (?, ?, ?, ?)',
                    (chat_id, number, country, expire_at_str))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to save user assignment: {e}")


def send_random_numbers(chat_id, country=None, edit=False):
    if country is None:
        country = user_current_country.get(chat_id)
        if not country:
            bot.send_message(chat_id, "❌ No country selected.")
            return
    numbers = numbers_by_country.get(country, [])
    if not numbers:
        bot.send_message(chat_id, f"❌ No numbers for {country}.")
        return
    
    selected_numbers = get_available_numbers(country, chat_id, count=2)
    if not selected_numbers:
        bot.send_message(chat_id, f"❌ No numbers available for {country}.")
        return
    
    user_current_country[chat_id] = country
    save_user_assignment(chat_id, selected_numbers, country)
    
    flag = country_to_flag(country)
    from datetime import timedelta
    expire_time = datetime.utcnow() + timedelta(minutes=ASSIGNMENT_MINUTES)
    expire_str  = expire_time.strftime("%H:%M UTC")
    
    text = f"{flag} <b>{country}</b> Numbers:\n\n"
    for i, num in enumerate(selected_numbers, 1):
        text += f"{i}. <code>{num}</code>\n"
    text += (
        f"\n⏳ Waiting for OTPs on any number...\n"
        f"🔔 Instant notifications enabled!\n"
        f"⏱ Numbers expire after: <b>90 Min</b>"
    )

    inline = types.InlineKeyboardMarkup()
    inline.row(
        types.InlineKeyboardButton("🔄 New Numbers",    callback_data="change_number"),
        types.InlineKeyboardButton("🌎 Change Country", callback_data="change_country")
    )

    if chat_id in user_messages and edit:
        try:
            bot.edit_message_text(
                text, chat_id, user_messages[chat_id].message_id,
                reply_markup=inline, parse_mode="HTML")
        except Exception:
            msg = bot.send_message(chat_id, text, reply_markup=inline, parse_mode="HTML")
            user_messages[chat_id] = msg
    else:
        msg = bot.send_message(chat_id, text, reply_markup=inline, parse_mode="HTML")
        user_messages[chat_id] = msg

#change3
# def send_random_numbers(chat_id, country=None, edit=False):
#     if country is None:
#         country = user_current_country.get(chat_id)
#         if not country:
#             bot.send_message(chat_id, "❌ No country selected.")
#             return
#     numbers = numbers_by_country.get(country, [])
#     if not numbers:
#         bot.send_message(chat_id, f"❌ No numbers for {country}.")
#         return
#     selected_numbers = random.sample(numbers, min(5, len(numbers)))
#     user_current_country[chat_id] = country
#     save_user_assignment(chat_id, selected_numbers, country)
#     flag = country_to_flag(country)
#     text = f"{flag} <b>{country}</b> Numbers:\n\n"
#     for i, num in enumerate(selected_numbers, 1):
#         text += f"{i}. <code>{num}</code>\n"
#     text += f"\n⏳ Waiting for OTPs on any number...\n🔔 Instant notifications enabled!"

#     inline = types.InlineKeyboardMarkup()
#     inline.row(
#         types.InlineKeyboardButton("🔄 New Numbers",    callback_data="change_number"),
#         types.InlineKeyboardButton("🌎 Change Country", callback_data="change_country")
#     )

#     if chat_id in user_messages and edit:
#         try:
#             bot.edit_message_text(
#                 text, chat_id, user_messages[chat_id].message_id,
#                 reply_markup=inline, parse_mode="HTML")
#         except Exception:
#             msg = bot.send_message(chat_id, text, reply_markup=inline, parse_mode="HTML")
#             user_messages[chat_id] = msg
#     else:
#         msg = bot.send_message(chat_id, text, reply_markup=inline, parse_mode="HTML")
#         user_messages[chat_id] = msg


@bot.message_handler(commands=["start"])
def start(message):
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
            markup.add(types.InlineKeyboardButton(
                f"🚀 Join {ch}", url=f"https://t.me/{ch[1:]}"))
        bot.send_message(chat_id,
            "❌ You must join all required channels to use the bot.",
            reply_markup=markup)
        return

    if not numbers_by_country:
        bot.send_message(chat_id, "❌ No countries available yet.")
        return

    # FIX: safe ensure_user call — works even with stub
    try:
        _rewards.ensure_user(chat_id, username)
    except Exception:
        pass

    reply_kb = types.ReplyKeyboardMarkup(resize_keyboard=True, is_persistent=True)
    reply_kb.row(
        types.KeyboardButton("📞 Number"),
        types.KeyboardButton("💰 Balance")
    )
    reply_kb.row(
        types.KeyboardButton("💸 Withdraw"),
        types.KeyboardButton("🔗 Wallet")
    )
    reply_kb.row(
        types.KeyboardButton("🆘 Support")
    )
    bot.send_message(chat_id, "✅ Menu ready!", reply_markup=reply_kb)

    markup = types.InlineKeyboardMarkup()
    for country in sorted(numbers_by_country.keys()):
        markup.add(types.InlineKeyboardButton(
            country, callback_data=f"user_select_{country}"))
    msg = bot.send_message(chat_id, "🌎 Choose a country:", reply_markup=markup)
    user_messages[chat_id] = msg

# ─────────────────────────────────────────────
# REPLY KEYBOARD BUTTON HANDLERS
# ─────────────────────────────────────────────
@bot.message_handler(func=lambda m: m.text == "📞 Number" and m.from_user.id != ADMIN_ID)
def kb_number(message):
    chat_id = message.chat.id
    update_active_user(chat_id, message.from_user.username)
    country = user_current_country.get(chat_id)
    if not country:
        if not numbers_by_country:
            return bot.send_message(chat_id, "❌ No countries available yet.")
        markup = types.InlineKeyboardMarkup()
        for c in sorted(numbers_by_country.keys()):
            markup.add(types.InlineKeyboardButton(c, callback_data=f"user_select_{c}"))
        msg = bot.send_message(chat_id, "🌎 Choose a country:", reply_markup=markup)
        user_messages[chat_id] = msg
    else:
        send_random_numbers(chat_id, country, edit=False)


@bot.message_handler(func=lambda m: m.text == "💰 Balance" and m.from_user.id != ADMIN_ID)
def kb_balance(message):
    chat_id = message.chat.id
    update_active_user(chat_id, message.from_user.username)
    if not _rewards_loaded:
        return bot.send_message(chat_id,
            "❌ Reward system is currently unavailable.\nPlease try again later.")
    try:
        _rewards.ensure_user(chat_id, message.from_user.username)
        user   = _rewards.get_user(chat_id)
        bal    = user.get("balance", 0.0) if user else 0.0
        total  = user.get("total_earned", 0.0) if user else 0.0
        wallet = user.get("wallet") or "❌ Not set"
        cfg    = _rewards._get_config()
        min_w  = cfg.get("min_withdrawal", 0.05)
        text = (
            f"💰 <b>Your Earnings</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💵 <b>Balance:</b>      <code>${bal:.5f}</code>\n"
            f"📈 <b>Total Earned:</b> <code>${total:.5f}</code>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🔗 <b>Wallet:</b>\n<code>{html.escape(str(wallet))}</code>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🔻 Min Withdraw: <code>${min_w:.3f}</code>"
        )
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("🔗 Set Wallet",       callback_data="menu_wallet"),
            types.InlineKeyboardButton("💸 Withdraw",         callback_data="menu_withdraw")
        )
        markup.row(
            types.InlineKeyboardButton("📊 Earnings History", callback_data="menu_earnings")
        )
        bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=markup)
    except Exception as e:
        bot.send_message(chat_id, f"❌ Error loading balance: {e}")


@bot.message_handler(func=lambda m: m.text == "💸 Withdraw" and m.from_user.id != ADMIN_ID)
def kb_withdraw(message):
    chat_id = message.chat.id
    update_active_user(chat_id, message.from_user.username)
    if not _rewards_loaded:
        return bot.send_message(chat_id,
            "❌ Reward system is currently unavailable.\nPlease try again later.")
    try:
        _rewards.ensure_user(chat_id, message.from_user.username)
        # FIX: delegate entirely to rewards.py's shared screen function
        from rewards import _show_withdraw_screen
        _show_withdraw_screen(bot, ADMIN_ID, chat_id,
            send_fn=lambda txt, **kw: bot.send_message(chat_id, txt, **kw))
    except Exception as e:
        bot.send_message(chat_id, f"❌ Error: {e}")


@bot.message_handler(func=lambda m: m.text == "🔗 Wallet" and m.from_user.id != ADMIN_ID)
def kb_wallet(message):
    chat_id = message.chat.id
    update_active_user(chat_id, message.from_user.username)
    if not _rewards_loaded:
        return bot.send_message(chat_id,
            "❌ Reward system is currently unavailable.\nPlease try again later.")
    try:
        _rewards.ensure_user(chat_id, message.from_user.username)
        user   = _rewards.get_user(chat_id)
        wallet = user.get("wallet") if user else None
        current = f"\n\n🔗 Current: <code>{html.escape(wallet)}</code>" if wallet else ""
        # FIX: send to user's private chat for next step handler
        msg = bot.send_message(
            chat_id,
            f"🔗 <b>Set Polygon (MATIC) Wallet</b>{current}\n\n"
            f"Send your wallet address\n(starts with <code>0x</code>, 42 characters):",
            parse_mode="HTML")
        bot.register_next_step_handler(msg, _wallet_step_kb, chat_id)
    except Exception as e:
        bot.send_message(chat_id, f"❌ Error: {e}")


def _wallet_step_kb(message, chat_id):
    """Step handler for the 🔗 Wallet keyboard button."""
    addr = message.text.strip() if message.text else ""
    if not addr.startswith("0x") or len(addr) != 42:
        bot.reply_to(message,
            "❌ Invalid address. Must start with <code>0x</code> and be 42 characters.\n"
            "Tap 🔗 Wallet again to retry.", parse_mode="HTML")
        return
    _rewards.set_wallet(chat_id, addr)
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("💸 Withdraw Now", callback_data="menu_withdraw"),
        types.InlineKeyboardButton("💰 Balance",      callback_data="show_balance")
    )
    bot.reply_to(message,
        f"✅ <b>Wallet Saved!</b>\n<code>{html.escape(addr)}</code>",
        parse_mode="HTML", reply_markup=markup)


@bot.message_handler(func=lambda m: m.text == "🆘 Support" and m.from_user.id != ADMIN_ID)
def kb_support(message):
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("💬 Contact Support", url=f"https://t.me/vasuhubbot")
    )
    markup.row(
        types.InlineKeyboardButton("📢 Channel", url=CHANNEL_LINK),
        types.InlineKeyboardButton("👥 Group",   url=CODE_GROUP)
    )
    bot.send_message(message.chat.id,
        f"🆘 <b>Support</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"For help, tap the button below or contact:\n"
        f"<b>@Vasuhubbot</b>",
        parse_mode="HTML", reply_markup=markup)


# ─────────────────────────────────────────────
# INLINE CALLBACK HANDLERS
# NOTE: show_balance, menu_wallet, menu_withdraw, menu_earnings, menu_back, wconfirm_*,
#       wapprove_*, wreject_* are ALL registered by rewards.py's register_handlers().
#       Do NOT add those callbacks here — duplicate handlers cause silent conflicts.
# ─────────────────────────────────────────────

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
    except Exception:
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
        merged   = list(dict.fromkeys(existing + numbers))
        numbers_by_country[choice] = merged
        save_data()
        file_path = os.path.join(NUMBERS_DIR, f"{choice}.txt")
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(merged))
        except Exception as e:
            logger.error(f"Failed to write numbers file: {e}")
        try:
            bot.answer_callback_query(call.id, f"✅ Added {len(numbers)} numbers!")
            bot.edit_message_text(
                f"✅ Added {len(numbers)} numbers to <b>{html.escape(choice)}</b>\n"
                f"Total: {len(merged)}",
                call.message.chat.id, call.message.message_id, parse_mode="HTML")
        except Exception:
            bot.send_message(call.message.chat.id, f"✅ Added {len(numbers)} to {choice}")
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
            markup.add(types.InlineKeyboardButton(
                country, callback_data=f"user_select_{country}"))
        if chat_id in user_messages:
            try:
                bot.edit_message_text(
                    "🌎 Select a country:", chat_id,
                    user_messages[chat_id].message_id, reply_markup=markup)
            except Exception:
                msg = bot.send_message(chat_id, "🌎 Select a country:", reply_markup=markup)
                user_messages[chat_id] = msg
        else:
            msg = bot.send_message(chat_id, "🌎 Select a country:", reply_markup=markup)
            user_messages[chat_id] = msg

# ─────────────────────────────────────────────
# BROADCAST
# ─────────────────────────────────────────────
def broadcast_message(message):
    text          = message.text
    success_count = 0
    fail_count    = 0
    for user_id in get_all_active_users():
        try:
            bot.send_message(
                user_id,
                f"📢 <b>Broadcast:</b>\n\n{html.escape(text)}",
                parse_mode="HTML")
            success_count += 1
        except Exception:
            fail_count += 1
        time.sleep(0.05)
    bot.reply_to(message,
        f"✅ Broadcast sent!\n✅ Success: {success_count}\n❌ Failed: {fail_count}")


@bot.message_handler(commands=["broadcast"])
def broadcast_start(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    msg = bot.reply_to(message, "✉️ Send the message to broadcast:")
    bot.register_next_step_handler(msg, broadcast_message)


@bot.message_handler(commands=["usercount"])
def user_count(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    sqlite_count = get_active_user_count()
    try:
        mongo_count = _rewards.get_user_count()
    except Exception:
        mongo_count = "N/A"
    bot.reply_to(message,
        f"👥 SQLite active users: <b>{sqlite_count}</b>\n"
        f"🍃 MongoDB users: <b>{mongo_count}</b>",
        parse_mode="HTML")


@bot.message_handler(commands=["stats"])
def show_stats(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM otp_records')
            total_otps = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM otp_records WHERE DATE(created_at) = DATE('now')")
            otps_today = cursor.fetchone()[0]
            cursor.execute('SELECT COUNT(*) FROM active_users')
            active_count = cursor.fetchone()[0]

        try:
            mongo_users = _rewards.get_user_count()
        except Exception:
            mongo_users = "N/A"

        try:
            reward_cfg    = _rewards._get_config()
            reward_status = "🟢 ON" if reward_cfg.get("rewards_enabled") else "🔴 OFF"
            reward_amt    = f"${reward_cfg.get('default_reward', 0.005):.5f}"
        except Exception:
            reward_status = "N/A"
            reward_amt    = "N/A"

        stats_text = (
            f"📊 <b>Bot Statistics</b>\n\n"
            f"📱 <b>OTPs:</b> {total_otps} total | {otps_today} today\n"
            f"👥 <b>Users (SQLite):</b> {active_count}\n"
            f"🍃 <b>Users (MongoDB):</b> {mongo_users}\n"
            f"🌍 <b>Countries:</b> {len(numbers_by_country)}\n"
            f"📞 <b>Numbers:</b> {sum(len(v) for v in numbers_by_country.values())}\n"
            f"🔌 <b>Panels:</b> {len(panel_registry)}\n"
            f"⚙️ <b>Group Queue:</b> {group_message_queue.qsize()}\n"
            f"⚙️ <b>OTP Queue:</b> {otp_processing_queue.qsize()}\n"
            f"🗑️ <b>Auto-Delete:</b> {str(AUTO_DELETE_MINUTES) + ' min' if AUTO_DELETE_MINUTES > 0 else 'Disabled'}\n"
            f"📡 <b>OTP Groups:</b> {len(OTP_GROUP_IDS)}\n"
            f"💰 <b>Rewards Module:</b> {'🟢 Loaded' if _rewards_loaded else '🔴 Stub'}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💰 <b>Rewards:</b> {reward_status} | Default: {reward_amt}"
        )
        bot.reply_to(message, stats_text, parse_mode="HTML")
    except Exception as e:
        bot.reply_to(message, f"❌ Failed to fetch stats: {e}")

# ─────────────────────────────────────────────
# ADMIN FILE UPLOAD
# ─────────────────────────────────────────────
@bot.message_handler(content_types=["document"])
def handle_document(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ You are not the admin.")
    if not message.document.file_name.endswith(".txt"):
        return bot.reply_to(message, "❌ Please upload a .txt file.")
    file_info       = bot.get_file(message.document.file_id)
    downloaded_file = bot.download_file(file_info.file_path)
    try:
        numbers = [line.strip()
                   for line in downloaded_file.decode("utf-8").splitlines()
                   if line.strip()]
    except Exception:
        return bot.reply_to(message, "❌ Failed to decode file.")
    if not numbers:
        return bot.reply_to(message, "❌ File is empty.")
    temp_uploads[message.from_user.id] = numbers
    markup = types.InlineKeyboardMarkup()
    for country in sorted(numbers_by_country.keys()):
        markup.add(types.InlineKeyboardButton(country, callback_data=f"addto_{country}"))
    markup.add(types.InlineKeyboardButton("➕ New Country", callback_data="addto_new"))
    bot.reply_to(message, "📂 File received. Select country:", reply_markup=markup)


def save_new_country(message, numbers):
    country = message.text.strip()
    if not country:
        return bot.reply_to(message, "❌ Invalid country name.")
    numbers_by_country[country] = list(
        dict.fromkeys([n.strip() for n in numbers if n.strip()]))
    save_data()
    file_path = os.path.join(NUMBERS_DIR, f"{country}.txt")
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(numbers_by_country[country]))
    except Exception as e:
        logger.error(f"Failed to write country file: {e}")
    bot.reply_to(message,
        f"✅ Saved {len(numbers_by_country[country])} numbers under "
        f"<b>{html.escape(country)}</b>",
        parse_mode="HTML")
    temp_uploads.pop(message.from_user.id, None)

# ─────────────────────────────────────────────
# ADMIN COMMANDS
# ─────────────────────────────────────────────
@bot.message_handler(commands=["setcountry"])
def set_country(message):
    global current_country
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not admin.")
    if len(message.text.split()) > 1:
        current_country = " ".join(message.text.split()[1:]).strip()
        if current_country not in numbers_by_country:
            numbers_by_country[current_country] = []
        save_data()
        bot.reply_to(message, f"✅ Country set to: {current_country}")
    else:
        bot.reply_to(message, "Usage: /setcountry <country name>")


@bot.message_handler(commands=["deletecountry"])
def delete_country(message):
    global current_country
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not admin.")
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
            bot.reply_to(message, f"✅ Deleted: {country}")
        else:
            bot.reply_to(message, f"❌ '{country}' not found.")
    else:
        bot.reply_to(message, "Usage: /deletecountry <country name>")


@bot.message_handler(commands=["cleannumbers"])
def clear_numbers(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not admin.")
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
        bot.reply_to(message, "Usage: /cleannumbers <country name>")


@bot.message_handler(commands=["listcountries"])
def list_countries(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not admin.")
    if not numbers_by_country:
        return bot.reply_to(message, "❌ No countries.")
    text = "🌍 <b>Countries:</b>\n\n"
    for country, nums in sorted(numbers_by_country.items()):
        text += f"• {country}: {len(nums)} numbers\n"
    bot.reply_to(message, text, parse_mode="HTML")


@bot.message_handler(commands=["addchat"])
def add_chat(message):
    global OTP_GROUP_IDS
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not admin.")
    if message.chat.type == "private":
        return bot.reply_to(message, "❌ Use in a group/channel.")
    chat_id_str   = str(message.chat.id)
    OTP_GROUP_IDS = [chat_id_str]
    data["otp_groups"] = OTP_GROUP_IDS
    save_data()
    bot.reply_to(message,
        f"✅ OTP group set to: <b>{html.escape(message.chat.title)}</b>\n"
        f"🆔 <code>{chat_id_str}</code>",
        parse_mode="HTML")


@bot.message_handler(commands=["autodelete"])
def set_autodelete(message):
    global AUTO_DELETE_MINUTES
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not admin.")
    args = message.text.split()
    if len(args) < 2:
        return bot.reply_to(message,
            f"🗑️ Auto-Delete: "
            f"{'✅ ' + str(AUTO_DELETE_MINUTES) + ' min' if AUTO_DELETE_MINUTES > 0 else '❌ Disabled'}\n"
            f"Usage: /autodelete &lt;minutes&gt; | 0 to disable",
            parse_mode="HTML")
    try:
        minutes             = int(args[1])
        AUTO_DELETE_MINUTES = minutes
        data["auto_delete_minutes"] = AUTO_DELETE_MINUTES
        save_data()
        bot.reply_to(message,
            f"✅ Auto-delete set to <b>{minutes} min</b>"
            if minutes > 0 else "✅ Auto-delete <b>disabled</b>",
            parse_mode="HTML")
    except ValueError:
        bot.reply_to(message, "❌ Invalid number.")


@bot.message_handler(commands=["adminhelp"])
def admin_help(message):
    if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not admin.")
    help_text = """
🔧 <b>Admin Commands</b>

📁 <b>Numbers Management:</b>
• Upload .txt file — Add numbers to a country
• /setcountry &lt;name&gt;
• /deletecountry &lt;name&gt;
• /cleannumbers &lt;name&gt;
• /listcountries

📊 <b>Statistics:</b>
• /stats — Full stats
• /usercount — Active users
• /panelstats — OTPs per panel

🔌 <b>Panel Management:</b>
• /panels — List + Stop/Restart buttons
• /panelstop &lt;name&gt;
• /panelstart &lt;name&gt;
• /panelrestart &lt;name&gt;

📢 <b>Communication:</b>
• /broadcast — Send to all users

🔧 <b>Group Management:</b>
• /addchat — Set group as OTP group
• /autodelete &lt;min&gt;

💰 <b>Reward System:</b>
• /rewardconfig — View full config
• /setreward &lt;amount&gt; — Set default $/OTP
• /setservicereward &lt;CODE&gt; &lt;amt&gt;
• /offreward &lt;CODE&gt; — Disable service reward
• /onreward &lt;CODE&gt; — Enable service reward
• /disablereward — Turn off ALL rewards
• /enablereward — Turn on ALL rewards
• /setminwithdraw &lt;amount&gt;
• /testreward &lt;user_id&gt;
• /pendingwithdrawals
• /approvewithdraw &lt;id&gt;
• /rejectwithdraw &lt;id&gt; [reason]
• /setcountryreward &lt;Country&gt; &lt;amt&gt;
• /offcountryreward &lt;Country&gt;
• /oncountryreward &lt;Country&gt;
• /listcountryrewards

👥 <b>User Data:</b>
• /exportusers — Download users .txt
• /importusers — Restore from export

🏳 <b>Flags:</b>
• /addflag IN &lt;emoji_id&gt;
• /removeflag IN
• /listflags
"""
    bot.reply_to(message, help_text, parse_mode="HTML")

# ─────────────────────────────────────────────
# CLEANUP
# ─────────────────────────────────────────────

def cleanup_old_otps():
    while True:
        try:
            time.sleep(300)  # har 5 minute check karo (90-min expiry ke liye)
            now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            with get_db() as conn:
                cursor = conn.cursor()
                # Expired assignments delete karo
                cursor.execute(
                    "DELETE FROM user_assignments WHERE expire_at IS NOT NULL AND expire_at <= ?",
                    (now_str,))
                expired = cursor.rowcount
                # Purane OTP records bhi clean karo (30 din se purane)
                cursor.execute(
                    "DELETE FROM otp_records WHERE created_at < datetime('now', '-30 days')")
                deleted = cursor.rowcount
                conn.commit()
                if expired > 0:
                    logger.info(f"🗑️ Expired {expired} number assignments")
                if deleted > 0:
                    logger.info(f"🗑️ Cleaned up {deleted} old OTP records")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

# change4
# def cleanup_old_otps():
#     while True:
#         try:
#             time.sleep(3600)
#             with get_db() as conn:
#                 cursor = conn.cursor()
#                 cursor.execute(
#                     "DELETE FROM otp_records WHERE created_at < datetime('now', '-30 days')")
#                 deleted = cursor.rowcount
#                 conn.commit()
#                 if deleted > 0:
#                     logger.info(f"🗑️ Cleaned up {deleted} old OTP records")
#         except Exception as e:
#             logger.error(f"Cleanup error: {e}")

# ─────────────────────────────────────────────
# REGISTER REWARD HANDLERS
# Must run BEFORE polling.
# rewards.py owns: show_balance, menu_wallet, menu_withdraw, menu_earnings,
#                  menu_back, wconfirm_*, wapprove_*, wreject_*
# ─────────────────────────────────────────────
try:
    _rewards.register_reward_commands(bot, ADMIN_ID)
    logger.info("✅ Reward commands registered")
except Exception as _reg_err:
    logger.error(f"❌ Failed to register reward commands: {_reg_err}")


# ─────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────
def startup():
    logger.info("🚀 Starting Multi-Panel OTP Bot (with Reward System)...")

    load_all_panels()

    threading.Thread(target=run_flask,             daemon=True, name="Flask").start()
    threading.Thread(target=group_sender_worker,   daemon=True, name="GroupSender").start()
    threading.Thread(target=personal_sender_worker,daemon=True, name="PersonalSender").start()
    threading.Thread(target=otp_processor_worker,  daemon=True, name="OTPProcessor").start()
    threading.Thread(target=cleanup_old_otps,      daemon=True, name="Cleanup").start()

    start_all_panels()

    threading.Thread(target=run_bot, daemon=True, name="BotPoller").start()

    logger.info(
        f"✅ All services started | "
        f"Panels: {len(panel_registry)} | "
        f"Rewards: {'✅' if _rewards_loaded else '❌ (stub)'}"
    )

    while True:
        time.sleep(60)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def run_bot():
    logger.info("🤖 Starting bot polling...")
    bot.infinity_polling(timeout=60, long_polling_timeout=30)


if __name__ == "__main__":
    startup()
