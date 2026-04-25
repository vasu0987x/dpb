import requests
import json
import time
import re
import html
import unicodedata
import queue
import threading
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import pycountry
import phonenumbers
from flask import Flask, Response

# ===== CONFIG =====
API_KEY  = "SFFRRT1SS2qGlJFye1BPQUk="
BASE_URL = "http://pscall.net/restapi/smsreport"
HEADERS  = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "http://pscall.net/",
    "Connection":      "keep-alive"
}

BOT_TOKEN    = "8321758039:AAFhgtxhnPe5e4n8tkG7dAGTEWl9fOqXn7k"
CHAT_IDS     = ["-1003787254360"]
BACKUP       = "https://t.me/+_8fy-j4nl2RjMGMx"
CHANNEL_LINK = "https://t.me/YUVRAJNUMBER"

seen_messages = set()
message_queue = queue.Queue()


# ========= TELEGRAM SENDER =========
def send_to_telegram(msg, kb=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    success = False

    for chat_id in CHAT_IDS:
        payload = {
            "chat_id":    chat_id,
            "text":       msg[:3900],
            "parse_mode": "HTML"
        }
        if kb:
            payload["reply_markup"] = kb.to_json()

        for i in range(3):
            try:
                r = requests.post(url, data=payload, timeout=10)
                if r.status_code == 200:
                    success = True
                    break
                else:
                    print(f"❌ Telegram Error ({chat_id}):", r.text)
            except Exception as e:
                print(f"❌ Telegram Exception ({chat_id}):", e)
            time.sleep(1)

    return success


# ========= QUEUE WORKER =========
def sender_worker():
    while True:
        msg, kb = message_queue.get()
        send_to_telegram(msg, kb)
        print("📤 Sent from queue")
        time.sleep(0.5)
        message_queue.task_done()


# ========= HELPERS =========
def fetch_sms(start=0, length=10):
    try:
        response = requests.get(
            BASE_URL,
            params={
                "key":    API_KEY,
                "start":  start,
                "length": length
            },
            headers=HEADERS,
            timeout=15
        )
        return response.json()
    except Exception:
        return None


def extract_otp(message: str) -> str | None:
    message = unicodedata.normalize("NFKD", message)
    message = re.sub(r"[\u200f\u200e\u202a-\u202e]", "", message)

    keyword_regex = re.search(r"(otp|code|pin|password)[^\d]{0,10}(\d[\d\-]{3,8})", message, re.I)
    if keyword_regex:
        return re.sub(r"\D", "", keyword_regex.group(2))

    reverse_regex = re.search(r"(\d[\d\-]{3,8})[^\w]{0,10}(otp|code|pin|password)", message, re.I)
    if reverse_regex:
        return re.sub(r"\D", "", reverse_regex.group(1))

    generic_regex = re.findall(r"\d{2,4}[-]?\d{2,4}", message)
    if generic_regex:
        return re.sub(r"\D", "", generic_regex[0])

    return None


def mask_number(number: str) -> str:
    if len(number) <= 4:
        return number
    mid   = len(number) // 2
    start = number[:mid - 1]
    end   = number[mid + 1:]
    return start + "**" + end


def country_from_number(number: str) -> tuple[str, str]:
    try:
        parsed = phonenumbers.parse("+" + number)
        region = phonenumbers.region_code_for_number(parsed)
        if not region:
            return "Unknown", "🌍"
        country_obj = pycountry.countries.get(alpha_2=region)
        if not country_obj:
            return "Unknown", "🌍"
        country = country_obj.name
        flag    = "".join([chr(127397 + ord(c)) for c in region])
        return country, flag
    except Exception:
        return "Unknown", "🌍"


def format_message(record):
    current_time = record.get("dateadded") or "Unknown"
    number       = str(record.get("num")  or "Unknown").strip().lstrip("0").lstrip("+")
    sender       = str(record.get("cli")  or "Unknown").strip()
    message      = str(record.get("sms")  or "").strip()

    country, flag = country_from_number(number)
    otp = extract_otp(message)
    otp_line = f"<blockquote>🔑 <b>OTP:</b> <code>{html.escape(otp)}</code></blockquote>\n" if otp else ""

    formatted = (
        f"{flag} <b>New {sender} OTP Received</b>\n\n"
        f"<blockquote>🕰 <b>Time:</b> <b>{html.escape(str(current_time))}</b></blockquote>\n"
        f"<blockquote>🌍 <b>Country:</b> <b>{html.escape(country)} {flag}</b></blockquote>\n"
        f"<blockquote>📱 <b>Service:</b> <b>{html.escape(sender)}</b></blockquote>\n"
        f"<blockquote>📞 <b>Number:</b> <b>{html.escape(mask_number(number))}</b></blockquote>\n"
        f"{otp_line}"
        f"<blockquote>✉️ <b>Full Message:</b></blockquote>\n"
        f"<blockquote><code>{html.escape(message)}</code></blockquote>\n\n"
    )

    keyboard = [
        [InlineKeyboardButton("🚀 Main Channel", url=CHANNEL_LINK)],
        [InlineKeyboardButton("📱 Numbers Channel", url=BACKUP)]
    ]

    return formatted, InlineKeyboardMarkup(keyboard)


# ========= MAIN FETCHER =========
def main_loop():
    print("🚀 PsCall OTP Monitor Started...")

    while True:
        stats = fetch_sms(start=0, length=10) or {}

        if stats.get("result") == "success":
            for record in stats.get("data", []):
                uid = f"{record.get('dateadded')}_{record.get('num')}_{record.get('sms')}"
                if uid not in seen_messages:
                    seen_messages.add(uid)
                    msg, kb = format_message(record)
                    message_queue.put((msg, kb))
                    print("🌀 Queued:", record.get("sms", "")[:60])

        time.sleep(0.2)


# ========= FLASK HEALTH CHECK =========
app = Flask(__name__)

@app.route("/health")
def health():
    return Response("OK", status=200)


# ========= START =========
if __name__ == "__main__":
    threading.Thread(target=sender_worker, daemon=True).start()
    threading.Thread(target=main_loop,     daemon=True).start()
    app.run(host="0.0.0.0", port=5003)
