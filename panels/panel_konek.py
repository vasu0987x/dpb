"""
panel_konek.py — Konek Panel
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
API token based — no login required
URL: http://51.77.216.195/crapi/konek
Same structure as panel_maitt.py — sirf BASE_URL alag hai
"""

import requests
import re
import time
import hashlib
import logging
import os
import phonenumbers
import pycountry
from collections import deque

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# ⚙️  CONFIG
# ─────────────────────────────────────────────
PANEL_NAME  = "konek"
API_TOKEN   = os.getenv("KONEK_API_TOKEN", "Api")
BASE_URL    = os.getenv("KONEK_BASE_URL",  "http://51.77.216.195/crapi/konek")
FETCH_DELAY = 3.0
# ─────────────────────────────────────────────


def _country_from_number(num: str):
    try:
        parsed = phonenumbers.parse("+" + num)
        region = phonenumbers.region_code_for_number(parsed)
        return pycountry.countries.get(alpha_2=region).name
    except Exception:
        return "Unknown"


def _extract_otp(message: str):
    text = message.replace("\n", " ").strip()
    m = re.search(r"\b(\d{3})[-\s](\d{3})\b", text)
    if m:
        return m.group(1) + m.group(2)
    m = re.search(r"(otp|code|pin|password|verification|код|кода)[^\d]{0,10}(\d{4,8})", text, re.I)
    if m:
        return m.group(2)
    m = re.search(r"(\d{4,8})[^\w]{0,10}(otp|code|pin|password|verification|код|кода)", text, re.I)
    if m:
        return m.group(1)
    for g in re.findall(r"\b\d{4,8}\b", text):
        if not (1900 <= int(g) <= 2099):
            return g
    return None


def start(otp_queue, seen_messages: set, seen_order: deque, max_seen: int):
    logger.info(f"[{PANEL_NAME}] 🚀 Starting panel (API token mode)...")

    while True:
        try:
            response = requests.get(
                f"{BASE_URL}/viewstats",
                params={
                    "token":   API_TOKEN,
                    "dt1":     "1970-01-01 00:00:00",
                    "dt2":     "2099-12-31 23:59:59",
                    "records": 10
                },
                timeout=8
            )

            if not response.text.strip():
                logger.warning(f"[{PANEL_NAME}] ⚠️ Empty response — token invalid or server down")
                time.sleep(5)
                continue

            try:
                stats = response.json()
            except Exception:
                logger.warning(f"[{PANEL_NAME}] ⚠️ Non-JSON response: {response.text[:100]}")
                time.sleep(5)
                continue

            if stats.get("status") == "error":
                logger.warning(f"[{PANEL_NAME}] ⚠️ API error: {stats.get('msg', 'Unknown')}")
                time.sleep(10)
                continue

            if stats.get("status") == "success":
                for record in stats.get("data", []):
                    num     = str(record.get("num", "")).strip().lstrip("0").lstrip("+")
                    sender  = str(record.get("cli")     or "Unknown").strip()
                    message = str(record.get("message") or "").strip()
                    time_   = str(record.get("dt")      or "").strip()

                    hash_id = hashlib.md5(f"{time_}{num}{message[:50]}".encode()).hexdigest()
                    if hash_id in seen_messages:
                        continue

                    seen_messages.add(hash_id)
                    seen_order.append(hash_id)
                    if len(seen_order) > max_seen:
                        old = seen_order.popleft()
                        seen_messages.discard(old)

                    country = _country_from_number(num)
                    otp     = _extract_otp(message)

                    otp_queue.put({
                        "hash_id":    hash_id,
                        "panel_name": PANEL_NAME,
                        "dt":         time_,
                        "country":    country,
                        "num":        num,
                        "cli":        sender,
                        "message":    message,
                        "otp":        otp
                    })
                    logger.info(f"[{PANEL_NAME}] 📱 {num} | {sender} | OTP: {otp or 'N/A'}")

        except requests.exceptions.Timeout:
            logger.warning(f"[{PANEL_NAME}] ⚠️ Timeout — retrying...")
            time.sleep(3)
            continue
        except requests.exceptions.ConnectionError:
            logger.warning(f"[{PANEL_NAME}] ⚠️ Connection error — retrying...")
            time.sleep(5)
            continue
        except Exception as e:
            logger.error(f"[{PANEL_NAME}] ❌ Error: {e}")
            time.sleep(2)
            continue

        time.sleep(FETCH_DELAY)
