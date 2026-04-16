"""
panel_np.py — Np Panel
━━━━━━━━━━━━━━━━━━━━━━
API token based — no login required
URL: http://147.135.212.197/crapi/st
Same structure as panel_konek.py — sirf BASE_URL alag hai
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
PANEL_NAME  = "Np"
API_TOKEN   = os.getenv("NP_API_TOKEN", "Api")
BASE_URL    = os.getenv("NP_BASE_URL",  "http://147.135.212.197/crapi/st")
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

            if not isinstance(stats, list):
                logger.warning(f"[{PANEL_NAME}] ⚠️ Unexpected response format")
                time.sleep(5)
                continue

            for record in stats:
                try:
                    if not isinstance(record, list) or len(record) < 4:
                        continue

                    # [0]=sender/cli, [1]=num, [2]=message, [3]=datetime
                    sender  = str(record[0]).strip()
                    num     = str(record[1]).strip().lstrip("0").lstrip("+")
                    message = str(record[2]).strip()
                    time_   = str(record[3]).strip()

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

                except Exception as e:
                    logger.error(f"[{PANEL_NAME}] ❌ Record error: {e}")

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
