"""
panel_grand.py — Grand Panel
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
API token based — Bearer header auth
URL: https://panel.grand-panel.com/api/v1/messages
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
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# ⚙️  CONFIG
# ─────────────────────────────────────────────
PANEL_NAME  = "grandpanel"
API_TOKEN   = os.getenv("GRANDPANEL_API_TOKEN", "Api")
BASE_URL    = os.getenv("GRANDPANEL_BASE_URL",  "https://panel.grand-panel.com")
FETCH_DELAY = 1.0
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


def _fetch_date(date_str: str, headers: dict):
    """Fetch all messages for a given date string YYYY-MM-DD. Returns list of raw records."""
    try:
        response = requests.get(
            f"{BASE_URL}/api/v1/messages",
            headers=headers,
            params={"date": date_str},
            timeout=8
        )

        logger.debug(f"[{PANEL_NAME}] HTTP {response.status_code} for date={date_str} | body: {response.text[:200]}")

        if response.status_code == 401:
            logger.warning(f"[{PANEL_NAME}] ⚠️ 401 Unauthorized — check API token")
            return None  # None = auth error, stop retrying

        if response.status_code == 404:
            logger.warning(f"[{PANEL_NAME}] ⚠️ 404 Not Found — check BASE_URL or endpoint path")
            return None

        if response.status_code != 200:
            logger.warning(f"[{PANEL_NAME}] ⚠️ HTTP {response.status_code}: {response.text[:100]}")
            return []

        if not response.text.strip():
            logger.warning(f"[{PANEL_NAME}] ⚠️ Empty response for date={date_str}")
            return []

        try:
            data = response.json()
        except Exception:
            logger.warning(f"[{PANEL_NAME}] ⚠️ Non-JSON response: {response.text[:100]}")
            return []

        messages = data.get("messages", [])
        logger.info(f"[{PANEL_NAME}] 📅 date={date_str} → {len(messages)} message(s) | total={data.get('total', '?')} | range={data.get('range', '?')}")
        return messages

    except requests.exceptions.Timeout:
        logger.warning(f"[{PANEL_NAME}] ⚠️ Timeout for date={date_str}")
        return []
    except requests.exceptions.ConnectionError:
        logger.warning(f"[{PANEL_NAME}] ⚠️ Connection error for date={date_str}")
        return []
    except Exception as e:
        logger.error(f"[{PANEL_NAME}] ❌ Unexpected error for date={date_str}: {e}")
        return []


def start(otp_queue, seen_messages: set, seen_order: deque, max_seen: int):
    logger.info(f"[{PANEL_NAME}] 🚀 Starting panel (Bearer token mode)...")
    logger.info(f"[{PANEL_NAME}] 🔑 Token: {API_TOKEN[:10]}...  | URL: {BASE_URL}")

    headers = {
        "Authorization": f"Bearer {API_TOKEN}"
    }

    auth_failed = False

    while True:
        if auth_failed:
            logger.error(f"[{PANEL_NAME}] 🔴 Auth failed — panel stopped. Fix token and restart.")
            time.sleep(60)
            continue

        # Fetch today + yesterday to handle timezone edge cases
        now_utc = datetime.now(timezone.utc)
        dates_to_fetch = [
            now_utc.strftime("%Y-%m-%d"),
            (now_utc - timedelta(days=1)).strftime("%Y-%m-%d"),
        ]

        for date_str in dates_to_fetch:
            records = _fetch_date(date_str, headers)

            if records is None:
                auth_failed = True
                break

            for record in records:
                num     = str(record.get("destination", "")).strip().lstrip("0").lstrip("+")
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

        time.sleep(FETCH_DELAY)
