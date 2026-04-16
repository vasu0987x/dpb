"""
rewards.py  —  Reward system for OTP Bot
FIXED VERSION — all handlers in register_handlers(), no duplicates with main.py
"""

import os
import io
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

from pymongo import MongoClient, ASCENDING, DESCENDING
import telebot
from telebot import types
import html

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# MONGO
# ─────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "link")
_client   = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
_db       = _client[os.getenv("MONGO_DB", "otpbot")]

col_config       = _db["reward_config"]
col_users        = _db["users"]
col_transactions = _db["transactions"]
col_withdrawals  = _db["withdrawals"]

col_users.create_index("chat_id", unique=True)
col_transactions.create_index([("chat_id", ASCENDING), ("created_at", DESCENDING)])
col_withdrawals.create_index([("status",   ASCENDING), ("created_at",  ASCENDING)])
col_withdrawals.create_index("chat_id")

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
_DEFAULT_CFG = {
    "_id":              "main",
    "rewards_enabled":  True,
    "default_reward":   0.005,
    "min_withdrawal":   0.05,
    "service_overrides": {},
    "country_overrides": {}   # ← YE ADD KARO
}

def _get_config() -> dict:
    doc = col_config.find_one({"_id": "main"})
    if not doc:
        col_config.insert_one(_DEFAULT_CFG.copy())
        return _DEFAULT_CFG.copy()
    return doc

def _save_config(update: dict):
    col_config.update_one({"_id": "main"}, {"$set": update}, upsert=True)

# ─────────────────────────────────────────
# USER HELPERS
# ─────────────────────────────────────────
def ensure_user(chat_id: int, username: str = None):
    col_users.update_one(
        {"chat_id": chat_id},
        {
            "$setOnInsert": {
                "chat_id":      chat_id,
                "username":     username,
                "balance":      0.0,
                "total_earned": 0.0,
                "wallet":       None,
                "created_at":   datetime.now(timezone.utc)
            },
            "$set": {"last_active": datetime.now(timezone.utc)}
        },
        upsert=True
    )
    if username:
        col_users.update_one({"chat_id": chat_id}, {"$set": {"username": username}})

def get_user(chat_id: int):
    return col_users.find_one({"chat_id": chat_id})

def add_balance(chat_id: int, amount: float, reason: str = "otp_reward", service: str = ""):
    col_users.update_one(
        {"chat_id": chat_id},
        {
            "$inc": {"balance": amount, "total_earned": amount},
            "$set": {"last_active": datetime.now(timezone.utc)},
            "$setOnInsert": {
                "username": None, "wallet": None,
                "created_at": datetime.now(timezone.utc)
            }
        },
        upsert=True
    )
    col_transactions.insert_one({
        "chat_id":    chat_id,
        "amount":     amount,
        "reason":     reason,
        "service":    service,
        "created_at": datetime.now(timezone.utc)
    })
    logger.info(f"[REWARD] +${amount:.5f} [{service}] -> {chat_id}")

def deduct_balance(chat_id: int, amount: float):
    col_users.update_one({"chat_id": chat_id}, {"$inc": {"balance": -amount}})

def set_wallet(chat_id: int, address: str):
    col_users.update_one({"chat_id": chat_id}, {"$set": {"wallet": address}})

def get_all_user_ids() -> list:
    return [u["chat_id"] for u in col_users.find({}, {"chat_id": 1})]

def get_user_count() -> int:
    return col_users.count_documents({})

# ─────────────────────────────────────────
# REWARD LOGIC
# ─────────────────────────────────────────
def _r(v: float) -> float:
    return float(Decimal(str(v)).quantize(Decimal("0.00001"), rounding=ROUND_DOWN))

def compute_reward(service_code: str, country: str = "") -> float | None:
    cfg = _get_config()
    if not cfg.get("rewards_enabled", True):
        logger.info(f"[REWARD] SKIP — globally disabled")
        return None

    svc = service_code.upper()
    ctry = country.strip() if country else ""

    # Service override check
    svc_overrides = cfg.get("service_overrides", {})
    svc_reward = None
    if svc in svc_overrides:
        sc = svc_overrides[svc]
        if not sc.get("enabled", True):
            logger.info(f"[REWARD] SKIP {svc} — service disabled")
            return None
        svc_reward = _r(sc.get("reward", cfg.get("default_reward", 0.005)))

    # Country override check
    ctry_overrides = cfg.get("country_overrides", {})
    ctry_reward = None
    if ctry and ctry in ctry_overrides:
        cc = ctry_overrides[ctry]
        if not cc.get("enabled", True):
            logger.info(f"[REWARD] SKIP {ctry} — country disabled")
            return None
        ctry_reward = _r(cc.get("reward", cfg.get("default_reward", 0.005)))

    # Priority: service override > country override > default
    if svc_reward is not None:
        return svc_reward
    if ctry_reward is not None:
        return ctry_reward
    return _r(cfg.get("default_reward", 0.005))

#change
# def compute_reward(service_code: str):
#     cfg = _get_config()
#     if not cfg.get("rewards_enabled", True):
#         logger.info(f"[REWARD] SKIP {service_code} — globally disabled")
#         return None
#     ov  = cfg.get("service_overrides", {})
#     svc = service_code.upper()
#     if svc in ov:
#         sc = ov[svc]
#         if not sc.get("enabled", True):
#             logger.info(f"[REWARD] SKIP {svc} — service disabled")
#             return None
#         return _r(sc.get("reward", cfg.get("default_reward", 0.005)))
#     return _r(cfg.get("default_reward", 0.005))


def process_otp_reward(chat_id: int, service_code: str, username: str = None, country: str = ""):
    try:
        amount = compute_reward(service_code, country=country)
        logger.info(f"[REWARD] chat={chat_id} svc={service_code} country={country} amount={amount}")
        if not amount or amount <= 0:
            return None
        ensure_user(chat_id, username)
        add_balance(chat_id, amount, reason="otp_reward", service=service_code)
        return amount
    except Exception as e:
        logger.error(f"[REWARD] FAILED chat={chat_id} svc={service_code}: {e}", exc_info=True)
        return None
#change
# def process_otp_reward(chat_id: int, service_code: str, username: str = None):
#     """
#     Main entry point called by otp_processor_worker in main.py.
#     Returns the reward amount if credited, else None.
#     """
#     try:
#         amount = compute_reward(service_code)
#         logger.info(f"[REWARD] chat={chat_id} svc={service_code} amount={amount}")
#         if not amount or amount <= 0:
#             return None
#         ensure_user(chat_id, username)
#         add_balance(chat_id, amount, reason="otp_reward", service=service_code)
#         return amount
#     except Exception as e:
#         logger.error(f"[REWARD] FAILED chat={chat_id} svc={service_code}: {e}", exc_info=True)
#         return None

# ─────────────────────────────────────────
# WITHDRAWAL HELPERS
# ─────────────────────────────────────────
def create_withdrawal(chat_id: int, amount: float, wallet: str) -> str:
    result = col_withdrawals.insert_one({
        "chat_id":     chat_id,
        "amount":      amount,
        "wallet":      wallet,
        "status":      "pending",
        "created_at":  datetime.now(timezone.utc),
        "resolved_at": None,
        "admin_note":  ""
    })
    return str(result.inserted_id)

def get_pending_withdrawals() -> list:
    from bson import ObjectId
    docs = list(col_withdrawals.find({"status": "pending"}).sort("created_at", ASCENDING))
    for d in docs:
        d["_id"] = str(d["_id"])
    return docs

def resolve_withdrawal(withdraw_id: str, approve: bool, admin_note: str = ""):
    from bson import ObjectId
    try:
        oid = ObjectId(withdraw_id)
    except Exception:
        return None
    doc = col_withdrawals.find_one({"_id": oid, "status": "pending"})
    if not doc:
        return None
    status = "approved" if approve else "rejected"
    col_withdrawals.update_one(
        {"_id": oid},
        {"$set": {"status": status,
                  "resolved_at": datetime.now(timezone.utc),
                  "admin_note": admin_note}}
    )
    if not approve:
        col_users.update_one({"chat_id": doc["chat_id"]}, {"$inc": {"balance": doc["amount"]}})
    doc["status"] = status
    doc["_id"]    = str(doc["_id"])
    return doc

# ─────────────────────────────────────────
# SHARED UI HELPERS
# ─────────────────────────────────────────
def _wallet_menu_markup():
    m = types.InlineKeyboardMarkup()
    m.row(
        types.InlineKeyboardButton("🔗 Set Wallet", callback_data="menu_wallet"),
        types.InlineKeyboardButton("💸 Withdraw",   callback_data="menu_withdraw")
    )
    m.row(types.InlineKeyboardButton("📊 Earnings History", callback_data="menu_earnings"))
    m.row(types.InlineKeyboardButton("🔙 Back to Numbers",  callback_data="menu_back"))
    return m


def _balance_text(chat_id: int) -> str:
    ensure_user(chat_id)
    user   = get_user(chat_id)
    bal    = user.get("balance",      0.0) if user else 0.0
    total  = user.get("total_earned", 0.0) if user else 0.0
    wallet = user.get("wallet") or "❌ Not set"
    cfg    = _get_config()
    min_w  = cfg.get("min_withdrawal", 0.05)
    return (
        f"💰 <b>Your Earnings</b>\n━━━━━━━━━━━━━━━\n"
        f"💵 <b>Balance:</b>      <code>${bal:.5f}</code>\n"
        f"📈 <b>Total Earned:</b> <code>${total:.5f}</code>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"🔗 <b>Wallet:</b>\n<code>{html.escape(str(wallet))}</code>\n"
        f"━━━━━━━━━━━━━━━\n🔻 Min Withdraw: <code>${min_w:.3f}</code>"
    )


def _show_withdraw_screen(bot, ADMIN_ID: int, chat_id: int, send_fn):
    """Shared withdraw screen logic used by both /withdraw command and menu_withdraw callback."""
    try:
        user   = get_user(chat_id)
        cfg    = _get_config()
        min_w  = cfg.get("min_withdrawal", 0.05)
        bal    = user.get("balance",  0.0) if user else 0.0
        wallet = user.get("wallet")        if user else None

        if not wallet:
            m = types.InlineKeyboardMarkup()
            m.row(types.InlineKeyboardButton("🔗 Set Wallet", callback_data="menu_wallet"))
            return send_fn(
                "❌ <b>No wallet set!</b>\nSet your Polygon address first.",
                parse_mode="HTML", reply_markup=m)

        if bal < min_w:
            m = types.InlineKeyboardMarkup()
            m.row(
                types.InlineKeyboardButton("💰 Balance",  callback_data="show_balance"),
                types.InlineKeyboardButton("📊 Earnings", callback_data="menu_earnings")
            )
            return send_fn(
                f"❌ <b>Insufficient Balance</b>\n"
                f"💵 Yours: <code>${bal:.5f}</code>\n"
                f"🔻 Min:   <code>${min_w:.3f}</code>\n\n"
                f"<i>Receive more OTPs to earn!</i>",
                parse_mode="HTML", reply_markup=m)

        # FIX: check pending withdrawal safely
        try:
            existing = col_withdrawals.find_one({"chat_id": chat_id, "status": "pending"})
        except Exception:
            existing = None

        if existing:
            return send_fn(
                "⏳ <b>Already pending.</b>\nWait for admin approval.",
                parse_mode="HTML")

        m = types.InlineKeyboardMarkup()
        m.row(
            # FIX: embed chat_id in callback so it's always correct
            types.InlineKeyboardButton("✅ Confirm", callback_data=f"wconfirm_{chat_id}"),
            types.InlineKeyboardButton("❌ Cancel",  callback_data="menu_back")
        )
        send_fn(
            f"💸 <b>Confirm Withdrawal</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💵 Amount: <code>${bal:.5f}</code>\n"
            f"🔗 To:     <code>{html.escape(wallet)}</code>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<i>Tap Confirm to send to admin.</i>",
            parse_mode="HTML", reply_markup=m)
    except Exception as e:
        logger.error(f"[_show_withdraw_screen] chat={chat_id}: {e}", exc_info=True)
        send_fn(f"❌ Error: {e}", parse_mode="HTML")


# ═══════════════════════════════════════════════════
#  MAIN REGISTRATION FUNCTION
#  Call ONCE before bot.infinity_polling()
# ═══════════════════════════════════════════════════
def register_handlers(bot: telebot.TeleBot, ADMIN_ID: int):
    """
    Registers ALL reward-related message + callback handlers.
    Called once from main.py before polling starts.
    """
    logger.info(f"[rewards] Registering handlers (ADMIN_ID={ADMIN_ID})...")

    # ══════════════════════════════════════
    # USER — /balance
    # ══════════════════════════════════════
    @bot.message_handler(commands=["balance"])
    def cmd_balance(message):
        chat_id = message.chat.id
        try:
            ensure_user(chat_id, message.from_user.username)
            text = _balance_text(chat_id)
            m = types.InlineKeyboardMarkup()
            m.row(
                types.InlineKeyboardButton("🔗 Set Wallet", callback_data="menu_wallet"),
                types.InlineKeyboardButton("💸 Withdraw",   callback_data="menu_withdraw")
            )
            m.row(types.InlineKeyboardButton("📊 Earnings History", callback_data="menu_earnings"))
            bot.reply_to(message, text, parse_mode="HTML", reply_markup=m)
        except Exception as e:
            bot.reply_to(message, f"❌ Error loading balance: {e}")

    # ══════════════════════════════════════
    # USER — /wallet
    # ══════════════════════════════════════
    @bot.message_handler(commands=["wallet"])
    def cmd_wallet(message):
        chat_id = message.chat.id
        try:
            ensure_user(chat_id, message.from_user.username)
            user   = get_user(chat_id)
            wallet = user.get("wallet") if user else None
            cur    = f"\n\n🔗 Current: <code>{html.escape(wallet)}</code>" if wallet else ""
            msg    = bot.reply_to(message,
                f"🔗 <b>Set Polygon Wallet</b>{cur}\n\n"
                f"Send address (<code>0x...</code>, 42 chars):",
                parse_mode="HTML")
            bot.register_next_step_handler(msg, _wallet_step_cmd, chat_id)
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")

    # ══════════════════════════════════════
    # USER — /withdraw
    # ══════════════════════════════════════
    @bot.message_handler(commands=["withdraw"])
    def cmd_withdraw(message):
        chat_id = message.chat.id
        ensure_user(chat_id, message.from_user.username)
        _show_withdraw_screen(bot, ADMIN_ID, chat_id,
            send_fn=lambda txt, **kw: bot.reply_to(message, txt, **kw))

    # ══════════════════════════════════════
    # ADMIN — /rewardconfig
    # ══════════════════════════════════════

    #change
    # ══════════════════════════════════════
# ADMIN — /setcountryreward
# ══════════════════════════════════════
    @bot.message_handler(commands=["setcountryreward"])
    def cmd_set_country_reward(message):
     if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
     args = message.text.split(maxsplit=2)
     if len(args) < 3:
        return bot.reply_to(message,
            "Usage: /setcountryreward &lt;Country Name&gt; &lt;amount&gt;\n"
            "Example: /setcountryreward Egypt 0.01\n"
            "<i>Country name exactly as added in bot</i>",
            parse_mode="HTML")
     country_name = args[1].strip()
     try:
        amount = float(args[2])
        assert amount >= 0
     except Exception:
        return bot.reply_to(message, "❌ Invalid amount.")
     cfg = _get_config()
     ov  = cfg.get("country_overrides", {})
     ov.setdefault(country_name, {"enabled": True})["reward"] = _r(amount)
     _save_config({"country_overrides": ov})
     bot.reply_to(message,
        f"✅ <b>{html.escape(country_name)}</b> reward → <code>${amount:.5f}</code>/OTP",
        parse_mode="HTML")


    @bot.message_handler(commands=["offcountryreward"])
    def cmd_off_country_reward(message):
     if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
     args = message.text.split(maxsplit=1)
     if len(args) < 2:
        return bot.reply_to(message, "Usage: /offcountryreward &lt;Country Name&gt;", parse_mode="HTML")
     country_name = args[1].strip()
     cfg = _get_config()
     ov  = cfg.get("country_overrides", {})
     ov.setdefault(country_name, {})["enabled"] = False
     _save_config({"country_overrides": ov})
     bot.reply_to(message,
        f"🔴 <b>{html.escape(country_name)}</b> rewards OFF",
        parse_mode="HTML")


    @bot.message_handler(commands=["oncountryreward"])
    def cmd_on_country_reward(message):
     if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
     args = message.text.split(maxsplit=1)
     if len(args) < 2:
        return bot.reply_to(message, "Usage: /oncountryreward &lt;Country Name&gt;", parse_mode="HTML")
     country_name = args[1].strip()
     cfg = _get_config()
     ov  = cfg.get("country_overrides", {})
     ov.setdefault(country_name, {})["enabled"] = True
     _save_config({"country_overrides": ov})
     bot.reply_to(message,
        f"🟢 <b>{html.escape(country_name)}</b> rewards ON",
        parse_mode="HTML")


    @bot.message_handler(commands=["listcountryrewards"])
    def cmd_list_country_rewards(message):
     if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
     cfg = _get_config()
     ov  = cfg.get("country_overrides", {})
     if not ov:
        return bot.reply_to(message,
            "📭 No country rewards set.\nAll countries use default: "
            f"<code>${cfg.get('default_reward', 0.005):.5f}</code>",
            parse_mode="HTML")
     text = "🌍 <b>Country Reward Overrides:</b>\n━━━━━━━━━━━━━━━\n"
     for country, cc in sorted(ov.items()):
        icon = "🟢" if cc.get("enabled", True) else "🔴"
        text += f"{icon} <b>{html.escape(country)}</b>: <code>${cc.get('reward', cfg.get('default_reward', 0.005)):.5f}</code>\n"
     text += f"\n💵 Default: <code>${cfg.get('default_reward', 0.005):.5f}</code>"
     bot.reply_to(message, text, parse_mode="HTML")


    @bot.message_handler(commands=["rewardconfig"])
    def cmd_reward_config(message):
     if message.from_user.id != ADMIN_ID:
        return bot.reply_to(message, "❌ Not authorized.")
     try:
        _client.admin.command("ping")
        db_status = "🟢 Connected"
     except Exception as dbe:
        db_status = f"🔴 DISCONNECTED — {dbe}"
     cfg  = _get_config()
     icon = "🟢" if cfg.get("rewards_enabled") else "🔴"
     text = (
        f"⚙️ <b>Reward Config</b>\n━━━━━━━━━━━━━━━\n"
        f"🍃 MongoDB: {db_status}\n"
        f"{icon} Rewards: <b>{'ON' if cfg.get('rewards_enabled') else 'OFF'}</b>\n"
        f"💵 Default: <code>${cfg.get('default_reward', 0.005):.5f}</code>/OTP\n"
        f"🔻 Min Withdraw: <code>${cfg.get('min_withdrawal', 0.05):.3f}</code>\n"
        f"━━━━━━━━━━━━━━━\n<b>Service Overrides:</b>\n"
    )
     for svc, sc in sorted(cfg.get("service_overrides", {}).items()):
        icon2 = "🟢" if sc.get("enabled", True) else "🔴"
        text += f"  {icon2} <b>{svc}</b>: <code>${sc.get('reward', cfg.get('default_reward', 0.005)):.5f}</code>\n"
     if not cfg.get("service_overrides"):
        text += "  <i>None (all use default)</i>\n"
     text += f"\n━━━━━━━━━━━━━━━\n<b>Country Overrides:</b>\n"
     for ctry, cc in sorted(cfg.get("country_overrides", {}).items()):
        icon3 = "🟢" if cc.get("enabled", True) else "🔴"
        text += f"  {icon3} <b>{html.escape(ctry)}</b>: <code>${cc.get('reward', cfg.get('default_reward', 0.005)):.5f}</code>\n"
     if not cfg.get("country_overrides"):
        text += "  <i>None (all use default)</i>\n"
     text += (
        f"\n━━━━━━━━━━━━━━━\n"
        f"/setreward &lt;amt&gt;\n"
        f"/setservicereward &lt;CODE&gt; &lt;amt&gt;\n"
        f"/offreward &lt;CODE&gt;   /onreward &lt;CODE&gt;\n"
        f"/disablereward    /enablereward\n"
        f"/setminwithdraw &lt;amt&gt;\n"
        f"/testreward &lt;user_id&gt;\n"
        f"/pendingwithdrawals\n"
        f"/exportusers   /importusers\n"
        f"━━━━━━━━━━━━━━━\n"
        f"/setcountryreward &lt;Country&gt; &lt;amt&gt;\n"
        f"/offcountryreward &lt;Country&gt;\n"
        f"/oncountryreward &lt;Country&gt;\n"
        f"/listcountryrewards"
    )
     bot.reply_to(message, text, parse_mode="HTML")
    # ══════════════════════════════════════
    # ADMIN — /setreward
    # ══════════════════════════════════════
    @bot.message_handler(commands=["setreward"])
    def cmd_set_reward(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        args = message.text.split()
        if len(args) < 2:
            cfg = _get_config()
            return bot.reply_to(message,
                f"Current: <code>${cfg.get('default_reward', 0.005):.5f}</code>/OTP\n"
                f"Usage: /setreward 0.005", parse_mode="HTML")
        try:
            amount = float(args[1])
            assert amount >= 0
        except Exception:
            return bot.reply_to(message, "❌ Invalid amount.")
        _save_config({"default_reward": _r(amount)})
        bot.reply_to(message, f"✅ Default reward → <code>${amount:.5f}</code>/OTP", parse_mode="HTML")

    # ══════════════════════════════════════
    # ADMIN — /setservicereward
    # ══════════════════════════════════════
    @bot.message_handler(commands=["setservicereward"])
    def cmd_set_service_reward(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        args = message.text.split()
        if len(args) < 3:
            return bot.reply_to(message, "Usage: /setservicereward WA 0.01")
        code = args[1].upper()
        try:
            amount = float(args[2])
        except Exception:
            return bot.reply_to(message, "❌ Invalid amount.")
        cfg = _get_config()
        ov  = cfg.get("service_overrides", {})
        ov.setdefault(code, {"enabled": True})["reward"] = _r(amount)
        _save_config({"service_overrides": ov})
        bot.reply_to(message, f"✅ <b>{code}</b> reward → <code>${amount:.5f}</code>", parse_mode="HTML")

    # ══════════════════════════════════════
    # ADMIN — /offreward /onreward
    # ══════════════════════════════════════
    @bot.message_handler(commands=["offreward"])
    def cmd_off_reward(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        args = message.text.split()
        if len(args) < 2:
            return bot.reply_to(message, "Usage: /offreward WA")
        code = args[1].upper()
        cfg  = _get_config()
        ov   = cfg.get("service_overrides", {})
        ov.setdefault(code, {})["enabled"] = False
        _save_config({"service_overrides": ov})
        bot.reply_to(message,
            f"🔴 <b>{code}</b> rewards OFF\n<i>OTPs delivered, no reward.</i>",
            parse_mode="HTML")

    @bot.message_handler(commands=["onreward"])
    def cmd_on_reward(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        args = message.text.split()
        if len(args) < 2:
            return bot.reply_to(message, "Usage: /onreward WA")
        code = args[1].upper()
        cfg  = _get_config()
        ov   = cfg.get("service_overrides", {})
        ov.setdefault(code, {})["enabled"] = True
        _save_config({"service_overrides": ov})
        bot.reply_to(message, f"🟢 <b>{code}</b> rewards ON", parse_mode="HTML")

    # ══════════════════════════════════════
    # ADMIN — /disablereward /enablereward
    # ══════════════════════════════════════
    @bot.message_handler(commands=["disablereward"])
    def cmd_disable_reward(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        _save_config({"rewards_enabled": False})
        bot.reply_to(message, "🔴 All rewards <b>DISABLED</b>", parse_mode="HTML")

    @bot.message_handler(commands=["enablereward"])
    def cmd_enable_reward(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        _save_config({"rewards_enabled": True})
        bot.reply_to(message, "🟢 All rewards <b>ENABLED</b>", parse_mode="HTML")

    # ══════════════════════════════════════
    # ADMIN — /setminwithdraw
    # ══════════════════════════════════════
    @bot.message_handler(commands=["setminwithdraw"])
    def cmd_min_withdraw(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        args = message.text.split()
        if len(args) < 2:
            cfg = _get_config()
            return bot.reply_to(message,
                f"Current min: <code>${cfg.get('min_withdrawal', 0.05):.3f}</code>\n"
                f"Usage: /setminwithdraw &lt;amount&gt;", parse_mode="HTML")
        try:
            amount = float(args[1])
            assert amount > 0
        except Exception:
            return bot.reply_to(message, "❌ Invalid amount.")
        _save_config({"min_withdrawal": _r(amount)})
        bot.reply_to(message, f"✅ Min withdrawal → <code>${amount:.3f}</code>", parse_mode="HTML")

    # ══════════════════════════════════════
    # ADMIN — /testreward
    # ══════════════════════════════════════
    @bot.message_handler(commands=["testreward"])
    def cmd_test_reward(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        args = message.text.split()
        if len(args) < 2:
            return bot.reply_to(message,
                "Usage: /testreward &lt;user_id&gt;\n<i>Credits 1 test reward to user.</i>",
                parse_mode="HTML")
        try:
            target = int(args[1])
        except ValueError:
            return bot.reply_to(message, "❌ Invalid user ID.")
        cfg    = _get_config()
        amount = cfg.get("default_reward", 0.005)
        ensure_user(target)
        add_balance(target, amount, reason="test_reward", service="TEST")
        user    = get_user(target)
        new_bal = user.get("balance", 0.0) if user else amount
        bot.reply_to(message,
            f"✅ <b>Test Reward Sent!</b>\n"
            f"👤 <code>{target}</code>\n"
            f"💵 +<code>${amount:.5f}</code>\n"
            f"💰 New Balance: <code>${new_bal:.5f}</code>",
            parse_mode="HTML")
        try:
            bot.send_message(target,
                f"💰 <b>Test Reward!</b>\n"
                f"✅ <b>+${amount:.5f}</b> added by admin\n"
                f"💵 Balance: <code>${new_bal:.5f}</code>",
                parse_mode="HTML")
        except Exception:
            pass

    # ══════════════════════════════════════
    # ADMIN — /pendingwithdrawals
    # ══════════════════════════════════════
    @bot.message_handler(commands=["pendingwithdrawals"])
    def cmd_pending(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        pending = get_pending_withdrawals()
        if not pending:
            return bot.reply_to(message, "✅ No pending withdrawals.")
        for doc in pending[:10]:
            created = doc.get("created_at", "")
            if hasattr(created, "strftime"):
                created = created.strftime("%Y-%m-%d %H:%M")
            try:
                u     = get_user(doc["chat_id"])
                uname = f"@{u['username']}" if u and u.get("username") else str(doc["chat_id"])
            except Exception:
                uname = str(doc["chat_id"])
            m = types.InlineKeyboardMarkup()
            m.row(
                types.InlineKeyboardButton("✅ Approve", callback_data=f"wapprove_{doc['_id']}"),
                types.InlineKeyboardButton("❌ Reject",  callback_data=f"wreject_{doc['_id']}")
            )
            bot.send_message(ADMIN_ID,
                f"💸 <b>Pending Withdrawal</b>\n"
                f"👤 {html.escape(str(uname))} (<code>{doc['chat_id']}</code>)\n"
                f"💵 <code>${doc['amount']:.5f}</code>\n"
                f"🔗 <code>{html.escape(doc['wallet'])}</code>\n"
                f"🕐 {created}\n🆔 <code>{doc['_id']}</code>",
                reply_markup=m, parse_mode="HTML")
        if len(pending) > 10:
            bot.send_message(ADMIN_ID, f"<i>...and {len(pending)-10} more</i>", parse_mode="HTML")

    # ══════════════════════════════════════
    # ADMIN — /approvewithdraw /rejectwithdraw
    # ══════════════════════════════════════
    @bot.message_handler(commands=["approvewithdraw"])
    def cmd_approve(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        args = message.text.split()
        if len(args) < 2:
            return bot.reply_to(message, "Usage: /approvewithdraw <id>")
        doc = resolve_withdrawal(args[1].strip(), approve=True)
        if not doc:
            return bot.reply_to(message, "❌ Not found or already resolved.")
        bot.reply_to(message,
            f"✅ Approved ${doc['amount']:.5f} → <code>{html.escape(doc['wallet'])}</code>",
            parse_mode="HTML")
        try:
            bot.send_message(doc["chat_id"],
                f"✅ <b>Withdrawal Approved!</b>\n"
                f"💵 ${doc['amount']:.5f} sent to\n<code>{html.escape(doc['wallet'])}</code>",
                parse_mode="HTML")
        except Exception:
            pass

    @bot.message_handler(commands=["rejectwithdraw"])
    def cmd_reject(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        args = message.text.split(maxsplit=2)
        if len(args) < 2:
            return bot.reply_to(message, "Usage: /rejectwithdraw <id> [reason]")
        note = args[2] if len(args) > 2 else ""
        doc  = resolve_withdrawal(args[1].strip(), approve=False, admin_note=note)
        if not doc:
            return bot.reply_to(message, "❌ Not found or already resolved.")
        bot.reply_to(message, f"❌ Rejected — ${doc['amount']:.5f} refunded to user.")
        try:
            bot.send_message(doc["chat_id"],
                f"❌ <b>Withdrawal Rejected</b>\n"
                f"💵 ${doc['amount']:.5f} refunded.\n"
                f"Reason: {html.escape(note or 'No reason given')}",
                parse_mode="HTML")
        except Exception:
            pass

    # ══════════════════════════════════════
    # ADMIN — /exportusers /importusers
    # ══════════════════════════════════════
    @bot.message_handler(commands=["exportusers"])
    def cmd_export_users(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        users = list(col_users.find({}, {"_id": 0}))
        if not users:
            return bot.reply_to(message, "❌ No users found.")
        lines = ["chat_id\tusername\tbalance\ttotal_earned\twallet\tcreated_at"]
        for u in users:
            created = u.get("created_at", "")
            if hasattr(created, "strftime"):
                created = created.strftime("%Y-%m-%d %H:%M")
            lines.append(
                f"{u.get('chat_id','')}\t{u.get('username','')}\t"
                f"{u.get('balance',0.0):.5f}\t{u.get('total_earned',0.0):.5f}\t"
                f"{u.get('wallet','')}\t{created}"
            )
        content = "\n".join(lines).encode("utf-8")
        bio      = io.BytesIO(content)
        bio.name = f"users_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
        bot.send_document(message.chat.id, bio,
            caption=f"👥 {len(users)} users exported")

    @bot.message_handler(commands=["importusers"])
    def cmd_import_users(message):
        if message.from_user.id != ADMIN_ID:
            return bot.reply_to(message, "❌ Not authorized.")
        bot.reply_to(message,
            "📥 Send the exported .txt file now.\n"
            "<i>Existing users will NOT be overwritten.</i>", parse_mode="HTML")
        bot.register_next_step_handler(message, _import_file_step)

    # ══════════════════════════════════════
    # STEP HANDLERS (defined here so they
    # close over `bot` properly)
    # ══════════════════════════════════════

    def _wallet_step_cmd(message, chat_id):
        """Step handler for /wallet command."""
        addr = (message.text or "").strip()
        if not addr.startswith("0x") or len(addr) != 42:
            bot.reply_to(message,
                "❌ Invalid. Must start with <code>0x</code>, 42 chars.\n"
                "Send /wallet to try again.",
                parse_mode="HTML")
            return
        set_wallet(chat_id, addr)
        m = types.InlineKeyboardMarkup()
        m.row(
            types.InlineKeyboardButton("💸 Withdraw Now", callback_data="menu_withdraw"),
            types.InlineKeyboardButton("💰 Balance",      callback_data="show_balance")
        )
        bot.reply_to(message,
            f"✅ <b>Wallet Saved!</b>\n<code>{html.escape(addr)}</code>",
            parse_mode="HTML", reply_markup=m)

    def _wallet_step_inline(message, chat_id):
        """Step handler for menu_wallet callback."""
        addr = (message.text or "").strip()
        if not addr.startswith("0x") or len(addr) != 42:
            bot.reply_to(message,
                "❌ Invalid. Must start with <code>0x</code>, 42 chars.\n"
                "Tap 🔗 Wallet again to retry.",
                parse_mode="HTML")
            return
        set_wallet(chat_id, addr)
        m = types.InlineKeyboardMarkup()
        m.row(
            types.InlineKeyboardButton("💸 Withdraw Now", callback_data="menu_withdraw"),
            types.InlineKeyboardButton("💰 Balance",      callback_data="show_balance")
        )
        bot.reply_to(message,
            f"✅ <b>Wallet Saved!</b>\n<code>{html.escape(addr)}</code>",
            parse_mode="HTML", reply_markup=m)

    def _import_file_step(message):
        if message.from_user.id != ADMIN_ID:
            return
        if not message.document:
            return bot.reply_to(message, "❌ Please send a .txt file.")
        try:
            fi      = bot.get_file(message.document.file_id)
            content = bot.download_file(fi.file_path).decode("utf-8")
            lines   = content.strip().splitlines()
            if not lines or not lines[0].startswith("chat_id"):
                return bot.reply_to(message, "❌ Invalid file format.")
            imported = 0
            for line in lines[1:]:
                parts = line.split("\t")
                if len(parts) < 2:
                    continue
                try:
                    cid  = int(parts[0])
                    unam = parts[1] or None
                    bal  = float(parts[2]) if len(parts) > 2 else 0.0
                    tot  = float(parts[3]) if len(parts) > 3 else 0.0
                    wal  = parts[4] if len(parts) > 4 and parts[4] else None
                    col_users.update_one(
                        {"chat_id": cid},
                        {"$setOnInsert": {
                            "chat_id": cid, "username": unam,
                            "balance": bal, "total_earned": tot, "wallet": wal,
                            "created_at":  datetime.now(timezone.utc),
                            "last_active": datetime.now(timezone.utc)
                        }},
                        upsert=True
                    )
                    imported += 1
                except Exception as le:
                    logger.warning(f"Import skip: {le}")
            bot.reply_to(message, f"✅ Imported/verified {imported} users.")
        except Exception as e:
            bot.reply_to(message, f"❌ Import failed: {e}")

    # ══════════════════════════════════════
    # INLINE CALLBACKS
    # ══════════════════════════════════════

    @bot.callback_query_handler(func=lambda c: c.data == "show_balance")
    def cb_show_balance(call):
        bot.answer_callback_query(call.id)
        # FIX: always use from_user.id for private actions, not chat.id
        chat_id = call.from_user.id
        try:
            ensure_user(chat_id, call.from_user.username)
            text = _balance_text(chat_id)
            bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=_wallet_menu_markup())
        except Exception as e:
            bot.send_message(chat_id, f"❌ Error: {e}")

    @bot.callback_query_handler(func=lambda c: c.data == "menu_wallet")
    def cb_menu_wallet(call):
        bot.answer_callback_query(call.id)
        # FIX: use from_user.id so it works in group chats too
        chat_id = call.from_user.id
        try:
            ensure_user(chat_id, call.from_user.username)
            user   = get_user(chat_id)
            wallet = user.get("wallet") if user else None
            cur    = f"\n\n🔗 Current: <code>{html.escape(wallet)}</code>" if wallet else ""
            msg    = bot.send_message(chat_id,
                f"🔗 <b>Set Polygon Wallet</b>{cur}\n\nSend address (<code>0x...</code>, 42 chars):",
                parse_mode="HTML")
            bot.register_next_step_handler(msg, _wallet_step_inline, chat_id)
        except Exception as e:
            bot.send_message(chat_id, f"❌ Error: {e}")

    @bot.callback_query_handler(func=lambda c: c.data == "menu_withdraw")
    def cb_menu_withdraw(call):
        bot.answer_callback_query(call.id)
        # FIX: use from_user.id consistently
        chat_id = call.from_user.id
        ensure_user(chat_id, call.from_user.username)
        _show_withdraw_screen(bot, ADMIN_ID, chat_id,
            send_fn=lambda txt, **kw: bot.send_message(chat_id, txt, **kw))

    @bot.callback_query_handler(func=lambda c: c.data.startswith("wconfirm_"))
    def cb_wconfirm(call):
        # FIX: extract target chat_id from callback data (set when button was created)
        # This is the user who requested withdrawal, not necessarily call.from_user.id
        try:
            target_chat_id = int(call.data.split("_", 1)[1])
        except (ValueError, IndexError):
            bot.answer_callback_query(call.id, "❌ Invalid request", show_alert=True)
            return

        # Security: only the owner can confirm their own withdrawal
        if call.from_user.id != target_chat_id:
            bot.answer_callback_query(call.id, "❌ Not your withdrawal", show_alert=True)
            return

        bot.answer_callback_query(call.id, "⏳ Processing...")
        chat_id = target_chat_id
        try:
            user   = get_user(chat_id)
            cfg    = _get_config()
            min_w  = cfg.get("min_withdrawal", 0.05)
            bal    = user.get("balance", 0.0) if user else 0.0
            wallet = user.get("wallet")       if user else None

            if not wallet or bal < min_w:
                return bot.send_message(chat_id, "❌ Conditions no longer met. Try again.")

            # FIX: safe pending check
            try:
                existing = col_withdrawals.find_one({"chat_id": chat_id, "status": "pending"})
            except Exception:
                existing = None

            if existing:
                return bot.send_message(chat_id, "⏳ Already have a pending request.")

            deduct_balance(chat_id, bal)
            wid      = create_withdrawal(chat_id, bal, wallet)
            username = call.from_user.username or str(chat_id)

            # FIX: safe edit with fallback send
            try:
                bot.edit_message_text(
                    f"✅ <b>Withdrawal Requested!</b>\n"
                    f"💵 <code>${bal:.5f}</code>\n"
                    f"🔗 <code>{html.escape(wallet)}</code>\n"
                    f"🆔 <code>{wid}</code>\n⏳ Awaiting admin approval.",
                    call.message.chat.id, call.message.message_id, parse_mode="HTML")
            except Exception:
                bot.send_message(chat_id, "✅ Withdrawal requested! Awaiting admin approval.")

            # Notify admin
            m = types.InlineKeyboardMarkup()
            m.row(
                types.InlineKeyboardButton("✅ Approve", callback_data=f"wapprove_{wid}"),
                types.InlineKeyboardButton("❌ Reject",  callback_data=f"wreject_{wid}")
            )
            bot.send_message(ADMIN_ID,
                f"💸 <b>New Withdrawal</b>\n"
                f"👤 @{html.escape(str(username))} (<code>{chat_id}</code>)\n"
                f"💵 <code>${bal:.5f}</code>\n"
                f"🔗 <code>{html.escape(wallet)}</code>\n"
                f"🆔 <code>{wid}</code>",
                reply_markup=m, parse_mode="HTML")

        except Exception as e:
            logger.error(f"[wconfirm] chat={chat_id}: {e}", exc_info=True)
            bot.send_message(chat_id, f"❌ Error: {e}")

    @bot.callback_query_handler(
        func=lambda c: c.data.startswith("wapprove_") or c.data.startswith("wreject_"))
    def cb_withdrawal_resolve(call):
        if call.from_user.id != ADMIN_ID:
            return bot.answer_callback_query(call.id, "❌ Not authorized")
        approve = call.data.startswith("wapprove_")
        wid     = call.data[9:] if approve else call.data[8:]
        doc     = resolve_withdrawal(wid, approve)
        if not doc:
            return bot.answer_callback_query(call.id, "❌ Not found / already resolved", show_alert=True)
        icon  = "✅" if approve else "❌"
        label = "Approved" if approve else "Rejected"
        bot.answer_callback_query(call.id, f"{icon} {label}!")
        try:
            bot.edit_message_text(
                f"{icon} <b>{label}</b>\n"
                f"💵 ${doc['amount']:.5f} → <code>{html.escape(doc['wallet'])}</code>",
                call.message.chat.id, call.message.message_id, parse_mode="HTML")
        except Exception:
            pass
        try:
            if approve:
                bot.send_message(doc["chat_id"],
                    f"✅ <b>Withdrawal Approved!</b>\n"
                    f"💵 ${doc['amount']:.5f} sent to\n<code>{html.escape(doc['wallet'])}</code>",
                    parse_mode="HTML")
            else:
                bot.send_message(doc["chat_id"],
                    f"❌ <b>Withdrawal Rejected</b>\n"
                    f"💵 ${doc['amount']:.5f} refunded.\n"
                    f"Reason: {html.escape(doc.get('admin_note', 'No reason given'))}",
                    parse_mode="HTML")
        except Exception:
            pass

    @bot.callback_query_handler(func=lambda c: c.data == "menu_earnings")
    def cb_menu_earnings(call):
        bot.answer_callback_query(call.id)
        # FIX: use from_user.id
        chat_id = call.from_user.id
        try:
            ensure_user(chat_id, call.from_user.username)
            txns = list(col_transactions.find(
                {"chat_id": chat_id}).sort("created_at", -1).limit(10))
            if not txns:
                return bot.send_message(chat_id,
                    "📊 <b>Earnings</b>\n\n<i>No transactions yet! Receive OTPs to earn.</i>",
                    parse_mode="HTML", reply_markup=_wallet_menu_markup())
            text = "📊 <b>Recent Earnings (last 10)</b>\n━━━━━━━━━━━━━━━\n"
            for t in txns:
                created = t.get("created_at", "")
                if hasattr(created, "strftime"):
                    created = created.strftime("%d %b %H:%M")
                svc = t.get("service", "")
                text += f"💵 <code>+${t.get('amount',0):.5f}</code> [{svc}] — {created}\n"
            user  = get_user(chat_id)
            total = user.get("total_earned", 0.0) if user else 0.0
            text += f"━━━━━━━━━━━━━━━\n📈 Total: <code>${total:.5f}</code>"
            bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=_wallet_menu_markup())
        except Exception as e:
            bot.send_message(chat_id, f"❌ Error loading earnings: {e}")

    @bot.callback_query_handler(func=lambda c: c.data == "menu_back")
    def cb_menu_back(call):
        bot.answer_callback_query(call.id)
        bot.send_message(call.from_user.id,
            "👇 Tap <b>📞 Number</b> to get numbers.", parse_mode="HTML")

    logger.info(f"[rewards] ✅ All handlers registered (ADMIN_ID={ADMIN_ID})")


# Backwards-compat alias
register_reward_commands = register_handlers
