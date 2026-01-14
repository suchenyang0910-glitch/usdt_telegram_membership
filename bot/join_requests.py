from datetime import datetime

from telegram import Update
from telegram.ext import ContextTypes

from config import JOIN_REQUEST_ENABLE, PAID_CHANNEL_ID
from core.models import admin_audit_log, get_user
from bot.i18n import normalize_lang, t
from bot.admin_report import send_admin_text


async def paid_channel_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not JOIN_REQUEST_ENABLE:
        return
    req = getattr(update, "chat_join_request", None)
    if not req:
        return
    chat = getattr(req, "chat", None)
    user = getattr(req, "from_user", None)
    if not chat or not user:
        return
    if int(getattr(chat, "id", 0) or 0) != int(PAID_CHANNEL_ID):
        return

    now = datetime.utcnow()
    telegram_id = int(getattr(user, "id", 0) or 0)
    u = None
    try:
        u = get_user(telegram_id)
    except Exception:
        u = None

    lang = normalize_lang((u or {}).get("language") or (getattr(user, "language_code", None) or "en"))
    paid_until = (u or {}).get("paid_until")
    is_whitelisted = int((u or {}).get("is_whitelisted") or 0) == 1
    is_blacklisted = int((u or {}).get("is_blacklisted") or 0) == 1 and not is_whitelisted
    active = bool(is_whitelisted or (paid_until and paid_until > now))

    if is_blacklisted:
        try:
            await context.bot.decline_chat_join_request(chat_id=PAID_CHANNEL_ID, user_id=telegram_id)
        except Exception:
            pass
        try:
            admin_audit_log("join_request", "decline_blacklisted", telegram_id, {"chat_id": int(PAID_CHANNEL_ID), "paid_until": paid_until})
        except Exception:
            pass
        try:
            await context.bot.send_message(chat_id=telegram_id, text="⛔ 你当前无法加入频道。如有误判请联系客服。")
        except Exception:
            pass
        return

    if active:
        try:
            try:
                await context.bot.unban_chat_member(chat_id=PAID_CHANNEL_ID, user_id=telegram_id, only_if_banned=True)
            except Exception:
                pass
            await context.bot.approve_chat_join_request(chat_id=PAID_CHANNEL_ID, user_id=telegram_id)
            try:
                admin_audit_log("join_request", "approve", telegram_id, {"chat_id": int(PAID_CHANNEL_ID), "paid_until": paid_until})
            except Exception:
                pass
            try:
                await context.bot.send_message(chat_id=telegram_id, text="✅ 已通过入群申请，欢迎加入。")
            except Exception:
                pass
        except Exception as e:
            try:
                await send_admin_text(
                    context.bot,
                    f"<b>Join Request 处理失败</b>\nuid=<code>{telegram_id}</code>\nerr=<code>{type(e).__name__}: {e}</code>",
                    parse_mode="HTML",
                )
            except Exception:
                pass
        return

    try:
        await context.bot.decline_chat_join_request(chat_id=PAID_CHANNEL_ID, user_id=telegram_id)
    except Exception:
        pass
    try:
        admin_audit_log("join_request", "decline", telegram_id, {"chat_id": int(PAID_CHANNEL_ID), "paid_until": paid_until})
    except Exception:
        pass
    try:
        await context.bot.send_message(chat_id=telegram_id, text=t(lang, "expired_notice"))
    except Exception:
        pass

