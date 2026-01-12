import html
import re
import time
import traceback
from datetime import datetime

from telegram import Update
from telegram.ext import ContextTypes

from config import ADMIN_USER_IDS, BOT_TOKEN

_last_sent: dict[str, float] = {}


def _redact(s: str) -> str:
    if not s:
        return s
    if BOT_TOKEN:
        s = s.replace(BOT_TOKEN, "<BOT_TOKEN_REDACTED>")
    s = re.sub(r"bot\d+:[A-Za-z0-9_-]{20,}", "bot<TOKEN_REDACTED>", s)
    return s


def _update_brief(update: Update | None) -> str:
    if not update:
        return "update=None"
    try:
        if update.effective_chat and update.effective_user:
            return f"chat_id={update.effective_chat.id} user_id={update.effective_user.id}"
        if update.effective_chat:
            return f"chat_id={update.effective_chat.id}"
        if update.effective_user:
            return f"user_id={update.effective_user.id}"
    except Exception:
        pass
    return update.to_dict().__class__.__name__


def _should_send(key: str, window_sec: int = 600) -> bool:
    now = time.time()
    last = _last_sent.get(key, 0)
    if now - last < window_sec:
        return False
    _last_sent[key] = now
    return True


async def application_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    if not ADMIN_USER_IDS:
        return

    err = getattr(context, "error", None)
    tb = "".join(traceback.format_exception(type(err), err, err.__traceback__)) if err else "unknown error"
    tb = _redact(tb)

    upd = update if isinstance(update, Update) else None
    header = f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n{_update_brief(upd)}\n{type(err).__name__ if err else 'Error'}: {err}"
    header = _redact(header)

    key = f"{type(err).__name__}:{str(err)[:200]}"
    if not _should_send(key):
        return

    text = f"<b>系统异常</b>\n<pre>{html.escape(header)}</pre>\n<pre>{html.escape(tb[-3500:])}</pre>"
    for admin_id in ADMIN_USER_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=text, parse_mode="HTML")
        except Exception:
            continue
