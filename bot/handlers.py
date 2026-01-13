# bot/handlers.py
from datetime import datetime
from decimal import Decimal
import random

import time

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config import (
    PLANS,
    BOT_USERNAME,
    ADMIN_USER_IDS,
    PAID_CHANNEL_ID,
    HIGHLIGHT_CHANNEL_ID,
    FREE_CHANNEL_ID_1,
    FREE_CHANNEL_ID_2,
    SUPPORT_ENABLE,
    SUPPORT_GROUP_ID,
    PAYMENT_MODE,
    RECEIVE_ADDRESS,
    PAYMENT_SUFFIX_ENABLE,
    PAYMENT_SUFFIX_MIN,
    PAYMENT_SUFFIX_MAX,
)
from core.models import (
    get_user,
    upsert_user_basic,
    allocate_address,
    create_pending_order,
    reset_user_address,
    support_store_mapping,
    support_get_user_id,
)
from core.utils import b58decode, b58encode
from bot.i18n import t, normalize_lang
from core.models import bind_inviter
import logging
logger = logging.getLogger(__name__)

_support_pending: dict[tuple[int, int], tuple[int, float]] = {}

def _is_admin(user_id: int) -> bool:
    return bool(ADMIN_USER_IDS) and user_id in ADMIN_USER_IDS

def _main_menu_kb(lang: str) -> InlineKeyboardMarkup:
    if lang == "zh":
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("⚡️ 月费 1.99", callback_data="pay_monthly"),
                    InlineKeyboardButton("⚡️ 季度 3.99", callback_data="pay_quarter"),
                    InlineKeyboardButton("⚡️ 年费 15.99", callback_data="pay_yearly"),
                ],
                [InlineKeyboardButton("📅 我的会员", callback_data="menu_status")],
                [InlineKeyboardButton("🎁 邀请赚钱", callback_data="menu_invite")],
            ]
        )
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Monthly 1.99", callback_data="pay_monthly"),
                InlineKeyboardButton("Quarter 3.99", callback_data="pay_quarter"),
                InlineKeyboardButton("Yearly 15.99", callback_data="pay_yearly"),
            ],
            [InlineKeyboardButton("📅 My Membership", callback_data="menu_status")],
            [InlineKeyboardButton("🎁 Invite", callback_data="menu_invite")],
        ]
    )


def _plans_kb(lang: str) -> InlineKeyboardMarkup:
    if lang == "zh":
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("⚡️ 月费 1.99", callback_data="pay_monthly")],
                [InlineKeyboardButton("⚡️ 季度 3.99", callback_data="pay_quarter")],
                [InlineKeyboardButton("⚡️ 年费 15.99", callback_data="pay_yearly")],
                [InlineKeyboardButton("⬅️ 返回", callback_data="menu_home")],
            ]
        )
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Monthly 1.99", callback_data="pay_monthly")],
            [InlineKeyboardButton("Quarter 3.99", callback_data="pay_quarter")],
            [InlineKeyboardButton("Yearly 15.99", callback_data="pay_yearly")],
            [InlineKeyboardButton("⬅️ Back", callback_data="menu_home")],
        ]
    )


def _pick_payment_amount(base: Decimal) -> Decimal:
    if PAYMENT_MODE != "single_address" or not RECEIVE_ADDRESS:
        return base.quantize(Decimal("0.000001"))

    enable = PAYMENT_SUFFIX_ENABLE

    if not enable:
        return base.quantize(Decimal("0.000001"))

    lo = Decimal(str(PAYMENT_SUFFIX_MIN))
    hi = Decimal(str(PAYMENT_SUFFIX_MAX))
    if hi <= lo:
        return base.quantize(Decimal("0.000001"))

    span_steps = int(((hi - lo) / Decimal("0.0001")).to_integral_value(rounding="ROUND_FLOOR"))
    if span_steps <= 0:
        return (base + lo).quantize(Decimal("0.000001"))

    r = random.randint(0, span_steps)
    suffix = lo + (Decimal(r) * Decimal("0.0001"))
    return (base + suffix).quantize(Decimal("0.000001"))


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    telegram_id = user.id
    username = user.username or ""
    lang = normalize_lang(user.language_code or "en")

    # 解析邀请参数 /start ref_xxx
    logger.info("handers.py--->解析邀请参数 /start ref_xxx")
    args = context.args or []
    if args and args[0].startswith("ref_"):
        code = args[0][4:]
        try:
            inviter_id = b58decode(code)
            if inviter_id != telegram_id:
                bind_inviter(telegram_id, inviter_id)
        except Exception:
            pass

    # 更新基础信息
    logger.info("handers.py--->更新基础信息")
    upsert_user_basic(telegram_id, username, lang)

    u = get_user(telegram_id)
    if u and u.get("wallet_addr"):
        addr = u["wallet_addr"]
        paid_until = u.get("paid_until")
    else:
        if PAYMENT_MODE == "single_address" and RECEIVE_ADDRESS:
            addr = RECEIVE_ADDRESS
        else:
            try:
                addr = allocate_address(telegram_id)
            except Exception:
                addr = None
        paid_until = None
        u = get_user(telegram_id)
    logger.info("handers.py--->upsert_user_basic")
    parts = []
    parts.append(t(lang, "welcome_title"))
    parts.append(t(lang, "welcome_body"))

    if paid_until and paid_until > datetime.utcnow():
        parts.append(t(lang, "current_status",
                       until=paid_until.strftime("%Y-%m-%d %H:%M:%S")))
    else:
        parts.append(t(lang, "no_membership"))

    parts.append(t(lang, "pricing_block"))
    parts.append(t(lang, "contact_hint", bot=BOT_USERNAME))

    await update.message.reply_text(
        "\n\n".join(parts),
        parse_mode="HTML",
        reply_markup=_main_menu_kb(lang),
    )
    

async def plans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = normalize_lang(update.effective_user.language_code or "en")
    if lang == "zh":
        await update.message.reply_text(t(lang, "pricing_block"), reply_markup=_plans_kb(lang))
        return
    lines = [t(lang, "plans_command_title")]
    for p in PLANS:
        lines.append(t(lang, "plan_line", name=p["name"], price=p["price"], days=p["days"]))
    await update.message.reply_text("\n".join(lines), parse_mode="HTML", reply_markup=_plans_kb(lang))

async def my_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        return

    username = f"@{user.username}" if user.username else ""
    name = (user.full_name or "").strip()

    text = (
        f"用户ID：<code>{user.id}</code>\n"
        f"群组/聊天ID：<code>{chat.id}</code>\n"
        f"聊天类型：<code>{chat.type}</code>\n"
        f"用户名：<code>{username}</code>\n"
        f"昵称：<code>{name}</code>"
    )
    await msg.reply_text(text, parse_mode="HTML")

async def chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    if not msg or not chat:
        return
    title = getattr(chat, "title", "") or ""
    text = (
        f"聊天/群组/频道ID：<code>{chat.id}</code>\n"
        f"类型：<code>{chat.type}</code>\n"
        f"标题：<code>{title}</code>"
    )
    await msg.reply_text(text, parse_mode="HTML")

async def support_user_inbox(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not SUPPORT_ENABLE or not SUPPORT_GROUP_ID:
        return
    msg = update.message
    user = update.effective_user
    if not msg or not user:
        return
    if msg.chat.type != "private":
        return
    if user.is_bot:
        return

    username = f"@{user.username}" if user.username else ""
    name = (user.full_name or "").strip()
    u = get_user(int(user.id)) or {}
    paid_until = u.get("paid_until")
    now = datetime.utcnow()
    if paid_until and paid_until > now:
        member = f"会员：是 | 到期：{paid_until.strftime('%Y-%m-%d %H:%M:%S UTC')}"
    elif paid_until:
        member = f"会员：已到期 | 到期：{paid_until.strftime('%Y-%m-%d %H:%M:%S UTC')}"
    else:
        member = "会员：否"

    body = msg.text or (msg.caption or "[媒体]")
    text = f"用户：<code>{user.id}</code> {username} {name} | {member}\n【消息】{body}".strip()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("回复", callback_data="support_reply:pending")]])
    ticket = await context.bot.send_message(chat_id=SUPPORT_GROUP_ID, text=text, parse_mode="HTML", reply_markup=kb)
    support_store_mapping(SUPPORT_GROUP_ID, ticket.message_id, int(user.id), int(msg.message_id))
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=SUPPORT_GROUP_ID,
            message_id=ticket.message_id,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("回复", callback_data=f"support_reply:{ticket.message_id}")]]
            ),
        )
    except Exception:
        pass

    if not msg.text:
        try:
            await context.bot.copy_message(
                chat_id=SUPPORT_GROUP_ID,
                from_chat_id=msg.chat_id,
                message_id=msg.message_id,
                reply_to_message_id=ticket.message_id,
            )
        except Exception:
            return

async def support_group_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not SUPPORT_ENABLE or not SUPPORT_GROUP_ID:
        return
    msg = update.message
    if not msg or not msg.reply_to_message:
        return
    if msg.chat_id != SUPPORT_GROUP_ID:
        return
    if msg.from_user and msg.from_user.is_bot:
        return

    reply_to_id = msg.reply_to_message.message_id
    user_id = support_get_user_id(SUPPORT_GROUP_ID, reply_to_id)
    if not user_id:
        return

    if msg.text:
        await context.bot.send_message(chat_id=user_id, text=msg.text)
        return

    try:
        await context.bot.copy_message(
            chat_id=user_id,
            from_chat_id=msg.chat_id,
            message_id=msg.message_id,
        )
    except Exception:
        return

async def support_reply_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not SUPPORT_ENABLE or not SUPPORT_GROUP_ID:
        return
    query = update.callback_query
    if not query or not query.data:
        return
    if not query.from_user or query.from_user.is_bot:
        return
    if query.message is None:
        return
    if query.message.chat_id != SUPPORT_GROUP_ID:
        return
    parts = query.data.split(":", 1)
    if len(parts) != 2:
        return
    try:
        ticket_id = int(parts[1])
    except Exception:
        return
    user_id = support_get_user_id(SUPPORT_GROUP_ID, ticket_id)
    if not user_id:
        await query.answer("未找到对应用户", show_alert=True)
        return
    _support_pending[(SUPPORT_GROUP_ID, int(query.from_user.id))] = (int(user_id), time.time() + 600)
    await query.answer("请在群里发送你的下一条消息（将转发给用户）", show_alert=True)

async def support_group_pending_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not SUPPORT_ENABLE or not SUPPORT_GROUP_ID:
        return
    msg = update.message
    if not msg:
        return
    if msg.chat_id != SUPPORT_GROUP_ID:
        return
    if msg.from_user is None or msg.from_user.is_bot:
        return
    key = (SUPPORT_GROUP_ID, int(msg.from_user.id))
    state = _support_pending.get(key)
    if not state:
        return
    user_id, exp = state
    if time.time() > exp:
        _support_pending.pop(key, None)
        return
    _support_pending.pop(key, None)

    if msg.text:
        try:
            await context.bot.send_message(chat_id=user_id, text=msg.text)
        except Exception:
            return
        return
    try:
        await context.bot.copy_message(
            chat_id=user_id,
            from_chat_id=msg.chat_id,
            message_id=msg.message_id,
        )
    except Exception:
        return

async def reset_addr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return

    target_id = user.id
    args = context.args or []
    if args:
        try:
            target_id = int(args[0])
        except Exception:
            target_id = user.id

    old_addr = reset_user_address(target_id)

    if PAYMENT_MODE == "single_address" and RECEIVE_ADDRESS:
        new_addr = RECEIVE_ADDRESS
    else:
        try:
            new_addr = allocate_address(target_id)
        except Exception:
            new_addr = None

    if update.message:
        if old_addr:
            await update.message.reply_text(
                f"已重置地址。\n旧地址：<code>{old_addr}</code>\n新地址：<code>{new_addr or '（暂不可用）'}</code>",
                parse_mode="HTML",
            )
        else:
            await update.message.reply_text(
                f"当前无已绑定地址。\n新地址：<code>{new_addr or '（暂不可用）'}</code>",
                parse_mode="HTML",
            )


async def diag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return

    bot = context.bot
    me = await bot.get_me()

    checks = [
        ("PAID_CHANNEL_ID", PAID_CHANNEL_ID),
        ("HIGHLIGHT_CHANNEL_ID", HIGHLIGHT_CHANNEL_ID),
        ("FREE_CHANNEL_ID_1", FREE_CHANNEL_ID_1),
        ("FREE_CHANNEL_ID_2", FREE_CHANNEL_ID_2),
    ]

    lines = [f"bot_id={me.id} username=@{me.username}"]
    for name, chat_id in checks:
        try:
            chat = await bot.get_chat(chat_id)
            try:
                member = await bot.get_chat_member(chat_id, me.id)
                status = getattr(member, "status", None) or "unknown"
            except Exception as e:
                status = f"get_chat_member failed: {type(e).__name__}: {e}"
            lines.append(f"{name}={chat_id} type={chat.type} title={getattr(chat, 'title', '')} status={status}")
        except Exception as e:
            lines.append(f"{name}={chat_id} ERROR: {type(e).__name__}: {e}")

    if update.message:
        await update.message.reply_text("\n".join(lines))


async def on_menu_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()

    user = update.effective_user
    lang = normalize_lang((user and user.language_code) or "en")
    telegram_id = user.id if user else None

    data = query.data or ""
    if data == "menu_home":
        await query.edit_message_reply_markup(reply_markup=_main_menu_kb(lang))
        return

    if data == "menu_plans":
        text = t(lang, "pricing_block") if lang == "zh" else t(lang, "plans_command_title")
        await query.edit_message_text(text=text, reply_markup=_plans_kb(lang))
        return

    if data == "menu_status" and telegram_id:
        u = get_user(telegram_id)
        paid_until = u.get("paid_until") if u else None
        if paid_until and paid_until > datetime.utcnow():
            msg = t(lang, "current_status", until=paid_until.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            msg = t(lang, "no_membership")
        await query.edit_message_text(text=msg, reply_markup=_main_menu_kb(lang))
        return

    if data == "menu_invite":
        if lang == "zh":
            msg = "发送 /invite 获取你的专属邀请链接与海报。"
        else:
            msg = "Send /invite to get your personal invite link."
        await query.edit_message_text(text=msg, reply_markup=_main_menu_kb(lang))
        return

    if data.startswith("pay_") and telegram_id:
        u = get_user(telegram_id)
        plan_map = {
            "pay_monthly": ("monthly", Decimal("9.99")),
            "pay_quarter": ("quarter", Decimal("19.99")),
            "pay_yearly": ("yearly", Decimal("79.99")),
        }
        plan_code, base_amount = plan_map.get(data, ("monthly", Decimal("9.99")))

        if PAYMENT_MODE == "single_address" and RECEIVE_ADDRESS:
            addr = RECEIVE_ADDRESS
        else:
            addr = (u and u.get("wallet_addr"))
            if not addr:
                try:
                    addr = allocate_address(telegram_id)
                except Exception:
                    addr = None

        amount = _pick_payment_amount(base_amount)
        if addr:
            create_pending_order(telegram_id, addr, amount, plan_code)

        if lang == "zh":
            msg = (
                "请使用 TRON（USDT-TRC20）转账指定金额：\n\n"
                f"金额：{amount} USDT（请按金额精确转账）\n"
                f"地址：<code>{addr or '（暂不可用）'}</code>\n\n"
                "系统每分钟自动检测到账；到账后会私信你“会员频道邀请链接”。\n"
                "如需续费，继续向同一地址转账即可。"
            )
        else:
            msg = (
                "Send USDT-TRC20 with the exact amount:\n\n"
                f"Amount: {amount} USDT\n"
                f"Address: <code>{addr or '(unavailable)'}</code>\n\n"
                "The system checks every minute and will DM you an invite link after payment is detected."
            )
        await query.edit_message_text(text=msg, reply_markup=_plans_kb(lang), parse_mode="HTML")
        return


async def invite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    telegram_id = tg_user.id
    lang = normalize_lang(tg_user.language_code or "en")

    u = get_user(telegram_id)
    username = (u and u.get("username")) or tg_user.username or str(telegram_id)
    invite_count = (u and u.get("invite_count")) or 0
    invite_days = (u and u.get("invite_reward_days")) or 0

    code = b58encode(telegram_id)
    link = f"https://t.me/{BOT_USERNAME}?start=ref_{code}"

    intro = t(lang, "invite_panel_intro")
    stats = t(lang, "invite_panel_stats", count=invite_count, days=invite_days)
    link_block = t(lang, "invite_panel_link_block", link=link, code=code)
    copy_hint = t(lang, "invite_panel_copy_hint", link=link)

    msg = intro + "\n\n" + stats + "\n" + link_block + "\n" + copy_hint
    await update.message.reply_text(msg)
    try:
        from bot.invite_poster import generate_invite_poster

        poster_buf = generate_invite_poster(telegram_id, username, lang)
        await update.message.reply_photo(photo=poster_buf)
    except Exception:
        return
