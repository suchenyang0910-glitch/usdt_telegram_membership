# bot/scheduler.py
from datetime import datetime, timedelta
from decimal import Decimal
import os

from telegram.ext import ContextTypes

from config import (
    PLANS,
    PAID_CHANNEL_ID,
    INVITE_REWARD,
    MIN_TX_AGE_SEC,
    PAYMENT_MODE,
    RECEIVE_ADDRESS,
    AMOUNT_EPS,
    LOG_RETENTION_DAYS,
    ADMIN_REPORT_HOURLY,
    USDT_ADDRESS_POOL,
    ADMIN_REPORT_TZ_OFFSET,
    ADMIN_REPORT_QUIET_START_HOUR,
    ADMIN_REPORT_QUIET_END_HOUR,
    HEALTH_ALERT_ENABLE,
    HEALTH_ALERT_DEPOSIT_STALE_MINUTES,
    HEALTH_ALERT_MIN_INTERVAL_MINUTES,
    JOIN_REQUEST_ENABLE,
    JOIN_REQUEST_LINK_EXPIRE_HOURS,
)
from core.models import (
    get_all_users,
    update_user_payment,
    get_unhandled_expired_users,
    mark_user_expired_handled,
    get_users_expiring_within_days,
    mark_user_reminded,
    get_user,
    get_inviter_id,
    add_invite_reward,
    insert_usdt_tx_if_new,
    set_usdt_tx_status,
    get_unassigned_usdt_txs,
    get_unassigned_usdt_txs_since,
    get_address_assigned_at,
    match_pending_order_by_amount,
    mark_order_success,
    get_success_orders_between,
)
from chain.tron_client import list_usdt_incoming, get_usdt_balance
from bot.payments import compute_new_paid_until
from bot.i18n import t, normalize_lang
from core.logging_setup import cleanup_old_logs
from bot.admin_report import notify_recharge_success, send_admin_text
import logging
logger = logging.getLogger(__name__)

_last_overnight_report_local_date = None
_last_health_alert_ts = None

async def cleanup_downloads_job(context: ContextTypes.DEFAULT_TYPE):
    roots = [
        os.path.join("tmp", "downloads"),
        os.path.join("tmp", "clips"),
        os.path.join("tmp", "userbot", "downloads"),
        os.path.join("tmp", "userbot", "clips"),
    ]
    removed = 0
    for root in roots:
        try:
            if not os.path.isdir(root):
                continue
            for name in os.listdir(root):
                fp = os.path.join(root, name)
                try:
                    if os.path.isfile(fp):
                        os.remove(fp)
                        removed += 1
                except Exception:
                    continue
        except Exception:
            continue
    if removed:
        logger.info("[tmp] cleanup_downloads_job removed=%s", removed)

async def cleanup_logs_job(context: ContextTypes.DEFAULT_TYPE):
    removed = cleanup_old_logs(LOG_RETENTION_DAYS)
    if removed:
        logger.info("[logs] 清理过期日志完成 removed=%s keep_days=%s", removed, LOG_RETENTION_DAYS)


async def hourly_admin_report_job(context: ContextTypes.DEFAULT_TYPE):
    if not ADMIN_REPORT_HOURLY:
        return

    bot = context.bot
    now = datetime.utcnow()
    local_now = now + timedelta(hours=int(ADMIN_REPORT_TZ_OFFSET))
    local_hour = int(local_now.hour)

    quiet_start = int(ADMIN_REPORT_QUIET_START_HOUR)
    quiet_end = int(ADMIN_REPORT_QUIET_END_HOUR)

    def _in_quiet(h: int) -> bool:
        if quiet_start == quiet_end:
            return False
        if quiet_start < quiet_end:
            return quiet_start <= h < quiet_end
        return (h >= quiet_start) or (h < quiet_end)

    global _last_overnight_report_local_date

    if _in_quiet(local_hour):
        return

    if local_hour == quiet_end:
        local_end = local_now.replace(hour=quiet_end, minute=0, second=0, microsecond=0)
        span_hours = (24 - quiet_start) + quiet_end if quiet_start > quiet_end else (quiet_end - quiet_start)
        local_start = local_end - timedelta(hours=span_hours)

        local_end_date = local_end.date()
        if _last_overnight_report_local_date == local_end_date:
            return
        _last_overnight_report_local_date = local_end_date

        start = local_start - timedelta(hours=int(ADMIN_REPORT_TZ_OFFSET))
        end = local_end - timedelta(hours=int(ADMIN_REPORT_TZ_OFFSET))
        title = "<b>夜间汇总（22:00-08:00）</b>"
        time_range = f"{local_start.strftime('%m-%d %H:%M')} - {local_end.strftime('%m-%d %H:%M')} 本地时间"
    else:
        start = now - timedelta(hours=1)
        end = now
        title = "<b>每小时充值汇报</b>"
        time_range = f"{(local_now - timedelta(hours=1)).strftime('%m-%d %H:%M')} - {local_now.strftime('%H:%M')} 本地时间"

    orders = get_success_orders_between(start, end)
    users = {int(o["telegram_id"]) for o in orders if o.get("telegram_id") is not None}
    total = sum((Decimal(str(o.get("amount") or 0)) for o in orders), Decimal("0"))

    base_addrs = []
    if PAYMENT_MODE == "single_address" and RECEIVE_ADDRESS:
        base_addrs = [RECEIVE_ADDRESS]
    else:
        base_addrs = USDT_ADDRESS_POOL
    addrs = list(dict.fromkeys([a for a in base_addrs if a]))
    balances = []
    for addr in addrs[:50]:
        bal = get_usdt_balance(addr)
        balances.append((addr, bal))

    lines = []
    lines.append(title)
    lines.append(f"时间：<code>{time_range}</code>")
    lines.append(f"充值人数：<code>{len(users)}</code>")
    lines.append(f"充值笔数：<code>{len(orders)}</code>")
    lines.append(f"充值总额：<code>{total}</code> USDT")
    lines.append("")
    lines.append("<b>地址余额（USDT）</b>")
    for addr, bal in balances:
        v = "N/A" if bal is None else str(bal)
        lines.append(f"<code>{addr}</code>  <code>{v}</code>")

    await send_admin_text(bot, "\n".join(lines), parse_mode="HTML")

async def check_deposits_job(context: ContextTypes.DEFAULT_TYPE):
    bot = context.bot
    confirm_before = datetime.utcnow() - timedelta(seconds=MIN_TX_AGE_SEC)

    if PAYMENT_MODE == "single_address" and RECEIVE_ADDRESS:
        addr = RECEIVE_ADDRESS
        for tx in list_usdt_incoming(addr):
            insert_usdt_tx_if_new(
                telegram_id=None,
                addr=addr,
                tx_id=tx["tx_id"],
                amount=tx["amount"],
                from_addr=tx.get("from"),
                block_time=(tx.get("block_time") and tx["block_time"].replace(tzinfo=None)),
            )

        eps = Decimal(str(AMOUNT_EPS))
        for tx in get_unassigned_usdt_txs(addr, confirm_before):
            tx_id = tx["tx_id"]
            tx_amount = Decimal(str(tx.get("amount") or 0))

            order = match_pending_order_by_amount(addr, tx_amount, eps)
            if not order:
                set_usdt_tx_status(tx_id, "unmatched")
                continue

            telegram_id = int(order["telegram_id"])
            plan_code = order["plan_code"]
            user = get_user(telegram_id)
            if not user:
                set_usdt_tx_status(tx_id, "unmatched")
                continue

            paid_until = user.get("paid_until")
            plan = next((p for p in PLANS if p["code"] == plan_code), None)
            if not plan:
                set_usdt_tx_status(tx_id, "unmatched")
                continue

            new_paid_until = compute_new_paid_until(paid_until, [plan])
            total_old = Decimal(str(user.get("total_received") or 0))
            new_total = total_old + Decimal(str(plan["price"]))

            update_user_payment(telegram_id, new_paid_until, new_total, plan_code)
            mark_order_success(int(order["id"]), tx_id)
            set_usdt_tx_status(
                tx_id,
                "processed",
                plan_code,
                Decimal(str(plan["price"])),
                datetime.utcnow(),
                telegram_id=telegram_id,
            )
            try:
                await notify_recharge_success(bot, telegram_id, tx_amount, plan_code, addr, tx_id)
            except Exception:
                pass

            lang = user.get("language") or "en"
            try:
                if JOIN_REQUEST_ENABLE:
                    expire_ts = int((datetime.utcnow() + timedelta(hours=int(JOIN_REQUEST_LINK_EXPIRE_HOURS))).timestamp())
                    invite_link = await bot.create_chat_invite_link(
                        chat_id=PAID_CHANNEL_ID,
                        expire_date=expire_ts,
                        creates_join_request=True,
                    )
                else:
                    invite_link = await bot.create_chat_invite_link(
                        chat_id=PAID_CHANNEL_ID,
                        expire_date=int(new_paid_until.timestamp()),
                        member_limit=1,
                    )
                msg = t(
                    lang,
                    "success_payment",
                    amount=str(tx_amount),
                    until=new_paid_until.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    link=invite_link.invite_link,
                )
                await bot.send_message(chat_id=telegram_id, text=msg)
            except Exception as e:
                logger.warning(f"[check_deposits] 创建邀请链接或发消息失败 uid={telegram_id}: {e}")

            inviter_id = get_inviter_id(telegram_id)
            if inviter_id and total_old == 0:
                reward_days = INVITE_REWARD.get(plan_code, 0)
                if reward_days > 0:
                    add_invite_reward(inviter_id, reward_days, telegram_id)

                    inviter = get_user(inviter_id)
                    if inviter:
                        inviter_lang = normalize_lang(inviter.get("language") or "en")
                        inviter_paid_until = inviter.get("paid_until")
                        base = (
                            inviter_paid_until
                            if (inviter_paid_until and inviter_paid_until > datetime.utcnow())
                            else datetime.utcnow()
                        )
                        inviter_new_until = base + timedelta(days=reward_days)
                        inviter_total = Decimal(str(inviter.get("total_received") or 0))
                        update_user_payment(inviter_id, inviter_new_until, inviter_total, "INVITE")

                        reward_msg = t(inviter_lang, "invite_reward_message", uid=telegram_id, days=reward_days)
                        try:
                            await bot.send_message(chat_id=inviter_id, text=reward_msg)
                        except Exception as e:
                            logger.warning(f"[check_deposits] 通知邀请人失败 inviter={inviter_id}: {e}")

        return

    users = get_all_users()
    for u in users:
        addr = u.get("wallet_addr")
        if not addr:
            continue
        assigned_at = get_address_assigned_at(addr)

        for tx in list_usdt_incoming(addr):
            insert_usdt_tx_if_new(
                telegram_id=None,
                addr=addr,
                tx_id=tx["tx_id"],
                amount=tx["amount"],
                from_addr=tx.get("from"),
                block_time=(tx.get("block_time") and tx["block_time"].replace(tzinfo=None)),
            )

        eps = Decimal(str(AMOUNT_EPS))
        pending = get_unassigned_usdt_txs_since(addr, confirm_before, assigned_at)
        if not pending:
            continue

        for tx in pending:
            tx_id = tx["tx_id"]
            tx_amount = Decimal(str(tx.get("amount") or 0))

            order = match_pending_order_by_amount(addr, tx_amount, eps)
            if not order:
                set_usdt_tx_status(tx_id, "unmatched")
                continue

            telegram_id = int(order["telegram_id"])
            plan_code = order["plan_code"]
            user = get_user(telegram_id)
            if not user:
                set_usdt_tx_status(tx_id, "unmatched")
                continue

            paid_until = user.get("paid_until")
            plan = next((p for p in PLANS if p["code"] == plan_code), None)
            if not plan:
                set_usdt_tx_status(tx_id, "unmatched")
                continue

            new_paid_until = compute_new_paid_until(paid_until, [plan])
            total_old = Decimal(str(user.get("total_received") or 0))
            new_total = total_old + Decimal(str(plan["price"]))

            update_user_payment(telegram_id, new_paid_until, new_total, plan_code)
            mark_order_success(int(order["id"]), tx_id)
            set_usdt_tx_status(
                tx_id,
                "processed",
                plan_code,
                Decimal(str(plan["price"])),
                datetime.utcnow(),
                telegram_id=telegram_id,
            )
            try:
                await notify_recharge_success(bot, telegram_id, tx_amount, plan_code, addr, tx_id)
            except Exception:
                pass

            try:
                if JOIN_REQUEST_ENABLE:
                    expire_ts = int((datetime.utcnow() + timedelta(hours=int(JOIN_REQUEST_LINK_EXPIRE_HOURS))).timestamp())
                    invite_link = await bot.create_chat_invite_link(
                        chat_id=PAID_CHANNEL_ID,
                        expire_date=expire_ts,
                        creates_join_request=True,
                    )
                else:
                    invite_link = await bot.create_chat_invite_link(
                        chat_id=PAID_CHANNEL_ID,
                        expire_date=int(new_paid_until.timestamp()),
                        member_limit=1,
                    )
                msg = t(
                    user.get("language") or "en",
                    "success_payment",
                    amount=str(tx_amount),
                    until=new_paid_until.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    link=invite_link.invite_link,
                )
                await bot.send_message(chat_id=telegram_id, text=msg)
            except Exception as e:
                logger.warning(f"[check_deposits] 创建邀请链接或发消息失败 uid={telegram_id}: {e}")

            inviter_id = get_inviter_id(telegram_id)
            if inviter_id and total_old == 0:
                reward_days = INVITE_REWARD.get(plan_code, 0)
                if reward_days > 0:
                    add_invite_reward(inviter_id, reward_days, telegram_id)

                    inviter = get_user(inviter_id)
                    if inviter:
                        inviter_lang = normalize_lang(inviter.get("language") or "en")
                        inviter_paid_until = inviter.get("paid_until")
                        base = (
                            inviter_paid_until
                            if (inviter_paid_until and inviter_paid_until > datetime.utcnow())
                            else datetime.utcnow()
                        )
                        inviter_new_until = base + timedelta(days=reward_days)
                        inviter_total = Decimal(str(inviter.get("total_received") or 0))
                        update_user_payment(inviter_id, inviter_new_until, inviter_total, "INVITE")

                        reward_msg = t(inviter_lang, "invite_reward_message", uid=telegram_id, days=reward_days)
                        try:
                            await bot.send_message(chat_id=inviter_id, text=reward_msg)
                        except Exception as e:
                            logger.warning(f"[check_deposits] 通知邀请人失败 inviter={inviter_id}: {e}")


async def check_expired_job(context: ContextTypes.DEFAULT_TYPE):
    bot = context.bot
    now = datetime.utcnow()
    expired_users = get_unhandled_expired_users(now)

    for u in expired_users:
        telegram_id = u["telegram_id"]
        lang = normalize_lang(u.get("language") or "en")
        try:
            await bot.ban_chat_member(chat_id=PAID_CHANNEL_ID, user_id=telegram_id)
            await bot.unban_chat_member(chat_id=PAID_CHANNEL_ID, user_id=telegram_id)
            msg = t(lang, "expired_notice")
            await bot.send_message(chat_id=telegram_id, text=msg)
            mark_user_expired_handled(telegram_id, now)
        except Exception as e:
            logger.warning(f"[check_expired] 踢用户失败 uid={telegram_id}: {e}")


async def check_expiring_job(context: ContextTypes.DEFAULT_TYPE):
    bot = context.bot
    now = datetime.utcnow()

    for days, col in ((3, "remind_3d_at"), (1, "remind_1d_at")):
        users = get_users_expiring_within_days(now, days, col)
        for u in users:
            telegram_id = u["telegram_id"]
            lang = normalize_lang(u.get("language") or "en")
            paid_until = u.get("paid_until")
            if not paid_until:
                continue
            try:
                msg = t(
                    lang,
                    "expiring_soon_notice",
                    days=days,
                    until=paid_until.strftime("%Y-%m-%d %H:%M:%S UTC"),
                )
                await bot.send_message(chat_id=telegram_id, text=msg)
                mark_user_reminded(telegram_id, col, now)
            except Exception as e:
                logger.warning(f"[check_expiring] 提醒失败 uid={telegram_id}: {e}")


async def health_alert_job(context: ContextTypes.DEFAULT_TYPE):
    if not HEALTH_ALERT_ENABLE:
        return
    bot = context.bot
    global _last_health_alert_ts

    conn = None
    try:
        from core.db import get_conn

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT MAX(processed_at) FROM usdt_txs WHERE status IN ('processed','credited')")
        row = cur.fetchone()
        cur.close()
    except Exception as e:
        try:
            await send_admin_text(bot, f"<b>健康告警：DB 异常</b>\nerr=<code>{type(e).__name__}: {e}</code>", parse_mode="HTML")
        except Exception:
            pass
        return
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    last_at = row[0] if row else None
    if not last_at:
        return
    now = datetime.utcnow()
    stale_minutes = (now - last_at).total_seconds() / 60.0
    if stale_minutes < float(HEALTH_ALERT_DEPOSIT_STALE_MINUTES):
        return

    if _last_health_alert_ts:
        gap = (now - _last_health_alert_ts).total_seconds() / 60.0
        if gap < float(HEALTH_ALERT_MIN_INTERVAL_MINUTES):
            return

    _last_health_alert_ts = now
    try:
        await send_admin_text(
            bot,
            (
                "<b>健康告警：入账延迟</b>\n"
                f"最后入账：<code>{last_at.strftime('%Y-%m-%d %H:%M:%S UTC')}</code>\n"
                f"延迟分钟：<code>{stale_minutes:.1f}</code>"
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass
