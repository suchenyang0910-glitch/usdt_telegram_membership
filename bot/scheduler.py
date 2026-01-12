# bot/scheduler.py
from datetime import datetime, timedelta
from decimal import Decimal

from telegram.ext import ContextTypes

from config import (
    PLANS,
    PAID_CHANNEL_ID,
    INVITE_REWARD,
    MIN_TX_AGE_SEC,
    PAYMENT_MODE,
    RECEIVE_ADDRESS,
    AMOUNT_EPS,
)
from core.models import (
    get_all_users,
    update_user_payment,
    get_unhandled_expired_users,
    mark_user_expired_handled,
    get_users_expiring_within_days,
    mark_user_reminded,
    create_order,
    get_user,
    get_inviter_id,
    add_invite_reward,
    insert_usdt_tx_if_new,
    get_pending_usdt_txs,
    set_usdt_tx_status,
    get_unassigned_usdt_txs,
    match_pending_order_by_amount,
    mark_order_success,
)
from chain.tron_client import list_usdt_incoming
from bot.payments import split_amount_to_plans, compute_new_paid_until
from bot.i18n import t, normalize_lang
import logging
logger = logging.getLogger(__name__)

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

            lang = user.get("language") or "en"
            try:
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
        telegram_id = u["telegram_id"]
        addr        = u.get("wallet_addr")
        if not addr:
            continue

        lang        = u.get("language") or "en"
        total_old = Decimal(str(u.get("total_received") or 0))
        paid_until = u.get("paid_until")

        for tx in list_usdt_incoming(addr):
            insert_usdt_tx_if_new(
                telegram_id=telegram_id,
                addr=addr,
                tx_id=tx["tx_id"],
                amount=tx["amount"],
                from_addr=tx.get("from"),
                block_time=(tx.get("block_time") and tx["block_time"].replace(tzinfo=None)),
            )

        pending = get_pending_usdt_txs(telegram_id, addr, confirm_before)
        if not pending:
            continue

        for tx in pending:
            tx_id = tx["tx_id"]
            tx_amount = Decimal(str(tx.get("amount") or 0))

            plans_used, remain = split_amount_to_plans(tx_amount)
            if not plans_used:
                set_usdt_tx_status(tx_id, "unmatched")
                continue

            credited = sum(p["price"] for p in plans_used)
            new_total = total_old + credited
            new_paid_until = compute_new_paid_until(paid_until, plans_used)
            last_plan_code = plans_used[-1]["code"]

            update_user_payment(telegram_id, new_paid_until, new_total, last_plan_code)
            create_order(telegram_id, addr, tx_amount, last_plan_code, tx_id=tx_id)
            set_usdt_tx_status(tx_id, "processed", last_plan_code, credited, datetime.utcnow(), telegram_id=telegram_id)

            try:
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
                reward_days = INVITE_REWARD.get(last_plan_code, 0)
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

            total_old = new_total
            paid_until = new_paid_until


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
