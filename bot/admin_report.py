import html
from datetime import datetime
from decimal import Decimal

from telegram import Bot

from config import ADMIN_REPORT_CHAT_ID, ADMIN_REPORT_ENABLE, ADMIN_USER_IDS


def _short_addr(addr: str) -> str:
    if not addr:
        return ""
    if len(addr) <= 12:
        return addr
    return f"{addr[:6]}...{addr[-4:]}"


def _targets() -> list[int]:
    if not ADMIN_REPORT_ENABLE:
        return []
    ids: set[int] = set()
    for x in ADMIN_USER_IDS:
        try:
            ids.add(int(x))
        except Exception:
            continue
    if ADMIN_REPORT_CHAT_ID is not None:
        ids.add(int(ADMIN_REPORT_CHAT_ID))
    return list(ids)


async def send_admin_text(bot: Bot, text: str, parse_mode: str | None = None):
    targets = _targets()
    for chat_id in targets:
        try:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)
        except Exception:
            continue


async def notify_recharge_success(
    bot: Bot,
    telegram_id: int,
    amount: Decimal,
    plan_code: str,
    addr: str,
    tx_id: str,
):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    text = (
        "<b>充值成功</b>\n"
        f"用户ID：<code>{telegram_id}</code>\n"
        f"金额：<code>{amount}</code> USDT\n"
        f"套餐：<code>{html.escape(plan_code)}</code>\n"
        f"地址：<code>{html.escape(_short_addr(addr))}</code>\n"
        f"TX：<code>{html.escape(tx_id[:10])}</code>\n"
        f"时间：<code>{ts}</code>"
    )
    await send_admin_text(bot, text, parse_mode="HTML")
