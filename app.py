import logging
import sqlite3
from datetime import datetime, timedelta, timezone

from telegram import Update
from telegram.ext import (
    Updater,
    CommandHandler,
    CallbackContext,
)

# ===== 基本配置 =====
BOT_TOKEN = "8547638320:AAEIAL8GMhsoJ43vc0Z8jJxT1qTE4u72yUs"
PAID_CHANNEL_INVITE_LINK = "https://t.me/+2NCjX3zEUQsxYzE9"  # 固定邀请链接（后续可换成动态）

DB_PATH = "pv_bot.db"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ===== 数据库相关 =====
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            created_at TEXT,
            expire_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def get_user(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT user_id, username, first_name, last_name, created_at, expire_at "
        "FROM users WHERE user_id = ?",
        (user_id,),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "user_id": row[0],
        "username": row[1],
        "first_name": row[2],
        "last_name": row[3],
        "created_at": row[4],
        "expire_at": row[5],
    }


def upsert_user(user_id: int, username: str, first_name: str, last_name: str):
    now_str = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO users (user_id, username, first_name, last_name, created_at, expire_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_name=excluded.last_name
        """,
        (user_id, username, first_name, last_name, now_str, None),
    )
    conn.commit()
    conn.close()


def set_membership(user_id: int, days: int):
    """识别到账后调用：给用户增加会员天数"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT expire_at FROM users WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    now = datetime.now(timezone.utc)
    if row and row[0]:
        base_time = max(now, datetime.fromisoformat(row[0]))
    else:
        base_time = now
    new_expire = base_time + timedelta(days=days)
    c.execute(
        "UPDATE users SET expire_at = ? WHERE user_id = ?",
        (new_expire.isoformat(), user_id),
    )
    conn.commit()
    conn.close()
    return new_expire


# ===== 命令处理函数 =====
def start(update: Update, context: CallbackContext):
    user = update.effective_user
    upsert_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
    )

    text = (
        "欢迎来到 PV Premium 付费频道 🔥\n\n"
        "这里不是泛滥资源，而是 *高质量内容集中营*。\n\n"
        "你将获得：\n"
        "• 每周 100+ 条精选完整视频\n"
        "• 按主题分类的系列合集，节省你大量时间\n"
        "• 持续更新，历史内容长期保留，随时回看\n"
        "• 仅限会员访问，不会在公开频道出现\n\n"
        "你目前还没有开通会员，可以随时充值开通：\n"
        "• 年度会员：79.99 USDT / 365 天\n"
        "• 季度会员：19.99 USDT / 90 天\n"
        "• 月度会员：9.99 USDT / 30 天\n\n"
        "请使用 *USDT-TRC20* 向以下地址转账：\n"
        "`TWAVjpfcdH68wQPFFnzrDPdZPAHhr7RAr2`\n\n"
        "转账完成后，你可以发送 TxID 给我，我会自动为你开通或续费频道访问权限，无需联系客服。\n\n"
        "如有问题，你可以随时私信 @PVvideo_Assistant_Bot 咨询。"
    )

    update.message.reply_text(text, parse_mode="Markdown")


def join(update: Update, context: CallbackContext):
    """示例命令：/join 发送邀请链接（含简单会员有效期校验）"""
    user = update.effective_user
    u = get_user(user.id)

    if not u or not u.get("expire_at"):
        update.message.reply_text(
            "你目前还没有有效会员，请先完成 USDT-TRC20 充值。\n\n"
            "转账地址：`TWAVjpfcdH68wQPFFnzrDPdZPAHhr7RAr2`",
            parse_mode="Markdown",
        )
        return

    expire_at = datetime.fromisoformat(u["expire_at"])
    if expire_at < datetime.now(timezone.utc):
        update.message.reply_text("你的会员已到期，请先续费后再尝试加入频道。")
        return

    update.message.reply_text(
        f"✅ 你的会员有效期至：{expire_at.strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
        f"点击下面链接加入或重新加入付费频道：\n{PAID_CHANNEL_INVITE_LINK}"
    )


# ===== 主入口 =====
def main():
    init_db()

    updater = Updater(token=BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("join", join))

    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    main()
