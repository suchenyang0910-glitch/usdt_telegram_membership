# main.py
import logging
from datetime import time

from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters

from config import BOT_TOKEN, PAID_CHANNEL_ID, AUTO_CLIP_FROM_PAID_CHANNEL
from core.logging_setup import setup_logging
from core.models import init_tables
from bot.handlers import (
    start,
    plans,
    invite,
    on_menu_button,
    reset_addr,
    my_id,
    diag,
    chat_id,
    support_user_inbox,
    support_group_reply,
    support_reply_button,
    support_group_pending_reply,
)
from bot.scheduler import (
    check_deposits_job,
    check_expired_job,
    check_expiring_job,
    cleanup_logs_job,
    hourly_admin_report_job,
    cleanup_downloads_job,
    health_alert_job,
)
from bot.clipper import private_channel_video_handler
from bot.uploader import build_upload_conversation_handler
from bot.error_notify import application_error_handler

logger = logging.getLogger(__name__)

def main():
    setup_logging()
    init_tables()

    app = Application.builder().token(BOT_TOKEN).build()

    # 命令
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("plans", plans))
    app.add_handler(CommandHandler("invite", invite))
    app.add_handler(CommandHandler("reset_addr", reset_addr))
    app.add_handler(CommandHandler("diag", diag))
    app.add_handler(CommandHandler("chatid", chat_id))
    app.add_handler(MessageHandler(filters.Regex(r"^(我的ID|我的id|myid|my id)$"), my_id))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, support_user_inbox, block=False))
    app.add_handler(MessageHandler(~filters.COMMAND, support_group_pending_reply, block=False))
    app.add_handler(MessageHandler(filters.REPLY, support_group_reply, block=False))
    app.add_handler(build_upload_conversation_handler())
    app.add_handler(CallbackQueryHandler(support_reply_button, pattern=r"^support_reply:", block=False))
    app.add_handler(CallbackQueryHandler(on_menu_button))
    app.add_error_handler(application_error_handler)

    # 监听付费频道的视频消息，用于自动剪辑推送到免费频道
    if AUTO_CLIP_FROM_PAID_CHANNEL:
        app.add_handler(
            MessageHandler(
                filters.VIDEO & filters.Chat(PAID_CHANNEL_ID),
                private_channel_video_handler,
            )
        )

    # 定时任务：每 60 秒检查到账，每小时检查过期
    app.job_queue.run_repeating(check_deposits_job, interval=60, first=10)
    app.job_queue.run_repeating(check_expired_job, interval=3600, first=60)
    app.job_queue.run_repeating(check_expiring_job, interval=3600, first=120)
    app.job_queue.run_repeating(cleanup_logs_job, interval=21600, first=300)
    app.job_queue.run_repeating(hourly_admin_report_job, interval=3600, first=600)
    app.job_queue.run_repeating(health_alert_job, interval=300, first=120)
    app.job_queue.run_daily(cleanup_downloads_job, time=time(hour=3, minute=0))

    logger.info("🚀 Bot is Running — 收款 / 续费 / 踢人 / 剪辑 / 邀请裂变 已开启")
    app.run_polling()

if __name__ == "__main__":
    main()
