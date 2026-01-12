# main.py
import os
import logging

from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters

from config import BOT_TOKEN, PAID_CHANNEL_ID, AUTO_CLIP_FROM_PAID_CHANNEL
from core.logging_setup import setup_logging
from core.models import init_tables
from bot.handlers import start, plans, invite, on_menu_button
from bot.scheduler import check_deposits_job, check_expired_job, check_expiring_job
from bot.clipper import private_channel_video_handler
from bot.uploader import build_upload_conversation_handler

logger = logging.getLogger(__name__)

def main():
    setup_logging()
    init_tables()

    app = Application.builder().token(BOT_TOKEN).build()

    # 命令
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("plans", plans))
    app.add_handler(CommandHandler("invite", invite))
    app.add_handler(build_upload_conversation_handler())
    app.add_handler(CallbackQueryHandler(on_menu_button))

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

    logger.info("🚀 Bot is Running — 收款 / 续费 / 踢人 / 剪辑 / 邀请裂变 已开启")
    app.run_polling()

if __name__ == "__main__":
    main()
