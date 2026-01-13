import logging
import os
import random
import subprocess
from datetime import datetime

from telegram import Update
from telegram.ext import (
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.error import BadRequest

from config import (
    ADMIN_USER_IDS,
    PAID_CHANNEL_ID,
    HIGHLIGHT_CHANNEL_ID,
    FREE_CHANNEL_IDS,
    DOWNLOAD_DIR,
    CLIP_DIR,
    CLIP_SECONDS,
    CLIP_RANDOM,
    SEND_RETRY,
    MAX_TG_DOWNLOAD_MB,
)
from bot.captions import highlight_caption
from core.models import create_video_post

logger = logging.getLogger(__name__)

WAIT_MEDIA = 1

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(CLIP_DIR, exist_ok=True)


def _is_admin(user_id: int) -> bool:
    return bool(ADMIN_USER_IDS) and user_id in ADMIN_USER_IDS


def _ffmpeg_clip(src: str, dst: str, start: int, length: int) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-ss",
        str(start),
        "-i",
        src,
        "-t",
        str(length),
        "-c",
        "copy",
        dst,
    ]
    res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    if res.returncode == 0:
        return
    cmd2 = [
        "ffmpeg",
        "-y",
        "-ss",
        str(start),
        "-i",
        src,
        "-t",
        str(length),
        "-vcodec",
        "libx264",
        "-acodec",
        "aac",
        dst,
    ]
    subprocess.run(cmd2, check=True)


async def upload_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return ConversationHandler.END
    await update.message.reply_text("请发送需要发布的完整视频（video 或 mp4 文件）。")
    return WAIT_MEDIA


async def upload_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return ConversationHandler.END
    await update.message.reply_text("已取消。")
    return ConversationHandler.END


async def upload_receive_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not _is_admin(user.id):
        return ConversationHandler.END

    msg = update.message
    if not msg:
        return ConversationHandler.END

    file_id = None
    file_size = None
    duration = None

    if msg.video:
        file_id = msg.video.file_id
        file_size = msg.video.file_size
        duration = msg.video.duration
    elif msg.document and (msg.document.mime_type or "").startswith("video/"):
        file_id = msg.document.file_id
        file_size = msg.document.file_size
    else:
        await msg.reply_text("未识别到视频文件，请重新发送。")
        return WAIT_MEDIA

    caption = msg.caption or ""

    try:
        paid_sent = await context.bot.send_video(
            chat_id=PAID_CHANNEL_ID,
            video=file_id,
            caption=caption if caption else None,
        )
        create_video_post(PAID_CHANNEL_ID, paid_sent.message_id, file_id, caption)
    except Exception as e:
        await msg.reply_text(f"发布到付费频道失败：{e}")
        return ConversationHandler.END

    if file_size and file_size > MAX_TG_DOWNLOAD_MB * 1024 * 1024:
        await msg.reply_text(
            f"已发布到付费频道（message_id={paid_sent.message_id}）。\n"
            f"但视频大小超过 {MAX_TG_DOWNLOAD_MB}MB，无法自动剪辑引流片段。"
        )
        return ConversationHandler.END

    src = os.path.join(DOWNLOAD_DIR, f"upload_{file_id}_{int(datetime.utcnow().timestamp())}.mp4")
    dst = os.path.join(CLIP_DIR, f"upload_{file_id}_clip_{int(datetime.utcnow().timestamp())}.mp4")

    try:
        tg_file = await context.bot.get_file(file_id)
        await tg_file.download_to_drive(src)
    except BadRequest as e:
        await msg.reply_text(f"下载失败：{e}")
        return ConversationHandler.END
    except Exception as e:
        await msg.reply_text(f"下载失败：{e}")
        return ConversationHandler.END

    clip_len = CLIP_SECONDS
    start = 0
    if duration and CLIP_RANDOM and duration > clip_len:
        start = random.randint(0, max(0, duration - clip_len))
    if duration and duration < clip_len:
        clip_len = duration

    try:
        _ffmpeg_clip(src, dst, start, clip_len)
    except Exception as e:
        logger.error("[uploader] ffmpeg 剪辑失败 file_id=%s err=%s", file_id, e, exc_info=True)
        await msg.reply_text(f"剪辑失败：{e}")
        try:
            os.remove(src)
        except Exception:
            pass
        return ConversationHandler.END

    caption_text = highlight_caption()
    highlight_msg = None
    targets = []
    for ch in ([HIGHLIGHT_CHANNEL_ID] + list(FREE_CHANNEL_IDS)):
        if ch and ch not in targets:
            targets.append(ch)

    for ch in targets:
        sent = None
        last_err = None
        for i in range(SEND_RETRY):
            try:
                with open(dst, "rb") as f:
                    sent = await context.bot.send_video(chat_id=ch, video=f, caption=caption_text)
                break
            except Exception as e:
                last_err = e
                continue
        if ch == HIGHLIGHT_CHANNEL_ID:
            highlight_msg = sent
        if sent is None and last_err is not None and ch == HIGHLIGHT_CHANNEL_ID:
            await msg.reply_text(f"引流频道发布失败：{last_err}")

    try:
        os.remove(src)
        os.remove(dst)
    except Exception:
        pass

    if highlight_msg:
        create_video_post(HIGHLIGHT_CHANNEL_ID, highlight_msg.message_id, file_id, caption_text)
        await msg.reply_text(
            f"发布完成：\n付费频道 message_id={paid_sent.message_id}\n引流频道 message_id={highlight_msg.message_id}"
        )
    else:
        await msg.reply_text(f"已发布到付费频道（message_id={paid_sent.message_id}），引流频道发布失败。")

    return ConversationHandler.END


def build_upload_conversation_handler() -> ConversationHandler:
    return ConversationHandler(
        entry_points=[CommandHandler("upload", upload_start)],
        states={
            WAIT_MEDIA: [
                MessageHandler(
                    (filters.VIDEO | filters.Document.VIDEO) & ~filters.COMMAND,
                    upload_receive_media,
                )
            ]
        },
        fallbacks=[CommandHandler("cancel", upload_cancel)],
        allow_reentry=True,
    )

