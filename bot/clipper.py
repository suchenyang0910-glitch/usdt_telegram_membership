# clipper.py-*- coding: utf-8 -*-
import os
import random
import subprocess
import time
import logging

from telegram import Update
from telegram.ext import ContextTypes
from telegram.error import BadRequest

from config import (
    DOWNLOAD_DIR,
    CLIP_DIR,
    CLIP_SECONDS,
    CLIP_START_OFFSET_SEC,
    CLIP_RANDOM,
    SEND_RETRY,
    FREE_CHANNEL_IDS,
    HIGHLIGHT_CHANNEL_ID,
    PAID_CHANNEL_ID,
    MAX_TG_DOWNLOAD_MB,
)
from bot.captions import compose_free_caption
from bot.admin_report import send_admin_text
from core.models import claim_clip_dispatch, mark_clip_dispatch_sent, unclaim_clip_dispatch

logger = logging.getLogger(__name__)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(CLIP_DIR, exist_ok=True)

async def private_channel_video_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    监听付费频道内的新视频：
    1）下载原视频（不超过约 19MB）
    2）用 ffmpeg 切一个短片段
    3）发到两个免费频道做试看引流
    """
    message = update.channel_post
    if not message or not message.video:
        return

    video = message.video
    file_size = getattr(video, "file_size", None)

    effective_limit_mb = min(int(MAX_TG_DOWNLOAD_MB), 20)
    if file_size and file_size > effective_limit_mb * 1024 * 1024:
        logger.warning(
            "[clipper] 视频过大，跳过剪辑 file_id=%s size=%.2fMB limit=%sMB",
            video.file_id,
            file_size / 1024 / 1024,
            effective_limit_mb,
        )
        try:
            await send_admin_text(
                context.bot,
                (
                    "<b>剪辑跳过：视频过大</b>\n"
                    f"频道消息ID：<code>{message.message_id}</code>\n"
                    f"大小：<code>{file_size / 1024 / 1024:.2f}</code> MB\n"
                    f"限制：<code>{effective_limit_mb}</code> MB\n"
                    "建议：请单独上传一个 30 秒内的小体积试看视频（<=20MB）用于引流。"
                ),
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    src = os.path.join(DOWNLOAD_DIR, f"{video.file_id}.mp4")
    dst = os.path.join(CLIP_DIR, f"{video.file_id}_clip.mp4")

    # 下载原视频
    try:
        file = await context.bot.get_file(video.file_id)
        await file.download_to_drive(src)
        logger.info(
            "[clipper] 下载原视频成功 file_id=%s path=%s size=%.2fMB",
            video.file_id,
            src,
            (file_size or 0) / 1024 / 1024,
        )
    except BadRequest as e:
        # 再保险：即使 file_size 没有或判断不准，一旦出现 File is too big，直接跳过
        if "File is too big" in str(e):
            logger.error(
                "[clipper] Telegram 报错 File is too big，无法下载 file_id=%s",
                video.file_id,
                exc_info=True,
            )
            try:
                await send_admin_text(
                    context.bot,
                    (
                        "<b>剪辑失败：File is too big</b>\n"
                        f"频道消息ID：<code>{message.message_id}</code>\n"
                        f"file_id：<code>{video.file_id}</code>\n"
                        "原因：Telegram Bot API 不支持下载超大文件\n"
                        "建议：请单独上传一个 30 秒内的小体积试看视频（<=20MB）用于引流。"
                    ),
                    parse_mode="HTML",
                )
            except Exception:
                pass
            return
        logger.error(
            "[clipper] get_file 发生 BadRequest file_id=%s err=%s",
            video.file_id,
            e,
            exc_info=True,
        )
        return
    except Exception as e:
        logger.error(
            "[clipper] 下载原视频失败 file_id=%s err=%s",
            video.file_id,
            e,
            exc_info=True,
        )
        return

    duration = video.duration or 0
    start_offset = int(CLIP_START_OFFSET_SEC or 0)
    clip_len = min(CLIP_SECONDS, duration) if duration else CLIP_SECONDS

    if start_offset > 0:
        if duration and duration > 0:
            max_start = max(0, int(duration - clip_len))
            start = min(start_offset, max_start)
            remain = max(1, int(duration - start))
            clip_len = min(int(clip_len), remain)
        else:
            start = 0
    else:
        if duration <= clip_len or not CLIP_RANDOM:
            start = 0
        else:
            start = random.randint(0, max(0, duration - clip_len))

    # 先尝试无损剪辑，不行再转码
    cmd = [
        "ffmpeg", "-y",
        "-ss", str(start),
        "-i", src,
        "-t", str(clip_len),
        "-c", "copy",
        dst,
    ]

    try:
        res = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if res.returncode != 0:
            logger.warning("[clipper] 复制剪辑失败，尝试转码方式 file_id=%s", video.file_id)
            cmd2 = [
                "ffmpeg", "-y",
                "-ss", str(start),
                "-i", src,
                "-t", str(clip_len),
                "-vcodec", "libx264",
                "-acodec", "aac",
                dst,
            ]
            subprocess.run(cmd2, check=True)
    except Exception as e:
        logger.error("[clipper] ffmpeg 剪辑失败 file_id=%s err=%s", video.file_id, e, exc_info=True)
        try:
            os.remove(src)
        except Exception:
            pass
        return

    caption = compose_free_caption(message.caption or "")

    targets = []
    for ch in ([HIGHLIGHT_CHANNEL_ID] + list(FREE_CHANNEL_IDS)):
        if ch and ch not in targets:
            targets.append(ch)
    logger.info("[clipper] targets=%s msg_id=%s", targets, getattr(message, "message_id", None))

    for ch in targets:
        last_err = None
        for i in range(SEND_RETRY):
            try:
                if not claim_clip_dispatch(PAID_CHANNEL_ID, int(message.message_id), int(ch), "bot"):
                    break
                with open(dst, "rb") as f:
                    await context.bot.send_video(chat_id=ch, video=f, caption=caption)
                logger.info("[clipper] 已将剪辑发送到频道 %s (尝试第 %s 次)", ch, i + 1)
                mark_clip_dispatch_sent(PAID_CHANNEL_ID, int(message.message_id), int(ch))
                break
            except Exception as e:
                last_err = e
                try:
                    unclaim_clip_dispatch(PAID_CHANNEL_ID, int(message.message_id), int(ch))
                except Exception:
                    pass
                logger.error(
                    "[clipper] 发送剪辑到 %s 失败（第 %s 次）: %s",
                    ch,
                    i + 1,
                    e,
                    exc_info=True,
                )
                time.sleep(1)
        if last_err:
            try:
                await send_admin_text(
                    context.bot,
                    (
                        "<b>剪辑发送失败</b>\n"
                        f"目标频道：<code>{ch}</code>\n"
                        f"来源消息ID：<code>{message.message_id}</code>\n"
                        f"err=<code>{type(last_err).__name__}: {last_err}</code>"
                    ),
                    parse_mode="HTML",
                )
            except Exception:
                pass

    # 清理临时文件
    try:
        os.remove(src)
        os.remove(dst)
    except Exception:
        pass
