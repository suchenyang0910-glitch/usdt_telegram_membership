import asyncio
import json
import os
import random
import subprocess
import sys
from datetime import datetime

from telethon import TelegramClient, events
from telethon.sessions import StringSession

from bot.captions import highlight_caption
from config import (
    ADMIN_REPORT_CHAT_ID,
    ADMIN_USER_IDS,
    FREE_CHANNEL_IDS,
    HIGHLIGHT_CHANNEL_ID,
    PAID_CHANNEL_ID,
    USERBOT_API_HASH,
    USERBOT_API_ID,
    USERBOT_CLIP_RANDOM,
    USERBOT_CLIP_SECONDS,
    USERBOT_ENABLE,
    USERBOT_NOTIFY_CHAT_ID,
    USERBOT_SESSION_NAME,
    USERBOT_STRING_SESSION,
)


def _targets() -> list[int]:
    if USERBOT_NOTIFY_CHAT_ID is not None:
        return [int(USERBOT_NOTIFY_CHAT_ID)]
    ids: list[int] = []
    for x in ADMIN_USER_IDS:
        try:
            ids.append(int(x))
        except Exception:
            continue
    if ADMIN_REPORT_CHAT_ID is not None:
        ids.append(int(ADMIN_REPORT_CHAT_ID))
    uniq = []
    for x in ids:
        if x not in uniq:
            uniq.append(x)
    return uniq


async def _notify(client: TelegramClient, text: str):
    for chat_id in _targets():
        try:
            await client.send_message(chat_id, text, parse_mode="html")
        except Exception:
            continue


def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


def _ffprobe_duration(path: str) -> int:
    try:
        res = subprocess.run(
            ["ffprobe", "-v", "error", "-print_format", "json", "-show_format", path],
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode != 0:
            return 0
        data = json.loads(res.stdout or "{}")
        dur = float(((data.get("format") or {}).get("duration")) or 0)
        return int(dur)
    except Exception:
        return 0


def _clip_video(src: str, dst: str, start: int, length: int) -> bool:
    cmd = [
        "ffmpeg",
        "-y",
        "-ss",
        str(start),
        "-i",
        src,
        "-t",
        str(length),
        "-vf",
        "scale='min(720,iw)':-2",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "28",
        "-c:a",
        "aac",
        "-b:a",
        "96k",
        "-movflags",
        "+faststart",
        dst,
    ]
    res = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    return res.returncode == 0 and os.path.exists(dst)


def _state_path() -> str:
    return os.path.join("tmp", "userbot", "processed.json")


def _load_state() -> set[int]:
    p = _state_path()
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        ids = data.get("message_ids") or []
        return {int(x) for x in ids}
    except Exception:
        return set()


def _save_state(ids: set[int]):
    p = _state_path()
    _ensure_dir(os.path.dirname(p))
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump({"message_ids": sorted(list(ids))[-5000:]}, f)
    except Exception:
        return


def _clip_targets() -> list[int]:
    targets = []
    for ch in ([HIGHLIGHT_CHANNEL_ID] + list(FREE_CHANNEL_IDS)):
        if ch and ch not in targets:
            targets.append(ch)
    return targets


async def main():
    if not USERBOT_ENABLE:
        raise SystemExit("USERBOT_ENABLE=0")
    if not USERBOT_API_ID or not USERBOT_API_HASH:
        raise SystemExit("USERBOT_API_ID/USERBOT_API_HASH missing")
    if not USERBOT_STRING_SESSION and not sys.stdin.isatty():
        raise SystemExit(
            "USERBOT_STRING_SESSION missing (non-interactive)\n"
            "Run once to generate it:\n"
            "docker compose run --rm userbot python userbot_session.py"
        )

    _ensure_dir(os.path.dirname(USERBOT_SESSION_NAME) or ".")
    if USERBOT_STRING_SESSION:
        client = TelegramClient(StringSession(USERBOT_STRING_SESSION), USERBOT_API_ID, USERBOT_API_HASH)
    else:
        client = TelegramClient(USERBOT_SESSION_NAME, USERBOT_API_ID, USERBOT_API_HASH)

    processed = _load_state()

    async with client:
        me = await client.get_me()
        await _notify(
            client,
            f"<b>userbot 已启动</b>\nuser_id=<code>{me.id}</code>\n监听频道：<code>{PAID_CHANNEL_ID}</code>",
        )

        @client.on(events.NewMessage(chats=PAID_CHANNEL_ID))
        async def on_new_message(event):
            msg = event.message
            if not msg:
                return
            if msg.id in processed:
                return
            if not msg.file or not (msg.video or (msg.file.mime_type or "").startswith("video/")):
                return

            ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            src_dir = os.path.join("tmp", "userbot", "downloads")
            dst_dir = os.path.join("tmp", "userbot", "clips")
            _ensure_dir(src_dir)
            _ensure_dir(dst_dir)

            src = os.path.join(src_dir, f"{PAID_CHANNEL_ID}_{msg.id}.mp4")
            dst = os.path.join(dst_dir, f"{PAID_CHANNEL_ID}_{msg.id}_clip.mp4")

            try:
                await _notify(
                    client,
                    f"<b>开始处理视频</b>\nmsg_id=<code>{msg.id}</code>\nsize=<code>{(msg.file.size or 0) / 1024 / 1024:.2f}</code> MB\n{ts}",
                )
                await client.download_media(msg, file=src)
            except Exception as e:
                await _notify(client, f"<b>下载失败</b>\nmsg_id=<code>{msg.id}</code>\nerr=<code>{type(e).__name__}</code>: {e}")
                return

            duration = _ffprobe_duration(src)
            clip_len = USERBOT_CLIP_SECONDS if duration <= 0 else min(USERBOT_CLIP_SECONDS, duration)
            if duration > clip_len and USERBOT_CLIP_RANDOM:
                start = random.randint(0, max(0, duration - clip_len))
            else:
                start = 0

            ok = _clip_video(src, dst, start, clip_len)
            if not ok:
                await _notify(client, f"<b>剪辑失败</b>\nmsg_id=<code>{msg.id}</code>\n{ts}")
                try:
                    os.remove(src)
                except Exception:
                    pass
                return

            caption = highlight_caption()
            targets = _clip_targets()
            sent = 0
            for ch in targets:
                try:
                    await client.send_file(ch, dst, caption=caption, supports_streaming=True)
                    sent += 1
                except Exception as e:
                    await _notify(
                        client,
                        f"<b>发送失败</b>\nmsg_id=<code>{msg.id}</code>\ntarget=<code>{ch}</code>\nerr=<code>{type(e).__name__}</code>: {e}",
                    )

            processed.add(int(msg.id))
            _save_state(processed)

            try:
                os.remove(src)
                os.remove(dst)
            except Exception:
                pass

            await _notify(client, f"<b>处理完成</b>\nmsg_id=<code>{msg.id}</code>\n已发送频道数：<code>{sent}</code>\n{ts}")

        await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())

