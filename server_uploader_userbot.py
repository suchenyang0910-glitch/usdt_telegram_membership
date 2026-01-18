import asyncio
import json
import os
import random
import subprocess
import time
from datetime import datetime

from telethon import TelegramClient
from telethon.sessions import StringSession

from bot.captions import compose_free_caption
from config import (
    FREE_CHANNEL_IDS,
    HIGHLIGHT_CHANNEL_ID,
    PAID_CHANNEL_ID,
    HEARTBEAT_USERBOT_FILE,
    USERBOT_API_HASH,
    USERBOT_API_ID,
    USERBOT_CLIP_RANDOM,
    USERBOT_CLIP_SECONDS,
    USERBOT_SESSION_NAME,
    USERBOT_STRING_SESSION,
)
from core.db import get_conn
from core.models import init_tables, local_uploader_update


def _work_root() -> str:
    return os.getenv("SERVER_UPLOAD_ROOT", "/app").strip() or "/app"


def _abs_in_root(rel: str) -> str:
    rel = (rel or "").lstrip("/")
    return os.path.join(_work_root(), rel)


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


def _clip_targets() -> list[int]:
    targets = []
    for ch in ([HIGHLIGHT_CHANNEL_ID] + list(FREE_CHANNEL_IDS)):
        if ch and ch not in targets:
            targets.append(int(ch))
    return targets


def _claim_next() -> dict | None:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM videos WHERE upload_status='pending' AND server_file_path IS NOT NULL AND server_file_path<>'' ORDER BY created_at ASC LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return None
    vid = int(row.get("id") or 0)
    cur2 = conn.cursor()
    cur2.execute("UPDATE videos SET upload_status='uploading' WHERE id=%s AND upload_status='pending'", (vid,))
    ok = cur2.rowcount == 1
    cur2.close()
    cur.close()
    conn.close()
    return row if ok else None


def _write_heartbeat():
    p = (HEARTBEAT_USERBOT_FILE or "").strip()
    if not p:
        return
    try:
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        tmp = p + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"ok": True, "ts": datetime.utcnow().isoformat()}, f, ensure_ascii=False)
        os.replace(tmp, p)
    except Exception:
        return


async def _process_job(client: TelegramClient, job: dict):
    video_id = int(job.get("id") or 0)
    server_file_path = (job.get("server_file_path") or "").strip()
    caption = (job.get("caption") or "").strip()
    if video_id <= 0 or not server_file_path:
        return

    src_path = _abs_in_root(server_file_path)
    if not os.path.exists(src_path):
        local_uploader_update(video_id, "failed", None, None, None, None, None, f"server file missing: {server_file_path}")
        return

    try:
        sent = await client.send_file(PAID_CHANNEL_ID, src_path, caption=caption, supports_streaming=True)
        paid_msg_id = int(getattr(sent, "id", 0) or 0)
        file_id = None
        try:
            file_id = str(getattr(getattr(sent, "file", None), "id", "") or "") or None
        except Exception:
            file_id = None

        duration = _ffprobe_duration(src_path)
        clip_len = int(USERBOT_CLIP_SECONDS or 30)
        if duration > 0:
            clip_len = min(clip_len, duration)
        if duration > clip_len and USERBOT_CLIP_RANDOM:
            start = random.randint(0, max(0, duration - clip_len))
        else:
            start = 0
        clip_dir = os.path.join(_work_root(), "tmp", "userbot", "clips")
        os.makedirs(clip_dir, exist_ok=True)
        clip_path = os.path.join(clip_dir, f"clip_{video_id}_{int(time.time())}.mp4")
        ok = _clip_video(src_path, clip_path, start, clip_len)
        free_ch = _clip_targets()[0] if _clip_targets() else None
        free_msg_id = None
        if ok and free_ch is not None:
            free_caption = compose_free_caption(caption)
            free_sent = await client.send_file(int(free_ch), clip_path, caption=free_caption, supports_streaming=True)
            free_msg_id = int(getattr(free_sent, "id", 0) or 0)

        local_uploader_update(
            video_id=video_id,
            upload_status="done",
            channel_id=int(PAID_CHANNEL_ID),
            message_id=paid_msg_id,
            free_channel_id=int(free_ch) if free_ch is not None else None,
            free_message_id=int(free_msg_id) if free_msg_id is not None else None,
            file_id=file_id,
            error=None,
        )
    except Exception as e:
        local_uploader_update(video_id, "failed", None, None, None, None, None, f"{type(e).__name__}: {e}")


async def main():
    if not USERBOT_API_ID or not USERBOT_API_HASH:
        raise SystemExit("USERBOT_API_ID/USERBOT_API_HASH missing")
    if not USERBOT_STRING_SESSION:
        raise SystemExit("USERBOT_STRING_SESSION missing")
    if not PAID_CHANNEL_ID:
        raise SystemExit("PAID_CHANNEL_ID missing")

    init_tables()

    name = USERBOT_SESSION_NAME or "tmp/userbot/telethon"
    sess = StringSession(USERBOT_STRING_SESSION)
    async with TelegramClient(sess or name, USERBOT_API_ID, USERBOT_API_HASH) as client:
        print(f"[server_uploader] start work_root={_work_root()} paid_channel_id={PAID_CHANNEL_ID}")
        last_hb = 0.0
        while True:
            now = time.time()
            if now - last_hb >= 60:
                _write_heartbeat()
                last_hb = now
            job = _claim_next()
            if not job:
                await asyncio.sleep(5)
                continue
            await _process_job(client, job)
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())

