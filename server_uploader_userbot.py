import asyncio
import json
import os
import random
import subprocess
import time
from datetime import datetime, timezone

from telethon import TelegramClient
from telethon.sessions import StringSession

from bot.captions import compose_free_caption
from config import (
    CLIP_DIR,
    CLIP_RANDOM,
    CLIP_SECONDS,
    CLIP_START_OFFSET_SEC,
    DOWNLOAD_DIR,
    FREE_CHANNEL_IDS,
    HIGHLIGHT_CHANNEL_ID,
    PAID_CHANNEL_ID,
    HEARTBEAT_USERBOT_FILE,
    USERBOT_API_HASH,
    USERBOT_API_ID,
    USERBOT_SESSION_NAME,
    USERBOT_STRING_SESSION,
)
from core.db import get_conn
from core.models import (
    claim_clip_dispatch_takeover,
    init_tables,
    local_uploader_update,
    mark_clip_dispatch_sent,
    unclaim_clip_dispatch,
    update_video_free_link,
)


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


def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


def _pick_start(duration: int, clip_len: int) -> int:
    dur = int(duration or 0)
    clip_len = max(1, int(clip_len or 0))
    base = max(0, int(CLIP_START_OFFSET_SEC or 0))
    if dur <= 0:
        return 0
    if dur <= clip_len:
        return 0
    if dur <= base + clip_len:
        return 0
    if CLIP_RANDOM:
        return random.randint(base, max(base, dur - clip_len))
    return base


async def _clip_and_send_from_paid_message(client: TelegramClient, paid_msg, caption_src: str) -> tuple[int | None, int | None, int]:
    if not paid_msg:
        return None, None, 0
    if not getattr(paid_msg, "file", None) or not (getattr(paid_msg, "video", None) or (getattr(getattr(paid_msg, "file", None), "mime_type", "") or "").startswith("video/")):
        return None, None, 0

    _ensure_dir(DOWNLOAD_DIR)
    _ensure_dir(CLIP_DIR)
    paid_msg_id = int(getattr(paid_msg, "id", 0) or 0)
    src = os.path.join(DOWNLOAD_DIR, f"{PAID_CHANNEL_ID}_{paid_msg_id}.mp4")
    dst = os.path.join(CLIP_DIR, f"{PAID_CHANNEL_ID}_{paid_msg_id}_clip.mp4")

    await client.download_media(paid_msg, file=src)
    duration = _ffprobe_duration(src)
    clip_len = int(CLIP_SECONDS or 30)
    if duration > 0:
        clip_len = min(clip_len, duration)
    start = _pick_start(duration, clip_len)

    ok = _clip_video(src, dst, start, clip_len)
    if not ok:
        try:
            os.remove(src)
        except Exception:
            pass
        return None, None, 0

    caption = compose_free_caption(caption_src or "")
    targets = _clip_targets()
    first_free: tuple[int | None, int | None] = (None, None)
    sent = 0
    for ch in targets:
        try:
            if not claim_clip_dispatch_takeover(PAID_CHANNEL_ID, paid_msg_id, int(ch), "server_userbot", 600):
                continue
            sent_msg = await client.send_file(int(ch), dst, caption=caption, supports_streaming=True)
            mark_clip_dispatch_sent(PAID_CHANNEL_ID, paid_msg_id, int(ch))
            try:
                update_video_free_link(PAID_CHANNEL_ID, paid_msg_id, int(ch), int(getattr(sent_msg, "id", 0) or 0))
            except Exception:
                pass
            sent += 1
            if first_free[0] is None:
                first_free = (int(ch), int(getattr(sent_msg, "id", 0) or 0))
        except Exception:
            try:
                unclaim_clip_dispatch(PAID_CHANNEL_ID, paid_msg_id, int(ch))
            except Exception:
                pass

    try:
        os.remove(src)
        os.remove(dst)
    except Exception:
        pass

    return first_free[0], first_free[1], sent


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
            json.dump({"ok": True, "ts": datetime.now(timezone.utc).isoformat()}, f, ensure_ascii=False)
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

        free_ch, free_msg_id, _sent_cnt = await _clip_and_send_from_paid_message(client, sent, caption)

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

    while True:
        try:
            init_tables()
            break
        except Exception as e:
            print(f"[server_uploader] db not ready: {type(e).__name__}: {e}")
            await asyncio.sleep(5)

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
            try:
                job = _claim_next()
            except Exception as e:
                print(f"[server_uploader] db error: {type(e).__name__}: {e}")
                await asyncio.sleep(5)
                continue
            if not job:
                await asyncio.sleep(5)
                continue
            await _process_job(client, job)
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())

