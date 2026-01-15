import asyncio
import json
import os
import random
import subprocess
import time
import urllib.request
from datetime import datetime

from telethon import TelegramClient
from telethon.sessions import StringSession

from bot.captions import compose_free_caption
from config import (
    FREE_CHANNEL_IDS,
    HIGHLIGHT_CHANNEL_ID,
    LOCAL_UPLOADER_TOKEN,
    PAID_CHANNEL_ID,
    USERBOT_API_HASH,
    USERBOT_API_ID,
    USERBOT_CLIP_RANDOM,
    USERBOT_CLIP_SECONDS,
    USERBOT_SESSION_NAME,
    USERBOT_STRING_SESSION,
)


def _base_url() -> str:
    return (os.getenv("LOCAL_UPLOADER_BASE_URL", "") or "").strip().rstrip("/")


def _work_dir() -> str:
    return (os.getenv("LOCAL_UPLOADER_DIR", "") or "").strip() or "."


def _http_json(method: str, url: str, headers: dict[str, str], body: dict | None = None) -> dict:
    data = None
    h = dict(headers or {})
    if body is not None:
        raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        data = raw
        h["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=h, method=method)
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    obj = json.loads(raw or "{}")
    return obj if isinstance(obj, dict) else {}


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


def _pick_first_free_target() -> int | None:
    for ch in _clip_targets():
        return int(ch)
    return None


async def _process_job(client: TelegramClient, base_url: str, job: dict):
    video_id = int(job.get("id") or 0)
    local_filename = (job.get("local_filename") or "").strip()
    caption = (job.get("caption") or "").strip()
    if video_id <= 0:
        return

    work_dir = _work_dir()
    src_path = os.path.join(work_dir, local_filename) if local_filename else ""
    if not src_path or not os.path.exists(src_path):
        _http_json(
            "POST",
            base_url + "/api/local_uploader/update",
            {"X-Local-Uploader-Token": LOCAL_UPLOADER_TOKEN},
            {"video_id": video_id, "upload_status": "failed", "error": f"file not found: {src_path}"},
        )
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
        clip_dir = os.path.join(work_dir, "clips")
        os.makedirs(clip_dir, exist_ok=True)
        clip_path = os.path.join(clip_dir, f"clip_{video_id}_{int(time.time())}.mp4")
        ok = _clip_video(src_path, clip_path, start, clip_len)
        free_ch = _pick_first_free_target()
        free_msg_id = None
        if ok and free_ch is not None:
            free_caption = compose_free_caption(caption)
            free_sent = await client.send_file(free_ch, clip_path, caption=free_caption, supports_streaming=True)
            free_msg_id = int(getattr(free_sent, "id", 0) or 0)
        _http_json(
            "POST",
            base_url + "/api/local_uploader/update",
            {"X-Local-Uploader-Token": LOCAL_UPLOADER_TOKEN},
            {
                "video_id": video_id,
                "upload_status": "done",
                "channel_id": int(PAID_CHANNEL_ID),
                "message_id": paid_msg_id,
                "free_channel_id": int(free_ch) if free_ch is not None else None,
                "free_message_id": int(free_msg_id) if free_msg_id is not None else None,
                "file_id": file_id,
            },
        )
    except Exception as e:
        _http_json(
            "POST",
            base_url + "/api/local_uploader/update",
            {"X-Local-Uploader-Token": LOCAL_UPLOADER_TOKEN},
            {"video_id": video_id, "upload_status": "failed", "error": f"{type(e).__name__}: {e}"},
        )


async def main():
    base_url = _base_url()
    if not base_url:
        raise SystemExit("LOCAL_UPLOADER_BASE_URL missing")
    if not LOCAL_UPLOADER_TOKEN:
        raise SystemExit("LOCAL_UPLOADER_TOKEN missing")
    if not USERBOT_API_ID or not USERBOT_API_HASH:
        raise SystemExit("USERBOT_API_ID/USERBOT_API_HASH missing")

    sess = None
    if USERBOT_STRING_SESSION:
        sess = StringSession(USERBOT_STRING_SESSION)
    name = USERBOT_SESSION_NAME or "pv_userbot"

    async with TelegramClient(sess or name, USERBOT_API_ID, USERBOT_API_HASH) as client:
        while True:
            try:
                data = _http_json(
                    "GET",
                    base_url + "/api/local_uploader/claim",
                    {"X-Local-Uploader-Token": LOCAL_UPLOADER_TOKEN},
                    None,
                )
                job = (data.get("job") or None) if isinstance(data, dict) else None
            except Exception:
                job = None
            if not job:
                await asyncio.sleep(5)
                continue
            await _process_job(client, base_url, job)
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())

