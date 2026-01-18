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
    PAID_CHANNEL_ID,
    USERBOT_CLIP_RANDOM,
    USERBOT_CLIP_SECONDS,
)


_LOCAL_ENV_LOADED = False


def _maybe_load_local_env():
    global _LOCAL_ENV_LOADED
    if _LOCAL_ENV_LOADED:
        return
    base = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base, "local_userbot.env")
    if not os.path.exists(env_path):
        _LOCAL_ENV_LOADED = True
        return
    try:
        with open(env_path, "r", encoding="utf-8-sig") as f:
            for line in f:
                s = (line or "").strip()
                if not s or s.startswith("#"):
                    continue
                if "=" not in s:
                    continue
                k, v = s.split("=", 1)
                k = (k or "").strip().lstrip("\ufeff")
                if not k:
                    continue
                if os.getenv(k, "").strip():
                    continue
                os.environ[k] = (v or "").strip()
    except Exception:
        _LOCAL_ENV_LOADED = True
        return
    _LOCAL_ENV_LOADED = True


_maybe_load_local_env()


def _base_url() -> str:
    return (os.getenv("LOCAL_UPLOADER_BASE_URL", "") or os.getenv("LOCAL_USERBOT_BASE_URL", "") or "").strip().rstrip("/")


def _work_dir() -> str:
    return (os.getenv("LOCAL_UPLOADER_DIR", "") or os.getenv("LOCAL_USERBOT_UPLOAD_DIR", "") or "").strip() or "."


def _api_id() -> int:
    v = (os.getenv("LOCAL_USERBOT_API_ID", "") or "").strip()
    return int(v) if v else 0


def _api_hash() -> str:
    return (os.getenv("LOCAL_USERBOT_API_HASH", "") or "").strip()


def _string_session() -> str:
    return (os.getenv("LOCAL_USERBOT_STRING_SESSION", "") or "").strip()


def _token() -> str:
    return (os.getenv("LOCAL_UPLOADER_TOKEN", "") or "").strip()


def _paid_channel_id() -> int:
    if PAID_CHANNEL_ID:
        return int(PAID_CHANNEL_ID)
    v = (os.getenv("LOCAL_USERBOT_UPLOAD_CHANNEL_ID", "") or os.getenv("LOCAL_USERBOT_CHANNEL_ID", "") or "").strip()
    return int(v) if v else 0


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
    base = []
    if HIGHLIGHT_CHANNEL_ID:
        base.append(int(HIGHLIGHT_CHANNEL_ID))
    for ch in list(FREE_CHANNEL_IDS or []):
        if ch:
            base.append(int(ch))
    if not base:
        fallback = (os.getenv("LOCAL_USERBOT_DOWNLOAD_CHANNEL_ID", "") or os.getenv("LOCAL_USERBOT_CHANNEL_ID", "") or "").strip()
        if fallback:
            try:
                base.append(int(fallback))
            except Exception:
                pass
    for ch in base:
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
            {"X-Local-Uploader-Token": _token()},
            {"video_id": video_id, "upload_status": "failed", "error": f"file not found: {src_path}"},
        )
        return

    try:
        sent = await client.send_file(_paid_channel_id(), src_path, caption=caption, supports_streaming=True)
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
            {"X-Local-Uploader-Token": _token()},
            {
                "video_id": video_id,
                "upload_status": "done",
                "channel_id": int(_paid_channel_id()),
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
            {"X-Local-Uploader-Token": _token()},
            {"video_id": video_id, "upload_status": "failed", "error": f"{type(e).__name__}: {e}"},
        )


async def main():
    base_url = _base_url()
    if not base_url:
        raise SystemExit("LOCAL_UPLOADER_BASE_URL missing")
    if not _token():
        raise SystemExit("LOCAL_UPLOADER_TOKEN missing")
    if not _api_id() or not _api_hash():
        raise SystemExit("LOCAL_USERBOT_API_ID/LOCAL_USERBOT_API_HASH missing")
    if not _paid_channel_id():
        raise SystemExit("PAID_CHANNEL_ID missing (or LOCAL_USERBOT_UPLOAD_CHANNEL_ID)")

    sess = None
    ss = _string_session()
    if ss:
        sess = StringSession(ss)
    name = "pv_local_uploader"

    async with TelegramClient(sess or name, _api_id(), _api_hash()) as client:
        print(f"[local_uploader] start base_url={base_url} work_dir={_work_dir()} paid_channel_id={_paid_channel_id()}")
        last_idle = 0.0
        while True:
            try:
                data = _http_json(
                    "GET",
                    base_url + "/api/local_uploader/claim",
                    {"X-Local-Uploader-Token": _token()},
                    None,
                )
                job = (data.get("job") or None) if isinstance(data, dict) else None
            except Exception:
                job = None
            if not job:
                now = time.time()
                if now - last_idle >= 60:
                    print("[local_uploader] idle (no pending upload jobs)")
                    last_idle = now
                await asyncio.sleep(5)
                continue
            try:
                print(f"[local_uploader] claimed video_id={job.get('id')} local_filename={job.get('local_filename')}")
            except Exception:
                pass
            await _process_job(client, base_url, job)
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())

