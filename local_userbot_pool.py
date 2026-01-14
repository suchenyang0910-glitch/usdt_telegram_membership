import asyncio
import json
import os
import shutil
import logging
import subprocess
from dataclasses import dataclass
from datetime import datetime
import time

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FileReferenceExpiredError

logger = logging.getLogger("local_userbot")


def _maybe_load_local_env():
    if os.getenv("LOCAL_USERBOT_API_ID", "").strip():
        return
    base = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base, "local_userbot.env")
    if not os.path.exists(env_path):
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
        return


_maybe_load_local_env()


@dataclass
class Settings:
    api_id: int
    api_hash: str
    download_channel_id: int
    upload_channel_id: int
    monitor_chat: str
    root: str
    poll_minutes: int
    upload_minutes: int
    upload_dir: str
    uploaded_dir: str
    state_file: str
    sessions: list[str]
    heartbeat_minutes: int
    restart_backoff_min_sec: int
    restart_backoff_max_sec: int
    max_clients: int
    download_timeout_sec: int
    download_stall_timeout_sec: int
    min_download_kbps: int
    max_download_timeout_sec: int
    fetch_limit: int
    validate_media: bool
    transcode_h264: bool
    ffprobe_bin: str
    ffmpeg_bin: str


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    return int(v)


def _load_sessions() -> list[str]:
    p = os.getenv("LOCAL_USERBOT_SESSIONS_FILE", "").strip()
    if p:
        if not os.path.exists(p):
            os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
            raise SystemExit(f"sessions file not found: {p}\nPlease create it and add one StringSession per line.")
        out: list[str] = []
        with open(p, "r", encoding="utf-8-sig") as f:
            for line in f:
                s = (line or "").strip().lstrip("\ufeff")
                if s.startswith("#"):
                    continue
                if s:
                    out.append(s)
        return out

    raw = os.getenv("LOCAL_USERBOT_STRING_SESSIONS_JSON", "").strip()
    if raw:
        data = json.loads(raw)
        return [str(x).strip() for x in data if str(x).strip()]

    raise SystemExit("missing LOCAL_USERBOT_SESSIONS_FILE or LOCAL_USERBOT_STRING_SESSIONS_JSON")


def _bin_exists(path: str) -> bool:
    p = (path or "").strip()
    if not p:
        return False
    if os.path.isabs(p) or (os.sep in p) or ("/" in p):
        return os.path.exists(p)
    return shutil.which(p) is not None


def load_settings() -> Settings:
    api_id = _env_int("LOCAL_USERBOT_API_ID", 0)
    api_hash = os.getenv("LOCAL_USERBOT_API_HASH", "").strip()
    legacy_channel_id = int(os.getenv("LOCAL_USERBOT_CHANNEL_ID", "0").strip() or 0)
    download_channel_id = int(os.getenv("LOCAL_USERBOT_DOWNLOAD_CHANNEL_ID", "0").strip() or 0)
    upload_channel_id = int(os.getenv("LOCAL_USERBOT_UPLOAD_CHANNEL_ID", "0").strip() or 0)
    if not download_channel_id and legacy_channel_id:
        download_channel_id = legacy_channel_id
    if not upload_channel_id and legacy_channel_id:
        upload_channel_id = legacy_channel_id
    root = os.getenv("LOCAL_USERBOT_ROOT", r"E:\资源\userbot").strip()
    poll_minutes = _env_int("LOCAL_USERBOT_POLL_MINUTES", 15)
    upload_minutes = _env_int("LOCAL_USERBOT_UPLOAD_MINUTES", 30)
    monitor_chat = os.getenv("LOCAL_USERBOT_MONITOR_CHAT", "").strip()
    heartbeat_minutes = _env_int("LOCAL_USERBOT_HEARTBEAT_MINUTES", 10)
    restart_backoff_min_sec = _env_int("LOCAL_USERBOT_RESTART_BACKOFF_MIN_SEC", 5)
    restart_backoff_max_sec = _env_int("LOCAL_USERBOT_RESTART_BACKOFF_MAX_SEC", 300)
    max_clients = _env_int("LOCAL_USERBOT_MAX_CLIENTS", 5)
    download_timeout_sec = _env_int("LOCAL_USERBOT_DOWNLOAD_TIMEOUT_SEC", 900)
    download_stall_timeout_sec = _env_int("LOCAL_USERBOT_DOWNLOAD_STALL_TIMEOUT_SEC", 180)
    min_download_kbps = _env_int("LOCAL_USERBOT_MIN_DOWNLOAD_KBPS", 128)
    max_download_timeout_sec = _env_int("LOCAL_USERBOT_MAX_DOWNLOAD_TIMEOUT_SEC", 7200)
    fetch_limit = _env_int("LOCAL_USERBOT_FETCH_LIMIT", 50)
    validate_media = os.getenv("LOCAL_USERBOT_VALIDATE_MEDIA", "1").strip() == "1"
    transcode_h264 = os.getenv("LOCAL_USERBOT_TRANSCODE_H264", "0").strip() == "1"
    ffprobe_bin = os.getenv("LOCAL_USERBOT_FFPROBE_BIN", "ffprobe").strip()
    ffmpeg_bin = os.getenv("LOCAL_USERBOT_FFMPEG_BIN", "ffmpeg").strip()

    upload_dir = os.getenv("LOCAL_USERBOT_UPLOAD_DIR", os.path.join(root, "upload_queue")).strip()
    uploaded_dir = os.getenv("LOCAL_USERBOT_UPLOADED_DIR", os.path.join(root, "uploaded")).strip()
    state_file = os.getenv("LOCAL_USERBOT_STATE_FILE", os.path.join(root, "state.json")).strip()
    sessions = _load_sessions()

    if not api_id or not api_hash:
        raise SystemExit("missing LOCAL_USERBOT_API_ID/LOCAL_USERBOT_API_HASH")
    if not download_channel_id:
        raise SystemExit("missing LOCAL_USERBOT_DOWNLOAD_CHANNEL_ID (or LOCAL_USERBOT_CHANNEL_ID)")
    if not upload_channel_id:
        raise SystemExit("missing LOCAL_USERBOT_UPLOAD_CHANNEL_ID (or LOCAL_USERBOT_CHANNEL_ID)")
    if len(sessions) < 1:
        raise SystemExit("need at least 1 session")
    if transcode_h264 and not _bin_exists(ffmpeg_bin):
        raise SystemExit("LOCAL_USERBOT_TRANSCODE_H264=1 but ffmpeg not found. Set LOCAL_USERBOT_FFMPEG_BIN to full path.")
    return Settings(
        api_id=api_id,
        api_hash=api_hash,
        download_channel_id=download_channel_id,
        upload_channel_id=upload_channel_id,
        monitor_chat=monitor_chat,
        root=root,
        poll_minutes=poll_minutes,
        upload_minutes=upload_minutes,
        upload_dir=upload_dir,
        uploaded_dir=uploaded_dir,
        state_file=state_file,
        sessions=sessions,
        heartbeat_minutes=max(1, heartbeat_minutes),
        restart_backoff_min_sec=max(1, restart_backoff_min_sec),
        restart_backoff_max_sec=max(restart_backoff_min_sec, restart_backoff_max_sec),
        max_clients=max(1, max_clients),
        download_timeout_sec=max(60, download_timeout_sec),
        download_stall_timeout_sec=max(30, download_stall_timeout_sec),
        min_download_kbps=max(16, min_download_kbps),
        max_download_timeout_sec=max(download_timeout_sec, max_download_timeout_sec),
        fetch_limit=max(10, min(500, fetch_limit)),
        validate_media=validate_media,
        transcode_h264=transcode_h264,
        ffprobe_bin=ffprobe_bin or "ffprobe",
        ffmpeg_bin=ffmpeg_bin or "ffmpeg",
    )


def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


def _calc_overall_timeout_sec(total_bytes: int, base_timeout_sec: int, min_kbps: int, max_timeout_sec: int) -> int:
    base = max(60, int(base_timeout_sec or 0))
    cap = max(base, int(max_timeout_sec or base))
    kbps = max(16, int(min_kbps or 0))
    if not total_bytes or total_bytes <= 0:
        return base
    est = int((float(total_bytes) / float(kbps * 1024)) * 2.0 + 60.0)
    return min(cap, max(base, est))


async def _download_media_with_timeouts(
    client: TelegramClient,
    msg,
    folder: str,
    base_timeout_sec: int,
    stall_timeout_sec: int,
    min_kbps: int,
    max_timeout_sec: int,
):
    total = int(getattr(getattr(msg, "file", None), "size", None) or 0)
    overall = _calc_overall_timeout_sec(total, base_timeout_sec, min_kbps, max_timeout_sec)
    stall = max(30, int(stall_timeout_sec or 0))
    loop = asyncio.get_running_loop()
    started = loop.time()
    last_progress = started
    last_bytes = 0

    def _progress(cur: int, tot: int):
        nonlocal last_progress, last_bytes
        cur_i = int(cur or 0)
        if cur_i != last_bytes:
            last_bytes = cur_i
            last_progress = loop.time()

    task = asyncio.create_task(client.download_media(msg, file=folder, progress_callback=_progress))
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=5)
            if task in done:
                return await task
            now = loop.time()
            if now - started > float(overall):
                task.cancel()
                raise asyncio.TimeoutError("download overall timeout")
            if now - last_progress > float(stall):
                task.cancel()
                raise asyncio.TimeoutError("download stalled")
    finally:
        if not task.done():
            task.cancel()


def setup_local_logging(root: str):
    _ensure_dir(os.path.join(root, "logs"))
    level = os.getenv("LOCAL_USERBOT_LOG_LEVEL", "INFO").upper()
    lvl = getattr(logging, level, logging.INFO)

    log_path = os.path.join(root, "logs", "local_userbot.log")
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setLevel(lvl)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s | %(message)s"))

    sh = logging.StreamHandler()
    sh.setLevel(lvl)
    sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s | %(message)s"))

    root_logger = logging.getLogger()
    if getattr(root_logger, "_local_userbot_logging_configured", False):
        return
    root_logger.setLevel(lvl)
    root_logger.addHandler(handler)
    root_logger.addHandler(sh)
    root_logger._local_userbot_logging_configured = True


class Notifier:
    def __init__(self, client: TelegramClient, monitor_chat: str):
        self._client = client
        self._monitor_chat = monitor_chat
        self._entity = None

    async def init(self):
        if not self._monitor_chat:
            return
        target = self._monitor_chat
        try:
            if isinstance(target, str) and target.strip() and (target.strip().lstrip("-").isdigit()):
                target = int(target.strip())
        except Exception:
            target = self._monitor_chat
        try:
            self._entity = await self._client.get_entity(target)
        except Exception:
            self._entity = None

    async def send(self, text: str):
        if not self._entity:
            return
        try:
            await self._client.send_message(self._entity, text)
        except Exception:
            return


def _load_state(path: str) -> set[int]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        ids = data.get("downloaded_message_ids") or []
        return {int(x) for x in ids}
    except Exception:
        return set()


def _save_state(path: str, ids: set[int]):
    _ensure_dir(os.path.dirname(path) or ".")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"downloaded_message_ids": sorted(list(ids))[-200000:]}, f, ensure_ascii=False)
    except Exception:
        return


def _folder_name(dt: datetime, msg_id: int) -> str:
    local = dt.astimezone()
    return f"{local.strftime('%Y%m%d_%H%M%S')}_{msg_id}"


async def _download_one(
    client: TelegramClient, channel_id: int, msg_id: int, base_dir: str, timeout_sec: int
) -> tuple[bool, str]:
    try:
        msg = await client.get_messages(channel_id, ids=msg_id)
    except Exception as e:
        return False, f"get_messages failed: {type(e).__name__}: {e}"
    if not msg:
        return False, "message missing"
    if not msg.file:
        return False, "no file"

    folder = os.path.join(base_dir, _folder_name(datetime.now(), int(msg.id)))
    _ensure_dir(folder)

    text = msg.message or ""
    try:
        with open(os.path.join(folder, "message.txt"), "w", encoding="utf-8") as f:
            f.write(text)
    except Exception:
        pass

    try:
        await _download_media_with_timeouts(
            client,
            msg,
            folder=folder,
            base_timeout_sec=int(timeout_sec),
            stall_timeout_sec=_env_int("LOCAL_USERBOT_DOWNLOAD_STALL_TIMEOUT_SEC", 180),
            min_kbps=_env_int("LOCAL_USERBOT_MIN_DOWNLOAD_KBPS", 128),
            max_timeout_sec=_env_int("LOCAL_USERBOT_MAX_DOWNLOAD_TIMEOUT_SEC", 7200),
        )
        return True, folder
    except asyncio.TimeoutError as e:
        try:
            shutil.rmtree(folder, ignore_errors=True)
        except Exception:
            pass
        return False, (str(e) or "download timeout")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        return False, f"download failed: {type(e).__name__}: {e}"


def _is_video_file(name: str) -> bool:
    n = name.lower()
    return n.endswith((".mp4", ".mkv", ".mov", ".webm", ".m4v", ".avi"))

def _run_cmd(cmd: list[str], timeout_sec: int) -> tuple[int, str]:
    try:
        res = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_sec,
        )
        out = (res.stdout or "") + (("\n" + res.stderr) if res.stderr else "")
        return int(res.returncode), out.strip()
    except Exception as e:
        return 99, f"{type(e).__name__}: {e}"


def _ffprobe_video_codec(ffprobe_bin: str, file_path: str) -> tuple[bool, str]:
    rc, out = _run_cmd(
        [
            ffprobe_bin,
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_streams",
            "-show_format",
            file_path,
        ],
        timeout_sec=15,
    )
    if rc != 0:
        if "FileNotFoundError" in out or "No such file or directory" in out:
            return True, "unknown"
        return False, f"ffprobe failed: {out[:200]}"
    try:
        data = json.loads(out or "{}")
        streams = data.get("streams") or []
        vstreams = [s for s in streams if (s.get("codec_type") == "video")]
        if not vstreams:
            return False, "no video stream"
        codec = (vstreams[0].get("codec_name") or "").lower()
        return True, codec
    except Exception as e:
        return False, f"ffprobe parse failed: {type(e).__name__}: {e}"


def _transcode_to_h264(ffmpeg_bin: str, src: str) -> tuple[bool, str]:
    dst = os.path.splitext(src)[0] + "_h264.mp4"
    rc, out = _run_cmd(
        [
            ffmpeg_bin,
            "-y",
            "-i",
            src,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-movflags",
            "+faststart",
            dst,
        ],
        timeout_sec=3600,
    )
    if rc != 0 or not os.path.exists(dst):
        try:
            if os.path.exists(dst):
                os.remove(dst)
        except Exception:
            pass
        return False, out[:300]
    try:
        os.remove(src)
    except Exception:
        pass
    return True, dst


def _list_video_files(folder: str) -> list[str]:
    out = []
    try:
        for name in os.listdir(folder):
            fp = os.path.join(folder, name)
            if os.path.isfile(fp) and _is_video_file(name):
                out.append(fp)
    except Exception:
        return []
    out.sort(key=lambda p: os.path.getsize(p) if os.path.exists(p) else 0, reverse=True)
    return out


def _is_media_message(msg) -> bool:
    try:
        if getattr(msg, "video", None):
            return True
        if getattr(msg, "photo", None):
            return True
        f = getattr(msg, "file", None)
        if not f:
            return False
        mime = (getattr(f, "mime_type", None) or "").lower()
        if mime.startswith("video/"):
            return True
        if mime.startswith("image/"):
            return True
        name = (getattr(f, "name", None) or "").lower()
        if name and _is_video_file(name):
            return True
    except Exception:
        return False
    return False

def _pick_downloaded_media_path(folder: str) -> str | None:
    try:
        entries = []
        for name in os.listdir(folder):
            fp = os.path.join(folder, name)
            if not os.path.isfile(fp):
                continue
            if name.lower() == "message.txt":
                continue
            try:
                if os.path.getsize(fp) <= 0:
                    continue
            except Exception:
                continue
            entries.append(fp)
        if not entries:
            return None
        entries.sort(key=lambda p: os.path.getsize(p), reverse=True)
        return entries[0]
    except Exception:
        return None


async def _download_many(
    client: TelegramClient,
    channel_id: int,
    messages: list,
    base_dir: str,
    timeout_sec: int,
) -> tuple[bool, str, int, int, list[int], list[int], list[int], str]:
    if not messages:
        return False, "empty group", 0, 0, [], [], [], "empty group"

    ids = sorted({int(getattr(m, "id", 0) or 0) for m in messages if int(getattr(m, "id", 0) or 0)})
    rep_id = min(ids) if ids else 0
    folder = os.path.join(base_dir, _folder_name(datetime.now(), rep_id))
    _ensure_dir(folder)

    try:
        refreshed = await client.get_messages(channel_id, ids=ids)
        if not isinstance(refreshed, list):
            refreshed = [refreshed] if refreshed else []
        refreshed = [m for m in refreshed if m]
    except Exception:
        refreshed = list(messages)

    texts = []
    for m in refreshed:
        t = (getattr(m, "message", None) or "").strip()
        if t:
            texts.append(t)
    try:
        with open(os.path.join(folder, "message.txt"), "w", encoding="utf-8") as f:
            f.write("\n\n".join(texts).strip())
    except Exception:
        pass

    images = 0
    videos = 0
    succeeded_ids: list[int] = []
    skipped_ids: list[int] = []
    failed_ids: list[int] = []
    fail_reason = ""
    try:
        for m in sorted(refreshed, key=lambda x: int(getattr(x, "id", 0) or 0)):
            mid = int(getattr(m, "id", 0) or 0)
            expect_size = int(getattr(getattr(m, "file", None), "size", None) or 0)
            is_video = bool(getattr(m, "video", None))
            if not is_video:
                f = getattr(m, "file", None)
                mime = (getattr(f, "mime_type", None) or "").lower()
                is_video = mime.startswith("video/")
            is_photo = bool(getattr(m, "photo", None))
            if not is_photo:
                f = getattr(m, "file", None)
                mime = (getattr(f, "mime_type", None) or "").lower()
                is_photo = mime.startswith("image/")

            downloaded_path = None
            timed_out = False
            for attempt in range(2):
                try:
                    await _download_media_with_timeouts(
                        client,
                        m,
                        folder=folder,
                        base_timeout_sec=int(timeout_sec),
                        stall_timeout_sec=_env_int("LOCAL_USERBOT_DOWNLOAD_STALL_TIMEOUT_SEC", 180),
                        min_kbps=_env_int("LOCAL_USERBOT_MIN_DOWNLOAD_KBPS", 128),
                        max_timeout_sec=_env_int("LOCAL_USERBOT_MAX_DOWNLOAD_TIMEOUT_SEC", 7200),
                    )
                    downloaded_path = _pick_downloaded_media_path(folder)
                    break
                except FileReferenceExpiredError:
                    try:
                        refreshed_one = await client.get_messages(channel_id, ids=mid)
                        if refreshed_one:
                            m = refreshed_one
                    except Exception:
                        pass
                    continue
                except asyncio.TimeoutError as e:
                    failed_ids.append(mid)
                    fail_reason = str(e) or "download timeout"
                    timed_out = True
                    break

            if timed_out:
                continue
            if not downloaded_path:
                skipped_ids.append(mid)
                continue

            if isinstance(downloaded_path, str) and os.path.exists(downloaded_path) and expect_size:
                try:
                    actual = os.path.getsize(downloaded_path)
                except Exception:
                    actual = 0
                if actual and actual < int(expect_size * 0.95):
                    failed_ids.append(mid)
                    fail_reason = f"size mismatch expect={expect_size} actual={actual}"
                    continue

            succeeded_ids.append(mid)
            if is_video:
                videos += 1
            elif is_photo:
                images += 1

        if not _pick_downloaded_media_path(folder):
            fail_reason = "no media files"
            return False, folder, images, videos, succeeded_ids, skipped_ids, failed_ids, fail_reason

        if failed_ids:
            return False, folder, images, videos, succeeded_ids, skipped_ids, failed_ids, (fail_reason or "some files invalid")

        if not succeeded_ids and skipped_ids:
            return False, folder, images, videos, succeeded_ids, skipped_ids, failed_ids, "all media skipped (file reference expired)"

        return True, folder, images, videos, succeeded_ids, skipped_ids, failed_ids, ""

    except asyncio.TimeoutError as e:
        try:
            shutil.rmtree(folder, ignore_errors=True)
        except Exception:
            pass
        return False, "download timeout", images, videos, succeeded_ids, skipped_ids, failed_ids, (str(e) or "download timeout")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        try:
            shutil.rmtree(folder, ignore_errors=True)
        except Exception:
            pass
        return False, f"download failed: {type(e).__name__}: {e}", images, videos, succeeded_ids, skipped_ids, failed_ids, str(e)


def _validate_and_fix_videos(s: Settings, folder: str) -> tuple[bool, str, int]:
    videos = _list_video_files(folder)
    if not videos:
        return True, "", 0

    transcoded = 0
    for fp in videos:
        ok, codec_or_err = _ffprobe_video_codec(s.ffprobe_bin, fp)
        if not ok:
            return False, f"{os.path.basename(fp)} invalid: {codec_or_err}", transcoded
        codec = codec_or_err
        if s.transcode_h264 and codec in ("hevc", "h265", "av1"):
            ok2, out2 = _transcode_to_h264(s.ffmpeg_bin, fp)
            if not ok2:
                return False, f"transcode failed: {os.path.basename(fp)} {out2}", transcoded
            transcoded += 1
    return True, "", transcoded


async def _download_many_with_postcheck(
    client: TelegramClient,
    channel_id: int,
    messages: list,
    base_dir: str,
    timeout_sec: int,
    settings: Settings,
) -> tuple[bool, str, int, int, list[int], list[int], list[int], str, int]:
    ok, info, images, videos, ok_ids, skipped_ids, failed_ids, reason = await _download_many(
        client, channel_id, messages, base_dir, timeout_sec
    )
    if not ok:
        return ok, info, images, videos, ok_ids, skipped_ids, failed_ids, reason, 0
    if not settings.validate_media:
        return ok, info, images, videos, ok_ids, skipped_ids, failed_ids, reason, 0
    ok2, err2, transcoded = _validate_and_fix_videos(settings, info)
    if not ok2:
        try:
            shutil.rmtree(info, ignore_errors=True)
        except Exception:
            pass
        return False, info, images, videos, ok_ids, skipped_ids, failed_ids, err2, transcoded
    return ok, info, images, videos, ok_ids, skipped_ids, failed_ids, reason, transcoded


async def download_loop(clients: list[TelegramClient], s: Settings):
    base_dir = os.path.join(s.root, "downloads")
    _ensure_dir(base_dir)
    downloaded = _load_state(s.state_file)
    notifier = Notifier(clients[0], s.monitor_chat)
    await notifier.init()

    while True:
        try:
            head = clients[0]
            await notifier.send("开始匹配频道资源")
            try:
                msgs = await head.get_messages(s.download_channel_id, limit=int(s.fetch_limit))
            except Exception as e:
                logger.warning("get_messages failed: %s: %s", type(e).__name__, e)
                await notifier.send(f"匹配频道资源失败：{type(e).__name__}: {e}")
                await asyncio.sleep(max(60, s.poll_minutes * 60))
                continue
            groups: dict[int, list] = {}
            for m in msgs:
                if not m or not getattr(m, "file", None):
                    continue
                if not _is_media_message(m):
                    continue
                key = int(getattr(m, "grouped_id", None) or int(m.id))
                groups.setdefault(key, []).append(m)

            todo_groups: list[tuple[int, list]] = []
            for key, items in groups.items():
                ids = [int(getattr(x, "id", 0) or 0) for x in items]
                if any(mid and mid not in downloaded for mid in ids):
                    todo_groups.append((key, items))
            todo_groups.sort(key=lambda x: min(int(getattr(m, "id", 0) or 0) for m in x[1]))

            media_msgs = sum(len(v) for v in groups.values())
            await notifier.send(
                f"匹配完成：拉取{len(msgs)}条，媒体{media_msgs}条，分组{len(groups)}组，待下载{len(todo_groups)}组"
            )

            for idx, (_, items) in enumerate(todo_groups):
                c = clients[idx % len(clients)]
                try:
                    rep = max(items, key=lambda x: int(getattr(getattr(x, "file", None), "size", None) or 0))
                    fname = (getattr(getattr(rep, "file", None), "name", None) or "").strip()
                    rep_id = int(getattr(rep, "id", 0) or 0)
                    title = fname or f"msg_{rep_id}"
                    cap = ""
                    for m in items:
                        t = (getattr(m, "message", None) or "").strip()
                        if t:
                            cap = t
                            break
                except Exception:
                    title = "media_group"
                    cap = ""
                await notifier.send(f"开始下载：{title}\n文案：{cap[:800]}")
                t0 = time.time()
                ok, info, images, videos, ok_ids, skipped_ids, failed_ids, reason, transcoded = await _download_many_with_postcheck(
                    c, s.download_channel_id, items, base_dir, s.download_timeout_sec, s
                )
                dt = time.time() - t0
                if ok:
                    for mid in ok_ids + skipped_ids:
                        if mid:
                            downloaded.add(int(mid))
                    _save_state(s.state_file, downloaded)
                    logger.info(
                        "download ok title=%s path=%s cost=%.1fs images=%s videos=%s skipped=%s",
                        title,
                        info,
                        dt,
                        images,
                        videos,
                        len(skipped_ids),
                    )
                    skipped_line = f"\n跳过：{len(skipped_ids)}" if skipped_ids else ""
                    transcoded_line = f"\n转码：{transcoded}" if transcoded else ""
                    await notifier.send(
                        f"完成下载：{title}\n用时：{dt:.1f}s\n图片：{images} 视频：{videos}{skipped_line}{transcoded_line}\n路径：{info}"
                    )
                else:
                    if reason == "all media skipped (file reference expired)":
                        for mid in ok_ids + skipped_ids:
                            if mid:
                                downloaded.add(int(mid))
                        _save_state(s.state_file, downloaded)
                        logger.warning("download skipped all title=%s cost=%.1fs ids=%s", title, dt, len(skipped_ids))
                        await notifier.send(f"下载跳过（媒体不可下载/已过期）：{title}\n用时：{dt:.1f}s\n跳过：{len(skipped_ids)}")
                        continue
                    if isinstance(info, str) and os.path.isdir(info):
                        try:
                            shutil.rmtree(info, ignore_errors=True)
                        except Exception:
                            pass
                    logger.warning("download failed title=%s cost=%.1fs err=%s", title, dt, (reason or info))
                    await notifier.send(f"下载失败：{title}\n用时：{dt:.1f}s\n原因：{reason or info}")
                    await asyncio.sleep(2)
        except Exception:
            logger.exception("download_loop error")

        await asyncio.sleep(max(60, s.poll_minutes * 60))


def _read_text_file(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return (f.read() or "").strip()
    except Exception:
        return ""


def _find_first_video(path: str) -> str | None:
    try:
        for name in os.listdir(path):
            fp = os.path.join(path, name)
            if os.path.isfile(fp) and _is_video_file(fp):
                return fp
    except Exception:
        return None
    return None


async def upload_loop(client: TelegramClient, s: Settings):
    _ensure_dir(s.upload_dir)
    _ensure_dir(s.uploaded_dir)
    notifier = Notifier(client, s.monitor_chat)
    await notifier.init()

    while True:
        try:
            folders = []
            files = []
            for name in os.listdir(s.upload_dir):
                fp = os.path.join(s.upload_dir, name)
                if os.path.isdir(fp):
                    folders.append(fp)
                elif os.path.isfile(fp) and _is_video_file(fp):
                    files.append(fp)
            files.sort(key=lambda x: os.path.getmtime(x))
            folders.sort(key=lambda x: os.path.getmtime(x))

            if folders:
                folder = folders[0]
                await notifier.send(f"开始匹配本地未上传文件夹：{os.path.basename(folder)}")
                video_fp = _find_first_video(folder)
                if not video_fp or not os.path.exists(video_fp) or os.path.getsize(video_fp) <= 0:
                    await notifier.send(f"文件夹视频异常，跳过：{folder}")
                else:
                    caption = _read_text_file(os.path.join(folder, "message.txt"))
                    title = os.path.basename(video_fp)
                    await notifier.send(f"开始上传：{title}\n文案：{caption[:800]}")
                    t0 = time.time()
                    await client.send_file(s.upload_channel_id, video_fp, caption=caption or None, supports_streaming=True)
                    dt = time.time() - t0
                    logger.info("upload ok title=%s cost=%.1fs", title, dt)
                    await notifier.send(f"完成上传：{title}\n用时：{dt:.1f}s")
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    dst = os.path.join(s.uploaded_dir, f"{ts}_{os.path.basename(folder)}")
                    try:
                        shutil.move(folder, dst)
                    except Exception:
                        pass

            elif files:
                fp = files[0]
                caption = _read_text_file(os.path.splitext(fp)[0] + ".txt")
                await notifier.send(f"开始上传：{os.path.basename(fp)}\n文案：{caption[:800]}")
                t0 = time.time()
                await client.send_file(s.upload_channel_id, fp, caption=caption or None, supports_streaming=True)
                dt = time.time() - t0
                logger.info("upload ok title=%s cost=%.1fs", os.path.basename(fp), dt)
                await notifier.send(f"完成上传：{os.path.basename(fp)}\n用时：{dt:.1f}s")
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                dst = os.path.join(s.uploaded_dir, f"{ts}_{os.path.basename(fp)}")
                try:
                    shutil.move(fp, dst)
                except Exception:
                    try:
                        os.remove(fp)
                    except Exception:
                        pass
                txt = os.path.splitext(fp)[0] + ".txt"
                if os.path.exists(txt):
                    try:
                        os.remove(txt)
                    except Exception:
                        pass
        except Exception:
            logger.exception("upload_loop error")

        await asyncio.sleep(max(60, s.upload_minutes * 60))


async def heartbeat_loop(clients: list[TelegramClient], notifier: Notifier, s: Settings):
    while True:
        try:
            ok = 0
            for c in clients:
                if c.is_connected():
                    ok += 1
            await notifier.send(
                "本地端运行中\n"
                f"客户端在线：{ok}/{len(clients)}\n"
                f"下载频道：{s.download_channel_id}\n"
                f"上传频道：{s.upload_channel_id}\n"
                f"下载轮询：{s.poll_minutes} 分钟\n"
                f"上传轮询：{s.upload_minutes} 分钟"
            )
        except Exception:
            logger.exception("heartbeat_loop error")
        await asyncio.sleep(max(60, s.heartbeat_minutes * 60))


async def client_watchdog_loop(clients: list[TelegramClient], notifier: Notifier):
    while True:
        for idx, c in enumerate(clients):
            try:
                if not c.is_connected():
                    await c.connect()
                    await notifier.send(f"客户端重连成功 idx={idx}")
                if not await c.is_user_authorized():
                    await notifier.send(f"客户端未授权 idx={idx}")
            except Exception as e:
                await notifier.send(f"客户端异常 idx={idx} err={type(e).__name__}: {e}")
        await asyncio.sleep(60)


async def run_forever():
    s = load_settings()
    setup_local_logging(s.root)
    _ensure_dir(s.root)

    backoff = s.restart_backoff_min_sec
    while True:
        clients: list[TelegramClient] = []
        notifier = None
        try:
            for i, sess in enumerate(s.sessions[: s.max_clients]):
                client = TelegramClient(StringSession(sess), s.api_id, s.api_hash)
                await client.connect()
                if not await client.is_user_authorized():
                    raise SystemExit(f"session[{i}] not authorized")
                clients.append(client)

            notifier = Notifier(clients[0], s.monitor_chat)
            await notifier.init()
            await notifier.send(
                "本地端启动完成\n"
                f"客户端数量：{len(clients)}\n"
                f"下载频道：{s.download_channel_id}\n"
                f"上传频道：{s.upload_channel_id}"
            )

            tasks = [
                asyncio.create_task(download_loop(clients, s)),
                asyncio.create_task(upload_loop(clients[0], s)),
                asyncio.create_task(heartbeat_loop(clients, notifier, s)),
                asyncio.create_task(client_watchdog_loop(clients, notifier)),
            ]
            backoff = s.restart_backoff_min_sec
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            raise
        except SystemExit as e:
            msg = str(e)
            logger.error("fatal: %s", msg)
            if notifier:
                await notifier.send(f"本地端启动失败：{msg}")
            raise
        except Exception as e:
            logger.exception("run crashed")
            if notifier:
                await notifier.send(f"本地端异常退出，将自动重启：{type(e).__name__}: {e}")
        finally:
            for c in clients:
                try:
                    await c.disconnect()
                except Exception:
                    continue
        await asyncio.sleep(backoff)
        backoff = min(s.restart_backoff_max_sec, backoff * 2)


async def main():
    await run_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        raise SystemExit(0)

