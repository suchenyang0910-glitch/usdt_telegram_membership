import asyncio
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

from telethon import TelegramClient
from telethon.sessions import StringSession


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
                if not s or s.startswith("#") or "=" not in s:
                    continue
                k, v = s.split("=", 1)
                k = (k or "").strip().lstrip("\ufeff")
                if not k or os.getenv(k, "").strip():
                    continue
                os.environ[k] = (v or "").strip()
    except Exception:
        return


_maybe_load_local_env()


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    return int(v)


def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


def _read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return (f.read() or "").strip()
    except Exception:
        return ""


def _write_text(path: str, text: str):
    _ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text or "")


def _load_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_json(path: str, data: dict):
    _ensure_dir(os.path.dirname(path) or ".")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def _is_image_file(path: str) -> bool:
    ext = os.path.splitext(path)[1].lower()
    return ext in (".jpg", ".jpeg", ".png", ".webp")


def _is_video_file(path: str) -> bool:
    ext = os.path.splitext(path)[1].lower()
    return ext in (".mp4", ".mov", ".mkv", ".webm", ".m4v")


def _ffprobe_ok(ffprobe_bin: str, path: str) -> tuple[bool, str]:
    try:
        p = subprocess.run(
            [ffprobe_bin, "-v", "error", "-print_format", "json", "-show_format", "-show_streams", path],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if p.returncode != 0:
            return False, (p.stderr or p.stdout or "ffprobe failed").strip()[:400]
        data = json.loads(p.stdout or "{}")
        fmt = data.get("format") or {}
        dur = float((fmt.get("duration") or 0) or 0)
        if dur <= 0.2:
            return False, "duration too small"
        return True, ""
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


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
    base = max(60, int(base_timeout_sec or 0))
    cap = max(base, int(max_timeout_sec or base))
    kbps = max(16, int(min_kbps or 0))
    overall = base if total <= 0 else min(cap, max(base, int((float(total) / float(kbps * 1024)) * 2.0 + 60.0)))
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


@dataclass
class Settings:
    api_id: int
    api_hash: str
    string_session: str
    download_channel_id: int
    upload_channel_id: int
    root: str
    lookback_days: int
    poll_minutes: int
    download_concurrency: int
    state_file: str
    uploaded_dir: str
    ffprobe_bin: str
    download_timeout_sec: int
    download_stall_timeout_sec: int
    min_download_kbps: int
    max_download_timeout_sec: int


def load_settings() -> Settings:
    api_id = _env_int("LOCAL_USERBOT_API_ID", 0)
    api_hash = os.getenv("LOCAL_USERBOT_API_HASH", "").strip()
    string_session = os.getenv("LOCAL_USERBOT_STRING_SESSION", "").strip()
    download_channel_id = int(os.getenv("LOCAL_USERBOT_DOWNLOAD_CHANNEL_ID", "0").strip() or 0)
    upload_channel_id = int(os.getenv("LOCAL_USERBOT_UPLOAD_CHANNEL_ID", "0").strip() or 0)
    root = os.getenv("LOCAL_USERBOT_ROOT", r"E:\资源\userbot").strip()
    lookback_days = _env_int("LOCAL_USERBOT_LOOKBACK_DAYS", 3)
    poll_minutes = _env_int("LOCAL_USERBOT_POLL_MINUTES", 15)
    download_concurrency = _env_int("LOCAL_USERBOT_DOWNLOAD_CONCURRENCY", 8)
    state_file = os.getenv("LOCAL_USERBOT_STATE_FILE", os.path.join(root, "state_single.json")).strip()
    uploaded_dir = os.getenv("LOCAL_USERBOT_UPLOADED_DIR", os.path.join(root, "uploaded")).strip()
    ffprobe_bin = os.getenv("LOCAL_USERBOT_FFPROBE_BIN", "ffprobe").strip() or "ffprobe"
    download_timeout_sec = _env_int("LOCAL_USERBOT_DOWNLOAD_TIMEOUT_SEC", 900)
    download_stall_timeout_sec = _env_int("LOCAL_USERBOT_DOWNLOAD_STALL_TIMEOUT_SEC", 180)
    min_download_kbps = _env_int("LOCAL_USERBOT_MIN_DOWNLOAD_KBPS", 128)
    max_download_timeout_sec = _env_int("LOCAL_USERBOT_MAX_DOWNLOAD_TIMEOUT_SEC", 7200)

    if not api_id or not api_hash:
        raise SystemExit("missing LOCAL_USERBOT_API_ID/LOCAL_USERBOT_API_HASH")
    if not string_session:
        raise SystemExit("missing LOCAL_USERBOT_STRING_SESSION")
    if not download_channel_id:
        raise SystemExit("missing LOCAL_USERBOT_DOWNLOAD_CHANNEL_ID")
    if not upload_channel_id:
        raise SystemExit("missing LOCAL_USERBOT_UPLOAD_CHANNEL_ID")

    return Settings(
        api_id=api_id,
        api_hash=api_hash,
        string_session=string_session,
        download_channel_id=download_channel_id,
        upload_channel_id=upload_channel_id,
        root=root,
        lookback_days=max(1, lookback_days),
        poll_minutes=max(1, poll_minutes),
        download_concurrency=max(1, min(32, download_concurrency)),
        state_file=state_file,
        uploaded_dir=uploaded_dir,
        ffprobe_bin=ffprobe_bin,
        download_timeout_sec=max(60, download_timeout_sec),
        download_stall_timeout_sec=max(30, download_stall_timeout_sec),
        min_download_kbps=max(16, min_download_kbps),
        max_download_timeout_sec=max(download_timeout_sec, max_download_timeout_sec),
    )


def _folder_name(dt: datetime, group_id: int) -> str:
    local = dt.astimezone()
    return f"{local.strftime('%Y%m%d_%H%M%S')}_{group_id}"


def _list_media_files(folder: str) -> tuple[list[str], list[str]]:
    imgs: list[str] = []
    vids: list[str] = []
    try:
        for name in os.listdir(folder):
            fp = os.path.join(folder, name)
            if not os.path.isfile(fp):
                continue
            if _is_image_file(fp):
                imgs.append(fp)
            elif _is_video_file(fp):
                vids.append(fp)
    except Exception:
        return [], []
    imgs.sort(key=lambda p: os.path.getmtime(p))
    vids.sort(key=lambda p: os.path.getmtime(p))
    return imgs, vids


def _state_get(path: str) -> dict:
    data = _load_json(path)
    if "downloaded_message_ids" not in data:
        data["downloaded_message_ids"] = []
    if "uploaded_group_ids" not in data:
        data["uploaded_group_ids"] = []
    if "group_folder" not in data:
        data["group_folder"] = {}
    return data


def _state_save(path: str, data: dict):
    dl = sorted({int(x) for x in (data.get("downloaded_message_ids") or []) if str(x).strip().lstrip("-").isdigit()})[-300000:]
    up = sorted({int(x) for x in (data.get("uploaded_group_ids") or []) if str(x).strip().lstrip("-").isdigit()})[-300000:]
    gf = data.get("group_folder") or {}
    if not isinstance(gf, dict):
        gf = {}
    _save_json(path, {"downloaded_message_ids": dl, "uploaded_group_ids": up, "group_folder": gf})


async def _scan_groups(client: TelegramClient, s: Settings) -> list[tuple[int, list]]:
    since = datetime.utcnow() - timedelta(days=int(s.lookback_days))
    groups: dict[int, list] = {}
    async for m in client.iter_messages(s.download_channel_id, offset_date=None):
        if not m:
            continue
        dt = getattr(m, "date", None)
        if dt and dt.replace(tzinfo=None) < since:
            break
        if not getattr(m, "file", None):
            continue
        key = int(getattr(m, "grouped_id", None) or int(m.id))
        groups.setdefault(key, []).append(m)
    out: list[tuple[int, list]] = []
    for key, items in groups.items():
        items.sort(key=lambda x: int(getattr(x, "id", 0) or 0))
        out.append((key, items))
    out.sort(key=lambda x: min(int(getattr(m, "id", 0) or 0) for m in x[1]))
    return out


async def _download_group(client: TelegramClient, s: Settings, group_id: int, items: list, state: dict) -> bool:
    ids = [int(getattr(x, "id", 0) or 0) for x in items if int(getattr(x, "id", 0) or 0)]
    downloaded = set(int(x) for x in (state.get("downloaded_message_ids") or []))
    if ids and all(mid in downloaded for mid in ids):
        return True

    dt = getattr(items[0], "date", None) or datetime.utcnow()
    base_dir = os.path.join(s.root, "downloads")
    _ensure_dir(base_dir)
    folder = os.path.join(base_dir, _folder_name(dt.replace(tzinfo=None), int(group_id)))
    _ensure_dir(folder)

    cap = ""
    for m in items:
        t = (getattr(m, "message", None) or "").strip()
        if t:
            cap = t
            break
    _write_text(os.path.join(folder, "message.txt"), cap)
    meta = {
        "group_id": int(group_id),
        "download_channel_id": int(s.download_channel_id),
        "message_ids": ids,
        "created_at_utc": datetime.utcnow().isoformat(),
    }
    _save_json(os.path.join(folder, "meta.json"), meta)

    for m in items:
        if not getattr(m, "file", None):
            continue
        await _download_media_with_timeouts(
            client,
            m,
            folder,
            s.download_timeout_sec,
            s.download_stall_timeout_sec,
            s.min_download_kbps,
            s.max_download_timeout_sec,
        )

    imgs, vids = _list_media_files(folder)
    if not imgs and not vids:
        return False

    for fp in vids:
        ok, err = _ffprobe_ok(s.ffprobe_bin, fp)
        if not ok:
            return False

    for mid in ids:
        downloaded.add(int(mid))
    state["downloaded_message_ids"] = sorted(list(downloaded))
    gf = state.get("group_folder") or {}
    if not isinstance(gf, dict):
        gf = {}
    gf[str(int(group_id))] = folder
    state["group_folder"] = gf
    _state_save(s.state_file, state)
    return True


def _next_half_hour_sleep() -> float:
    now = datetime.now()
    target_min = 30 if now.minute < 30 else 60
    if target_min == 60:
        nxt = now.replace(minute=30, second=0, microsecond=0) + timedelta(hours=1)
    else:
        nxt = now.replace(minute=30, second=0, microsecond=0)
    return max(1.0, (nxt - now).total_seconds())


async def _upload_one_completed_folder(client: TelegramClient, s: Settings):
    state = _state_get(s.state_file)
    uploaded = set(int(x) for x in (state.get("uploaded_group_ids") or []))
    gf = state.get("group_folder") or {}
    if not isinstance(gf, dict):
        gf = {}

    candidates: list[tuple[int, str]] = []
    for k, folder in gf.items():
        try:
            gid = int(str(k).strip())
        except Exception:
            continue
        if gid in uploaded:
            continue
        if not folder or not os.path.isdir(folder):
            continue
        imgs, vids = _list_media_files(folder)
        if not imgs and not vids:
            continue
        candidates.append((gid, folder))
    candidates.sort(key=lambda x: os.path.getmtime(x[1]))
    if not candidates:
        return

    gid, folder = candidates[0]
    caption = _read_text(os.path.join(folder, "message.txt"))
    imgs, vids = _list_media_files(folder)
    files = imgs + vids
    if not files:
        return

    for fp in vids:
        ok, _ = _ffprobe_ok(s.ffprobe_bin, fp)
        if not ok:
            return

    for i in range(0, len(files), 10):
        batch = files[i : i + 10]
        cap = caption if i == 0 else None
        await client.send_file(s.upload_channel_id, batch, caption=cap, supports_streaming=True)

    _ensure_dir(s.uploaded_dir)
    dst = os.path.join(s.uploaded_dir, os.path.basename(folder))
    try:
        if os.path.exists(dst):
            dst = dst + "_" + str(int(time.time()))
        shutil.move(folder, dst)
    except Exception:
        pass

    uploaded.add(int(gid))
    state["uploaded_group_ids"] = sorted(list(uploaded))
    _state_save(s.state_file, state)


async def run_forever():
    s = load_settings()
    _ensure_dir(s.root)
    _ensure_dir(os.path.join(s.root, "downloads"))
    _ensure_dir(s.uploaded_dir)

    client = TelegramClient(StringSession(s.string_session), s.api_id, s.api_hash)

    async with client:
        sem = asyncio.Semaphore(int(s.download_concurrency))

        async def download_tick():
            state = _state_get(s.state_file)
            downloaded = set(int(x) for x in (state.get("downloaded_message_ids") or []))
            groups = await _scan_groups(client, s)
            tasks: list[asyncio.Task] = []

            async def _one(gid: int, items: list):
                async with sem:
                    try:
                        await _download_group(client, s, gid, items, state)
                    except Exception:
                        return

            for gid, items in groups:
                ids = [int(getattr(x, "id", 0) or 0) for x in items if int(getattr(x, "id", 0) or 0)]
                if ids and all(mid in downloaded for mid in ids):
                    continue
                tasks.append(asyncio.create_task(_one(gid, items)))
            if tasks:
                await asyncio.gather(*tasks)

        async def download_loop():
            while True:
                await download_tick()
                await asyncio.sleep(float(s.poll_minutes) * 60.0)

        async def upload_loop():
            while True:
                await asyncio.sleep(_next_half_hour_sleep())
                try:
                    await _upload_one_completed_folder(client, s)
                except Exception:
                    continue

        await asyncio.gather(download_loop(), upload_loop())


if __name__ == "__main__":
    asyncio.run(run_forever())

