import asyncio
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FileReferenceExpiredError, FloodWaitError


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


def _now_local() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _ext_from_mime(mime: str) -> str:
    m = (mime or "").strip().lower()
    if m == "video/mp4":
        return ".mp4"
    if m in ("video/quicktime", "video/mov"):
        return ".mov"
    if m in ("video/x-matroska", "video/mkv"):
        return ".mkv"
    if m == "video/webm":
        return ".webm"
    return ".bin"


def _msg_ext(msg) -> str:
    try:
        f = getattr(msg, "file", None)
        ext = getattr(f, "ext", None)
        if isinstance(ext, str) and ext.strip():
            e = ext.strip().lower()
            return e if e.startswith(".") else ("." + e)
        mime = getattr(f, "mime_type", None) or ""
        return _ext_from_mime(str(mime))
    except Exception:
        return ".bin"


def _msg_expected_size(msg) -> int:
    try:
        return int(getattr(getattr(msg, "file", None), "size", None) or 0)
    except Exception:
        return 0


async def _download_media_with_timeouts(
    client: TelegramClient,
    msg,
    out_path: str,
    base_timeout_sec: int,
    stall_timeout_sec: int,
    min_kbps: int,
    max_timeout_sec: int,
):
    total = int(getattr(getattr(msg, "file", None), "size", None) or 0)
    base = int(base_timeout_sec or 0)
    cap = int(max_timeout_sec or 0)
    kbps = max(16, int(min_kbps or 0))
    stall = int(stall_timeout_sec or 0)

    overall: float | None = None
    if base > 0:
        base2 = max(60, base)
        if cap > 0:
            cap2 = max(base2, cap)
            overall = float(base2 if total <= 0 else min(cap2, max(base2, int((float(total) / float(kbps * 1024)) * 2.0 + 60.0))))
        else:
            overall = None
    stall2: float | None = None
    if stall > 0:
        stall2 = float(max(30, stall))

    loop = asyncio.get_running_loop()
    started = loop.time()
    last_progress = started
    last_bytes = 0
    last_print = started

    def _progress(cur: int, tot: int):
        nonlocal last_progress, last_bytes
        cur_i = int(cur or 0)
        if cur_i != last_bytes:
            last_bytes = cur_i
            last_progress = loop.time()

    async def _reporter():
        nonlocal last_print
        while True:
            await asyncio.sleep(15)
            now = loop.time()
            if now - last_print < 15:
                continue
            last_print = now
            total_b = int(total or 0)
            cur_b = int(last_bytes or 0)
            elapsed = max(1.0, now - started)
            speed = float(cur_b) / elapsed
            if total_b > 0:
                pct = (float(cur_b) / float(total_b)) * 100.0
                eta = max(0, int((total_b - cur_b) / max(1.0, speed)))
                print(
                    f"[{_now_local()}] progress {os.path.basename(out_path)} "
                    f"{cur_b/1024/1024:.1f}/{total_b/1024/1024:.1f}MB "
                    f"({pct:.1f}%) {speed/1024/1024:.2f}MB/s eta={eta}s"
                )
            else:
                print(
                    f"[{_now_local()}] progress {os.path.basename(out_path)} "
                    f"{cur_b/1024/1024:.1f}MB {speed/1024/1024:.2f}MB/s"
                )

    out_dir = os.path.dirname(out_path) or "."
    os.makedirs(out_dir, exist_ok=True)
    task = asyncio.create_task(client.download_media(msg, file=out_path, progress_callback=_progress))
    reporter_task = asyncio.create_task(_reporter())
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=5)
            if task in done:
                return await task
            now = loop.time()
            if overall is not None and (now - started) > overall:
                task.cancel()
                raise asyncio.TimeoutError("download overall timeout")
            if stall2 is not None and (now - last_progress) > stall2:
                task.cancel()
                raise asyncio.TimeoutError("download stalled")
    finally:
        try:
            reporter_task.cancel()
        except Exception:
            pass
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
    download_max_attempts: int
    upload_max_mb: int
    bigfile_threshold_mb: int
    bigfile_concurrency: int


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
    download_max_attempts = _env_int("LOCAL_USERBOT_DOWNLOAD_MAX_ATTEMPTS", 0)
    upload_max_mb = _env_int("LOCAL_USERBOT_UPLOAD_MAX_MB", 0)
    bigfile_threshold_mb = _env_int("LOCAL_USERBOT_BIGFILE_THRESHOLD_MB", 512)
    bigfile_concurrency = _env_int("LOCAL_USERBOT_BIGFILE_CONCURRENCY", 2)

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
        download_timeout_sec=max(0, download_timeout_sec),
        download_stall_timeout_sec=max(0, download_stall_timeout_sec),
        min_download_kbps=max(16, min_download_kbps),
        max_download_timeout_sec=max(0, max_download_timeout_sec),
        download_max_attempts=max(0, download_max_attempts),
        upload_max_mb=max(0, upload_max_mb),
        bigfile_threshold_mb=max(1, bigfile_threshold_mb),
        bigfile_concurrency=max(1, min(8, bigfile_concurrency)),
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
    since = datetime.now(timezone.utc) - timedelta(days=int(s.lookback_days))
    groups: dict[int, list] = {}
    async for m in client.iter_messages(s.download_channel_id, offset_date=None):
        if not m:
            continue
        dt = getattr(m, "date", None)
        if dt:
            if getattr(dt, "tzinfo", None) is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt < since:
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


async def _download_group(
    client: TelegramClient,
    s: Settings,
    group_id: int,
    items: list,
    state_lock: asyncio.Lock,
    big_sem: asyncio.Semaphore,
) -> bool:
    ids = [int(getattr(x, "id", 0) or 0) for x in items if int(getattr(x, "id", 0) or 0)]
    async with state_lock:
        state0 = _state_get(s.state_file)
    downloaded0 = set(int(x) for x in (state0.get("downloaded_message_ids") or []))
    if ids and all(mid in downloaded0 for mid in ids):
        return True

    dt = getattr(items[0], "date", None) or datetime.now(timezone.utc)
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    base_dir = os.path.join(s.root, "downloads")
    _ensure_dir(base_dir)
    folder = os.path.join(base_dir, _folder_name(dt, int(group_id)))
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
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _save_json(os.path.join(folder, "meta.json"), meta)

    for m in items:
        if not getattr(m, "file", None):
            continue
        msg_id = int(getattr(m, "id", 0) or 0)
        if not msg_id:
            continue
        expect = _msg_expected_size(m)
        ext = _msg_ext(m)
        out_path = os.path.join(folder, f"{msg_id}{ext}")
        attempts = 0
        while True:
            attempts += 1
            if s.download_max_attempts and attempts > int(s.download_max_attempts):
                return False
            try:
                try:
                    if os.path.exists(out_path):
                        os.remove(out_path)
                except Exception:
                    pass

                print(
                    f"[{_now_local()}] download msg_id={msg_id} size={expect/1024/1024:.1f}MB -> {out_path} attempt={attempts}"
                )
                if expect >= int(s.bigfile_threshold_mb) * 1024 * 1024:
                    async with big_sem:
                        fp = await _download_media_with_timeouts(
                            client,
                            m,
                            out_path,
                            s.download_timeout_sec,
                            s.download_stall_timeout_sec,
                            s.min_download_kbps,
                            s.max_download_timeout_sec,
                        )
                else:
                    fp = await _download_media_with_timeouts(
                        client,
                        m,
                        out_path,
                        s.download_timeout_sec,
                        s.download_stall_timeout_sec,
                        s.min_download_kbps,
                        s.max_download_timeout_sec,
                    )
                if not fp or not os.path.isfile(fp):
                    await asyncio.sleep(min(30, 5 * attempts))
                    continue

                if expect > 0:
                    try:
                        got = int(os.path.getsize(fp) or 0)
                    except Exception:
                        got = 0
                    if got != int(expect):
                        try:
                            os.remove(fp)
                        except Exception:
                            pass
                        await asyncio.sleep(min(60, 5 * attempts))
                        continue
                break
            except FileReferenceExpiredError:
                try:
                    m2 = await client.get_messages(s.download_channel_id, ids=msg_id)
                    if m2:
                        m = m2
                        expect = _msg_expected_size(m)
                        ext2 = _msg_ext(m)
                        out_path = os.path.join(folder, f"{msg_id}{ext2}")
                except Exception:
                    pass
                await asyncio.sleep(min(60, 5 * attempts))
                continue
            except FloodWaitError as e:
                wait_s = int(getattr(e, "seconds", None) or 0) or 60
                await asyncio.sleep(min(3600, wait_s + 5))
                continue
            except asyncio.TimeoutError:
                await asyncio.sleep(min(60, 5 * attempts))
                continue
            except Exception:
                await asyncio.sleep(min(60, 5 * attempts))
                continue

    imgs, vids = _list_media_files(folder)
    if not imgs and not vids:
        return False

    for fp in vids:
        ok, err = _ffprobe_ok(s.ffprobe_bin, fp)
        if not ok:
            return False

    async with state_lock:
        state = _state_get(s.state_file)
        downloaded = set(int(x) for x in (state.get("downloaded_message_ids") or []))
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


async def _upload_one_completed_folder(client: TelegramClient, s: Settings, state_lock: asyncio.Lock):
    async with state_lock:
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

    if int(s.upload_max_mb or 0) > 0:
        limit = int(s.upload_max_mb) * 1024 * 1024
        for fp in files:
            try:
                if os.path.getsize(fp) > limit:
                    return
            except Exception:
                continue

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

    async with state_lock:
        state2 = _state_get(s.state_file)
        uploaded2 = set(int(x) for x in (state2.get("uploaded_group_ids") or []))
        uploaded2.add(int(gid))
        state2["uploaded_group_ids"] = sorted(list(uploaded2))
        gf2 = state2.get("group_folder") or {}
        if isinstance(gf2, dict):
            gf2.pop(str(int(gid)), None)
            state2["group_folder"] = gf2
        _state_save(s.state_file, state2)


async def run_forever():
    s = load_settings()
    _ensure_dir(s.root)
    _ensure_dir(os.path.join(s.root, "downloads"))
    _ensure_dir(s.uploaded_dir)

    client = TelegramClient(StringSession(s.string_session), s.api_id, s.api_hash)

    async with client:
        print("local_userbot_single started")
        print(f"download_channel_id={s.download_channel_id} upload_channel_id={s.upload_channel_id}")
        print(f"lookback_days={s.lookback_days} poll_minutes={s.poll_minutes} download_concurrency={s.download_concurrency}")
        print(f"root={s.root}")
        sem = asyncio.Semaphore(int(s.download_concurrency))
        big_sem = asyncio.Semaphore(int(s.bigfile_concurrency))
        state_lock = asyncio.Lock()

        async def download_tick():
            async with state_lock:
                state = _state_get(s.state_file)
            downloaded = set(int(x) for x in (state.get("downloaded_message_ids") or []))
            groups = await _scan_groups(client, s)
            tasks: list[asyncio.Task] = []

            async def _one(gid: int, items: list):
                async with sem:
                    try:
                        await _download_group(client, s, gid, items, state_lock, big_sem)
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
                print("download tick")
                await download_tick()
                await asyncio.sleep(float(s.poll_minutes) * 60.0)

        async def upload_loop():
            while True:
                sleep_s = float(_next_half_hour_sleep())
                next_ts = datetime.now() + timedelta(seconds=int(sleep_s))
                print(f"next upload at {next_ts.strftime('%Y-%m-%d %H:%M:%S')} (in {int(sleep_s)}s)")
                try:
                    await asyncio.sleep(sleep_s)
                except asyncio.CancelledError:
                    return
                try:
                    await _upload_one_completed_folder(client, s, state_lock)
                except Exception:
                    continue

        await asyncio.gather(download_loop(), upload_loop())


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        pass

