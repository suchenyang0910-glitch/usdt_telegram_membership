import asyncio
import json
import os
import shutil
import logging
from dataclasses import dataclass
from datetime import datetime
import time

from telethon import TelegramClient
from telethon.sessions import StringSession

logger = logging.getLogger("local_userbot")


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
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s:
                    out.append(s)
        return out

    raw = os.getenv("LOCAL_USERBOT_STRING_SESSIONS_JSON", "").strip()
    if raw:
        data = json.loads(raw)
        return [str(x).strip() for x in data if str(x).strip()]

    raise SystemExit("missing LOCAL_USERBOT_SESSIONS_FILE or LOCAL_USERBOT_STRING_SESSIONS_JSON")


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
    )


def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


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
        try:
            self._entity = await self._client.get_entity(self._monitor_chat)
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
        await asyncio.wait_for(client.download_media(msg, file=folder), timeout=float(timeout_sec))
        return True, folder
    except asyncio.TimeoutError:
        try:
            shutil.rmtree(folder, ignore_errors=True)
        except Exception:
            pass
        return False, "download timeout"
    except asyncio.CancelledError:
        raise
    except Exception as e:
        return False, f"download failed: {type(e).__name__}: {e}"


def _is_video_file(name: str) -> bool:
    n = name.lower()
    return n.endswith((".mp4", ".mkv", ".mov", ".webm", ".m4v", ".avi"))

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
    messages: list,
    base_dir: str,
    timeout_sec: int,
) -> tuple[bool, str, int, int]:
    if not messages:
        return False, "empty group", 0, 0

    rep_id = min(int(getattr(m, "id", 0) or 0) for m in messages)
    folder = os.path.join(base_dir, _folder_name(datetime.now(), rep_id))
    _ensure_dir(folder)

    texts = []
    for m in messages:
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
    try:
        for m in sorted(messages, key=lambda x: int(getattr(x, "id", 0) or 0)):
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

            fp = await asyncio.wait_for(client.download_media(m, file=folder), timeout=float(timeout_sec))
            if fp and isinstance(fp, str) and os.path.exists(fp) and expect_size:
                try:
                    actual = os.path.getsize(fp)
                except Exception:
                    actual = 0
                if actual and actual < int(expect_size * 0.95):
                    raise RuntimeError(f"size mismatch expect={expect_size} actual={actual}")
            if is_video:
                videos += 1
            elif is_photo:
                images += 1
        if not _pick_downloaded_media_path(folder):
            raise RuntimeError("no media files")
        return True, folder, images, videos
    except asyncio.TimeoutError:
        try:
            shutil.rmtree(folder, ignore_errors=True)
        except Exception:
            pass
        return False, "download timeout", images, videos
    except asyncio.CancelledError:
        raise
    except Exception as e:
        try:
            shutil.rmtree(folder, ignore_errors=True)
        except Exception:
            pass
        return False, f"download failed: {type(e).__name__}: {e}", images, videos


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
            msgs = await head.get_messages(s.download_channel_id, limit=50)
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
                ok, info, images, videos = await _download_many(c, items, base_dir, s.download_timeout_sec)
                dt = time.time() - t0
                if ok:
                    for m in items:
                        mid = int(getattr(m, "id", 0) or 0)
                        if mid:
                            downloaded.add(mid)
                    _save_state(s.state_file, downloaded)
                    logger.info("download ok title=%s path=%s cost=%.1fs images=%s videos=%s", title, info, dt, images, videos)
                    await notifier.send(f"完成下载：{title}\n用时：{dt:.1f}s\n图片：{images} 视频：{videos}\n路径：{info}")
                else:
                    logger.warning("download failed title=%s cost=%.1fs err=%s", title, dt, info)
                    await notifier.send(f"下载失败：{title}\n用时：{dt:.1f}s\n原因：{info}")
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

