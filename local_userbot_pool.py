import asyncio
import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime

from telethon import TelegramClient
from telethon.sessions import StringSession


@dataclass
class Settings:
    api_id: int
    api_hash: str
    download_channel_id: int
    upload_channel_id: int
    root: str
    poll_minutes: int
    upload_minutes: int
    upload_dir: str
    uploaded_dir: str
    state_file: str
    sessions: list[str]


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    return int(v)


def _load_sessions() -> list[str]:
    p = os.getenv("LOCAL_USERBOT_SESSIONS_FILE", "").strip()
    if p:
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
        root=root,
        poll_minutes=poll_minutes,
        upload_minutes=upload_minutes,
        upload_dir=upload_dir,
        uploaded_dir=uploaded_dir,
        state_file=state_file,
        sessions=sessions,
    )


def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


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


async def _download_one(client: TelegramClient, channel_id: int, msg_id: int, base_dir: str) -> tuple[bool, str]:
    try:
        msg = await client.get_messages(channel_id, ids=msg_id)
    except Exception as e:
        return False, f"get_messages failed: {type(e).__name__}: {e}"
    if not msg:
        return False, "message missing"
    if not msg.file:
        return False, "no file"

    folder = os.path.join(base_dir, _folder_name(msg.date, int(msg.id)))
    _ensure_dir(folder)

    text = msg.message or ""
    try:
        with open(os.path.join(folder, "message.txt"), "w", encoding="utf-8") as f:
            f.write(text)
    except Exception:
        pass

    try:
        await client.download_media(msg, file=folder)
        return True, folder
    except Exception as e:
        return False, f"download failed: {type(e).__name__}: {e}"


def _is_video_file(name: str) -> bool:
    n = name.lower()
    return n.endswith((".mp4", ".mkv", ".mov", ".webm", ".m4v", ".avi"))


async def download_loop(clients: list[TelegramClient], s: Settings):
    base_dir = os.path.join(s.root, "downloads")
    _ensure_dir(base_dir)
    downloaded = _load_state(s.state_file)

    while True:
        try:
            head = clients[0]
            msgs = await head.get_messages(s.download_channel_id, limit=50)
            todo = []
            for m in msgs:
                if not m or not m.file:
                    continue
                if int(m.id) in downloaded:
                    continue
                todo.append(int(m.id))
            todo.sort()

            for idx, msg_id in enumerate(todo):
                c = clients[idx % len(clients)]
                ok, info = await _download_one(c, s.download_channel_id, msg_id, base_dir)
                if ok:
                    downloaded.add(msg_id)
                    _save_state(s.state_file, downloaded)
                else:
                    await asyncio.sleep(2)
        except Exception:
            pass

        await asyncio.sleep(max(60, s.poll_minutes * 60))


async def upload_loop(client: TelegramClient, s: Settings):
    _ensure_dir(s.upload_dir)
    _ensure_dir(s.uploaded_dir)

    while True:
        try:
            files = []
            for name in os.listdir(s.upload_dir):
                fp = os.path.join(s.upload_dir, name)
                if os.path.isfile(fp) and _is_video_file(fp):
                    files.append(fp)
            files.sort(key=lambda x: os.path.getmtime(x))

            if files:
                fp = files[0]
                caption = ""
                txt = os.path.splitext(fp)[0] + ".txt"
                if os.path.exists(txt):
                    try:
                        with open(txt, "r", encoding="utf-8") as f:
                            caption = f.read().strip()
                    except Exception:
                        caption = ""

                await client.send_file(s.upload_channel_id, fp, caption=caption or None, supports_streaming=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                dst = os.path.join(s.uploaded_dir, f"{ts}_{os.path.basename(fp)}")
                try:
                    shutil.move(fp, dst)
                except Exception:
                    try:
                        os.remove(fp)
                    except Exception:
                        pass
                if os.path.exists(txt):
                    try:
                        os.remove(txt)
                    except Exception:
                        pass
        except Exception:
            pass

        await asyncio.sleep(max(60, s.upload_minutes * 60))


async def main():
    s = load_settings()
    _ensure_dir(s.root)

    clients: list[TelegramClient] = []
    for i, sess in enumerate(s.sessions[:5]):
        client = TelegramClient(StringSession(sess), s.api_id, s.api_hash)
        await client.connect()
        if not await client.is_user_authorized():
            raise SystemExit(f"session[{i}] not authorized")
        clients.append(client)

    tasks = [
        asyncio.create_task(download_loop(clients, s)),
        asyncio.create_task(upload_loop(clients[0], s)),
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())

