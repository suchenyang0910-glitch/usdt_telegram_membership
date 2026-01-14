import asyncio
import json
import os
from dataclasses import dataclass

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


@dataclass
class Settings:
    api_id: int
    api_hash: str
    string_session: str
    out_file: str


def load_settings() -> Settings:
    api_id = _env_int("LOCAL_USERBOT_API_ID", 0)
    api_hash = os.getenv("LOCAL_USERBOT_API_HASH", "").strip()
    string_session = os.getenv("LOCAL_USERBOT_STRING_SESSION", "").strip()
    out_file = os.getenv("LOCAL_USERBOT_CHATS_OUT", "tmp/local_userbot_chats.json").strip() or "tmp/local_userbot_chats.json"
    if not api_id or not api_hash:
        raise SystemExit("missing LOCAL_USERBOT_API_ID/LOCAL_USERBOT_API_HASH")
    if not string_session:
        raise SystemExit("missing LOCAL_USERBOT_STRING_SESSION")
    return Settings(api_id=api_id, api_hash=api_hash, string_session=string_session, out_file=out_file)


async def main():
    s = load_settings()
    os.makedirs(os.path.dirname(s.out_file) or ".", exist_ok=True)
    client = TelegramClient(StringSession(s.string_session), s.api_id, s.api_hash)
    async with client:
        rows = []
        async for d in client.iter_dialogs():
            ent = d.entity
            title = getattr(ent, "title", None) or getattr(ent, "first_name", None) or ""
            username = getattr(ent, "username", None) or ""
            chat_id = int(getattr(ent, "id", 0) or 0)
            kind = type(ent).__name__
            rows.append({"id": chat_id, "title": str(title), "username": str(username), "type": str(kind)})

        with open(s.out_file, "w", encoding="utf-8") as f:
            json.dump({"count": len(rows), "chats": rows}, f, ensure_ascii=False, indent=2)

        for r in rows:
            t = (r.get("title") or "").replace("\n", " ").strip()
            u = (r.get("username") or "").strip()
            tp = (r.get("type") or "").strip()
            cid = r.get("id")
            if u:
                print(f"{cid}\t{tp}\t{t}\t@{u}")
            else:
                print(f"{cid}\t{tp}\t{t}")


if __name__ == "__main__":
    asyncio.run(main())

