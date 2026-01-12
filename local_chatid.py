import asyncio
import os

from telethon import TelegramClient
from telethon.sessions import StringSession


def _env_int(name: str, default: int = 0) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default


async def main():
    api_id = _env_int("LOCAL_USERBOT_API_ID", 0)
    api_hash = os.getenv("LOCAL_USERBOT_API_HASH", "").strip()
    session = os.getenv("LOCAL_USERBOT_STRING_SESSION", "").strip()
    query = os.getenv("LOCAL_CHAT_QUERY", "").strip()

    if not api_id or not api_hash:
        raise SystemExit("missing LOCAL_USERBOT_API_ID/LOCAL_USERBOT_API_HASH")
    if not session:
        raise SystemExit("missing LOCAL_USERBOT_STRING_SESSION")

    client = TelegramClient(StringSession(session), api_id, api_hash)
    async with client:
        if query:
            ent = await client.get_entity(query)
            name = getattr(ent, "title", None) or getattr(ent, "username", None) or ""
            print(f"id={ent.id}  type={ent.__class__.__name__}  name={name}")
            return

        async for d in client.iter_dialogs():
            ent = d.entity
            name = getattr(ent, "title", None) or getattr(ent, "username", None) or ""
            print(f"id={ent.id}  type={ent.__class__.__name__}  name={name}")


if __name__ == "__main__":
    asyncio.run(main())

