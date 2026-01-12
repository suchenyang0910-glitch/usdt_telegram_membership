import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession

from config import USERBOT_API_HASH, USERBOT_API_ID
import os


async def main():
    api_id = USERBOT_API_ID or int(os.getenv("LOCAL_USERBOT_API_ID", "0") or 0)
    api_hash = USERBOT_API_HASH or os.getenv("LOCAL_USERBOT_API_HASH", "").strip()
    if not api_id or not api_hash:
        raise SystemExit("missing USERBOT_API_ID/USERBOT_API_HASH or LOCAL_USERBOT_API_ID/LOCAL_USERBOT_API_HASH")

    client = TelegramClient(StringSession(), api_id, api_hash)
    async with client:
        await client.start()
        print(client.session.save())


if __name__ == "__main__":
    asyncio.run(main())

