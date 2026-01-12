import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession

from config import USERBOT_API_HASH, USERBOT_API_ID


async def main():
    if not USERBOT_API_ID or not USERBOT_API_HASH:
        raise SystemExit("USERBOT_API_ID/USERBOT_API_HASH missing")

    client = TelegramClient(StringSession(), USERBOT_API_ID, USERBOT_API_HASH)
    async with client:
        await client.start()
        print(client.session.save())


if __name__ == "__main__":
    asyncio.run(main())

