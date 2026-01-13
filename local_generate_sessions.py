import asyncio
import os

from telethon import TelegramClient
from telethon.sessions import StringSession


def _env_int(name: str, default: int = 0) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default


async def _ainput(prompt: str) -> str:
    return await asyncio.to_thread(input, prompt)


async def main():
    api_id = _env_int("LOCAL_USERBOT_API_ID", 0)
    api_hash = os.getenv("LOCAL_USERBOT_API_HASH", "").strip()
    sessions_file = os.getenv("LOCAL_USERBOT_SESSIONS_FILE", r"E:\资源\userbot\sessions.txt").strip()

    if not api_id or not api_hash:
        raise SystemExit("missing LOCAL_USERBOT_API_ID/LOCAL_USERBOT_API_HASH")

    os.makedirs(os.path.dirname(sessions_file) or ".", exist_ok=True)

    while True:
        ans = (await _ainput("开始登录一个账号并写入 sessions.txt？(y/n): ")).strip().lower()
        if ans not in ("y", "yes"):
            break

        client = TelegramClient(StringSession(), api_id, api_hash)
        async with client:
            await client.start()
            s = client.session.save()
            with open(sessions_file, "a", encoding="utf-8") as f:
                f.write(s.strip() + "\n")
            print(f"已写入：{sessions_file}")

    print("完成")


if __name__ == "__main__":
    asyncio.run(main())

