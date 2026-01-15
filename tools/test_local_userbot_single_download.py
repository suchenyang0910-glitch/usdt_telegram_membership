import asyncio
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from local_userbot_single import _download_media_with_timeouts


class FakeMsgFile:
    def __init__(self, size: int):
        self.size = size


class FakeMsg:
    def __init__(self, msg_id: int, size: int):
        self.id = msg_id
        self.file = FakeMsgFile(size)


class FakeClient:
    def __init__(self, bytes_per_tick: int = 1024 * 1024):
        self.bytes_per_tick = int(bytes_per_tick)

    async def download_media(self, msg, file: str, progress_callback=None):
        os.makedirs(file, exist_ok=True)
        dst = os.path.join(file, f"{msg.id}.bin")
        total = int(getattr(getattr(msg, "file", None), "size", None) or 0)
        cur = 0
        with open(dst, "wb") as f:
            while cur < total:
                await asyncio.sleep(0.01)
                chunk = min(self.bytes_per_tick, total - cur)
                f.write(b"\0" * chunk)
                cur += chunk
                if progress_callback:
                    progress_callback(cur, total)
        return dst


async def main():
    with tempfile.TemporaryDirectory() as td:
        client = FakeClient(bytes_per_tick=256 * 1024)
        msg = FakeMsg(1, 5 * 1024 * 1024)
        started = time.time()
        fp = await _download_media_with_timeouts(
            client,
            msg,
            td,
            base_timeout_sec=0,
            stall_timeout_sec=0,
            min_kbps=128,
            max_timeout_sec=0,
        )
        assert fp and os.path.exists(fp)
        assert os.path.getsize(fp) == msg.file.size
        assert time.time() - started < 10


if __name__ == "__main__":
    asyncio.run(main())

