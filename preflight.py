import os
import subprocess
import sys
import time
from typing import Iterable

from core.logging_setup import setup_logging
from config import (
    ADMIN_USER_IDS,
    BOT_TOKEN,
    BOT_USERNAME,
    DB_HOST,
    DB_NAME,
    DB_PORT,
    DB_USER,
    FREE_CHANNEL_ID_1,
    FREE_CHANNEL_ID_2,
    HIGHLIGHT_CHANNEL_ID,
    MAX_TG_DOWNLOAD_MB,
    MIN_TX_AGE_SEC,
    PAID_CHANNEL_ID,
    TRONGRID_API_KEY,
)
from core.db import get_conn
from core.models import init_tables


def _mask(s: str, keep: int = 4) -> str:
    if not s:
        return ""
    if len(s) <= keep:
        return "*" * len(s)
    return s[:keep] + "*" * (len(s) - keep)


def _check_required(name: str, value) -> list[str]:
    if value is None:
        return [f"{name} 未设置"]
    if isinstance(value, str) and not value.strip():
        return [f"{name} 未设置"]
    return []


def _check_int(name: str, value) -> list[str]:
    issues = _check_required(name, value)
    if issues:
        return issues
    if not isinstance(value, int):
        return [f"{name} 必须是 int"]
    if value == 0:
        return [f"{name} 不能为 0"]
    return []


def _print_kv(title: str, pairs: Iterable[tuple[str, str]]):
    print(f"\n== {title} ==")
    for k, v in pairs:
        print(f"- {k}: {v}")


def check_ffmpeg() -> list[str]:
    try:
        p = subprocess.run(
            ["ffmpeg", "-version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=False,
        )
        if p.returncode != 0:
            return ["ffmpeg 可执行文件存在但不可用（返回码非0）"]
        return []
    except FileNotFoundError:
        return ["未找到 ffmpeg（请安装并加入 PATH）"]
    except Exception as e:
        return [f"ffmpeg 检测失败：{e}"]


def check_db() -> list[str]:
    last_err = None
    for _ in range(30):
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            conn.close()
            return []
        except Exception as e:
            last_err = e
            time.sleep(2)
    return [f"MySQL 连接失败：{last_err}"]


def main() -> int:
    setup_logging()
    issues: list[str] = []

    issues += _check_required("BOT_TOKEN", BOT_TOKEN)
    issues += _check_required("BOT_USERNAME", BOT_USERNAME)
    issues += _check_int("PAID_CHANNEL_ID", PAID_CHANNEL_ID)
    issues += _check_int("HIGHLIGHT_CHANNEL_ID", HIGHLIGHT_CHANNEL_ID)
    issues += _check_int("FREE_CHANNEL_ID_1", FREE_CHANNEL_ID_1)
    issues += _check_int("FREE_CHANNEL_ID_2", FREE_CHANNEL_ID_2)

    if not ADMIN_USER_IDS:
        issues.append("ADMIN_USER_IDS 为空（/upload 将无人可用）")

    if not TRONGRID_API_KEY:
        issues.append("TRONGRID_API_KEY 未设置（可能被限流，强烈建议配置）")

    if MIN_TX_AGE_SEC < 0:
        issues.append("MIN_TX_AGE_SEC 不能为负数")

    if MAX_TG_DOWNLOAD_MB < 1:
        issues.append("MAX_TG_DOWNLOAD_MB 太小")

    issues += check_db()
    issues += check_ffmpeg()

    _print_kv(
        "运行参数摘要",
        [
            ("BOT_TOKEN", _mask(BOT_TOKEN)),
            ("BOT_USERNAME", str(BOT_USERNAME)),
            ("ADMIN_USER_IDS", ",".join(str(x) for x in ADMIN_USER_IDS)),
            ("PAID_CHANNEL_ID", str(PAID_CHANNEL_ID)),
            ("HIGHLIGHT_CHANNEL_ID", str(HIGHLIGHT_CHANNEL_ID)),
            ("FREE_CHANNEL_ID_1", str(FREE_CHANNEL_ID_1)),
            ("FREE_CHANNEL_ID_2", str(FREE_CHANNEL_ID_2)),
            ("MIN_TX_AGE_SEC", str(MIN_TX_AGE_SEC)),
            ("MAX_TG_DOWNLOAD_MB", str(MAX_TG_DOWNLOAD_MB)),
            ("DB", f"{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"),
        ],
    )

    if issues:
        _print_kv("发现问题", [(str(i + 1), m) for i, m in enumerate(issues)])
        print("\n自检未通过：请修复以上问题后再启动 main.py")
        return 1

    try:
        init_tables()
    except Exception as e:
        print(f"\n建表/迁移失败：{e}")
        return 1

    print("\n自检通过：可以启动 main.py 了")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

