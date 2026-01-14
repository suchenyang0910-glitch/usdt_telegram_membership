# config.py
import os
import json
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()
load_dotenv(".env.secrets")

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))


def _abs_path(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return PROJECT_ROOT
    if os.path.isabs(p):
        return p
    return os.path.join(PROJECT_ROOT, p)


def _load_json_file(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _resolve_cfg_path(p: str, default_filename: str) -> str:
    p = _abs_path(p)
    if not p:
        return p
    try:
        if os.path.isdir(p):
            return os.path.join(p, default_filename)
    except Exception:
        pass
    return p


_cfg_default_path = _resolve_cfg_path(
    os.getenv("APP_CONFIG_DEFAULT_FILE", "config/app_config.defaults.json"),
    "app_config.defaults.json",
)
_cfg_path = _resolve_cfg_path(
    os.getenv("APP_CONFIG_FILE", "config/app_config.json"),
    "app_config.json",
)
_CFG_DEFAULT = _load_json_file(_cfg_default_path)
_CFG_LOCAL = _load_json_file(_cfg_path)
_CFG = {**_CFG_DEFAULT, **_CFG_LOCAL}

_ENV_ONLY_KEYS = {
    "BOT_TOKEN",
    "BOT_USERNAME",
    "PAID_CHANNEL_ID",
    "HIGHLIGHT_CHANNEL_ID",
    "FREE_CHANNEL_ID_1",
    "FREE_CHANNEL_ID_2",
    "FREE_CHANNEL_IDS",
    "TRONGRID_API_KEY",
    "MIN_TX_AGE_SEC",
    "PAYMENT_MODE",
    "RECEIVE_ADDRESS",
    "PAYMENT_SUFFIX_ENABLE",
    "PAYMENT_SUFFIX_MIN",
    "PAYMENT_SUFFIX_MAX",
    "USDT_ADDRESS_POOL",
    "DB_HOST",
    "DB_PORT",
    "DB_USER",
    "DB_PASS",
    "DB_NAME",
    "MYSQL_ROOT_PASSWORD",
    "MYSQL_DATABASE",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
}


def _env_present(name: str) -> bool:
    return name in os.environ


def _cfg_value(name: str, default=None):
    if _env_present(name):
        return os.environ.get(name)
    if name in _ENV_ONLY_KEYS:
        return default
    if name in _CFG:
        return _CFG.get(name)
    return default


def _to_bool(x, default: bool = False) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return default
    s = str(x).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off", ""):
        return False
    return default


def _to_int(x, default: int = 0) -> int:
    if isinstance(x, int) and not isinstance(x, bool):
        return x
    if x is None:
        return default
    try:
        return int(str(x).strip())
    except Exception:
        return default


def _to_float(x, default: float = 0.0) -> float:
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return float(x)
    if x is None:
        return default
    try:
        return float(str(x).strip())
    except Exception:
        return default


def _to_decimal(x, default: str) -> Decimal:
    if isinstance(x, Decimal):
        return x
    if x is None or str(x).strip() == "":
        return Decimal(str(default))
    return Decimal(str(x).strip())


def _to_int_list(x) -> list[int]:
    if x is None:
        return []
    if isinstance(x, list):
        out: list[int] = []
        for v in x:
            try:
                iv = int(v)
            except Exception:
                continue
            if iv not in out:
                out.append(iv)
        return out
    s = str(x).replace("\n", ",")
    out: list[int] = []
    for part in s.split(","):
        part = (part or "").strip()
        if not part:
            continue
        try:
            iv = int(part)
        except Exception:
            continue
        if iv not in out:
            out.append(iv)
    return out


def _to_str_list(x) -> list[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(v).strip() for v in x if str(v).strip()]
    s = str(x).replace("\n", ",")
    return [p.strip() for p in s.split(",") if p.strip()]


# Telegram Bot
BOT_TOKEN = str(_cfg_value("BOT_TOKEN", "") or "").strip()
BOT_USERNAME = str(_cfg_value("BOT_USERNAME", "") or "").strip() or None

# 频道ID（必须是真实的 chat_id 整数）
PAID_CHANNEL_ID = _to_int(_cfg_value("PAID_CHANNEL_ID", "-1002581584398"))   # 付费频道
FREE_CHANNEL_ID_1 = _to_int(_cfg_value("FREE_CHANNEL_ID_1", "-1003257652639")) # 免费频道 1（引流频道）
FREE_CHANNEL_ID_2 = _to_int(_cfg_value("FREE_CHANNEL_ID_2", "-1003257652639")) # 免费频道 2（可选）
HIGHLIGHT_CHANNEL_ID = _to_int(_cfg_value("HIGHLIGHT_CHANNEL_ID", str(FREE_CHANNEL_ID_1)))

FREE_CHANNEL_IDS: list[int] = []
_free_ids_env = _cfg_value("FREE_CHANNEL_IDS", "")
if _free_ids_env:
    for _id in _to_int_list(_free_ids_env):
        if _id and _id not in FREE_CHANNEL_IDS:
            FREE_CHANNEL_IDS.append(_id)
else:
    for _id in (FREE_CHANNEL_ID_1, FREE_CHANNEL_ID_2):
        if _id and _id not in FREE_CHANNEL_IDS:
            FREE_CHANNEL_IDS.append(_id)

ADMIN_USER_IDS = _to_int_list(_cfg_value("ADMIN_USER_IDS", ""))

AUTO_CLIP_FROM_PAID_CHANNEL = _to_bool(_cfg_value("AUTO_CLIP_FROM_PAID_CHANNEL", "0"), False)

_admin_report_chat_id = str(_cfg_value("ADMIN_REPORT_CHAT_ID", "") or "").strip()
ADMIN_REPORT_CHAT_ID = int(_admin_report_chat_id) if _admin_report_chat_id else None
ADMIN_REPORT_ENABLE = _to_bool(_cfg_value("ADMIN_REPORT_ENABLE", "1"), True)
ADMIN_REPORT_HOURLY = _to_bool(_cfg_value("ADMIN_REPORT_HOURLY", "1"), True)
ADMIN_REPORT_TZ_OFFSET = _to_int(_cfg_value("ADMIN_REPORT_TZ_OFFSET", "7"), 7)
ADMIN_REPORT_QUIET_START_HOUR = _to_int(_cfg_value("ADMIN_REPORT_QUIET_START_HOUR", "22"), 22)
ADMIN_REPORT_QUIET_END_HOUR = _to_int(_cfg_value("ADMIN_REPORT_QUIET_END_HOUR", "8"), 8)
HEALTH_ALERT_ENABLE = _to_bool(_cfg_value("HEALTH_ALERT_ENABLE", "1"), True)
HEALTH_ALERT_DEPOSIT_STALE_MINUTES = _to_int(_cfg_value("HEALTH_ALERT_DEPOSIT_STALE_MINUTES", "30"), 30)
HEALTH_ALERT_MIN_INTERVAL_MINUTES = _to_int(_cfg_value("HEALTH_ALERT_MIN_INTERVAL_MINUTES", "30"), 30)

EXPIRING_REMIND_ENABLE = _to_bool(_cfg_value("EXPIRING_REMIND_ENABLE", "1"), True)
EXPIRING_REMIND_DAYS = str(_cfg_value("EXPIRING_REMIND_DAYS", "7,3,1") or "").strip()
EXPIRED_RECALL_ENABLE = _to_bool(_cfg_value("EXPIRED_RECALL_ENABLE", "1"), True)
EXPIRED_RECALL_DAYS = str(_cfg_value("EXPIRED_RECALL_DAYS", "1,3,7") or "").strip()

SUPPORT_ENABLE = _to_bool(_cfg_value("SUPPORT_ENABLE", "0"), False)
_support_group_id = str(_cfg_value("SUPPORT_GROUP_ID", "") or "").strip()
SUPPORT_GROUP_ID = int(_support_group_id) if _support_group_id else None

JOIN_REQUEST_ENABLE = _to_bool(_cfg_value("JOIN_REQUEST_ENABLE", "0"), False)
JOIN_REQUEST_LINK_EXPIRE_HOURS = _to_int(_cfg_value("JOIN_REQUEST_LINK_EXPIRE_HOURS", "24"), 24)

# 10 个 USDT-TRC20 收款地址（替换成你的地址）
_DEFAULT_USDT_ADDRESS_POOL = [
    "TWAVjpfcdH68wQPFFnzrDPdZPAHhr7RAr2",
    "TFfmSMHke79PX28Zb8graVKmzNSvyW3nnN",
    "TNr2VBxXZiiXufdNsi79bywKBZHMQqLetG",
    "TTuBYLvNFxeszkLJEJ6koqRtcU6f93Lsga",
    "TETFpjRzZw2uoeQAj5RbSkfPbwvjvRdVVX",
    "TMuuy5WywRtx86eeFmyEaJ7cFRr2zEZy7U",
    "TVSMrAmXpoVAJRLkQiaVmVovBfci2ns9Tf",
    "TBYarsk3oV6XfonZRECV5uNH44W67PQbCk",
    "TUVeiBb515nuDv65qZ4PNYxSxWYnV5V5oo",
    "TS1SrKRXMx3w1HWceh7Fzn2TWq7yHzQRXY",
]

_pool_env = _cfg_value("USDT_ADDRESS_POOL", "")
if _pool_env:
    USDT_ADDRESS_POOL = _to_str_list(_pool_env)
else:
    USDT_ADDRESS_POOL = _DEFAULT_USDT_ADDRESS_POOL

RECEIVE_ADDRESS = str(_cfg_value("RECEIVE_ADDRESS", "") or "").strip()
PAYMENT_MODE = str(_cfg_value("PAYMENT_MODE", "address_pool") or "").strip().lower()
PAYMENT_SUFFIX_ENABLE = _to_bool(_cfg_value("PAYMENT_SUFFIX_ENABLE", "0"), False)
PAYMENT_SUFFIX_MIN = _to_decimal(_cfg_value("PAYMENT_SUFFIX_MIN", "0.0001"), "0.0001")
PAYMENT_SUFFIX_MAX = _to_decimal(_cfg_value("PAYMENT_SUFFIX_MAX", "0.0099"), "0.0099")

# TronGrid 查询 USDT 交易
USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRX_API_URL   = "https://api.trongrid.io/v1/accounts/{}/transactions/trc20?limit=200&only_to=true"
TRONGRID_API_KEY = str(_cfg_value("TRONGRID_API_KEY", "") or "").strip()
MIN_TX_AGE_SEC = _to_int(_cfg_value("MIN_TX_AGE_SEC", "60"), 60)

# MySQL
DB_HOST = str(_cfg_value("DB_HOST", "127.0.0.1") or "").strip()
DB_PORT = _to_int(_cfg_value("DB_PORT", "3306"), 3306)
DB_USER = str(_cfg_value("DB_USER", "root") or "").strip()
DB_PASS = str(_cfg_value("DB_PASS", "panss") or "").strip()
DB_NAME = str(_cfg_value("DB_NAME", "usdt_membership") or "").strip()

# 会员套餐
PLANS = [
    {"code": "yearly",  "name": "年度会员", "price": Decimal("15.99"), "days": 365},
    {"code": "quarter", "name": "季度会员", "price": Decimal("3.99"), "days": 90},
    {"code": "monthly", "name": "月度会员", "price": Decimal("1.99"),  "days": 30},
]

AMOUNT_EPS = Decimal("0.000001")  # 金额容差

# 剪辑
DOWNLOAD_DIR = _abs_path(str(_cfg_value("DOWNLOAD_DIR", "tmp/downloads") or "tmp/downloads"))
CLIP_DIR     = _abs_path(str(_cfg_value("CLIP_DIR", "tmp/clips") or "tmp/clips"))
CLIP_SECONDS = _to_int(_cfg_value("CLIP_SECONDS", "30"), 30)
CLIP_RANDOM  = _to_bool(_cfg_value("CLIP_RANDOM", "1"), True)
SEND_RETRY   = 3
MAX_TG_DOWNLOAD_MB = _to_int(_cfg_value("MAX_TG_DOWNLOAD_MB", "19"), 19)

HEARTBEAT_FILE = _abs_path(str(_cfg_value("HEARTBEAT_FILE", "tmp/heartbeat_app.json") or "tmp/heartbeat_app.json"))
HEARTBEAT_USERBOT_FILE = _abs_path(str(_cfg_value("HEARTBEAT_USERBOT_FILE", "tmp/heartbeat_userbot.json") or "tmp/heartbeat_userbot.json"))
HEARTBEAT_INTERVAL_SEC = _to_int(_cfg_value("HEARTBEAT_INTERVAL_SEC", "60"), 60)

MATCH_ORDER_LOOKBACK_HOURS = _to_int(_cfg_value("MATCH_ORDER_LOOKBACK_HOURS", "72"), 72)
MATCH_ORDER_PREFER_RECENT = _to_bool(_cfg_value("MATCH_ORDER_PREFER_RECENT", "1"), True)

USERBOT_ENABLE = _to_bool(_cfg_value("USERBOT_ENABLE", "0"), False)
USERBOT_API_ID = _to_int(_cfg_value("USERBOT_API_ID", "0"), 0)
USERBOT_API_HASH = str(_cfg_value("USERBOT_API_HASH", "") or "").strip()
USERBOT_STRING_SESSION = str(_cfg_value("USERBOT_STRING_SESSION", "") or "").strip()
USERBOT_SESSION_NAME = str(_cfg_value("USERBOT_SESSION_NAME", "tmp/userbot/telethon") or "").strip() or "tmp/userbot/telethon"
USERBOT_CLIP_SECONDS = _to_int(_cfg_value("USERBOT_CLIP_SECONDS", str(CLIP_SECONDS)), int(CLIP_SECONDS))
USERBOT_CLIP_RANDOM = _to_bool(_cfg_value("USERBOT_CLIP_RANDOM", "1" if CLIP_RANDOM else "0"), bool(CLIP_RANDOM))
_userbot_notify_chat_id = str(_cfg_value("USERBOT_NOTIFY_CHAT_ID", "") or "").strip()
USERBOT_NOTIFY_CHAT_ID = int(_userbot_notify_chat_id) if _userbot_notify_chat_id else None

ADMIN_WEB_ENABLE = _to_bool(_cfg_value("ADMIN_WEB_ENABLE", "0"), False)
ADMIN_WEB_HOST = str(_cfg_value("ADMIN_WEB_HOST", "0.0.0.0") or "").strip() or "0.0.0.0"
ADMIN_WEB_PORT = _to_int(_cfg_value("ADMIN_WEB_PORT", "8080"), 8080)
ADMIN_WEB_USER = str(_cfg_value("ADMIN_WEB_USER", "") or "").strip()
ADMIN_WEB_PASS = str(_cfg_value("ADMIN_WEB_PASS", "") or "").strip()
ADMIN_WEB_RO_USER = str(_cfg_value("ADMIN_WEB_RO_USER", "") or "").strip()
ADMIN_WEB_RO_PASS = str(_cfg_value("ADMIN_WEB_RO_PASS", "") or "").strip()
ADMIN_WEB_ALLOW_IPS = str(_cfg_value("ADMIN_WEB_ALLOW_IPS", "") or "").strip()
ADMIN_WEB_TRUST_PROXY = _to_bool(_cfg_value("ADMIN_WEB_TRUST_PROXY", "0"), False)
ADMIN_WEB_ACTIONS_ENABLE = _to_bool(_cfg_value("ADMIN_WEB_ACTIONS_ENABLE", "1"), True)

# 广播（admin_web）
BROADCAST_SLEEP_SEC = _to_float(_cfg_value("BROADCAST_SLEEP_SEC", "0.15"), 0.15)
BROADCAST_ABORT_MIN_SENT = _to_int(_cfg_value("BROADCAST_ABORT_MIN_SENT", "50"), 50)
BROADCAST_ABORT_FAIL_RATE = _to_float(_cfg_value("BROADCAST_ABORT_FAIL_RATE", "0.7"), 0.7)

# 邀请奖励（按套餐 code 区分）
INVITE_REWARD = {
    "monthly":  3,   # 邀月卡 → 奖励 3 天
    "quarter":  10,  # 季卡 → 奖励 10 天
    "yearly":   30,  # 年卡 → 奖励 30 天
}

# 日志
LOG_FILE = _abs_path(str(_cfg_value("LOG_FILE", "logs/bot.log") or "logs/bot.log"))
RUNTIME_LOG_FILE = _abs_path(str(_cfg_value("RUNTIME_LOG_FILE", "logs/runtime.log") or "logs/runtime.log"))
LOG_LEVEL = str(_cfg_value("LOG_LEVEL", "INFO") or "INFO").upper()
LOG_MAX_BYTES = _to_int(_cfg_value("LOG_MAX_BYTES", str(10 * 1024 * 1024)), 10 * 1024 * 1024)
LOG_BACKUP_COUNT = _to_int(_cfg_value("LOG_BACKUP_COUNT", "10"), 10)
LOG_RETENTION_DAYS = _to_int(_cfg_value("LOG_RETENTION_DAYS", "3"), 3)
