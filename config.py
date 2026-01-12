# config.py
import os
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

# Telegram Bot
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BOT_USERNAME = os.getenv("BOT_USERNAME")

# 频道ID（必须是真实的 chat_id 整数）
PAID_CHANNEL_ID   = int(os.getenv("PAID_CHANNEL_ID", "-1002581584398"))   # 付费频道
FREE_CHANNEL_ID_1 = int(os.getenv("FREE_CHANNEL_ID_1", "-1003257652639")) # 免费频道 1（引流频道）
FREE_CHANNEL_ID_2 = int(os.getenv("FREE_CHANNEL_ID_2", "-1003257652639")) # 免费频道 2（可选）
HIGHLIGHT_CHANNEL_ID = int(os.getenv("HIGHLIGHT_CHANNEL_ID", str(FREE_CHANNEL_ID_1)))

ADMIN_USER_IDS = []
for _x in (os.getenv("ADMIN_USER_IDS", "")).split(","):
    _x = _x.strip()
    if not _x:
        continue
    try:
        ADMIN_USER_IDS.append(int(_x))
    except Exception:
        continue

AUTO_CLIP_FROM_PAID_CHANNEL = os.getenv("AUTO_CLIP_FROM_PAID_CHANNEL", "0") == "1"

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

_pool_env = os.getenv("USDT_ADDRESS_POOL", "").strip()
if _pool_env:
    _pool_env = _pool_env.replace("\n", ",")
    USDT_ADDRESS_POOL = [x.strip() for x in _pool_env.split(",") if x.strip()]
else:
    USDT_ADDRESS_POOL = _DEFAULT_USDT_ADDRESS_POOL

RECEIVE_ADDRESS = os.getenv("RECEIVE_ADDRESS", "").strip()
PAYMENT_MODE = os.getenv("PAYMENT_MODE", "address_pool").strip().lower()
PAYMENT_SUFFIX_ENABLE = os.getenv("PAYMENT_SUFFIX_ENABLE", "0") == "1"
PAYMENT_SUFFIX_MIN = Decimal(os.getenv("PAYMENT_SUFFIX_MIN", "0.0001"))
PAYMENT_SUFFIX_MAX = Decimal(os.getenv("PAYMENT_SUFFIX_MAX", "0.0099"))

# TronGrid 查询 USDT 交易
USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRX_API_URL   = "https://api.trongrid.io/v1/accounts/{}/transactions/trc20?limit=200&only_to=true"
TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "")
MIN_TX_AGE_SEC = int(os.getenv("MIN_TX_AGE_SEC", "60"))

# MySQL
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "panss")
DB_NAME = os.getenv("DB_NAME", "usdt_membership")

# 会员套餐
PLANS = [
    {"code": "yearly",  "name": "年度会员", "price": Decimal("79.99"), "days": 365},
    {"code": "quarter", "name": "季度会员", "price": Decimal("19.99"), "days": 90},
    {"code": "monthly", "name": "月度会员", "price": Decimal("9.99"),  "days": 30},
]

AMOUNT_EPS = Decimal("0.000001")  # 金额容差

# 剪辑
DOWNLOAD_DIR = "tmp/downloads"
CLIP_DIR     = "tmp/clips"
CLIP_SECONDS = int(os.getenv("CLIP_SECONDS", "30"))
CLIP_RANDOM  = os.getenv("CLIP_RANDOM", "1") == "1"
SEND_RETRY   = 3
MAX_TG_DOWNLOAD_MB = int(os.getenv("MAX_TG_DOWNLOAD_MB", "19"))

# 邀请奖励（按套餐 code 区分）
INVITE_REWARD = {
    "monthly":  3,   # 邀月卡 → 奖励 3 天
    "quarter":  10,  # 季卡 → 奖励 10 天
    "yearly":   30,  # 年卡 → 奖励 30 天
}

# 日志
LOG_FILE = "logs/bot.log"
RUNTIME_LOG_FILE = os.getenv("RUNTIME_LOG_FILE", "logs/runtime.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "10"))
