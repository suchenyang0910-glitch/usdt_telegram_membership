# chain/tron_client.py
import requests
from decimal import Decimal
from datetime import datetime, timezone

from config import TRX_API_URL, USDT_CONTRACT, TRONGRID_API_KEY
import logging
logger = logging.getLogger(__name__)


def _headers() -> dict:
    if TRONGRID_API_KEY:
        return {"TRON-PRO-API-KEY": TRONGRID_API_KEY}
    return {}


def list_usdt_incoming(addr: str) -> list[dict]:
    url = TRX_API_URL.format(addr)
    try:
        resp = requests.get(url, headers=_headers(), timeout=8)
    except Exception as e:
        logger.warning(f"[tron_client] 请求失败 addr={addr} err={e}")
        return []

    if resp.status_code != 200:
        logger.warning(f"[tron_client] 非200 addr={addr} code={resp.status_code} body={resp.text[:200]}")
        return []

    try:
        data = resp.json()
    except Exception as e:
        logger.warning(f"[tron_client] JSON解析失败 addr={addr} err={e} body={resp.text[:200]}")
        return []

    txs: list[dict] = []
    for tx in data.get("data", []):
        token_info = tx.get("token_info") or {}
        contract = token_info.get("address")
        decimals = token_info.get("decimals", 6)

        if contract != USDT_CONTRACT:
            continue
        if tx.get("to") != addr:
            continue

        tx_id = tx.get("transaction_id") or tx.get("txID") or tx.get("hash")
        if not tx_id:
            continue

        value_str = tx.get("value", "0")
        try:
            raw = Decimal(str(value_str))
            amount = raw / (Decimal(10) ** Decimal(decimals))
        except Exception:
            continue

        ts_ms = tx.get("block_timestamp")
        block_time = None
        if ts_ms is not None:
            try:
                block_time = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
            except Exception:
                block_time = None

        txs.append(
            {
                "tx_id": tx_id,
                "from": tx.get("from"),
                "to": addr,
                "amount": amount,
                "block_time": block_time,
            }
        )

    txs.sort(key=lambda x: x["block_time"] or datetime.min.replace(tzinfo=timezone.utc))
    return txs
def get_usdt_received(addr: str) -> Decimal:
    """
    使用 TronGrid 查询某地址累计收到的 USDT 数量（单位：USDT）
    """
    total = Decimal("0")
    for tx in list_usdt_incoming(addr):
        total += tx["amount"]

    return total


def get_usdt_balance(addr: str) -> Decimal | None:
    url = f"https://api.trongrid.io/v1/accounts/{addr}"
    try:
        resp = requests.get(url, headers=_headers(), timeout=8)
    except Exception as e:
        logger.warning(f"[tron_client] balance 请求失败 addr={addr} err={e}")
        return None
    if resp.status_code != 200:
        logger.warning(f"[tron_client] balance 非200 addr={addr} code={resp.status_code} body={resp.text[:200]}")
        return None
    try:
        data = resp.json()
    except Exception as e:
        logger.warning(f"[tron_client] balance JSON解析失败 addr={addr} err={e} body={resp.text[:200]}")
        return None

    items = data.get("data") or []
    if not items:
        return Decimal("0")
    row = items[0] or {}

    trc20 = row.get("trc20") or []
    for entry in trc20:
        if not isinstance(entry, dict):
            continue
        if USDT_CONTRACT in entry:
            try:
                return Decimal(str(entry[USDT_CONTRACT])) / Decimal("1000000")
            except Exception:
                return None

        for k, v in entry.items():
            if str(k).lower() == str(USDT_CONTRACT).lower():
                try:
                    return Decimal(str(v)) / Decimal("1000000")
                except Exception:
                    return None
    return None
