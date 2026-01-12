# bot/payments.py
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Tuple

from config import PLANS, AMOUNT_EPS

def split_amount_to_plans(delta: Decimal) -> Tuple[List[Dict], Decimal]:
    """
    把新增金额拆成若干套餐（贪心：价格从大到小）
    """
    delta = Decimal(str(delta))
    eps = Decimal(str(AMOUNT_EPS))
    remain = delta
    result: List[Dict] = []

    for plan in sorted(PLANS, key=lambda x: x["price"], reverse=True):
        price = plan["price"]
        count = (remain + eps) // price
        if count > 0:
            for _ in range(int(count)):
                result.append(plan)
                remain -= price

    return result, remain

def compute_new_paid_until(old_paid_until: datetime, plans: List[Dict]) -> datetime:
    now = datetime.utcnow()
    base = old_paid_until if (old_paid_until and old_paid_until > now) else now
    total_days = sum(p["days"] for p in plans)
    return base + timedelta(days=total_days)