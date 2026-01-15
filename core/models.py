# core/models.py
import json
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Optional

from core.db import get_conn
from config import USDT_ADDRESS_POOL, DB_NAME


def init_tables():
    conn = get_conn(); cur = conn.cursor()

    # 用户表
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        telegram_id      BIGINT PRIMARY KEY,
        username         VARCHAR(64),
        language         VARCHAR(8),
        wallet_addr      VARCHAR(64),
        addr_index       INT,
        total_received   DECIMAL(18,6) DEFAULT 0,
        paid_until       DATETIME NULL,
        last_plan        VARCHAR(32),
        inviter_id       BIGINT DEFAULT NULL,
        invite_count     INT DEFAULT 0,
        invite_reward_days INT DEFAULT 0,
        created_at       DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # 订单记录
    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id           BIGINT AUTO_INCREMENT PRIMARY KEY,
        telegram_id  BIGINT,
        addr         VARCHAR(64),
        amount       DECIMAL(18,6),
        plan_code    VARCHAR(32),
        status       VARCHAR(16),
        tx_id        VARCHAR(128),
        created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # 视频表（可用于后台统计）
    cur.execute("""
    CREATE TABLE IF NOT EXISTS videos (
        id            BIGINT AUTO_INCREMENT PRIMARY KEY,
        channel_id    BIGINT,
        message_id    BIGINT,
        file_id       VARCHAR(128),
        caption       TEXT,
        view_count    INT DEFAULT 0,
        created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # 邀请记录
    cur.execute("""
    CREATE TABLE IF NOT EXISTS invites (
        id           BIGINT AUTO_INCREMENT PRIMARY KEY,
        inviter_id   BIGINT NOT NULL,
        invitee_id   BIGINT NOT NULL,
        reward_days  INT DEFAULT 0,
        created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS address_pool (
        addr         VARCHAR(64) PRIMARY KEY,
        assigned_to  BIGINT NULL,
        assigned_at  DATETIME NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS usdt_txs (
        tx_id           VARCHAR(128) PRIMARY KEY,
        telegram_id     BIGINT,
        addr            VARCHAR(64),
        from_addr       VARCHAR(64),
        amount          DECIMAL(18,6),
        block_time      DATETIME NULL,
        status          VARCHAR(16) DEFAULT 'seen',
        plan_code       VARCHAR(32) NULL,
        credited_amount DECIMAL(18,6) NULL,
        processed_at    DATETIME NULL,
        created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS support_relay (
            id              BIGINT AUTO_INCREMENT PRIMARY KEY,
            group_chat_id   BIGINT NOT NULL,
            group_msg_id    BIGINT NOT NULL,
            user_id         BIGINT NOT NULL,
            user_msg_id     BIGINT NULL,
            created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_support_map (group_chat_id, group_msg_id),
            INDEX idx_support_user (user_id, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_audit (
            id           BIGINT AUTO_INCREMENT PRIMARY KEY,
            actor        VARCHAR(128) NOT NULL,
            action       VARCHAR(64) NOT NULL,
            target_id    BIGINT NULL,
            payload      JSON NULL,
            created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_admin_audit_action_time (action, created_at),
            INDEX idx_admin_audit_target_time (target_id, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clip_dispatch (
            id              BIGINT AUTO_INCREMENT PRIMARY KEY,
            source_chat_id  BIGINT NOT NULL,
            source_msg_id   BIGINT NOT NULL,
            target_chat_id  BIGINT NOT NULL,
            origin          VARCHAR(32) NOT NULL,
            status          VARCHAR(16) NOT NULL,
            created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_clip_dispatch (source_chat_id, source_msg_id, target_chat_id),
            INDEX idx_clip_dispatch_status_time (status, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS coupons (
            code          VARCHAR(32) PRIMARY KEY,
            kind          VARCHAR(16) NOT NULL,
            value         DECIMAL(18,6) NOT NULL,
            plan_codes    VARCHAR(256) NULL,
            max_uses      INT NULL,
            used_count    INT DEFAULT 0,
            expires_at    DATETIME NULL,
            active        TINYINT DEFAULT 1,
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS access_codes (
            code          VARCHAR(64) PRIMARY KEY,
            days          INT NOT NULL,
            plan_code     VARCHAR(32) NULL,
            max_uses      INT DEFAULT 1,
            used_count    INT DEFAULT 0,
            expires_at    DATETIME NULL,
            note          VARCHAR(256) NULL,
            created_by    VARCHAR(128) NULL,
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_used_at  DATETIME NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS broadcast_jobs (
            id            BIGINT AUTO_INCREMENT PRIMARY KEY,
            segment       VARCHAR(64) NOT NULL,
            source        VARCHAR(64) NULL,
            text          TEXT NOT NULL,
            parse_mode    VARCHAR(16) NULL,
            status        VARCHAR(16) NOT NULL,
            created_by    VARCHAR(128) NULL,
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
            started_at    DATETIME NULL,
            finished_at   DATETIME NULL,
            total         INT DEFAULT 0,
            success       INT DEFAULT 0,
            failed        INT DEFAULT 0
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS broadcast_logs (
            id            BIGINT AUTO_INCREMENT PRIMARY KEY,
            job_id        BIGINT NOT NULL,
            telegram_id   BIGINT NOT NULL,
            status        VARCHAR(16) NOT NULL,
            error         VARCHAR(256) NULL,
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_broadcast_logs_job (job_id, created_at),
            INDEX idx_broadcast_logs_uid (telegram_id, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    conn.commit()

    _ensure_columns(
        conn,
        "users",
        {
            "remind_7d_at": "remind_7d_at DATETIME NULL",
            "remind_3d_at": "remind_3d_at DATETIME NULL",
            "remind_1d_at": "remind_1d_at DATETIME NULL",
            "expired_handled_at": "expired_handled_at DATETIME NULL",
            "expired_recall_1d_at": "expired_recall_1d_at DATETIME NULL",
            "expired_recall_3d_at": "expired_recall_3d_at DATETIME NULL",
            "expired_recall_7d_at": "expired_recall_7d_at DATETIME NULL",
            "first_source": "first_source VARCHAR(64) NULL",
            "first_source_at": "first_source_at DATETIME NULL",
            "last_source": "last_source VARCHAR(64) NULL",
            "last_source_at": "last_source_at DATETIME NULL",
            "pending_coupon": "pending_coupon VARCHAR(32) NULL",
            "is_blacklisted": "is_blacklisted TINYINT DEFAULT 0",
            "is_whitelisted": "is_whitelisted TINYINT DEFAULT 0",
            "note": "note VARCHAR(256) NULL",
        },
    )

    _ensure_columns(
        conn,
        "orders",
        {
            "base_amount": "base_amount DECIMAL(18,6) NULL",
            "discount_amount": "discount_amount DECIMAL(18,6) NULL",
            "coupon_code": "coupon_code VARCHAR(32) NULL",
            "coupon_used": "coupon_used TINYINT DEFAULT 0",
        },
    )

    _ensure_indexes(
        conn,
        [
            ("usdt_txs", "idx_usdt_txs_uid_status", "INDEX idx_usdt_txs_uid_status (telegram_id, status)"),
            ("usdt_txs", "idx_usdt_txs_addr_status", "INDEX idx_usdt_txs_addr_status (addr, status)"),
            ("address_pool", "idx_address_pool_assigned", "INDEX idx_address_pool_assigned (assigned_to)"),
            ("orders", "idx_orders_status_amount", "INDEX idx_orders_status_amount (status, amount, created_at)"),
            ("users", "idx_users_paid_until", "INDEX idx_users_paid_until (paid_until)"),
            ("users", "idx_users_source", "INDEX idx_users_source (last_source, last_source_at)"),
        ],
    )

    _ensure_columns(
        conn,
        "broadcast_jobs",
        {
            "button_text": "button_text VARCHAR(64) NULL",
            "button_url": "button_url VARCHAR(512) NULL",
            "disable_preview": "disable_preview TINYINT DEFAULT 0",
            "media_type": "media_type VARCHAR(16) NULL",
            "media": "media VARCHAR(512) NULL",
        },
    )

    _sync_address_pool(conn)

    cur.close(); conn.close()


def _ensure_columns(conn, table: str, columns: Dict[str, str]):
    cur = conn.cursor()
    for col, col_def in columns.items():
        cur.execute(
            """
            SELECT 1
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
            LIMIT 1
            """,
            (DB_NAME, table, col),
        )
        exists = cur.fetchone() is not None
        if exists:
            continue
        cur.execute(f"ALTER TABLE `{table}` ADD COLUMN {col_def}")
    conn.commit()
    cur.close()


def _ensure_indexes(conn, defs: list[tuple[str, str, str]]):
    cur = conn.cursor()
    for table, index_name, ddl in defs:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND INDEX_NAME=%s
            LIMIT 1
            """,
            (DB_NAME, table, index_name),
        )
        exists = cur.fetchone() is not None
        if exists:
            continue
        cur.execute(f"ALTER TABLE `{table}` ADD {ddl}")
    conn.commit()
    cur.close()


def _sync_address_pool(conn):
    cur = conn.cursor()
    for addr in USDT_ADDRESS_POOL:
        cur.execute("INSERT IGNORE INTO address_pool (addr) VALUES (%s)", (addr,))
    if USDT_ADDRESS_POOL:
        placeholders = ",".join(["%s"] * len(USDT_ADDRESS_POOL))
        cur.execute(
            f"""
            DELETE FROM address_pool
            WHERE assigned_to IS NULL
              AND addr NOT IN ({placeholders})
            """,
            tuple(USDT_ADDRESS_POOL),
        )
    conn.commit()
    cur.close()


# ------------ 用户基本操作 ------------

def get_user(telegram_id: int) -> Optional[Dict]:
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE telegram_id=%s", (telegram_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row

def upsert_user_basic(telegram_id: int, username: str, language: str):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO users (telegram_id, username, language)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE username=%s, language=%s
    """, (telegram_id, username, language, username, language))
    conn.commit()
    cur.close(); conn.close()


def set_user_source(telegram_id: int, source: str):
    source = (source or "").strip()
    if not source:
        return
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        UPDATE users
        SET
          first_source = COALESCE(first_source, %s),
          first_source_at = COALESCE(first_source_at, UTC_TIMESTAMP()),
          last_source = %s,
          last_source_at = UTC_TIMESTAMP()
        WHERE telegram_id=%s
        """,
        (source, source, int(telegram_id)),
    )
    conn.commit()
    cur.close(); conn.close()


def set_user_pending_coupon(telegram_id: int, coupon_code: str | None):
    coupon_code = (coupon_code or "").strip()
    if coupon_code == "":
        coupon_code = None
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE users SET pending_coupon=%s WHERE telegram_id=%s", (coupon_code, int(telegram_id)))
    conn.commit()
    cur.close(); conn.close()


def admin_audit_log(actor: str, action: str, target_id: int | None, payload: dict):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "INSERT INTO admin_audit (actor, action, target_id, payload) VALUES (%s,%s,%s,%s)",
        (actor, action, target_id, json.dumps(payload, ensure_ascii=False)),
    )
    conn.commit()
    cur.close(); conn.close()


def create_access_code(
    code: str,
    days: int,
    plan_code: str | None,
    max_uses: int,
    expires_at: datetime | None,
    note: str | None,
    created_by: str | None,
):
    code = (code or "").strip()
    if not code:
        raise ValueError("code required")
    days = int(days)
    if days == 0:
        raise ValueError("days required")
    max_uses = max(1, int(max_uses))
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO access_codes (code, days, plan_code, max_uses, used_count, expires_at, note, created_by)
        VALUES (%s,%s,%s,%s,0,%s,%s,%s)
        ON DUPLICATE KEY UPDATE days=VALUES(days), plan_code=VALUES(plan_code), max_uses=VALUES(max_uses), expires_at=VALUES(expires_at), note=VALUES(note), created_by=VALUES(created_by)
        """,
        (code, days, plan_code, max_uses, expires_at, (note or "")[:256] if note else None, created_by),
    )
    conn.commit()
    cur.close(); conn.close()


def get_access_code(code: str) -> Optional[Dict]:
    code = (code or "").strip()
    if not code:
        return None
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM access_codes WHERE code=%s LIMIT 1", (code,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row


def redeem_access_code(code: str, telegram_id: int) -> tuple[bool, str, datetime | None, int]:
    code = (code or "").strip()
    if not code:
        return False, "code empty", None, 0
    telegram_id = int(telegram_id)
    conn = get_conn()
    try:
        conn.start_transaction()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT is_blacklisted, is_whitelisted FROM users WHERE telegram_id=%s FOR UPDATE", (telegram_id,))
        urow = cur.fetchone() or {}
        if int(urow.get("is_blacklisted") or 0) == 1 and int(urow.get("is_whitelisted") or 0) != 1:
            conn.rollback()
            return False, "user blacklisted", None, 0
        cur.execute("SELECT * FROM access_codes WHERE code=%s FOR UPDATE", (code,))
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return False, "code not found", None, 0
        expires_at = row.get("expires_at")
        if expires_at and expires_at <= datetime.utcnow():
            conn.rollback()
            return False, "code expired", None, 0
        used = int(row.get("used_count") or 0)
        max_uses = int(row.get("max_uses") or 1)
        if used >= max_uses:
            conn.rollback()
            return False, "code used up", None, 0
        days = int(row.get("days") or 0)
        if days == 0:
            conn.rollback()
            return False, "bad code", None, 0
        plan_code = (row.get("plan_code") or "ACCESS").strip()

        cur2 = conn.cursor()
        cur2.execute(
            """
            UPDATE access_codes
            SET used_count=used_count+1, last_used_at=UTC_TIMESTAMP()
            WHERE code=%s
            """,
            (code,),
        )
        cur2.execute(
            """
            UPDATE users
            SET paid_until = DATE_ADD(
              IF(paid_until IS NULL OR paid_until < UTC_TIMESTAMP(), UTC_TIMESTAMP(), paid_until),
              INTERVAL %s DAY
            ),
            last_plan=%s
            WHERE telegram_id=%s
            """,
            (days, plan_code, telegram_id),
        )
        conn.commit()

        cur3 = conn.cursor()
        cur3.execute("SELECT paid_until FROM users WHERE telegram_id=%s", (telegram_id,))
        paid_row = cur3.fetchone()
        paid_until = paid_row[0] if paid_row else None
        return True, "", paid_until, days
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, f"{type(e).__name__}: {e}", None, 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


def set_user_flags(telegram_id: int, is_blacklisted: bool | int | None, is_whitelisted: bool | int | None, note: str | None):
    telegram_id = int(telegram_id)
    b = None if is_blacklisted is None else (1 if bool(is_blacklisted) else 0)
    w = None if is_whitelisted is None else (1 if bool(is_whitelisted) else 0)
    note_v = (note or "").strip()
    if note_v == "":
        note_v = None
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        UPDATE users
        SET is_blacklisted=COALESCE(%s, is_blacklisted),
            is_whitelisted=COALESCE(%s, is_whitelisted),
            note=%s
        WHERE telegram_id=%s
        """,
        (b, w, (note_v or "")[:256] if note_v else None, telegram_id),
    )
    conn.commit()
    cur.close(); conn.close()

def allocate_address(telegram_id: int) -> str:
    u = get_user(telegram_id)
    if u and u.get("wallet_addr"):
        return u["wallet_addr"]

    conn = get_conn()
    try:
        conn.start_transaction()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT addr FROM address_pool WHERE assigned_to IS NULL LIMIT 1 FOR UPDATE")
        row = cur.fetchone()
        if not row:
            conn.rollback()
            raise RuntimeError("收款地址池已满，请扩容 USDT_ADDRESS_POOL")
        addr = row["addr"]

        cur2 = conn.cursor()
        cur2.execute(
            """
            UPDATE address_pool
            SET assigned_to=%s, assigned_at=UTC_TIMESTAMP()
            WHERE addr=%s AND assigned_to IS NULL
            """,
            (telegram_id, addr),
        )
        if cur2.rowcount != 1:
            conn.rollback()
            raise RuntimeError("分配收款地址冲突，请重试")

        try:
            idx = USDT_ADDRESS_POOL.index(addr)
        except Exception:
            idx = None

        cur2.execute(
            """
            INSERT INTO users (telegram_id, wallet_addr, addr_index)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE wallet_addr=%s, addr_index=%s
            """,
            (telegram_id, addr, idx, addr, idx),
        )
        conn.commit()
        cur2.close(); cur.close()
        return addr
    finally:
        conn.close()


def reset_user_address(telegram_id: int) -> str | None:
    u = get_user(telegram_id)
    if not u or not u.get("wallet_addr"):
        return None
    old_addr = u["wallet_addr"]

    conn = get_conn()
    try:
        conn.start_transaction()
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET wallet_addr=NULL, addr_index=NULL WHERE telegram_id=%s",
            (telegram_id,),
        )
        cur.execute(
            "UPDATE address_pool SET assigned_to=NULL, assigned_at=NULL WHERE assigned_to=%s",
            (telegram_id,),
        )
        conn.commit()
        cur.close()
        return old_addr
    finally:
        conn.close()


def get_all_users() -> List[Dict]:
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def update_user_payment(telegram_id: int,
                        new_paid_until: datetime,
                        new_total: Decimal,
                        last_plan: str):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET paid_until=%s, total_received=%s, last_plan=%s
        WHERE telegram_id=%s
    """, (new_paid_until, str(new_total), last_plan, telegram_id))
    conn.commit()
    cur.close(); conn.close()

def get_expired_users(now: datetime) -> List[Dict]:
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT * FROM users
        WHERE paid_until IS NOT NULL AND paid_until < %s
    """, (now,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def get_unhandled_expired_users(now: datetime) -> List[Dict]:
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT * FROM users
        WHERE paid_until IS NOT NULL
          AND paid_until < %s
          AND expired_handled_at IS NULL
        """,
        (now,),
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def mark_user_expired_handled(telegram_id: int, handled_at: datetime | None = None):
    handled_at = handled_at or datetime.utcnow()
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "UPDATE users SET expired_handled_at=%s WHERE telegram_id=%s",
        (handled_at, telegram_id),
    )
    conn.commit()
    cur.close(); conn.close()


def get_users_expiring_within_days(now: datetime, days: int, reminder_column: str) -> List[Dict]:
    until = now + timedelta(days=days)
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute(
        f"""
        SELECT * FROM users
        WHERE paid_until IS NOT NULL
          AND paid_until > %s
          AND paid_until <= %s
          AND {reminder_column} IS NULL
        """,
        (now, until),
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def mark_user_reminded(telegram_id: int, reminder_column: str, reminded_at: datetime | None = None):
    reminded_at = reminded_at or datetime.utcnow()
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        f"UPDATE users SET {reminder_column}=%s WHERE telegram_id=%s",
        (reminded_at, telegram_id),
    )
    conn.commit()
    cur.close(); conn.close()


def get_expired_users_for_recall(now: datetime, days_after: int, recall_column: str) -> List[Dict]:
    days_after = int(days_after or 0)
    if days_after <= 0:
        return []
    cutoff = now - timedelta(days=days_after)
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute(
        f"""
        SELECT * FROM users
        WHERE paid_until IS NOT NULL
          AND paid_until <= %s
          AND paid_until <= %s
          AND {recall_column} IS NULL
        """,
        (now, cutoff),
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


# ------------ 订单 ------------

def create_order(telegram_id: int, addr: str, amount: Decimal,
                 plan_code: str, tx_id: Optional[str] = None):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO orders (telegram_id, addr, amount, plan_code, status, tx_id)
        VALUES (%s, %s, %s, %s, 'success', %s)
    """, (telegram_id, addr, str(amount), plan_code, tx_id))
    conn.commit()
    cur.close(); conn.close()


def create_pending_order(telegram_id: int, addr: str, amount: Decimal, plan_code: str) -> int:
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO orders (telegram_id, addr, amount, base_amount, discount_amount, coupon_code, coupon_used, plan_code, status, tx_id)
        VALUES (%s, %s, %s, NULL, NULL, NULL, 0, %s, 'pending', NULL)
        """,
        (telegram_id, addr, str(amount), plan_code),
    )
    order_id = cur.lastrowid
    conn.commit()
    cur.close(); conn.close()
    return int(order_id)


def get_coupon(code: str) -> Optional[Dict]:
    code = (code or "").strip()
    if not code:
        return None
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM coupons WHERE code=%s LIMIT 1", (code,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row


def _coupon_applicable(coupon: Dict, plan_code: str, now: datetime) -> bool:
    if not coupon:
        return False
    if int(coupon.get("active") or 0) != 1:
        return False
    exp = coupon.get("expires_at")
    if exp and exp <= now:
        return False
    max_uses = coupon.get("max_uses")
    used = int(coupon.get("used_count") or 0)
    if max_uses is not None and used >= int(max_uses):
        return False
    plan_codes = (coupon.get("plan_codes") or "").strip()
    if plan_codes:
        allowed = {x.strip() for x in plan_codes.split(",") if x.strip()}
        if allowed and plan_code not in allowed:
            return False
    return True


def coupon_basic_valid(code: str) -> bool:
    c = get_coupon(code)
    if not c:
        return False
    if int(c.get("active") or 0) != 1:
        return False
    exp = c.get("expires_at")
    if exp and exp <= datetime.utcnow():
        return False
    max_uses = c.get("max_uses")
    used = int(c.get("used_count") or 0)
    if max_uses is not None and used >= int(max_uses):
        return False
    return True


def _apply_coupon(base_amount: Decimal, coupon: Dict) -> tuple[Decimal, Decimal]:
    base_amount = Decimal(str(base_amount))
    kind = (coupon.get("kind") or "").strip().lower()
    value = Decimal(str(coupon.get("value") or 0))
    discount = Decimal("0")
    if kind == "percent":
        if value > 0:
            discount = (base_amount * value / Decimal("100"))
    elif kind == "fixed":
        if value > 0:
            discount = value
    if discount < 0:
        discount = Decimal("0")
    if discount > base_amount:
        discount = base_amount
    final = base_amount - discount
    if final < Decimal("0.01"):
        final = Decimal("0.01")
        discount = base_amount - final
    final = final.quantize(Decimal("0.000001"))
    discount = discount.quantize(Decimal("0.000001"))
    return final, discount


def compute_amount_after_coupon(base_amount: Decimal, plan_code: str, coupon_code: str | None) -> tuple[Decimal, Decimal, str | None]:
    now = datetime.utcnow()
    base_amount = Decimal(str(base_amount)).quantize(Decimal("0.000001"))
    coupon_code = (coupon_code or "").strip() or None
    if not coupon_code:
        return base_amount, Decimal("0.000000"), None
    coupon = get_coupon(coupon_code)
    if not coupon or not _coupon_applicable(coupon, plan_code, now):
        return base_amount, Decimal("0.000000"), None
    final, discount = _apply_coupon(base_amount, coupon)
    return final, discount, coupon_code


def create_pending_order_priced(
    telegram_id: int,
    addr: str,
    amount: Decimal,
    base_amount: Decimal,
    plan_code: str,
    coupon_code: str | None,
) -> tuple[int, Decimal]:
    now = datetime.utcnow()
    amount = Decimal(str(amount)).quantize(Decimal("0.000001"))
    base_amount = Decimal(str(base_amount)).quantize(Decimal("0.000001"))
    coupon_code = (coupon_code or "").strip() or None
    final_amount = amount
    discount_amount = Decimal("0.000000")
    applied_code = None
    if coupon_code:
        coupon = get_coupon(coupon_code)
        if coupon and _coupon_applicable(coupon, plan_code, now):
            discounted, discount_amount = _apply_coupon(base_amount, coupon)
            final_amount = discounted
            applied_code = coupon_code

    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO orders (telegram_id, addr, amount, base_amount, discount_amount, coupon_code, coupon_used, plan_code, status, tx_id)
        VALUES (%s, %s, %s, %s, %s, %s, 0, %s, 'pending', NULL)
        """,
        (int(telegram_id), addr, str(amount), str(base_amount), str(discount_amount), applied_code, plan_code),
    )
    order_id = cur.lastrowid
    conn.commit()
    cur.close(); conn.close()
    return int(order_id), final_amount


def match_pending_order_by_amount(addr: str, amount: Decimal, eps: Decimal) -> Optional[Dict]:
    from config import MATCH_ORDER_LOOKBACK_HOURS, MATCH_ORDER_PREFER_RECENT
    return match_pending_order_by_amount_v2(
        addr=addr,
        amount=amount,
        eps=eps,
        tx_time=None,
        lookback_hours=int(MATCH_ORDER_LOOKBACK_HOURS),
        prefer_recent=bool(MATCH_ORDER_PREFER_RECENT),
    )


def match_pending_order_by_amount_v2(
    addr: str,
    amount: Decimal,
    eps: Decimal,
    tx_time: datetime | None,
    lookback_hours: int,
    prefer_recent: bool,
) -> Optional[Dict]:
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    low = amount - eps
    high = amount + eps
    lookback_hours = max(1, min(int(lookback_hours or 0), 720))
    where = ["addr=%s", "status='pending'", "amount BETWEEN %s AND %s", f"created_at >= (UTC_TIMESTAMP() - INTERVAL {lookback_hours} HOUR)"]
    params: list = [addr, str(low), str(high)]
    if tx_time:
        where.append("created_at <= %s")
        params.append(tx_time)
    order_by = "created_at DESC" if prefer_recent else "created_at ASC"
    cur.execute(
        f"""
        SELECT *
        FROM orders
        WHERE {' AND '.join(where)}
        ORDER BY {order_by}
        LIMIT 1
        """,
        tuple(params),
    )
    row = cur.fetchone()
    cur.close(); conn.close()
    return row


def mark_order_success(order_id: int, tx_id: str):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "UPDATE orders SET status='success', tx_id=%s WHERE id=%s",
        (tx_id, order_id),
    )
    conn.commit()
    cur.close(); conn.close()

    conn2 = get_conn(); cur2 = conn2.cursor()
    cur2.execute("SELECT coupon_code, coupon_used FROM orders WHERE id=%s LIMIT 1", (order_id,))
    row = cur2.fetchone()
    if row:
        coupon_code = row[0]
        coupon_used = int(row[1] or 0)
        if coupon_code and coupon_used == 0:
            cur2.execute("UPDATE orders SET coupon_used=1 WHERE id=%s", (order_id,))
            cur2.execute("UPDATE coupons SET used_count=used_count+1 WHERE code=%s", (coupon_code,))
            conn2.commit()
    cur2.close(); conn2.close()


def get_success_orders_between(start: datetime, end: datetime) -> List[Dict]:
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT telegram_id, addr, amount, plan_code, tx_id, created_at
        FROM orders
        WHERE status='success' AND created_at >= %s AND created_at < %s
        ORDER BY created_at ASC
        """,
        (start, end),
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def support_store_mapping(group_chat_id: int, group_msg_id: int, user_id: int, user_msg_id: int | None) -> None:
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO support_relay (group_chat_id, group_msg_id, user_id, user_msg_id)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE user_id=VALUES(user_id), user_msg_id=VALUES(user_msg_id)
        """,
        (group_chat_id, group_msg_id, user_id, user_msg_id),
    )
    conn.commit()
    cur.close(); conn.close()


def support_get_user_id(group_chat_id: int, group_msg_id: int) -> int | None:
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        "SELECT user_id FROM support_relay WHERE group_chat_id=%s AND group_msg_id=%s",
        (group_chat_id, group_msg_id),
    )
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def insert_usdt_tx_if_new(
    telegram_id: int | None,
    addr: str,
    tx_id: str,
    amount: Decimal,
    from_addr: str | None = None,
    block_time: datetime | None = None,
) -> bool:
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        INSERT IGNORE INTO usdt_txs (tx_id, telegram_id, addr, from_addr, amount, block_time, status)
        VALUES (%s, %s, %s, %s, %s, %s, 'seen')
        """,
        (tx_id, telegram_id, addr, from_addr or "", str(amount), block_time),
    )
    inserted = cur.rowcount == 1
    conn.commit()
    cur.close(); conn.close()
    return inserted


def get_address_assigned_at(addr: str) -> datetime | None:
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT assigned_at FROM address_pool WHERE addr=%s", (addr,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        return None
    return row[0]


def get_unassigned_usdt_txs(addr: str, confirm_before: datetime) -> List[Dict]:
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT *
        FROM usdt_txs
        WHERE addr=%s AND status='seen' AND telegram_id IS NULL
          AND (block_time IS NULL OR block_time <= %s)
        ORDER BY block_time ASC, created_at ASC
        """,
        (addr, confirm_before),
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def get_unassigned_usdt_txs_since(addr: str, confirm_before: datetime, since: datetime | None) -> List[Dict]:
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    if since:
        cur.execute(
            """
            SELECT *
            FROM usdt_txs
            WHERE addr=%s AND status='seen' AND telegram_id IS NULL
              AND block_time IS NOT NULL
              AND block_time >= %s AND block_time <= %s
            ORDER BY block_time ASC, created_at ASC
            """,
            (addr, since, confirm_before),
        )
    else:
        cur.execute(
            """
            SELECT *
            FROM usdt_txs
            WHERE addr=%s AND status='seen' AND telegram_id IS NULL
              AND block_time IS NOT NULL
              AND block_time <= %s
            ORDER BY block_time ASC, created_at ASC
            """,
            (addr, confirm_before),
        )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def get_pending_usdt_txs(telegram_id: int, addr: str, confirm_before: datetime) -> List[Dict]:
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT * FROM usdt_txs
        WHERE telegram_id=%s AND addr=%s AND status='seen'
          AND (block_time IS NULL OR block_time <= %s)
        ORDER BY block_time ASC, created_at ASC
        """,
        (telegram_id, addr, confirm_before),
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def set_usdt_tx_status(
    tx_id: str,
    status: str,
    plan_code: str | None = None,
    credited_amount: Decimal | None = None,
    processed_at: datetime | None = None,
    telegram_id: int | None = None,
):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        UPDATE usdt_txs
        SET status=%s, telegram_id=COALESCE(%s, telegram_id), plan_code=%s, credited_amount=%s, processed_at=%s
        WHERE tx_id=%s
        """,
        (
            status,
            telegram_id,
            plan_code,
            str(credited_amount) if credited_amount is not None else None,
            processed_at,
            tx_id,
        ),
    )
    conn.commit()
    cur.close(); conn.close()


# ------------ 邀请体系 ------------

def bind_inviter(invitee_id: int, inviter_id: int):
    """首次 /start ref_xxx 时绑定邀请人，只能绑一次，且不能自己邀请自己"""
    if invitee_id == inviter_id:
        return
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET inviter_id=%s
        WHERE telegram_id=%s AND inviter_id IS NULL
    """, (inviter_id, invitee_id))
    conn.commit()
    cur.close(); conn.close()

def get_inviter_id(invitee_id: int) -> Optional[int]:
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT inviter_id FROM users WHERE telegram_id=%s", (invitee_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        return None
    return row[0]

def add_invite_reward(inviter_id: int, days: int, invitee_id: int):
    if days <= 0:
        return
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        UPDATE users
        SET invite_count = invite_count + 1,
            invite_reward_days = invite_reward_days + %s
        WHERE telegram_id=%s
    """, (days, inviter_id))
    cur.execute("""
        INSERT INTO invites (inviter_id, invitee_id, reward_days)
        VALUES (%s, %s, %s)
    """, (inviter_id, invitee_id, days))
    conn.commit()
    cur.close(); conn.close()


# ------------ 视频记录 ------------

def create_video_post(channel_id: int, message_id: int, file_id: str, caption: str | None = None):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO videos (channel_id, message_id, file_id, caption)
        VALUES (%s, %s, %s, %s)
        """,
        (channel_id, message_id, file_id, caption or ""),
    )
    conn.commit()
    cur.close(); conn.close()


def claim_clip_dispatch(source_chat_id: int, source_msg_id: int, target_chat_id: int, origin: str) -> bool:
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        INSERT IGNORE INTO clip_dispatch (source_chat_id, source_msg_id, target_chat_id, origin, status)
        VALUES (%s, %s, %s, %s, 'sending')
        """,
        (int(source_chat_id), int(source_msg_id), int(target_chat_id), (origin or "")[:32]),
    )
    ok = int(cur.rowcount or 0) == 1
    conn.commit()
    cur.close(); conn.close()
    return ok


def claim_clip_dispatch_takeover(
    source_chat_id: int, source_msg_id: int, target_chat_id: int, origin: str, takeover_after_sec: int = 600
) -> bool:
    takeover_after_sec = int(takeover_after_sec or 0)
    if takeover_after_sec < 0:
        takeover_after_sec = 0

    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            INSERT IGNORE INTO clip_dispatch (source_chat_id, source_msg_id, target_chat_id, origin, status)
            VALUES (%s, %s, %s, %s, 'sending')
            """,
            (int(source_chat_id), int(source_msg_id), int(target_chat_id), (origin or "")[:32]),
        )
        if int(cur.rowcount or 0) == 1:
            conn.commit()
            cur.close()
            return True

        cur.execute(
            """
            SELECT status,
                   GREATEST(
                       IFNULL(TIMESTAMPDIFF(SECOND, updated_at, UTC_TIMESTAMP()), 0),
                       IFNULL(TIMESTAMPDIFF(SECOND, created_at, UTC_TIMESTAMP()), 0)
                   ) AS age_sec
            FROM clip_dispatch
            WHERE source_chat_id=%s AND source_msg_id=%s AND target_chat_id=%s
            LIMIT 1
            """,
            (int(source_chat_id), int(source_msg_id), int(target_chat_id)),
        )
        row = cur.fetchone() or {}
        status = str((row.get("status") or "")).strip().lower()
        age_sec = int(row.get("age_sec") or 0)

        if status == "sent":
            conn.commit()
            cur.close()
            return False

        if takeover_after_sec and age_sec >= takeover_after_sec:
            cur.execute(
                """
                UPDATE clip_dispatch
                SET origin=%s, status='sending'
                WHERE source_chat_id=%s AND source_msg_id=%s AND target_chat_id=%s
                """,
                ((origin or "")[:32], int(source_chat_id), int(source_msg_id), int(target_chat_id)),
            )
            ok = int(cur.rowcount or 0) == 1
            conn.commit()
            cur.close()
            return ok

        conn.commit()
        cur.close()
        return False
    finally:
        conn.close()


def mark_clip_dispatch_sent(source_chat_id: int, source_msg_id: int, target_chat_id: int):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        UPDATE clip_dispatch
        SET status='sent'
        WHERE source_chat_id=%s AND source_msg_id=%s AND target_chat_id=%s
        """,
        (int(source_chat_id), int(source_msg_id), int(target_chat_id)),
    )
    conn.commit()
    cur.close(); conn.close()


def unclaim_clip_dispatch(source_chat_id: int, source_msg_id: int, target_chat_id: int):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM clip_dispatch
        WHERE source_chat_id=%s AND source_msg_id=%s AND target_chat_id=%s
        """,
        (int(source_chat_id), int(source_msg_id), int(target_chat_id)),
    )
    conn.commit()
    cur.close(); conn.close()
