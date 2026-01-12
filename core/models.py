# core/models.py
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

    conn.commit()

    _ensure_columns(
        conn,
        "users",
        {
            "remind_3d_at": "remind_3d_at DATETIME NULL",
            "remind_1d_at": "remind_1d_at DATETIME NULL",
            "expired_handled_at": "expired_handled_at DATETIME NULL",
        },
    )

    _ensure_indexes(
        conn,
        [
            ("usdt_txs", "idx_usdt_txs_uid_status", "INDEX idx_usdt_txs_uid_status (telegram_id, status)"),
            ("usdt_txs", "idx_usdt_txs_addr_status", "INDEX idx_usdt_txs_addr_status (addr, status)"),
            ("address_pool", "idx_address_pool_assigned", "INDEX idx_address_pool_assigned (assigned_to)"),
            ("orders", "idx_orders_status_amount", "INDEX idx_orders_status_amount (status, amount, created_at)"),
        ],
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
        INSERT INTO orders (telegram_id, addr, amount, plan_code, status, tx_id)
        VALUES (%s, %s, %s, %s, 'pending', NULL)
        """,
        (telegram_id, addr, str(amount), plan_code),
    )
    order_id = cur.lastrowid
    conn.commit()
    cur.close(); conn.close()
    return int(order_id)


def match_pending_order_by_amount(addr: str, amount: Decimal, eps: Decimal) -> Optional[Dict]:
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    low = amount - eps
    high = amount + eps
    cur.execute(
        """
        SELECT *
        FROM orders
        WHERE addr=%s AND status='pending' AND amount BETWEEN %s AND %s
        ORDER BY created_at ASC
        LIMIT 1
        """,
        (addr, str(low), str(high)),
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
