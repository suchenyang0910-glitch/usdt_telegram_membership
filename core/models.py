import json
from datetime import datetime, timedelta
from decimal import Decimal

from config import AMOUNT_EPS, MATCH_ORDER_LOOKBACK_HOURS, MATCH_ORDER_PREFER_RECENT, PLANS
from core.db import get_conn


def _utc_now() -> datetime:
    return datetime.utcnow()

def _index_exists(cur, table: str, index_name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(1)
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND INDEX_NAME = %s
        """,
        (table, index_name),
    )
    row = cur.fetchone()
    try:
        return int(row[0] or 0) > 0
    except Exception:
        return False


def _ensure_index(cur, table: str, index_name: str, columns_sql: str):
    if _index_exists(cur, table, index_name):
        return
    cur.execute(f"CREATE INDEX {index_name} ON {table} ({columns_sql})")

def _column_exists(cur, table: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(1)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        """,
        (table, column_name),
    )
    row = cur.fetchone()
    try:
        return int(row[0] or 0) > 0
    except Exception:
        return False


def _ensure_column(cur, table: str, column_name: str, column_sql: str):
    if _column_exists(cur, table, column_name):
        return
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column_sql}")


def init_tables():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            telegram_id BIGINT PRIMARY KEY,
            username VARCHAR(128),
            language VARCHAR(16),
            paid_until DATETIME NULL,
            total_received DECIMAL(24,8) DEFAULT 0,
            last_plan VARCHAR(32),
            wallet_addr VARCHAR(128),
            inviter_id BIGINT NULL,
            invite_count INT DEFAULT 0,
            invite_reward_days INT DEFAULT 0,
            pending_coupon VARCHAR(64),
            first_source VARCHAR(64),
            last_source VARCHAR(64),
            last_source_at DATETIME NULL,
            is_blacklisted TINYINT DEFAULT 0,
            is_whitelisted TINYINT DEFAULT 0,
            note VARCHAR(256),
            expired_handled_at DATETIME NULL,
            remind_7d_at DATETIME NULL,
            remind_3d_at DATETIME NULL,
            remind_1d_at DATETIME NULL,
            expired_recall_3d_at DATETIME NULL,
            expired_recall_7d_at DATETIME NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS address_pool (
            addr VARCHAR(128) PRIMARY KEY,
            assigned_to BIGINT NULL,
            assigned_at DATETIME NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    _ensure_index(cur, "address_pool", "idx_address_pool_assigned_to", "assigned_to")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            telegram_id BIGINT NOT NULL,
            addr VARCHAR(128),
            amount DECIMAL(24,8) NOT NULL,
            base_amount DECIMAL(24,8) NULL,
            discount_amount DECIMAL(24,8) NULL,
            coupon_code VARCHAR(64) NULL,
            coupon_used TINYINT DEFAULT 0,
            plan_code VARCHAR(32) NOT NULL,
            status VARCHAR(16) DEFAULT 'pending',
            tx_id VARCHAR(128) NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    _ensure_index(cur, "orders", "idx_orders_addr_status", "addr, status")
    _ensure_index(cur, "orders", "idx_orders_telegram_created", "telegram_id, created_at")
    _ensure_index(cur, "orders", "idx_orders_status_created", "status, created_at")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS usdt_txs (
            tx_id VARCHAR(128) PRIMARY KEY,
            telegram_id BIGINT NULL,
            addr VARCHAR(128) NULL,
            from_addr VARCHAR(128) NULL,
            amount DECIMAL(24,8) NOT NULL,
            status VARCHAR(16) DEFAULT 'seen',
            plan_code VARCHAR(32) NULL,
            credited_amount DECIMAL(24,8) NULL,
            processed_at DATETIME NULL,
            block_time DATETIME NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    _ensure_index(cur, "usdt_txs", "idx_usdt_txs_addr_created", "addr, created_at")
    _ensure_index(cur, "usdt_txs", "idx_usdt_txs_status_created", "status, created_at")
    _ensure_index(cur, "usdt_txs", "idx_usdt_txs_telegram_created", "telegram_id, created_at")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS coupons (
            code VARCHAR(64) PRIMARY KEY,
            kind VARCHAR(16) NOT NULL,
            value DECIMAL(24,8) NOT NULL,
            plan_codes VARCHAR(256) NULL,
            max_uses INT NULL,
            used_count INT DEFAULT 0,
            expires_at DATETIME NULL,
            active TINYINT DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS access_codes (
            code VARCHAR(64) PRIMARY KEY,
            days INT NOT NULL,
            plan_code VARCHAR(32) NULL,
            max_uses INT DEFAULT 1,
            used_count INT DEFAULT 0,
            expires_at DATETIME NULL,
            note VARCHAR(256) NULL,
            created_by VARCHAR(64) NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_used_at DATETIME NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_audit (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            actor VARCHAR(64) NOT NULL,
            action VARCHAR(128) NOT NULL,
            target_id BIGINT NULL,
            payload TEXT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS support_mapping (
            group_id BIGINT NOT NULL,
            ticket_message_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            user_message_id BIGINT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (group_id, ticket_message_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clip_dispatch (
            paid_channel_id BIGINT NOT NULL,
            paid_message_id BIGINT NOT NULL,
            target_channel_id BIGINT NOT NULL,
            actor VARCHAR(64) NULL,
            status VARCHAR(16) DEFAULT 'sending',
            claimed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            sent_at DATETIME NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (paid_channel_id, paid_message_id, target_channel_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS videos (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            channel_id BIGINT,
            message_id BIGINT,
            file_id VARCHAR(128),
            caption TEXT,
            view_count INT DEFAULT 0,
            category_id INT DEFAULT 0,
            free_channel_id BIGINT NULL,
            free_message_id BIGINT NULL,
            is_hot TINYINT DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    _ensure_column(cur, "videos", "category_id", "category_id INT DEFAULT 0")
    _ensure_column(cur, "videos", "free_channel_id", "free_channel_id BIGINT NULL")
    _ensure_column(cur, "videos", "free_message_id", "free_message_id BIGINT NULL")
    _ensure_column(cur, "videos", "is_hot", "is_hot TINYINT DEFAULT 0")
    _ensure_column(cur, "videos", "cover_url", "cover_url VARCHAR(512) NULL")
    _ensure_column(cur, "videos", "tags", "tags VARCHAR(512) NULL")
    _ensure_column(cur, "videos", "is_published", "is_published TINYINT DEFAULT 1")
    _ensure_column(cur, "videos", "sort_order", "sort_order INT DEFAULT 0")
    _ensure_column(cur, "videos", "published_at", "published_at DATETIME NULL")
    _ensure_column(cur, "videos", "upload_status", "upload_status VARCHAR(16) DEFAULT 'done'")
    _ensure_column(cur, "videos", "local_filename", "local_filename VARCHAR(256) NULL")
    _ensure_column(cur, "videos", "error_message", "error_message VARCHAR(256) NULL")
    _ensure_column(cur, "videos", "server_file_path", "server_file_path VARCHAR(512) NULL")
    _ensure_column(cur, "videos", "server_file_size", "server_file_size BIGINT DEFAULT 0")
    _ensure_column(cur, "videos", "video_url", "video_url VARCHAR(1024) NULL")
    _ensure_column(cur, "videos", "preview_url", "preview_url VARCHAR(1024) NULL")
    _ensure_index(cur, "videos", "idx_videos_channel_msg", "channel_id, message_id")
    _ensure_index(cur, "videos", "idx_videos_created", "created_at")
    _ensure_index(cur, "videos", "idx_videos_view_count", "view_count")
    _ensure_index(cur, "videos", "idx_videos_category", "category_id")
    _ensure_index(cur, "videos", "idx_videos_publish_sort", "is_published, sort_order, published_at")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS video_views (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            telegram_id BIGINT NOT NULL,
            video_id BIGINT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    _ensure_index(cur, "video_views", "idx_video_views_user_time", "telegram_id, created_at")
    _ensure_index(cur, "video_views", "idx_video_views_video_time", "video_id, created_at")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(64) NOT NULL,
            is_visible TINYINT DEFAULT 1,
            sort_order INT DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS banners (
            id INT AUTO_INCREMENT PRIMARY KEY,
            image_url VARCHAR(512) NOT NULL,
            link_url VARCHAR(512),
            is_active TINYINT DEFAULT 1,
            sort_order INT DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS video_download_jobs (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            source_url VARCHAR(1024) NULL,
            caption TEXT NULL,
            filename VARCHAR(256) NULL,
            file_size BIGINT DEFAULT 0,
            progress INT DEFAULT 0,
            status VARCHAR(16) DEFAULT 'pending',
            started_at DATETIME NULL,
            finished_at DATETIME NULL,
            error_message VARCHAR(256) NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    _ensure_index(cur, "video_download_jobs", "idx_vdj_status_created", "status, created_at")
    _ensure_index(cur, "video_download_jobs", "idx_vdj_updated", "updated_at")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS poker_players (
            id INT AUTO_INCREMENT PRIMARY KEY,
            telegram_id BIGINT NOT NULL,
            username VARCHAR(128),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    _ensure_index(cur, "poker_players", "idx_poker_players_telegram", "telegram_id")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS poker_ledgers (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            player_id INT NOT NULL,
            amount DECIMAL(24,8) NOT NULL,
            note VARCHAR(256),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    _ensure_index(cur, "poker_ledgers", "idx_poker_ledgers_player_time", "player_id, created_at")

    cur.close()
    try:
        conn.close()
    except Exception:
        pass


def admin_audit_log(category: str, action: str, target_id: int | None, payload: dict | None = None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO admin_audit (actor, action, target_id, payload) VALUES (%s,%s,%s,%s)",
        ((category or "system")[:64], (action or "")[:128], int(target_id) if target_id else None, json.dumps(payload or {}, ensure_ascii=False)),
    )
    cur.close()
    conn.close()


def get_user(telegram_id: int) -> dict | None:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users WHERE telegram_id=%s LIMIT 1", (int(telegram_id),))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def upsert_user_basic(telegram_id: int, username: str, language: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO users (telegram_id, username, language)
        VALUES (%s,%s,%s)
        ON DUPLICATE KEY UPDATE username=VALUES(username), language=VALUES(language)
        """,
        (int(telegram_id), (username or "")[:128] or None, (language or "")[:16] or None),
    )
    cur.close()
    conn.close()


def bind_inviter(telegram_id: int, inviter_id: int):
    telegram_id = int(telegram_id)
    inviter_id = int(inviter_id)
    if telegram_id <= 0 or inviter_id <= 0 or telegram_id == inviter_id:
        return
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT inviter_id FROM users WHERE telegram_id=%s LIMIT 1", (telegram_id,))
    row = cur.fetchone()
    if row and row.get("inviter_id"):
        cur.close()
        conn.close()
        return
    cur2 = conn.cursor()
    cur2.execute("UPDATE users SET inviter_id=%s WHERE telegram_id=%s", (inviter_id, telegram_id))
    cur2.execute("UPDATE users SET invite_count=invite_count+1 WHERE telegram_id=%s", (inviter_id,))
    cur2.close()
    cur.close()
    conn.close()


def set_user_source(telegram_id: int, source: str):
    telegram_id = int(telegram_id)
    src = (source or "").strip()[:64] or None
    if telegram_id <= 0 or not src:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE users
        SET
          first_source = IF(first_source IS NULL OR first_source='', %s, first_source),
          last_source = %s,
          last_source_at = UTC_TIMESTAMP()
        WHERE telegram_id=%s
        """,
        (src, src, telegram_id),
    )
    cur.close()
    conn.close()


def set_user_pending_coupon(telegram_id: int, code: str | None):
    telegram_id = int(telegram_id)
    c = (code or "").strip()[:64] if code else None
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET pending_coupon=%s WHERE telegram_id=%s", (c, telegram_id))
    cur.close()
    conn.close()


def _plan_by_code(code: str) -> dict | None:
    c = (code or "").strip()
    for p in PLANS:
        if (p.get("code") or "").strip() == c:
            return p
    return None


def coupon_basic_valid(code: str) -> bool:
    c = (code or "").strip()
    if not c:
        return False
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM coupons WHERE code=%s LIMIT 1", (c,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return False
    if int(row.get("active") or 0) != 1:
        return False
    expires_at = row.get("expires_at")
    if expires_at and expires_at <= _utc_now():
        return False
    max_uses = row.get("max_uses")
    used_count = int(row.get("used_count") or 0)
    if max_uses is not None and str(max_uses).strip() != "":
        if used_count >= int(max_uses):
            return False
    kind = (row.get("kind") or "").strip().lower()
    if kind not in ("percent", "fixed"):
        return False
    return True


def compute_amount_after_coupon(base_amount: Decimal, plan_code: str, coupon_code: str | None):
    base = Decimal(str(base_amount))
    code = (coupon_code or "").strip() if coupon_code else ""
    if not code:
        return base, None, None
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM coupons WHERE code=%s LIMIT 1", (code,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return base, None, None
    if int(row.get("active") or 0) != 1:
        return base, None, None
    expires_at = row.get("expires_at")
    if expires_at and expires_at <= _utc_now():
        return base, None, None
    plan_codes = (row.get("plan_codes") or "").strip()
    if plan_codes:
        allowed = [x.strip() for x in plan_codes.split(",") if x.strip()]
        if plan_code not in allowed:
            return base, None, None
    max_uses = row.get("max_uses")
    used_count = int(row.get("used_count") or 0)
    if max_uses is not None and str(max_uses).strip() != "":
        if used_count >= int(max_uses):
            return base, None, None
    kind = (row.get("kind") or "").strip().lower()
    value = Decimal(str(row.get("value") or "0"))
    discount = Decimal("0")
    if kind == "percent":
        discount = (base * value / Decimal("100")).quantize(Decimal("0.00000001"))
    elif kind == "fixed":
        discount = value
    if discount <= 0:
        return base, None, None
    out = base - discount
    if out < Decimal("0"):
        out = Decimal("0")
    return out, discount, code


def allocate_address(telegram_id: int) -> str | None:
    telegram_id = int(telegram_id)
    if telegram_id <= 0:
        return None
    user = get_user(telegram_id)
    if user and user.get("wallet_addr"):
        return str(user.get("wallet_addr"))
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT addr FROM address_pool WHERE assigned_to=%s LIMIT 1", (telegram_id,))
    row = cur.fetchone()
    if row and row.get("addr"):
        addr = str(row["addr"])
        cur.close()
        conn.close()
        conn2 = get_conn()
        cur2 = conn2.cursor()
        cur2.execute("UPDATE users SET wallet_addr=%s WHERE telegram_id=%s", (addr, telegram_id))
        cur2.close()
        conn2.close()
        return addr
    cur.execute("SELECT addr FROM address_pool WHERE assigned_to IS NULL LIMIT 1")
    row2 = cur.fetchone()
    if not row2 or not row2.get("addr"):
        cur.close()
        conn.close()
        return None
    addr = str(row2["addr"])
    cur.close()
    conn.close()
    conn3 = get_conn()
    cur3 = conn3.cursor()
    cur3.execute("UPDATE address_pool SET assigned_to=%s, assigned_at=UTC_TIMESTAMP() WHERE addr=%s AND assigned_to IS NULL", (telegram_id, addr))
    cur3.execute(
        "INSERT INTO users (telegram_id, wallet_addr) VALUES (%s,%s) ON DUPLICATE KEY UPDATE wallet_addr=VALUES(wallet_addr)",
        (telegram_id, addr),
    )
    cur3.close()
    conn3.close()
    return addr


def reset_user_address(telegram_id: int) -> str | None:
    telegram_id = int(telegram_id)
    if telegram_id <= 0:
        return None
    u = get_user(telegram_id)
    old = str(u.get("wallet_addr")) if u and u.get("wallet_addr") else None
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET wallet_addr=NULL WHERE telegram_id=%s", (telegram_id,))
    if old:
        cur.execute("UPDATE address_pool SET assigned_to=NULL, assigned_at=NULL WHERE addr=%s AND assigned_to=%s", (old, telegram_id))
    cur.close()
    conn.close()
    return old


def create_pending_order(telegram_id: int, addr: str, amount: Decimal, plan_code: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO orders (telegram_id, addr, amount, plan_code, status) VALUES (%s,%s,%s,%s,'pending')",
        (int(telegram_id), (addr or "")[:128], str(Decimal(str(amount))), (plan_code or "")[:32]),
    )
    cur.close()
    conn.close()


def create_pending_order_priced(telegram_id: int, addr: str, amount: Decimal, base_amount: Decimal, plan_code: str, coupon_code: str):
    a = Decimal(str(amount))
    b = Decimal(str(base_amount))
    disc = (b - a)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO orders (telegram_id, addr, amount, base_amount, discount_amount, coupon_code, coupon_used, plan_code, status)
        VALUES (%s,%s,%s,%s,%s,%s,0,%s,'pending')
        """,
        (int(telegram_id), (addr or "")[:128], str(a), str(b), str(disc), (coupon_code or "")[:64], (plan_code or "")[:32]),
    )
    cur.close()
    conn.close()


def mark_order_success(order_id: int, tx_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE orders SET status='success', tx_id=%s WHERE id=%s", ((tx_id or "")[:128], int(order_id)))
    cur.close()
    conn.close()


def update_user_payment(telegram_id: int, paid_until: datetime, total_received: Decimal, plan_code: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE users SET paid_until=%s, total_received=%s, last_plan=%s WHERE telegram_id=%s",
        (paid_until, str(Decimal(str(total_received))), (plan_code or "")[:32], int(telegram_id)),
    )
    cur.close()
    conn.close()


def insert_usdt_tx_if_new(telegram_id: int | None, addr: str, tx_id: str, amount, from_addr: str | None, block_time: datetime | None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT IGNORE INTO usdt_txs (tx_id, telegram_id, addr, from_addr, amount, status, block_time)
        VALUES (%s,%s,%s,%s,%s,'seen',%s)
        """,
        ((tx_id or "")[:128], int(telegram_id) if telegram_id else None, (addr or "")[:128], (from_addr or "")[:128] if from_addr else None, str(Decimal(str(amount))), block_time),
    )
    cur.close()
    conn.close()


def get_unassigned_usdt_txs(addr: str, confirm_before: datetime) -> list[dict]:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT * FROM usdt_txs
        WHERE addr=%s
          AND (telegram_id IS NULL OR telegram_id=0)
          AND (block_time IS NULL OR block_time <= %s)
          AND status IN ('seen','unmatched')
        ORDER BY created_at DESC
        LIMIT 200
        """,
        ((addr or "")[:128], confirm_before),
    )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def get_address_assigned_at(addr: str) -> datetime | None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT assigned_at FROM address_pool WHERE addr=%s LIMIT 1", ((addr or "")[:128],))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None


def get_unassigned_usdt_txs_since(addr: str, confirm_before: datetime, since: datetime | None) -> list[dict]:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    if since:
        cur.execute(
            """
            SELECT * FROM usdt_txs
            WHERE addr=%s
              AND (telegram_id IS NULL OR telegram_id=0)
              AND (block_time IS NULL OR block_time <= %s)
              AND created_at >= %s
              AND status IN ('seen','unmatched')
            ORDER BY created_at DESC
            LIMIT 200
            """,
            ((addr or "")[:128], confirm_before, since),
        )
    else:
        cur.execute(
            """
            SELECT * FROM usdt_txs
            WHERE addr=%s
              AND (telegram_id IS NULL OR telegram_id=0)
              AND (block_time IS NULL OR block_time <= %s)
              AND status IN ('seen','unmatched')
            ORDER BY created_at DESC
            LIMIT 200
            """,
            ((addr or "")[:128], confirm_before),
        )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def match_pending_order_by_amount_v2(*, addr: str, amount: Decimal, eps: Decimal, tx_time: datetime | None, lookback_hours: int, prefer_recent: bool) -> dict | None:
    a = Decimal(str(amount))
    e = Decimal(str(eps))
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    params: list = [(addr or "")[:128], int(lookback_hours)]
    time_clause = ""
    if tx_time:
        time_clause = " AND created_at <= %s "
        params.append(tx_time)
    order_by = "created_at DESC" if prefer_recent else "created_at ASC"
    cur.execute(
        f"""
        SELECT * FROM orders
        WHERE addr=%s
          AND status='pending'
          AND created_at >= (UTC_TIMESTAMP() - INTERVAL %s HOUR)
          {time_clause}
        ORDER BY {order_by}
        LIMIT 200
        """,
        tuple(params),
    )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    for r in rows:
        try:
            oa = Decimal(str(r.get("amount") or "0"))
        except Exception:
            continue
        if abs(oa - a) <= e:
            return r
    return None


def match_pending_order_by_amount(addr: str, amount: Decimal, eps: Decimal | None = None, tx_time: datetime | None = None) -> dict | None:
    return match_pending_order_by_amount_v2(
        addr=addr,
        amount=amount,
        eps=eps if eps is not None else AMOUNT_EPS,
        tx_time=tx_time,
        lookback_hours=int(MATCH_ORDER_LOOKBACK_HOURS),
        prefer_recent=bool(MATCH_ORDER_PREFER_RECENT),
    )


def set_usdt_tx_status(tx_id: str, status: str, plan_code: str | None = None, credited_amount: Decimal | None = None, processed_at: datetime | None = None, telegram_id: int | None = None):
    tx_id = (tx_id or "").strip()[:128]
    st = (status or "").strip()[:16]
    sets = ["status=%s"]
    params: list = [st]
    if plan_code is not None:
        sets.append("plan_code=%s")
        params.append((plan_code or "")[:32] or None)
    if credited_amount is not None:
        sets.append("credited_amount=%s")
        params.append(str(Decimal(str(credited_amount))))
    if processed_at is not None:
        sets.append("processed_at=%s")
        params.append(processed_at)
    if telegram_id is not None:
        sets.append("telegram_id=%s")
        params.append(int(telegram_id))
    params.append(tx_id)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE usdt_txs SET {', '.join(sets)} WHERE tx_id=%s", tuple(params))
    cur.close()
    conn.close()


def get_success_orders_between(start: datetime, end: datetime) -> list[dict]:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT * FROM orders
        WHERE status='success' AND created_at BETWEEN %s AND %s
        ORDER BY created_at DESC
        """,
        (start, end),
    )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def get_all_users() -> list[dict]:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM users ORDER BY telegram_id DESC LIMIT 50000")
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def get_inviter_id(telegram_id: int) -> int | None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT inviter_id FROM users WHERE telegram_id=%s LIMIT 1", (int(telegram_id),))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None
    try:
        v = int(row[0])
        return v if v > 0 else None
    except Exception:
        return None


def add_invite_reward(inviter_id: int, reward_days: int, invitee_id: int):
    inviter_id = int(inviter_id)
    reward_days = int(reward_days)
    if inviter_id <= 0 or reward_days <= 0:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET invite_reward_days=invite_reward_days+%s WHERE telegram_id=%s", (reward_days, inviter_id))
    try:
        admin_audit_log("invite", "reward", inviter_id, {"days": reward_days, "invitee_id": int(invitee_id)})
    except Exception:
        pass
    cur.close()
    conn.close()


def _safe_user_col(col: str) -> str | None:
    allow = {
        "remind_7d_at",
        "remind_3d_at",
        "remind_1d_at",
        "expired_recall_3d_at",
        "expired_recall_7d_at",
    }
    c = (col or "").strip()
    return c if c in allow else None


def get_unhandled_expired_users(now: datetime) -> list[dict]:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT * FROM users
        WHERE paid_until IS NOT NULL
          AND paid_until <= %s
          AND (expired_handled_at IS NULL)
        ORDER BY paid_until ASC
        LIMIT 5000
        """,
        (now,),
    )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def mark_user_expired_handled(telegram_id: int, now: datetime):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET expired_handled_at=%s WHERE telegram_id=%s", (now, int(telegram_id)))
    cur.close()
    conn.close()


def get_users_expiring_within_days(now: datetime, days: int, reminded_col: str) -> list[dict]:
    col = _safe_user_col(reminded_col)
    if not col:
        return []
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        f"""
        SELECT * FROM users
        WHERE paid_until IS NOT NULL
          AND paid_until > %s
          AND paid_until <= (%s + INTERVAL %s DAY)
          AND ({col} IS NULL)
        ORDER BY paid_until ASC
        LIMIT 5000
        """,
        (now, now, int(days)),
    )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def get_expired_users_for_recall(now: datetime, days_after: int, reminded_col: str) -> list[dict]:
    col = _safe_user_col(reminded_col)
    if not col:
        return []
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        f"""
        SELECT * FROM users
        WHERE paid_until IS NOT NULL
          AND paid_until <= (%s - INTERVAL %s DAY)
          AND ({col} IS NULL)
        ORDER BY paid_until DESC
        LIMIT 5000
        """,
        (now, int(days_after)),
    )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def mark_user_reminded(telegram_id: int, reminded_col: str, now: datetime):
    col = _safe_user_col(reminded_col)
    if not col:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE users SET {col}=%s WHERE telegram_id=%s", (now, int(telegram_id)))
    cur.close()
    conn.close()


def redeem_access_code(code: str, telegram_id: int) -> tuple[bool, str | None, datetime | None, int]:
    c = (code or "").strip()
    if not c:
        return False, "code required", None, 0
    telegram_id = int(telegram_id)
    if telegram_id <= 0:
        return False, "bad telegram_id", None, 0
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM access_codes WHERE code=%s LIMIT 1", (c,))
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return False, "invalid code", None, 0
    expires_at = row.get("expires_at")
    if expires_at and expires_at <= _utc_now():
        cur.close()
        conn.close()
        return False, "expired", None, 0
    max_uses = int(row.get("max_uses") or 1)
    used = int(row.get("used_count") or 0)
    if used >= max_uses:
        cur.close()
        conn.close()
        return False, "used up", None, 0
    days = int(row.get("days") or 0)
    if days == 0:
        cur.close()
        conn.close()
        return False, "bad days", None, 0
    cur2 = conn.cursor()
    cur2.execute("UPDATE access_codes SET used_count=used_count+1, last_used_at=UTC_TIMESTAMP() WHERE code=%s AND used_count < max_uses", (c,))
    cur2.execute("INSERT IGNORE INTO users (telegram_id) VALUES (%s)", (telegram_id,))
    cur2.close()
    cur.close()
    conn.close()
    u = get_user(telegram_id) or {}
    old_paid_until = u.get("paid_until")
    base = _utc_now()
    if old_paid_until and old_paid_until > base:
        base = old_paid_until
    new_paid_until = base + timedelta(days=days)
    total_received = Decimal(str(u.get("total_received") or "0"))
    update_user_payment(telegram_id, new_paid_until, total_received, str(u.get("last_plan") or "")[:32])
    return True, None, new_paid_until, days


def support_store_mapping(group_id: int, ticket_message_id: int, user_id: int, user_message_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO support_mapping (group_id, ticket_message_id, user_id, user_message_id)
        VALUES (%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE user_id=VALUES(user_id), user_message_id=VALUES(user_message_id)
        """,
        (int(group_id), int(ticket_message_id), int(user_id), int(user_message_id)),
    )
    cur.close()
    conn.close()


def support_get_user_id(group_id: int, ticket_or_reply_msg_id: int) -> int | None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id FROM support_mapping WHERE group_id=%s AND ticket_message_id=%s LIMIT 1",
        (int(group_id), int(ticket_or_reply_msg_id)),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None
    try:
        uid = int(row[0])
        return uid if uid > 0 else None
    except Exception:
        return None


def claim_clip_dispatch(paid_channel_id: int, paid_message_id: int, target_channel_id: int, actor: str) -> bool:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT status FROM clip_dispatch WHERE paid_channel_id=%s AND paid_message_id=%s AND target_channel_id=%s LIMIT 1",
        (int(paid_channel_id), int(paid_message_id), int(target_channel_id)),
    )
    row = cur.fetchone()
    if row:
        st = (row.get("status") or "").strip()
        cur.close()
        conn.close()
        return False if st in ("sending", "sent") else False
    cur2 = conn.cursor()
    cur2.execute(
        """
        INSERT IGNORE INTO clip_dispatch (paid_channel_id, paid_message_id, target_channel_id, actor, status, claimed_at, updated_at)
        VALUES (%s,%s,%s,%s,'sending',UTC_TIMESTAMP(),UTC_TIMESTAMP())
        """,
        (int(paid_channel_id), int(paid_message_id), int(target_channel_id), (actor or "")[:64]),
    )
    ok = cur2.rowcount == 1
    cur2.close()
    cur.close()
    conn.close()
    return ok


def claim_clip_dispatch_takeover(paid_channel_id: int, paid_message_id: int, target_channel_id: int, actor: str, ttl_seconds: int) -> bool:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT status, claimed_at FROM clip_dispatch WHERE paid_channel_id=%s AND paid_message_id=%s AND target_channel_id=%s LIMIT 1",
        (int(paid_channel_id), int(paid_message_id), int(target_channel_id)),
    )
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return claim_clip_dispatch(paid_channel_id, paid_message_id, target_channel_id, actor)
    st = (row.get("status") or "").strip()
    claimed_at = row.get("claimed_at")
    if st == "sent":
        cur.close()
        conn.close()
        return False
    if st == "sending" and claimed_at:
        age = (_utc_now() - claimed_at).total_seconds()
        if age <= float(ttl_seconds):
            cur.close()
            conn.close()
            return False
    cur2 = conn.cursor()
    cur2.execute(
        """
        UPDATE clip_dispatch
        SET actor=%s, status='sending', claimed_at=UTC_TIMESTAMP(), updated_at=UTC_TIMESTAMP()
        WHERE paid_channel_id=%s AND paid_message_id=%s AND target_channel_id=%s
        """,
        ((actor or "")[:64], int(paid_channel_id), int(paid_message_id), int(target_channel_id)),
    )
    ok = cur2.rowcount >= 1
    cur2.close()
    cur.close()
    conn.close()
    return ok


def mark_clip_dispatch_sent(paid_channel_id: int, paid_message_id: int, target_channel_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE clip_dispatch
        SET status='sent', sent_at=UTC_TIMESTAMP(), updated_at=UTC_TIMESTAMP()
        WHERE paid_channel_id=%s AND paid_message_id=%s AND target_channel_id=%s
        """,
        (int(paid_channel_id), int(paid_message_id), int(target_channel_id)),
    )
    cur.close()
    conn.close()


def unclaim_clip_dispatch(paid_channel_id: int, paid_message_id: int, target_channel_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM clip_dispatch WHERE paid_channel_id=%s AND paid_message_id=%s AND target_channel_id=%s",
        (int(paid_channel_id), int(paid_message_id), int(target_channel_id)),
    )
    cur.close()
    conn.close()


def create_video_post(channel_id: int, message_id: int, file_id: str, caption: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO videos (channel_id, message_id, file_id, caption) VALUES (%s,%s,%s,%s)",
        (int(channel_id), int(message_id), (file_id or "")[:128], caption or ""),
    )
    cur.close()
    conn.close()


def update_video_free_link(paid_channel_id: int, paid_message_id: int, free_channel_id: int, free_message_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE videos
        SET free_channel_id=%s, free_message_id=%s
        WHERE channel_id=%s AND message_id=%s
        """,
        (int(free_channel_id), int(free_message_id), int(paid_channel_id), int(paid_message_id)),
    )
    cur.close()
    conn.close()


def list_categories(visible_only: bool = True) -> list[dict]:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    if visible_only:
        cur.execute("SELECT * FROM categories WHERE is_visible=1 ORDER BY sort_order ASC, id DESC")
    else:
        cur.execute("SELECT * FROM categories ORDER BY sort_order ASC, id DESC")
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def upsert_category(id: int | None, name: str, is_visible: bool, sort_order: int):
    nm = (name or "").strip()
    if not nm:
        return
    cid = int(id or 0)
    conn = get_conn()
    cur = conn.cursor()
    if cid > 0:
        cur.execute(
            "UPDATE categories SET name=%s, is_visible=%s, sort_order=%s WHERE id=%s",
            (nm, 1 if bool(is_visible) else 0, int(sort_order or 0), cid),
        )
    else:
        cur.execute(
            "INSERT INTO categories (name, is_visible, sort_order) VALUES (%s, %s, %s)",
            (nm, 1 if bool(is_visible) else 0, int(sort_order or 0)),
        )
    cur.close()
    conn.close()


def delete_category(id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM categories WHERE id=%s", (int(id),))
    cur.close()
    conn.close()


def list_banners(active_only: bool = True) -> list[dict]:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    if active_only:
        cur.execute("SELECT * FROM banners WHERE is_active=1 ORDER BY sort_order ASC, id DESC")
    else:
        cur.execute("SELECT * FROM banners ORDER BY sort_order ASC, id DESC")
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def upsert_banner(id: int | None, image_url: str, link_url: str, is_active: bool, sort_order: int):
    img = (image_url or "").strip()
    if not img:
        return
    bid = int(id or 0)
    conn = get_conn()
    cur = conn.cursor()
    if bid > 0:
        cur.execute(
            "UPDATE banners SET image_url=%s, link_url=%s, is_active=%s, sort_order=%s WHERE id=%s",
            (img, (link_url or "").strip()[:512] or None, 1 if bool(is_active) else 0, int(sort_order or 0), bid),
        )
    else:
        cur.execute(
            "INSERT INTO banners (image_url, link_url, is_active, sort_order) VALUES (%s, %s, %s, %s)",
            (img, (link_url or "").strip()[:512] or None, 1 if bool(is_active) else 0, int(sort_order or 0)),
        )
    cur.close()
    conn.close()


def delete_banner(id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM banners WHERE id=%s", (int(id),))
    cur.close()
    conn.close()


def set_video_category(video_id: int, category_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE videos SET category_id=%s WHERE id=%s", (int(category_id), int(video_id)))
    cur.close()
    conn.close()


def record_video_view(telegram_id: int, video_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO video_views (telegram_id, video_id) VALUES (%s,%s)",
        (int(telegram_id), int(video_id)),
    )
    cur.close()
    conn.close()


def user_viewed_tags(telegram_id: int, limit: int = 200) -> list[dict]:
    limit = max(1, min(int(limit or 200), 2000))
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT v.tags
        FROM video_views vv
        JOIN videos v ON v.id = vv.video_id
        WHERE vv.telegram_id=%s
        ORDER BY vv.created_at DESC
        LIMIT %s
        """,
        (int(telegram_id), limit),
    )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    counts: dict[str, int] = {}
    for r in rows:
        tags = (r.get("tags") or "").strip()
        if not tags:
            continue
        for t in tags.replace("，", ",").split(","):
            tt = (t or "").strip()
            if not tt:
                continue
            counts[tt] = counts.get(tt, 0) + 1
    out = [{"tag": k, "count": v} for k, v in counts.items()]
    out.sort(key=lambda x: (-int(x.get("count") or 0), str(x.get("tag") or "")))
    return out[:50]


def poker_upsert_player(telegram_id: int, username: str | None) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM poker_players WHERE telegram_id=%s LIMIT 1", (int(telegram_id),))
    row = cur.fetchone()
    if row:
        pid = int(row[0] or 0)
        cur.execute("UPDATE poker_players SET username=%s WHERE id=%s", ((username or "")[:128] or None, pid))
    else:
        cur.execute(
            "INSERT INTO poker_players (telegram_id, username) VALUES (%s,%s)",
            (int(telegram_id), (username or "")[:128] or None),
        )
        pid = int(cur.lastrowid or 0)
    cur.close()
    conn.close()
    return pid


def poker_add_ledger(player_id: int, amount: Decimal, note: str | None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO poker_ledgers (player_id, amount, note) VALUES (%s,%s,%s)",
        (int(player_id), Decimal(str(amount or "0")), (note or "")[:256] or None),
    )
    cur.close()
    conn.close()


def poker_list_ledgers(limit: int = 200, days: int = 30) -> list[dict]:
    limit = max(1, min(int(limit or 200), 500))
    days = max(1, min(int(days or 30), 365))
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          l.id,
          p.telegram_id,
          p.username,
          l.amount,
          l.note,
          l.created_at
        FROM poker_ledgers l
        JOIN poker_players p ON p.id = l.player_id
        WHERE l.created_at >= (UTC_TIMESTAMP() - INTERVAL %s DAY)
        ORDER BY l.created_at DESC, l.id DESC
        LIMIT %s
        """,
        (int(days), int(limit)),
    )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def poker_balances() -> list[dict]:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT
          p.id AS player_id,
          p.telegram_id,
          p.username,
          COALESCE(SUM(l.amount),0) AS balance,
          MAX(l.created_at) AS last_at
        FROM poker_players p
        LEFT JOIN poker_ledgers l ON l.player_id = p.id
        GROUP BY p.id, p.telegram_id, p.username
        ORDER BY balance DESC, p.id ASC
        """
    )
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def admin_create_video_job(local_filename: str, caption: str, cover_url: str, tags: str, category_id: int, sort_order: int, is_published: bool, published_at: datetime | None, upload_status: str = "pending", server_file_path: str | None = None, server_file_size: int | None = None, video_url: str | None = None, preview_url: str | None = None) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO videos (channel_id, message_id, file_id, caption, view_count, category_id, cover_url, tags, is_published, sort_order, published_at, upload_status, local_filename, server_file_path, server_file_size, video_url, preview_url)
        VALUES (NULL, NULL, NULL, %s, 0, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            caption or "",
            int(category_id or 0),
            (cover_url or "").strip()[:512] or None,
            (tags or "").strip()[:512] or None,
            1 if bool(is_published) else 0,
            int(sort_order or 0),
            published_at,
            (upload_status or "pending").strip()[:16],
            (local_filename or "").strip()[:256] or None,
            (server_file_path or "").strip()[:512] or None,
            int(server_file_size or 0),
            (video_url or "").strip()[:1024] or None,
            (preview_url or "").strip()[:1024] or None,
        ),
    )
    vid = int(cur.lastrowid or 0)
    cur.close()
    conn.close()
    return vid


def admin_update_video_meta(video_id: int, caption: str, cover_url: str, tags: str, category_id: int, sort_order: int, is_published: bool, published_at: datetime | None, local_filename: str, video_url: str, preview_url: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE videos
        SET caption=%s, cover_url=%s, tags=%s, category_id=%s, sort_order=%s, is_published=%s, published_at=%s, local_filename=%s, video_url=%s, preview_url=%s,
            upload_status=IF(%s IS NULL OR %s='', upload_status, 'done')
        WHERE id=%s
        """,
        (
            caption or "",
            (cover_url or "").strip()[:512] or None,
            (tags or "").strip()[:512] or None,
            int(category_id or 0),
            int(sort_order or 0),
            1 if bool(is_published) else 0,
            published_at,
            (local_filename or "").strip()[:256] or None,
            (video_url or "").strip()[:1024] or None,
            (preview_url or "").strip()[:1024] or None,
            (video_url or "").strip()[:1024] or None,
            (video_url or "").strip()[:1024] or None,
            int(video_id),
        ),
    )
    cur.close()
    conn.close()


def admin_set_video_publish(video_id: int, is_published: bool):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE videos SET is_published=%s WHERE id=%s", (1 if bool(is_published) else 0, int(video_id)))
    cur.close()
    conn.close()


def admin_set_video_sort(video_id: int, sort_order: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE videos SET sort_order=%s WHERE id=%s", (int(sort_order or 0), int(video_id)))
    cur.close()
    conn.close()


def list_videos_admin(q: str, limit: int = 200, status: str | None = None) -> list[dict]:
    limit = max(1, min(int(limit or 200), 2000))
    qq = (q or "").strip()
    st = (status or "").strip().lower()
    where = ["1=1"]
    params: list = []
    if qq:
        where.append("caption LIKE %s")
        params.append(f"%{qq}%")
    if st in ("pending", "uploading", "done", "failed"):
        where.append("upload_status=%s")
        params.append(st)
    sql = f"""
        SELECT id, caption, cover_url, tags, category_id, sort_order, is_published, published_at, upload_status, local_filename,
               server_file_path, server_file_size,
               video_url, preview_url,
               channel_id, message_id, free_channel_id, free_message_id, view_count, created_at
        FROM videos
        WHERE {' AND '.join(where)}
        ORDER BY sort_order DESC, published_at DESC, created_at DESC
        LIMIT %s
    """
    params.append(limit)
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, tuple(params))
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def local_uploader_claim_next() -> dict | None:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM videos WHERE upload_status='pending' AND (server_file_path IS NULL OR server_file_path='') ORDER BY created_at ASC LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return None
    vid = int(row.get("id") or 0)
    cur2 = conn.cursor()
    cur2.execute("UPDATE videos SET upload_status='uploading' WHERE id=%s AND upload_status='pending'", (vid,))
    ok = cur2.rowcount == 1
    cur2.close()
    cur.close()
    conn.close()
    return row if ok else None


def local_uploader_update(video_id: int, upload_status: str, channel_id: int | None, message_id: int | None, free_channel_id: int | None, free_message_id: int | None, file_id: str | None, error: str | None):
    st = (upload_status or "").strip().lower()
    if st not in ("pending", "uploading", "done", "failed"):
        st = "failed"
    conn = get_conn()
    cur = conn.cursor()
    sets = ["upload_status=%s"]
    params: list = [st]
    if channel_id is not None:
        sets.append("channel_id=%s")
        params.append(int(channel_id))
    if message_id is not None:
        sets.append("message_id=%s")
        params.append(int(message_id))
    if free_channel_id is not None:
        sets.append("free_channel_id=%s")
        params.append(int(free_channel_id))
    if free_message_id is not None:
        sets.append("free_message_id=%s")
        params.append(int(free_message_id))
    if file_id is not None:
        sets.append("file_id=%s")
        params.append((file_id or "")[:128] or None)
    if st == "done":
        sets.append("published_at=IF(published_at IS NULL, UTC_TIMESTAMP(), published_at)")
    if error is not None:
        sets.append("error_message=%s")
        params.append((error or "")[:256] or None)
    params.append(int(video_id))
    cur.execute(f"UPDATE videos SET {', '.join(sets)} WHERE id=%s", tuple(params))
    cur.close()
    conn.close()


def admin_create_download_job(source_url: str, caption: str, filename: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO video_download_jobs (source_url, caption, filename, file_size, progress, status)
        VALUES (%s,%s,%s,0,0,'pending')
        """,
        ((source_url or "").strip()[:1024] or None, (caption or "").strip() or None, (filename or "").strip()[:256] or None),
    )
    rid = int(cur.lastrowid or 0)
    cur.close()
    conn.close()
    return rid


def list_download_jobs(limit: int = 200, status: str | None = None) -> list[dict]:
    limit = max(1, min(int(limit or 200), 2000))
    st = (status or "").strip().lower()
    where = ["1=1"]
    params: list = []
    if st in ("pending", "downloading", "done", "failed"):
        where.append("status=%s")
        params.append(st)
    sql = f"""
        SELECT id, status, progress, caption, filename, file_size, started_at, finished_at, created_at, updated_at, error_message, source_url
        FROM video_download_jobs
        WHERE {' AND '.join(where)}
        ORDER BY id DESC
        LIMIT %s
    """
    params.append(limit)
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, tuple(params))
    rows = cur.fetchall() or []
    cur.close()
    conn.close()
    return rows


def local_downloader_claim_next() -> dict | None:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM video_download_jobs WHERE status='pending' ORDER BY id ASC LIMIT 1")
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        return None
    jid = int(row.get("id") or 0)
    cur2 = conn.cursor()
    cur2.execute(
        "UPDATE video_download_jobs SET status='downloading', started_at=IFNULL(started_at, UTC_TIMESTAMP()), updated_at=UTC_TIMESTAMP() WHERE id=%s AND status='pending'",
        (jid,),
    )
    ok = cur2.rowcount == 1
    cur2.close()
    cur.close()
    conn.close()
    return row if ok else None


def local_downloader_update(job_id: int, status: str, progress: int | None, file_size: int | None, filename: str | None, started_at: datetime | None, finished_at: datetime | None, error: str | None):
    st = (status or "").strip().lower()
    if st not in ("pending", "downloading", "done", "failed"):
        st = "failed"
    sets = ["status=%s", "updated_at=UTC_TIMESTAMP()"]
    params: list = [st]
    if progress is not None:
        sets.append("progress=%s")
        params.append(max(0, min(int(progress), 100)))
    if file_size is not None:
        sets.append("file_size=%s")
        params.append(max(0, int(file_size)))
    if filename is not None:
        sets.append("filename=%s")
        params.append((filename or "").strip()[:256] or None)
    if started_at is not None:
        sets.append("started_at=%s")
        params.append(started_at)
    if finished_at is not None:
        sets.append("finished_at=%s")
        params.append(finished_at)
    if st in ("done", "failed"):
        sets.append("finished_at=IFNULL(finished_at, UTC_TIMESTAMP())")
    if error is not None:
        sets.append("error_message=%s")
        params.append((error or "")[:256] or None)
    params.append(int(job_id))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE video_download_jobs SET {', '.join(sets)} WHERE id=%s", tuple(params))
    cur.close()
    conn.close()
