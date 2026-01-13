import base64
import json
import os
import secrets
import socket
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from urllib import request as urlrequest
from urllib import parse as urlparse2

from config import (
    ADMIN_WEB_ACTIONS_ENABLE,
    ADMIN_WEB_ENABLE,
    ADMIN_WEB_HOST,
    ADMIN_WEB_PASS,
    ADMIN_WEB_PORT,
    ADMIN_WEB_USER,
    BOT_TOKEN,
    PAID_CHANNEL_ID,
)
from core.db import get_conn
from core.models import init_tables


def _utc_now() -> datetime:
    return datetime.utcnow()


def _json_bytes(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")


def _basic_auth_ok(headers) -> bool:
    if not ADMIN_WEB_USER or not ADMIN_WEB_PASS:
        return False
    v = headers.get("Authorization", "")
    if not v.startswith("Basic "):
        return False
    try:
        raw = base64.b64decode(v.split(" ", 1)[1].strip()).decode("utf-8", errors="ignore")
    except Exception:
        return False
    if ":" not in raw:
        return False
    u, p = raw.split(":", 1)
    return secrets.compare_digest(u, ADMIN_WEB_USER) and secrets.compare_digest(p, ADMIN_WEB_PASS)


def _html_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


INDEX_HTML = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>PV Admin</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;max-width:1100px;margin:24px auto;padding:0 14px;color:#111}
    .grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:12px 0}
    .card{border:1px solid #ddd;border-radius:10px;padding:12px}
    .k{color:#666;font-size:12px}
    .v{font-size:22px;font-weight:700;margin-top:6px}
    input,button{padding:10px;border-radius:10px;border:1px solid #ccc}
    button{cursor:pointer;background:#111;color:#fff;border-color:#111}
    table{width:100%;border-collapse:collapse;margin-top:10px}
    th,td{border-bottom:1px solid #eee;padding:8px;text-align:left;font-size:13px;vertical-align:top}
    .row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
    .muted{color:#666;font-size:12px}
  </style>
</head>
<body>
  <h2>PV 管理后台</h2>
  <div class="muted">需要浏览器 Basic Auth 登录。时间默认 UTC。</div>

  <div class="grid" id="cards"></div>

  <h3>用户查询</h3>
  <div class="row">
    <input id="q" placeholder="telegram_id 或 username" style="min-width:280px" />
    <button onclick="loadUsers()">查询</button>
  </div>
  <div id="users"></div>

  <h3>用户详情 / 操作</h3>
  <div class="row">
    <input id="uid" placeholder="telegram_id" style="min-width:280px" />
    <button onclick="loadUserDetail()">加载详情</button>
  </div>
  <div class="row" style="margin-top:10px">
    <input id="days" placeholder="续费天数（可负数）" style="min-width:220px" />
    <input id="note" placeholder="操作备注（可选）" style="min-width:320px" />
    <button onclick="extendUser()">执行续费/扣减</button>
    <button onclick="resendInvite()">重发入群链接</button>
  </div>
  <div class="muted" id="opResult" style="margin-top:8px"></div>
  <div id="detail"></div>

  <h3>最近订单</h3>
  <div class="row">
    <button onclick="loadOrders()">刷新</button>
    <span class="muted" id="ordersHint"></span>
  </div>
  <div id="orders"></div>

<script>
async function jget(url){
  const r = await fetch(url);
  if(!r.ok){ throw new Error(await r.text()); }
  return await r.json();
}

async function jpost(url, body){
  const r = await fetch(url, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(body||{})});
  if(!r.ok){ throw new Error(await r.text()); }
  return await r.json();
}

function cardsHtml(s){
  const items = [
    ["总用户", s.users_total],
    ["有效会员", s.active_members],
    ["24h 充值人数", s.payers_24h],
    ["24h 充值总额", s.amount_24h + " USDT"],
    ["到期<24h", s.expiring_24h],
    ["地址池占用", s.addr_assigned + "/" + s.addr_total],
    ["订单(24h)", s.orders_24h],
    ["最后入账", s.last_credited_at || "—"],
  ];
  return items.map(([k,v]) => `<div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>`).join("");
}

async function loadStats(){
  const s = await jget("/api/stats");
  document.getElementById("cards").innerHTML = cardsHtml(s);
}

function tableHtml(rows){
  if(!rows.length) return "<div class='muted'>无数据</div>";
  const keys = Object.keys(rows[0]);
  const head = "<tr>" + keys.map(k=>`<th>${k}</th>`).join("") + "</tr>";
  const body = rows.map(r=>"<tr>"+keys.map(k=>`<td>${(r[k]??"")}</td>`).join("")+"</tr>").join("");
  return `<table><thead>${head}</thead><tbody>${body}</tbody></table>`;
}

async function loadUsers(){
  const q = document.getElementById("q").value.trim();
  const url = "/api/users?q=" + encodeURIComponent(q) + "&limit=50";
  const data = await jget(url);
  document.getElementById("users").innerHTML = tableHtml(data.items);
}

async function loadUserDetail(){
  const uid = document.getElementById("uid").value.trim();
  if(!uid){ return; }
  const data = await jget("/api/user_detail?telegram_id=" + encodeURIComponent(uid));
  const blocks = [];
  blocks.push("<h4>用户</h4>" + tableHtml([data.user||{}]));
  blocks.push("<h4>最近订单</h4>" + tableHtml(data.orders||[]));
  blocks.push("<h4>最近入账</h4>" + tableHtml(data.txs||[]));
  document.getElementById("detail").innerHTML = blocks.join("");
}

async function extendUser(){
  const uid = document.getElementById("uid").value.trim();
  const days = document.getElementById("days").value.trim();
  const note = document.getElementById("note").value.trim();
  if(!uid || !days){ return; }
  const r = await jpost("/api/user_extend", {telegram_id: uid, days: parseInt(days,10), note});
  document.getElementById("opResult").innerText = "完成：paid_until=" + (r.paid_until || "NULL");
  await loadUserDetail();
}

async function resendInvite(){
  const uid = document.getElementById("uid").value.trim();
  const note = document.getElementById("note").value.trim();
  if(!uid){ return; }
  const r = await jpost("/api/user_resend_invite", {telegram_id: uid, note});
  document.getElementById("opResult").innerText = "已发送入群链接。";
}

async function loadOrders(){
  const data = await jget("/api/orders?hours=24&limit=50");
  document.getElementById("ordersHint").innerText = "时间范围: 最近 24 小时";
  document.getElementById("orders").innerHTML = tableHtml(data.items);
}

loadStats();
loadOrders();
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    server_version = "PVAdmin/1.0"

    def _send(self, code: int, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _unauthorized(self):
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("WWW-Authenticate", 'Basic realm="PV Admin"')
        self.end_headers()

    def _require_auth(self) -> bool:
        if _basic_auth_ok(self.headers):
            return True
        self._unauthorized()
        return False

    def do_GET(self):
        u = urlparse(self.path)
        path = u.path

        if path == "/health":
            body = _json_bytes({"ok": True, "ts": _utc_now().isoformat()})
            return self._send(200, body, "application/json; charset=utf-8")

        if not self._require_auth():
            return

        if path == "/":
            return self._send(200, INDEX_HTML.encode("utf-8"), "text/html; charset=utf-8")

        if path == "/api/stats":
            body = _json_bytes(stats())
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/users":
            qs = parse_qs(u.query)
            q = (qs.get("q", [""])[0] or "").strip()
            limit = int((qs.get("limit", ["50"])[0] or "50"))
            body = _json_bytes({"items": list_users(q=q, limit=limit)})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/user_detail":
            qs = parse_qs(u.query)
            telegram_id = int((qs.get("telegram_id", ["0"])[0] or "0"))
            body = _json_bytes(user_detail(telegram_id))
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/orders":
            qs = parse_qs(u.query)
            hours = int((qs.get("hours", ["24"])[0] or "24"))
            limit = int((qs.get("limit", ["50"])[0] or "50"))
            body = _json_bytes({"items": list_orders(hours=hours, limit=limit)})
            return self._send(200, body, "application/json; charset=utf-8")

        self._send(404, b"not found", "text/plain; charset=utf-8")

    def do_POST(self):
        u = urlparse(self.path)
        path = u.path

        if not self._require_auth():
            return
        if not ADMIN_WEB_ACTIONS_ENABLE:
            return self._send(403, b"actions disabled", "text/plain; charset=utf-8")

        try:
            n = int(self.headers.get("Content-Length") or "0")
        except Exception:
            n = 0
        raw = self.rfile.read(n) if n > 0 else b"{}"
        try:
            data = json.loads(raw.decode("utf-8", errors="ignore") or "{}")
        except Exception:
            data = {}

        actor = ADMIN_WEB_USER
        if path == "/api/user_extend":
            telegram_id = int(str(data.get("telegram_id") or "0"))
            days = int(data.get("days") or 0)
            note = (data.get("note") or "").strip()
            paid_until = user_extend_days(telegram_id, days, actor=actor, note=note, ip=self.client_address[0])
            body = _json_bytes({"ok": True, "paid_until": paid_until})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/user_resend_invite":
            telegram_id = int(str(data.get("telegram_id") or "0"))
            note = (data.get("note") or "").strip()
            ok, err = resend_invite_link(telegram_id, actor=actor, note=note, ip=self.client_address[0])
            code = 200 if ok else 500
            body = _json_bytes({"ok": ok, "error": err})
            return self._send(code, body, "application/json; charset=utf-8")

        self._send(404, b"not found", "text/plain; charset=utf-8")

    def log_message(self, format, *args):
        return


def _q_one(sql: str, params=()):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _q_all(sql: str, params=()):
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params)
        return cur.fetchall() or []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _exec(sql: str, params=()):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        cur.close()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _audit(actor: str, action: str, target_id: int | None, payload: dict):
    _exec(
        "INSERT INTO admin_audit (actor, action, target_id, payload) VALUES (%s,%s,%s,%s)",
        (actor, action, target_id, json.dumps(payload, ensure_ascii=False)),
    )


def stats() -> dict:
    users_total = int(_q_one("SELECT COUNT(*) FROM users") or 0)
    active_members = int(_q_one("SELECT COUNT(*) FROM users WHERE paid_until IS NOT NULL AND paid_until > UTC_TIMESTAMP()") or 0)
    expiring_24h = int(
        _q_one(
            "SELECT COUNT(*) FROM users WHERE paid_until IS NOT NULL AND paid_until BETWEEN UTC_TIMESTAMP() AND (UTC_TIMESTAMP() + INTERVAL 24 HOUR)"
        )
        or 0
    )
    addr_total = int(_q_one("SELECT COUNT(*) FROM address_pool") or 0)
    addr_assigned = int(_q_one("SELECT COUNT(*) FROM address_pool WHERE assigned_to IS NOT NULL") or 0)
    orders_24h = int(_q_one("SELECT COUNT(*) FROM orders WHERE status='paid' AND created_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR)") or 0)
    amount_24h = _q_one("SELECT COALESCE(SUM(amount),0) FROM orders WHERE status='paid' AND created_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR)") or 0
    payers_24h = int(
        _q_one(
            "SELECT COUNT(DISTINCT telegram_id) FROM orders WHERE status='paid' AND created_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR)"
        )
        or 0
    )
    last_credited_at = _q_one("SELECT MAX(processed_at) FROM usdt_txs WHERE status IN ('processed','credited')") or None

    return {
        "users_total": users_total,
        "active_members": active_members,
        "expiring_24h": expiring_24h,
        "addr_total": addr_total,
        "addr_assigned": addr_assigned,
        "orders_24h": orders_24h,
        "amount_24h": str(amount_24h),
        "payers_24h": payers_24h,
        "last_credited_at": last_credited_at,
    }


def list_users(q: str, limit: int) -> list[dict]:
    limit = max(1, min(int(limit), 200))
    if not q:
        sql = "SELECT telegram_id, username, paid_until, total_received, wallet_addr, created_at FROM users ORDER BY created_at DESC LIMIT %s"
        return _q_all(sql, (limit,))

    if q.isdigit():
        sql = "SELECT telegram_id, username, paid_until, total_received, wallet_addr, created_at FROM users WHERE telegram_id=%s LIMIT %s"
        return _q_all(sql, (int(q), limit))

    sql = "SELECT telegram_id, username, paid_until, total_received, wallet_addr, created_at FROM users WHERE username LIKE %s ORDER BY created_at DESC LIMIT %s"
    return _q_all(sql, (f"%{q}%", limit))


def list_orders(hours: int, limit: int) -> list[dict]:
    hours = max(1, min(int(hours), 720))
    limit = max(1, min(int(limit), 200))
    sql = """
        SELECT id, telegram_id, addr, amount, plan_code, status, tx_id, created_at
        FROM orders
        WHERE created_at >= (UTC_TIMESTAMP() - INTERVAL %s HOUR)
        ORDER BY created_at DESC
        LIMIT %s
    """
    return _q_all(sql, (hours, limit))


def user_detail(telegram_id: int) -> dict:
    user_rows = _q_all(
        "SELECT telegram_id, username, paid_until, total_received, wallet_addr, inviter_id, invite_count, created_at FROM users WHERE telegram_id=%s LIMIT 1",
        (telegram_id,),
    )
    user = user_rows[0] if user_rows else None
    orders = _q_all(
        """
        SELECT id, telegram_id, addr, amount, plan_code, status, tx_id, created_at
        FROM orders
        WHERE telegram_id=%s
        ORDER BY created_at DESC
        LIMIT 20
        """,
        (telegram_id,),
    )
    txs = _q_all(
        """
        SELECT tx_id, amount, addr, from_addr, status, plan_code, processed_at, created_at
        FROM usdt_txs
        WHERE telegram_id=%s
        ORDER BY created_at DESC
        LIMIT 20
        """,
        (telegram_id,),
    )
    return {"user": user, "orders": orders, "txs": txs}


def user_extend_days(telegram_id: int, days: int, actor: str, note: str, ip: str) -> str | None:
    if telegram_id <= 0 or days == 0:
        raise ValueError("bad params")
    _exec(
        """
        UPDATE users
        SET paid_until = DATE_ADD(
          IF(paid_until IS NULL OR paid_until < UTC_TIMESTAMP(), UTC_TIMESTAMP(), paid_until),
          INTERVAL %s DAY
        )
        WHERE telegram_id=%s
        """,
        (int(days), int(telegram_id)),
    )
    row = _q_all("SELECT paid_until FROM users WHERE telegram_id=%s LIMIT 1", (telegram_id,))
    paid_until = row[0]["paid_until"] if row else None
    _audit(actor, "user_extend_days", telegram_id, {"days": days, "note": note, "ip": ip, "paid_until": paid_until})
    return str(paid_until) if paid_until is not None else None


def _bot_api(method: str, payload: dict) -> tuple[bool, dict | str]:
    if not BOT_TOKEN:
        return False, "BOT_TOKEN missing"
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    data = urlparse2.urlencode(payload).encode("utf-8")
    req = urlrequest.Request(url, data=data, method="POST")
    try:
        with urlrequest.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        obj = json.loads(raw or "{}")
        if obj.get("ok"):
            return True, obj.get("result") or {}
        return False, (obj.get("description") or raw)
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def resend_invite_link(telegram_id: int, actor: str, note: str, ip: str) -> tuple[bool, str]:
    if telegram_id <= 0:
        return False, "bad telegram_id"
    exp = int((_utc_now() + timedelta(hours=24)).timestamp())
    ok, res = _bot_api(
        "createChatInviteLink",
        {"chat_id": str(PAID_CHANNEL_ID), "expire_date": str(exp), "member_limit": "1"},
    )
    if not ok:
        _audit(actor, "resend_invite_failed", telegram_id, {"note": note, "ip": ip, "error": str(res)})
        return False, str(res)
    link = str((res or {}).get("invite_link") or "")
    if not link:
        _audit(actor, "resend_invite_failed", telegram_id, {"note": note, "ip": ip, "error": "empty invite_link"})
        return False, "empty invite_link"
    msg = f"✅ 入群链接（24h有效，仅限1人）：\n{link}"
    ok2, res2 = _bot_api("sendMessage", {"chat_id": str(telegram_id), "text": msg})
    if not ok2:
        _audit(actor, "resend_invite_failed", telegram_id, {"note": note, "ip": ip, "error": str(res2)})
        return False, str(res2)
    _audit(actor, "resend_invite", telegram_id, {"note": note, "ip": ip, "link": link})
    return True, ""


def main():
    if not ADMIN_WEB_ENABLE:
        raise SystemExit("ADMIN_WEB_ENABLE=0")
    if not ADMIN_WEB_USER or not ADMIN_WEB_PASS:
        raise SystemExit("ADMIN_WEB_USER/ADMIN_WEB_PASS missing")

    init_tables()
    httpd = ThreadingHTTPServer((ADMIN_WEB_HOST, int(ADMIN_WEB_PORT)), Handler)
    httpd.serve_forever()


if __name__ == "__main__":
    main()

