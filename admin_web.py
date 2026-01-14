import base64
import csv
import io
import json
import os
import secrets
import socket
import threading
import time
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
    BROADCAST_ABORT_FAIL_RATE,
    BROADCAST_ABORT_MIN_SENT,
    BROADCAST_SLEEP_SEC,
    JOIN_REQUEST_ENABLE,
    JOIN_REQUEST_LINK_EXPIRE_HOURS,
    PAID_CHANNEL_ID,
)
from core.db import get_conn
from core.models import init_tables


def _utc_now() -> datetime:
    return datetime.utcnow()


def _json_bytes(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")


def _csv_bytes(rows: list[dict], cols: list[str]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(cols)
    for r in rows:
        w.writerow([("" if r.get(c) is None else str(r.get(c))) for c in cols])
    return buf.getvalue().encode("utf-8-sig")


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
    input,button,textarea{padding:10px;border-radius:10px;border:1px solid #ccc}
    button{cursor:pointer;background:#111;color:#fff;border-color:#111}
    textarea{min-width:520px;min-height:90px}
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
    <button onclick="toggleBlacklist()">拉黑/取消拉黑</button>
    <button onclick="toggleWhitelist()">白名单/取消白名单</button>
  </div>
  <div class="muted" id="opResult" style="margin-top:8px"></div>
  <div id="detail"></div>

  <h3>运营工具</h3>
  <div class="row">
    <div>
      <div class="muted">优惠码（kind=percent 或 fixed）</div>
      <div class="row">
        <input id="couponCode" placeholder="code" style="min-width:160px" />
        <input id="couponKind" placeholder="kind" style="min-width:120px" />
        <input id="couponValue" placeholder="value" style="min-width:120px" />
        <input id="couponPlans" placeholder="plan_codes(可选,逗号)" style="min-width:240px" />
        <input id="couponMax" placeholder="max_uses(可选)" style="min-width:140px" />
        <input id="couponExpH" placeholder="expire_hours(可选)" style="min-width:160px" />
        <button onclick="createCoupon()">创建/更新</button>
        <button onclick="loadCoupons()">刷新</button>
      </div>
      <div id="coupons"></div>
    </div>
  </div>

  <div class="row" style="margin-top:16px">
    <div>
      <div class="muted">兑换码（/redeem 使用）</div>
      <div class="row">
        <input id="acCode" placeholder="code" style="min-width:200px" />
        <input id="acDays" placeholder="days" style="min-width:120px" />
        <input id="acMax" placeholder="max_uses" style="min-width:140px" />
        <input id="acExpH" placeholder="expire_hours(可选)" style="min-width:160px" />
        <input id="acNote" placeholder="note(可选)" style="min-width:240px" />
        <button onclick="createAccessCode()">创建/更新</button>
        <button onclick="loadAccessCodes()">刷新</button>
      </div>
      <div id="accessCodes"></div>
    </div>
  </div>

  <div class="row" style="margin-top:16px">
    <div>
      <div class="muted">广播（segment=all/active/expired/expiring1d/expiring3d/non_member，可选 source）</div>
      <div class="row">
        <input id="bcSegment" placeholder="segment" style="min-width:180px" />
        <input id="bcSource" placeholder="source(可选)" style="min-width:220px" />
        <input id="bcParseMode" placeholder="parse_mode(可选: HTML)" style="min-width:180px" />
        <input id="bcBtnText" placeholder="button_text(可选)" style="min-width:200px" />
        <input id="bcBtnUrl" placeholder="button_url(可选)" style="min-width:320px" />
        <label class="muted"><input id="bcNoPreview" type="checkbox" /> 不显示预览</label>
        <button onclick="previewBroadcast()">预览人数</button>
        <button onclick="createBroadcast()">创建</button>
        <button onclick="loadBroadcasts()">刷新</button>
      </div>
      <div class="row">
        <textarea id="bcText" placeholder="广播内容（纯文本）"></textarea>
      </div>
      <div class="row">
        <input id="bcJobId" placeholder="job_id 查看发送日志" style="min-width:200px" />
        <button onclick="loadBroadcastLogs()">查看日志</button>
        <button onclick="pauseBroadcast()">暂停</button>
        <button onclick="resumeBroadcast()">继续</button>
      </div>
      <div id="broadcastLogs"></div>
      <div id="broadcasts"></div>
    </div>
  </div>

  <h3>导出 CSV</h3>
  <div class="row">
    <button onclick="exportUsers()">导出用户</button>
    <button onclick="exportOrders()">导出订单(7d)</button>
    <button onclick="exportTxs()">导出入账(7d)</button>
    <button onclick="exportAudit()">导出审计(7d)</button>
  </div>

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

async function toggleBlacklist(){
  const uid = document.getElementById("uid").value.trim();
  const note = document.getElementById("note").value.trim();
  if(!uid){ return; }
  const r = await jpost("/api/user_flags", {telegram_id: uid, toggle: "black", note});
  document.getElementById("opResult").innerText = "已更新：blacklisted=" + (r.is_blacklisted ? "1":"0");
  await loadUserDetail();
}

async function toggleWhitelist(){
  const uid = document.getElementById("uid").value.trim();
  const note = document.getElementById("note").value.trim();
  if(!uid){ return; }
  const r = await jpost("/api/user_flags", {telegram_id: uid, toggle: "white", note});
  document.getElementById("opResult").innerText = "已更新：whitelisted=" + (r.is_whitelisted ? "1":"0");
  await loadUserDetail();
}

async function loadCoupons(){
  const data = await jget("/api/coupons?limit=50");
  document.getElementById("coupons").innerHTML = tableHtml(data.items||[]);
}

async function createCoupon(){
  const body = {
    code: document.getElementById("couponCode").value.trim(),
    kind: document.getElementById("couponKind").value.trim(),
    value: document.getElementById("couponValue").value.trim(),
    plan_codes: document.getElementById("couponPlans").value.trim(),
    max_uses: document.getElementById("couponMax").value.trim(),
    expire_hours: document.getElementById("couponExpH").value.trim(),
  };
  await jpost("/api/coupons_create", body);
  await loadCoupons();
}

async function loadAccessCodes(){
  const data = await jget("/api/access_codes?limit=50");
  document.getElementById("accessCodes").innerHTML = tableHtml(data.items||[]);
}

async function createAccessCode(){
  const body = {
    code: document.getElementById("acCode").value.trim(),
    days: parseInt(document.getElementById("acDays").value.trim(),10),
    max_uses: parseInt(document.getElementById("acMax").value.trim()||"1",10),
    expire_hours: document.getElementById("acExpH").value.trim(),
    note: document.getElementById("acNote").value.trim(),
  };
  await jpost("/api/access_codes_create", body);
  await loadAccessCodes();
}

async function loadBroadcasts(){
  const data = await jget("/api/broadcast_jobs?limit=30");
  document.getElementById("broadcasts").innerHTML = tableHtml(data.items||[]);
}

async function loadBroadcastLogs(){
  const id = document.getElementById("bcJobId").value.trim();
  if(!id){ return; }
  const data = await jget("/api/broadcast_logs?job_id=" + encodeURIComponent(id) + "&limit=200");
  document.getElementById("broadcastLogs").innerHTML = tableHtml(data.items||[]);
}

async function previewBroadcast(){
  const seg = document.getElementById("bcSegment").value.trim();
  const src = document.getElementById("bcSource").value.trim();
  const data = await jget("/api/broadcast_preview?segment=" + encodeURIComponent(seg) + "&source=" + encodeURIComponent(src));
  document.getElementById("opResult").innerText = "预览：目标人数=" + (data.count||0);
}

async function pauseBroadcast(){
  const id = document.getElementById("bcJobId").value.trim();
  if(!id){ return; }
  await jpost("/api/broadcast_pause", {id: parseInt(id,10)});
  await loadBroadcasts();
}

async function resumeBroadcast(){
  const id = document.getElementById("bcJobId").value.trim();
  if(!id){ return; }
  await jpost("/api/broadcast_resume", {id: parseInt(id,10)});
  await loadBroadcasts();
}

async function createBroadcast(){
  const body = {
    segment: document.getElementById("bcSegment").value.trim(),
    source: document.getElementById("bcSource").value.trim(),
    parse_mode: document.getElementById("bcParseMode").value.trim(),
    button_text: document.getElementById("bcBtnText").value.trim(),
    button_url: document.getElementById("bcBtnUrl").value.trim(),
    disable_preview: document.getElementById("bcNoPreview").checked ? 1 : 0,
    text: document.getElementById("bcText").value,
  };
  const r = await jpost("/api/broadcast_create", body);
  await loadBroadcasts();
  if(r && r.id){ await jpost("/api/broadcast_run", {id: r.id}); await loadBroadcasts(); }
}

function exportUsers(){
  const q = document.getElementById("q").value.trim();
  window.location = "/api/export/users.csv?q=" + encodeURIComponent(q) + "&limit=2000";
}
function exportOrders(){
  window.location = "/api/export/orders.csv?hours=168&limit=5000";
}
function exportTxs(){
  window.location = "/api/export/txs.csv?hours=168&limit=5000";
}
function exportAudit(){
  window.location = "/api/export/admin_audit.csv?hours=168&limit=5000";
}

async function loadOrders(){
  const data = await jget("/api/orders?hours=24&limit=50");
  document.getElementById("ordersHint").innerText = "时间范围: 最近 24 小时";
  document.getElementById("orders").innerHTML = tableHtml(data.items);
}

loadStats();
loadOrders();
loadCoupons();
loadAccessCodes();
loadBroadcasts();
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

    def _send_csv(self, filename: str, body: bytes):
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
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

        if path == "/api/coupons":
            qs = parse_qs(u.query)
            limit = int((qs.get("limit", ["50"])[0] or "50"))
            body = _json_bytes({"items": list_coupons(limit=limit)})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/access_codes":
            qs = parse_qs(u.query)
            limit = int((qs.get("limit", ["50"])[0] or "50"))
            body = _json_bytes({"items": list_access_codes(limit=limit)})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/broadcast_jobs":
            qs = parse_qs(u.query)
            limit = int((qs.get("limit", ["30"])[0] or "30"))
            body = _json_bytes({"items": list_broadcast_jobs(limit=limit)})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/broadcast_logs":
            qs = parse_qs(u.query)
            job_id = int((qs.get("job_id", ["0"])[0] or "0"))
            limit = int((qs.get("limit", ["200"])[0] or "200"))
            body = _json_bytes({"items": list_broadcast_logs(job_id=job_id, limit=limit)})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/broadcast_preview":
            qs = parse_qs(u.query)
            segment = (qs.get("segment", [""])[0] or "").strip()
            source = (qs.get("source", [""])[0] or "").strip()
            targets = _pick_broadcast_targets(segment, source or None)
            body = _json_bytes({"count": len(targets), "sample": targets[:10]})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/export/users.csv":
            qs = parse_qs(u.query)
            q = (qs.get("q", [""])[0] or "").strip()
            limit = int((qs.get("limit", ["2000"])[0] or "2000"))
            rows = list_users(q=q, limit=limit)
            cols = ["telegram_id", "username", "paid_until", "total_received", "wallet_addr", "first_source", "last_source", "last_source_at", "is_blacklisted", "is_whitelisted", "note", "created_at"]
            body = _csv_bytes(rows, cols)
            return self._send_csv("users.csv", body)

        if path == "/api/export/orders.csv":
            qs = parse_qs(u.query)
            hours = int((qs.get("hours", ["168"])[0] or "168"))
            limit = int((qs.get("limit", ["5000"])[0] or "5000"))
            rows = list_orders(hours=hours, limit=limit)
            cols = ["id", "telegram_id", "addr", "amount", "plan_code", "status", "tx_id", "created_at"]
            body = _csv_bytes(rows, cols)
            return self._send_csv("orders.csv", body)

        if path == "/api/export/txs.csv":
            qs = parse_qs(u.query)
            hours = int((qs.get("hours", ["168"])[0] or "168"))
            limit = int((qs.get("limit", ["5000"])[0] or "5000"))
            rows = list_txs(hours=hours, limit=limit)
            cols = ["tx_id", "telegram_id", "addr", "from_addr", "amount", "status", "plan_code", "credited_amount", "processed_at", "block_time", "created_at"]
            body = _csv_bytes(rows, cols)
            return self._send_csv("txs.csv", body)

        if path == "/api/export/broadcast_logs.csv":
            qs = parse_qs(u.query)
            job_id = int((qs.get("job_id", ["0"])[0] or "0"))
            limit = int((qs.get("limit", ["20000"])[0] or "20000"))
            rows = list_broadcast_logs(job_id=job_id, limit=limit)
            cols = ["telegram_id", "status", "error", "created_at"]
            body = _csv_bytes(rows, cols)
            return self._send_csv(f"broadcast_{job_id}.csv", body)

        if path == "/api/export/admin_audit.csv":
            qs = parse_qs(u.query)
            hours = int((qs.get("hours", ["168"])[0] or "168"))
            limit = int((qs.get("limit", ["5000"])[0] or "5000"))
            rows = list_admin_audit(hours=hours, limit=limit)
            cols = ["id", "actor", "action", "target_id", "payload", "created_at"]
            body = _csv_bytes(rows, cols)
            return self._send_csv("admin_audit.csv", body)

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

        if path == "/api/coupons_create":
            upsert_coupon(
                code=(data.get("code") or "").strip(),
                kind=(data.get("kind") or "").strip(),
                value=(data.get("value") or "").strip(),
                plan_codes=(data.get("plan_codes") or "").strip(),
                max_uses=(data.get("max_uses") or ""),
                expire_hours=(data.get("expire_hours") or ""),
            )
            body = _json_bytes({"ok": True})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/access_codes_create":
            upsert_access_code(
                code=(data.get("code") or "").strip(),
                days=int(data.get("days") or 0),
                max_uses=int(data.get("max_uses") or 1),
                expire_hours=(data.get("expire_hours") or ""),
                note=(data.get("note") or "").strip(),
                created_by=actor,
            )
            body = _json_bytes({"ok": True})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/broadcast_create":
            bid = create_broadcast_job_v2(
                segment=(data.get("segment") or "").strip(),
                source=(data.get("source") or "").strip(),
                text=(data.get("text") or ""),
                parse_mode=(data.get("parse_mode") or "").strip(),
                button_text=(data.get("button_text") or "").strip(),
                button_url=(data.get("button_url") or "").strip(),
                disable_preview=int(data.get("disable_preview") or 0),
                created_by=actor,
            )
            body = _json_bytes({"ok": True, "id": bid})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/broadcast_run":
            bid = int(data.get("id") or 0)
            ok = run_broadcast_async(bid)
            body = _json_bytes({"ok": ok})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/broadcast_pause":
            bid = int(data.get("id") or 0)
            ok = broadcast_set_status(bid, "paused")
            body = _json_bytes({"ok": ok})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/broadcast_resume":
            bid = int(data.get("id") or 0)
            ok = broadcast_set_status(bid, "running")
            if ok:
                run_broadcast_async(bid)
            body = _json_bytes({"ok": ok})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/user_flags":
            telegram_id = int(str(data.get("telegram_id") or "0"))
            toggle = (data.get("toggle") or "").strip()
            note = (data.get("note") or "").strip()
            res = user_toggle_flags(telegram_id, toggle=toggle, note=note, actor=actor, ip=self.client_address[0])
            body = _json_bytes({"ok": True, **res})
            return self._send(200, body, "application/json; charset=utf-8")

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
    orders_24h = int(_q_one("SELECT COUNT(*) FROM orders WHERE status='success' AND created_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR)") or 0)
    amount_24h = _q_one("SELECT COALESCE(SUM(amount),0) FROM orders WHERE status='success' AND created_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR)") or 0
    payers_24h = int(
        _q_one(
            "SELECT COUNT(DISTINCT telegram_id) FROM orders WHERE status='success' AND created_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR)"
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


def list_txs(hours: int, limit: int) -> list[dict]:
    hours = max(1, min(int(hours), 720))
    limit = max(1, min(int(limit), 20000))
    sql = """
        SELECT tx_id, telegram_id, addr, from_addr, amount, status, plan_code, credited_amount, processed_at, block_time, created_at
        FROM usdt_txs
        WHERE created_at >= (UTC_TIMESTAMP() - INTERVAL %s HOUR)
        ORDER BY created_at DESC
        LIMIT %s
    """
    return _q_all(sql, (hours, limit))


def list_admin_audit(hours: int, limit: int) -> list[dict]:
    hours = max(1, min(int(hours), 720))
    limit = max(1, min(int(limit), 20000))
    sql = """
        SELECT id, actor, action, target_id, payload, created_at
        FROM admin_audit
        WHERE created_at >= (UTC_TIMESTAMP() - INTERVAL %s HOUR)
        ORDER BY created_at DESC
        LIMIT %s
    """
    return _q_all(sql, (hours, limit))


def list_coupons(limit: int) -> list[dict]:
    limit = max(1, min(int(limit), 200))
    sql = """
        SELECT code, kind, value, plan_codes, max_uses, used_count, expires_at, active, created_at
        FROM coupons
        ORDER BY created_at DESC
        LIMIT %s
    """
    return _q_all(sql, (limit,))


def upsert_coupon(code: str, kind: str, value: str, plan_codes: str, max_uses, expire_hours):
    code = (code or "").strip()
    if not code:
        raise ValueError("code required")
    kind = (kind or "").strip().lower()
    if kind not in ("percent", "fixed"):
        raise ValueError("kind must be percent/fixed")
    try:
        v = float(value)
    except Exception:
        raise ValueError("bad value")
    plan_codes = (plan_codes or "").strip() or None
    max_uses_v = None
    if str(max_uses).strip():
        max_uses_v = int(max_uses)
    expires_at = None
    if str(expire_hours).strip():
        expires_at = _utc_now() + timedelta(hours=int(expire_hours))
    _exec(
        """
        INSERT INTO coupons (code, kind, value, plan_codes, max_uses, used_count, expires_at, active)
        VALUES (%s,%s,%s,%s,%s,0,%s,1)
        ON DUPLICATE KEY UPDATE kind=VALUES(kind), value=VALUES(value), plan_codes=VALUES(plan_codes), max_uses=VALUES(max_uses), expires_at=VALUES(expires_at), active=1
        """,
        (code, kind, str(v), plan_codes, max_uses_v, expires_at),
    )


def list_access_codes(limit: int) -> list[dict]:
    limit = max(1, min(int(limit), 200))
    sql = """
        SELECT code, days, plan_code, max_uses, used_count, expires_at, note, created_by, created_at, last_used_at
        FROM access_codes
        ORDER BY created_at DESC
        LIMIT %s
    """
    return _q_all(sql, (limit,))


def upsert_access_code(code: str, days: int, max_uses: int, expire_hours, note: str, created_by: str):
    code = (code or "").strip()
    if not code:
        raise ValueError("code required")
    days = int(days)
    if days == 0:
        raise ValueError("days required")
    max_uses = max(1, int(max_uses))
    expires_at = None
    if str(expire_hours).strip():
        expires_at = _utc_now() + timedelta(hours=int(expire_hours))
    _exec(
        """
        INSERT INTO access_codes (code, days, plan_code, max_uses, used_count, expires_at, note, created_by)
        VALUES (%s,%s,NULL,%s,0,%s,%s,%s)
        ON DUPLICATE KEY UPDATE days=VALUES(days), max_uses=VALUES(max_uses), expires_at=VALUES(expires_at), note=VALUES(note), created_by=VALUES(created_by)
        """,
        (code, days, max_uses, expires_at, (note or "")[:256] if note else None, created_by),
    )


def create_broadcast_job(segment: str, source: str, text: str, created_by: str) -> int:
    segment = (segment or "").strip() or "all"
    source = (source or "").strip() or None
    text = (text or "").strip()
    if not text:
        raise ValueError("text required")
    parse_mode = None
    button_text = None
    button_url = None
    disable_preview = 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO broadcast_jobs (segment, source, text, parse_mode, button_text, button_url, disable_preview, status, created_by)
            VALUES (%s,%s,%s,%s,%s,%s,%s,'created',%s)
            """,
            (segment, source, text, parse_mode, button_text, button_url, int(disable_preview), created_by),
        )
        bid = int(cur.lastrowid)
        conn.commit()
        cur.close()
        return bid
    finally:
        try:
            conn.close()
        except Exception:
            pass


def create_broadcast_job_v2(
    segment: str,
    source: str,
    text: str,
    parse_mode: str,
    button_text: str,
    button_url: str,
    disable_preview: int,
    created_by: str,
) -> int:
    segment = (segment or "").strip() or "all"
    source = (source or "").strip() or None
    text = (text or "").strip()
    if not text:
        raise ValueError("text required")
    parse_mode = (parse_mode or "").strip() or None
    if parse_mode and parse_mode not in ("HTML", "Markdown", "MarkdownV2"):
        raise ValueError("bad parse_mode")
    button_text = (button_text or "").strip() or None
    button_url = (button_url or "").strip() or None
    if button_text and not button_url:
        raise ValueError("button_url required")
    if button_url and not button_text:
        button_text = "打开"
    disable_preview = 1 if int(disable_preview or 0) == 1 else 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO broadcast_jobs (segment, source, text, parse_mode, button_text, button_url, disable_preview, status, created_by)
            VALUES (%s,%s,%s,%s,%s,%s,%s,'created',%s)
            """,
            (segment, source, text, parse_mode, button_text, button_url, disable_preview, created_by),
        )
        bid = int(cur.lastrowid)
        conn.commit()
        cur.close()
        return bid
    finally:
        try:
            conn.close()
        except Exception:
            pass


def list_broadcast_jobs(limit: int) -> list[dict]:
    limit = max(1, min(int(limit), 200))
    sql = """
        SELECT id, segment, source, status, created_by, created_at, started_at, finished_at, total, success, failed
        FROM broadcast_jobs
        ORDER BY created_at DESC
        LIMIT %s
    """
    return _q_all(sql, (limit,))


def list_broadcast_logs(job_id: int, limit: int) -> list[dict]:
    job_id = int(job_id or 0)
    limit = max(1, min(int(limit), 500))
    if job_id <= 0:
        return []
    sql = """
        SELECT telegram_id, status, error, created_at
        FROM broadcast_logs
        WHERE job_id=%s
        ORDER BY created_at DESC
        LIMIT %s
    """
    return _q_all(sql, (job_id, limit))


def broadcast_set_status(job_id: int, status: str) -> bool:
    job_id = int(job_id or 0)
    status = (status or "").strip()
    if job_id <= 0:
        return False
    if status not in ("paused", "running", "aborted", "done", "created"):
        return False
    _exec("UPDATE broadcast_jobs SET status=%s WHERE id=%s", (status, job_id))
    return True


_broadcast_lock = threading.Lock()
_broadcast_running: set[int] = set()


def _pick_broadcast_targets(segment: str, source: str | None) -> list[int]:
    segment = (segment or "").strip() or "all"
    where = ["(is_blacklisted IS NULL OR is_blacklisted=0)"]
    params: list = []
    if segment == "active":
        where.append("paid_until IS NOT NULL AND paid_until > UTC_TIMESTAMP()")
    elif segment == "expired":
        where.append("paid_until IS NOT NULL AND paid_until <= UTC_TIMESTAMP()")
    elif segment == "expiring1d":
        where.append("paid_until IS NOT NULL AND paid_until BETWEEN UTC_TIMESTAMP() AND (UTC_TIMESTAMP() + INTERVAL 1 DAY)")
    elif segment == "expiring3d":
        where.append("paid_until IS NOT NULL AND paid_until BETWEEN UTC_TIMESTAMP() AND (UTC_TIMESTAMP() + INTERVAL 3 DAY)")
    elif segment == "non_member":
        where.append("(paid_until IS NULL OR paid_until <= UTC_TIMESTAMP())")
    if source:
        where.append("last_source=%s")
        params.append(source)
    sql = f"SELECT telegram_id FROM users WHERE {' AND '.join(where)} ORDER BY telegram_id DESC LIMIT 20000"
    rows = _q_all(sql, tuple(params))
    out: list[int] = []
    for r in rows:
        try:
            out.append(int(r["telegram_id"]))
        except Exception:
            continue
    return out


def _broadcast_update(job_id: int, **fields):
    sets = []
    params = []
    for k, v in fields.items():
        sets.append(f"{k}=%s")
        params.append(v)
    if not sets:
        return
    params.append(int(job_id))
    _exec(f"UPDATE broadcast_jobs SET {', '.join(sets)} WHERE id=%s", tuple(params))


def _broadcast_log(job_id: int, telegram_id: int, status: str, error: str | None = None):
    _exec(
        "INSERT INTO broadcast_logs (job_id, telegram_id, status, error) VALUES (%s,%s,%s,%s)",
        (int(job_id), int(telegram_id), status, (error or "")[:256] if error else None),
    )


def _run_broadcast(job_id: int):
    row = _q_all("SELECT * FROM broadcast_jobs WHERE id=%s LIMIT 1", (int(job_id),))
    if not row:
        return
    job = row[0]
    if (job.get("status") or "") in ("paused", "aborted", "done"):
        return
    segment = job.get("segment") or "all"
    source = job.get("source")
    text = job.get("text") or ""
    parse_mode = (job.get("parse_mode") or "").strip() or None
    button_text = (job.get("button_text") or "").strip() or None
    button_url = (job.get("button_url") or "").strip() or None
    disable_preview = int(job.get("disable_preview") or 0) == 1
    targets = _pick_broadcast_targets(segment, source)
    done_rows = _q_all("SELECT telegram_id FROM broadcast_logs WHERE job_id=%s", (int(job_id),))
    done = set()
    for r in done_rows:
        try:
            done.add(int(r.get("telegram_id")))
        except Exception:
            continue
    targets = [x for x in targets if int(x) not in done]
    _broadcast_update(job_id, status="running", started_at=_utc_now(), total=len(targets))
    ok_n = 0
    fail_n = 0
    for uid in targets:
        srow = _q_one("SELECT status FROM broadcast_jobs WHERE id=%s", (int(job_id),))
        if srow in ("paused", "aborted", "done"):
            _broadcast_update(job_id, status=str(srow))
            return
        payload = {"chat_id": str(uid), "text": text}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if disable_preview:
            payload["disable_web_page_preview"] = "true"
        if button_url:
            bt = button_text or "打开"
            payload["reply_markup"] = json.dumps(
                {"inline_keyboard": [[{"text": bt, "url": button_url}]]},
                ensure_ascii=False,
            )
        ok, res = _bot_api("sendMessage", payload)
        if not ok and isinstance(res, dict) and res.get("retry_after"):
            try:
                time.sleep(float(res.get("retry_after") or 1))
            except Exception:
                time.sleep(1.0)
            ok, res = _bot_api("sendMessage", payload)
        if ok:
            ok_n += 1
            _broadcast_log(job_id, uid, "sent", None)
        else:
            fail_n += 1
            _broadcast_log(job_id, uid, "failed", str(res))
        _broadcast_update(job_id, success=ok_n, failed=fail_n)
        sent = ok_n + fail_n
        if sent >= int(BROADCAST_ABORT_MIN_SENT):
            rate = float(fail_n) / max(1.0, float(sent))
            if rate >= float(BROADCAST_ABORT_FAIL_RATE):
                _broadcast_update(job_id, status="aborted", finished_at=_utc_now(), success=ok_n, failed=fail_n)
                return
        try:
            time.sleep(max(0.0, float(BROADCAST_SLEEP_SEC)))
        except Exception:
            time.sleep(0.15)
    _broadcast_update(job_id, status="done", finished_at=_utc_now(), success=ok_n, failed=fail_n)


def run_broadcast_async(job_id: int) -> bool:
    job_id = int(job_id or 0)
    if job_id <= 0:
        return False
    with _broadcast_lock:
        if job_id in _broadcast_running:
            return False
        _broadcast_running.add(job_id)

    def _runner():
        try:
            _run_broadcast(job_id)
        finally:
            with _broadcast_lock:
                _broadcast_running.discard(job_id)

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    return True


def user_detail(telegram_id: int) -> dict:
    user_rows = _q_all(
        "SELECT telegram_id, username, paid_until, total_received, wallet_addr, inviter_id, invite_count, is_blacklisted, is_whitelisted, note, first_source, last_source, last_source_at, created_at FROM users WHERE telegram_id=%s LIMIT 1",
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


def user_toggle_flags(telegram_id: int, toggle: str, note: str, actor: str, ip: str) -> dict:
    telegram_id = int(telegram_id)
    toggle = (toggle or "").strip()
    note = (note or "").strip() or None
    row = _q_all("SELECT is_blacklisted, is_whitelisted FROM users WHERE telegram_id=%s LIMIT 1", (telegram_id,))
    if not row:
        raise ValueError("user not found")
    cur_black = int(row[0].get("is_blacklisted") or 0)
    cur_white = int(row[0].get("is_whitelisted") or 0)
    new_black = cur_black
    new_white = cur_white
    if toggle == "black":
        new_black = 0 if cur_black == 1 else 1
    elif toggle == "white":
        new_white = 0 if cur_white == 1 else 1
    else:
        raise ValueError("bad toggle")
    _exec(
        "UPDATE users SET is_blacklisted=%s, is_whitelisted=%s, note=%s WHERE telegram_id=%s",
        (new_black, new_white, note, telegram_id),
    )
    _audit(actor, "user_flags", telegram_id, {"toggle": toggle, "black": new_black, "white": new_white, "note": note, "ip": ip})
    return {"is_blacklisted": new_black, "is_whitelisted": new_white}


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
        d = obj.get("description") or raw
        params = obj.get("parameters") or {}
        if isinstance(params, dict) and params.get("retry_after"):
            return False, {"description": d, "retry_after": params.get("retry_after")}
        return False, d
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def resend_invite_link(telegram_id: int, actor: str, note: str, ip: str) -> tuple[bool, str]:
    if telegram_id <= 0:
        return False, "bad telegram_id"
    exp = int((_utc_now() + timedelta(hours=int(JOIN_REQUEST_LINK_EXPIRE_HOURS))).timestamp())
    payload = {"chat_id": str(PAID_CHANNEL_ID), "expire_date": str(exp)}
    if JOIN_REQUEST_ENABLE:
        payload["creates_join_request"] = "true"
    else:
        payload["member_limit"] = "1"
    ok, res = _bot_api("createChatInviteLink", payload)
    if not ok:
        _audit(actor, "resend_invite_failed", telegram_id, {"note": note, "ip": ip, "error": str(res)})
        return False, str(res)
    link = str((res or {}).get("invite_link") or "")
    if not link:
        _audit(actor, "resend_invite_failed", telegram_id, {"note": note, "ip": ip, "error": "empty invite_link"})
        return False, "empty invite_link"
    msg = f"✅ 入群链接（{int(JOIN_REQUEST_LINK_EXPIRE_HOURS)}h有效）：\n{link}"
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

