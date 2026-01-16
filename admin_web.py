import base64
import csv
import hmac
import hashlib
import io
import json
import mimetypes
import cgi
import os
import secrets
import socket
import threading
import time
from datetime import datetime, timedelta
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
from urllib.parse import parse_qs, urlparse
from urllib import request as urlrequest
from urllib import parse as urlparse2

from config import (
    AMOUNT_EPS,
    ADMIN_WEB_ACTIONS_ENABLE,
    ADMIN_WEB_ENABLE,
    ADMIN_WEB_ALLOW_IPS,
    ADMIN_WEB_HOST,
    ADMIN_WEB_PASS,
    ADMIN_WEB_PORT,
    ADMIN_WEB_RO_PASS,
    ADMIN_WEB_RO_USER,
    ADMIN_WEB_TRUST_PROXY,
    ADMIN_WEB_USER,
    BOT_TOKEN,
    BOT_USERNAME,
    BROADCAST_ABORT_FAIL_RATE,
    BROADCAST_ABORT_MIN_SENT,
    BROADCAST_SLEEP_SEC,
    HEARTBEAT_FILE,
    HEARTBEAT_USERBOT_FILE,
    JOIN_REQUEST_ENABLE,
    JOIN_REQUEST_LINK_EXPIRE_HOURS,
    LOCAL_UPLOADER_TOKEN,
    PAID_CHANNEL_ID,
    PLANS,
)
from core.db import get_conn
from core.models import (
    admin_create_video_job,
    admin_set_video_publish,
    admin_set_video_sort,
    admin_update_video_meta,
    get_user,
    init_tables,
    list_banners,
    list_categories,
    list_videos_admin,
    local_uploader_claim_next,
    local_uploader_update,
    mark_order_success,
    record_video_view,
    set_usdt_tx_status,
    set_video_category,
    upsert_banner,
    upsert_category,
    update_user_payment,
    user_viewed_tags,
    delete_banner,
    delete_category,
)
from bot.payments import compute_new_paid_until


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


def _validate_webapp_init_data(init_data: str, bot_token: str) -> dict | None:
    try:
        parsed = parse_qs(init_data)
        hash_val = parsed.get("hash", [""])[0]
        if not hash_val:
            return None
        
        data_check_arr = []
        for k, v in parsed.items():
            if k == "hash":
                continue
            data_check_arr.append(f"{k}={v[0]}")
        
        data_check_arr.sort()
        data_check_string = "\n".join(data_check_arr)
        
        secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        
        if calculated_hash == hash_val:
            user_json = parsed.get("user", ["{}"])[0]
            return json.loads(user_json)
        return None
    except Exception:
        return None


def _basic_auth_ok(headers) -> bool:
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
    if ADMIN_WEB_USER and ADMIN_WEB_PASS and secrets.compare_digest(u, ADMIN_WEB_USER) and secrets.compare_digest(p, ADMIN_WEB_PASS):
        return True
    if ADMIN_WEB_RO_USER and ADMIN_WEB_RO_PASS and secrets.compare_digest(u, ADMIN_WEB_RO_USER) and secrets.compare_digest(p, ADMIN_WEB_RO_PASS):
        return True
    return False


def _basic_auth_identity(headers) -> tuple[str, str] | None:
    v = headers.get("Authorization", "")
    if not v.startswith("Basic "):
        return None
    try:
        raw = base64.b64decode(v.split(" ", 1)[1].strip()).decode("utf-8", errors="ignore")
    except Exception:
        return None
    if ":" not in raw:
        return None
    u, p = raw.split(":", 1)
    if ADMIN_WEB_USER and ADMIN_WEB_PASS and secrets.compare_digest(u, ADMIN_WEB_USER) and secrets.compare_digest(p, ADMIN_WEB_PASS):
        return (u, "admin")
    if ADMIN_WEB_RO_USER and ADMIN_WEB_RO_PASS and secrets.compare_digest(u, ADMIN_WEB_RO_USER) and secrets.compare_digest(p, ADMIN_WEB_RO_PASS):
        return (u, "readonly")
    return None


def _parse_allow_ips(raw: str) -> list[ipaddress._BaseNetwork]:
    out: list[ipaddress._BaseNetwork] = []
    s = (raw or "").strip()
    if not s:
        return out
    for part in s.replace("\n", ",").split(","):
        part = (part or "").strip()
        if not part:
            continue
        try:
            if "/" in part:
                out.append(ipaddress.ip_network(part, strict=False))
            else:
                if ":" in part:
                    out.append(ipaddress.ip_network(part + "/128", strict=False))
                else:
                    out.append(ipaddress.ip_network(part + "/32", strict=False))
        except Exception:
            continue
    return out


_ALLOW_IPS = _parse_allow_ips(ADMIN_WEB_ALLOW_IPS)


def _client_ip(handler: BaseHTTPRequestHandler) -> str:
    if ADMIN_WEB_TRUST_PROXY:
        xf = (handler.headers.get("X-Forwarded-For") or "").strip()
        if xf:
            return xf.split(",")[0].strip()
    return str(handler.client_address[0] or "")


def _ip_allowed(ip: str) -> bool:
    if not _ALLOW_IPS:
        return True
    try:
        addr = ipaddress.ip_address(ip)
    except Exception:
        return False
    for n in _ALLOW_IPS:
        try:
            if addr in n:
                return True
        except Exception:
            continue
    return False


def _html_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _uploads_dir() -> str:
    base = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp", "uploads")
    os.makedirs(base, exist_ok=True)
    return base


def _safe_join(base_dir: str, rel: str) -> str | None:
    rel = (rel or "").lstrip("/")
    if not rel or ".." in rel:
        return None
    full = os.path.abspath(os.path.join(base_dir, rel))
    if not full.startswith(os.path.abspath(base_dir)):
        return None
    return full


def _public_base_url(handler: BaseHTTPRequestHandler) -> str:
    proto = (handler.headers.get("X-Forwarded-Proto") or "").strip() or "http"
    host = (handler.headers.get("Host") or "").strip()
    if not host:
        host = f"{handler.server.server_address[0]}:{handler.server.server_address[1]}"
    return f"{proto}://{host}"


INDEX_HTML = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>PV Admin</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:0;color:#111;background:#fafafa}
    .layout{display:flex;min-height:100vh}
    .side{width:240px;background:#111;color:#fff;padding:18px 14px;position:sticky;top:0;height:100vh;box-sizing:border-box}
    .brand{font-weight:800;font-size:16px}
    .nav{margin-top:14px;display:flex;flex-direction:column;gap:6px}
    .nav a{color:#fff;text-decoration:none;padding:10px 10px;border-radius:10px;display:block}
    .nav a.active{background:#2b2b2b}
    .main{flex:1;padding:22px 18px;box-sizing:border-box;max-width:1200px}
    .grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:12px 0}
    .card{border:1px solid #ddd;border-radius:10px;padding:12px;background:#fff}
    .k{color:#666;font-size:12px}
    .v{font-size:22px;font-weight:700;margin-top:6px}
    input,button,textarea{padding:10px;border-radius:10px;border:1px solid #ccc}
    button{cursor:pointer;background:#111;color:#fff;border-color:#111}
    textarea{min-width:520px;min-height:90px}
    table{width:100%;border-collapse:collapse;margin-top:10px}
    th,td{border-bottom:1px solid #eee;padding:8px;text-align:left;font-size:13px;vertical-align:top}
    .row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
    .muted{color:#666;font-size:12px}
    .page{display:none}
    .page.active{display:block}
    .panel{background:#fff;border:1px solid #e7e7e7;border-radius:12px;padding:14px}
  </style>
</head>
<body>
  <div class="layout">
    <aside class="side">
      <div class="brand">PV Admin</div>
      <div class="muted" style="color:#cfcfcf;margin-top:6px">UTC</div>
      <nav class="nav">
        <a href="#home" data-page="home" onclick="showPage('home')">首页</a>
        <a href="#miniapp" data-page="miniapp" onclick="showPage('miniapp')">小程序配置</a>
        <a href="#users" data-page="users" onclick="showPage('users')">用户列表</a>
        <a href="#ops" data-page="ops" onclick="showPage('ops')">运营工具</a>
        <a href="#reconcile" data-page="reconcile" onclick="showPage('reconcile')">对账工具</a>
        <a href="#videos" data-page="videos" onclick="showPage('videos')">视频管理</a>
      </nav>
    </aside>
    <main class="main">
      <h2>PV 管理后台</h2>
      <div class="muted">需要浏览器 Basic Auth 登录。时间默认 UTC。</div>

      <div class="page" id="page-home">
        <h3>首页</h3>
        <div class="grid" id="cards"></div>
      </div>

      <div class="page" id="page-miniapp">
  <h3>小程序配置</h3>
  <div class="row">
    <div>
      <div class="muted">分类管理</div>
      <div class="row">
        <input id="catId" placeholder="ID(可选)" style="min-width:120px" />
        <input id="catName" placeholder="名称" style="min-width:160px" />
        <input id="catSort" placeholder="排序(0-99)" style="min-width:80px" />
        <label><input id="catVisible" type="checkbox" checked /> 显示</label>
        <button onclick="upsertCategory()">添加/更新</button>
        <button onclick="loadCategories()">刷新</button>
      </div>
      <div id="categories"></div>
    </div>
  </div>
  <div class="row" style="margin-top:10px">
    <div>
      <div class="muted">Banner 管理</div>
      <div class="row">
        <input id="banId" placeholder="ID(可选)" style="min-width:120px" />
        <input id="banImg" placeholder="图片URL(或先上传)" style="min-width:240px" />
        <input id="banFile" type="file" accept="image/*" style="min-width:260px" />
        <button onclick="uploadBannerImage()">上传图片</button>
        <input id="banLink" placeholder="跳转URL" style="min-width:240px" />
        <input id="banSort" placeholder="排序" style="min-width:80px" />
        <label><input id="banActive" type="checkbox" checked /> 启用</label>
        <button onclick="upsertBanner()">添加/更新</button>
        <button onclick="loadBanners()">刷新</button>
      </div>
      <div id="banners"></div>
    </div>
  </div>

      </div>

      <div class="page" id="page-users">
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

      </div>

      <div class="page" id="page-ops">
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
        <input id="couponBatchN" placeholder="批量数量(可选)" style="min-width:140px" />
        <input id="couponPrefix" placeholder="prefix(可选)" style="min-width:140px" />
        <button onclick="createCoupon()">创建/更新</button>
        <button onclick="generateCoupons()">批量生成</button>
        <button onclick="exportCoupons()">导出</button>
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
        <input id="acBatchN" placeholder="批量数量(可选)" style="min-width:140px" />
        <input id="acPrefix" placeholder="prefix(可选)" style="min-width:140px" />
        <button onclick="createAccessCode()">创建/更新</button>
        <button onclick="generateAccessCodes()">批量生成</button>
        <button onclick="exportAccessCodes()">导出</button>
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
        <input id="bcMediaType" placeholder="media_type(可选: photo/video)" style="min-width:220px" />
        <input id="bcMedia" placeholder="media(url 或 file_id，可选)" style="min-width:320px" />
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

      </div>

      <div class="page" id="page-reconcile">
  <h3>对账工具</h3>
  <div class="row">
    <input id="rePendingMin" placeholder="pending 超过分钟(默认60)" style="min-width:240px" />
    <button onclick="loadReconcile()">刷新</button>
  </div>
  <div class="row" style="margin-top:10px">
    <input id="reTxId" placeholder="tx_id" style="min-width:360px" />
    <input id="reOrderId" placeholder="order_id" style="min-width:200px" />
    <input id="reNote" placeholder="备注(可选)" style="min-width:320px" />
    <button onclick="assignReconcile()">人工绑定</button>
    <button onclick="retryTxMatch()">重试自动匹配(tx)</button>
  </div>
  <div id="reconcile"></div>

  <h3>最近订单</h3>
  <div class="row">
    <button onclick="loadOrders()">刷新</button>
    <span class="muted" id="ordersHint"></span>
  </div>
  <div id="orders"></div>

      </div>

      <div class="page" id="page-videos">
        <h3>视频管理</h3>
        <div class="panel">
          <div class="row">
            <input id="vQ" placeholder="搜索标题/文案" style="min-width:260px" />
            <input id="vStatus" placeholder="status(pending/uploading/done/failed)" style="min-width:260px" />
            <button onclick="loadVideosAdmin()">查询</button>
          </div>
          <div id="videosAdmin"></div>
        </div>
        <div style="height:12px"></div>
        <div class="panel">
          <div class="muted">视频上传（创建上传任务，本地 userbot 拉取并上传到频道后回填链接）</div>
          <div class="row" style="margin-top:10px">
            <input id="vLocal" placeholder="本地文件名（local userbot 识别）" style="min-width:320px" />
            <input id="vVideoFile" type="file" accept="video/*" style="min-width:320px" onchange="useSelectedVideoName()" />
            <select id="vCategorySel" style="min-width:220px;padding:10px;border-radius:10px;border:1px solid #ccc">
              <option value="0">请选择分类</option>
            </select>
            <input id="vSort" placeholder="排序(越大越靠前)" style="min-width:160px" />
            <label class="muted"><input id="vPub" type="checkbox" checked /> 上架</label>
          </div>
          <div class="muted" style="margin-top:6px">说明：这里不会把视频上传到服务器，只会读取文件名。请把视频文件放到本地 userbot 的 LOCAL_UPLOADER_DIR 目录下。</div>
          <div class="row" style="margin-top:10px">
            <input id="vCover" placeholder="展示图片URL(可选)" style="min-width:420px" />
            <input id="vCoverFile" type="file" accept="image/*" style="min-width:260px" />
            <button onclick="uploadCoverImage()">上传封面</button>
            <input id="vTags" placeholder="标签(逗号分隔,可选)" style="min-width:420px" />
          </div>
          <div class="row" style="margin-top:10px">
            <textarea id="vCaption" placeholder="文案内容（标题/描述）"></textarea>
          </div>
          <div class="row" style="margin-top:10px">
            <button onclick="createVideoJob()">创建上传任务</button>
          </div>
          <div class="muted" id="vResult" style="margin-top:8px"></div>
        </div>
      </div>

    </main>
  </div>

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
    ["App 心跳", (s.hb_app_age_sec==null ? "—" : (s.hb_app_age_sec + "s"))],
    ["Userbot 心跳", (s.hb_userbot_age_sec==null ? "—" : (s.hb_userbot_age_sec + "s"))],
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

function showPage(name){
  const pages = ["home","miniapp","users","ops","reconcile","videos"];
  pages.forEach(p=>{
    const el = document.getElementById("page-"+p);
    if(el) el.classList.toggle("active", p===name);
    document.querySelectorAll(`.nav a[data-page='${p}']`).forEach(a=>a.classList.toggle("active", p===name));
  });
  try{ history.replaceState(null,"","#"+name); }catch(e){}
  if(name === "videos"){
    try{ loadCategories(); }catch(e){}
  }
}

function initPage(){
  const h = (location.hash||"").replace("#","").trim();
  const pages = new Set(["home","miniapp","users","ops","reconcile","videos"]);
  showPage(pages.has(h)?h:"home");
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
  blocks.push("<h4>浏览标签</h4>" + tableHtml(data.viewed_tags||[]));
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

async function generateCoupons(){
  const body = {
    kind: document.getElementById("couponKind").value.trim(),
    value: document.getElementById("couponValue").value.trim(),
    plan_codes: document.getElementById("couponPlans").value.trim(),
    max_uses: document.getElementById("couponMax").value.trim(),
    expire_hours: document.getElementById("couponExpH").value.trim(),
    count: parseInt(document.getElementById("couponBatchN").value.trim()||"0",10),
    prefix: document.getElementById("couponPrefix").value.trim(),
  };
  const r = await jpost("/api/coupons_generate", body);
  document.getElementById("opResult").innerText = "已生成优惠码：" + (r.created||0);
  await loadCoupons();
}

function exportCoupons(){
  window.location = "/api/export/coupons.csv?limit=20000";
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

async function generateAccessCodes(){
  const body = {
    days: parseInt(document.getElementById("acDays").value.trim(),10),
    max_uses: parseInt(document.getElementById("acMax").value.trim()||"1",10),
    expire_hours: document.getElementById("acExpH").value.trim(),
    note: document.getElementById("acNote").value.trim(),
    count: parseInt(document.getElementById("acBatchN").value.trim()||"0",10),
    prefix: document.getElementById("acPrefix").value.trim(),
  };
  const r = await jpost("/api/access_codes_generate", body);
  document.getElementById("opResult").innerText = "已生成兑换码：" + (r.created||0);
  await loadAccessCodes();
}

function exportAccessCodes(){
  window.location = "/api/export/access_codes.csv?limit=20000";
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
    media_type: document.getElementById("bcMediaType").value.trim(),
    media: document.getElementById("bcMedia").value.trim(),
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

async function loadReconcile(){
  const mins = parseInt((document.getElementById("rePendingMin").value.trim()||"60"),10);
  const data = await jget("/api/reconcile?pending_min=" + encodeURIComponent(mins) + "&limit=50");
  const blocks = [];
  blocks.push("<h4>未匹配/待处理入账</h4>" + tableHtml(data.unmatched_txs||[]));
  blocks.push("<h4>超时 pending 订单</h4>" + tableHtml(data.pending_orders||[]));
  document.getElementById("reconcile").innerHTML = blocks.join("");
}

async function assignReconcile(){
  const tx_id = document.getElementById("reTxId").value.trim();
  const order_id = document.getElementById("reOrderId").value.trim();
  const note = document.getElementById("reNote").value.trim();
  if(!tx_id || !order_id){ return; }
  await jpost("/api/reconcile_assign", {tx_id, order_id: parseInt(order_id,10), note});
  document.getElementById("opResult").innerText = "完成：已人工绑定并记账";
  await loadReconcile();
}

async function retryTxMatch(){
  const tx_id = document.getElementById("reTxId").value.trim();
  const note = document.getElementById("reNote").value.trim();
  if(!tx_id){ return; }
  const r = await jpost("/api/reconcile_retry_tx", {tx_id, note});
  document.getElementById("opResult").innerText = "重试结果：" + (r.ok ? "ok" : "fail") + (r.error ? (" " + r.error) : "");
  await loadReconcile();
}

async function loadCategories(){
  const data = await jget("/api/categories");
  const items = data.items || [];
  const rows = (items || []).map(x => {
    const id = x.id ?? "";
    const name = x.name ?? "";
    const sort = x.sort_order ?? 0;
    const vis = x.is_visible ? 1 : 0;
    return `
      <tr>
        <td>${id}</td>
        <td>${name}</td>
        <td>${sort}</td>
        <td>${vis}</td>
        <td>
          <button onclick="editCategory('${id}','${String(name).replace(/'/g,'&#39;')}','${sort}','${vis}')">编辑</button>
          <button onclick="deleteCategory('${id}')">删除</button>
        </td>
      </tr>
    `;
  }).join("");
  const html = `
    <table>
      <thead><tr><th>id</th><th>name</th><th>sort_order</th><th>is_visible</th><th>op</th></tr></thead>
      <tbody>${rows || ""}</tbody>
    </table>
  `;
  document.getElementById("categories").innerHTML = items.length ? html : "<div class='muted'>无数据</div>";

  const sel = document.getElementById("vCategorySel");
  if (sel) {
    sel.innerHTML = `<option value="0">未分类(0)</option>` + items.map(x => `<option value="${x.id}">${x.id} - ${x.name}</option>`).join("");
  }
}
async function upsertCategory(){
  const body = {
    id: parseInt(document.getElementById("catId").value.trim()||"0",10),
    name: document.getElementById("catName").value.trim(),
    sort_order: parseInt(document.getElementById("catSort").value.trim()||"0",10),
    is_visible: document.getElementById("catVisible").checked
  };
  await jpost("/api/categories_upsert", body);
  document.getElementById("catId").value = "";
  document.getElementById("catName").value = "";
  await loadCategories();
}
function editCategory(id, name, sort, vis){
  document.getElementById("catId").value = id || "";
  document.getElementById("catName").value = name || "";
  document.getElementById("catSort").value = sort || "0";
  document.getElementById("catVisible").checked = String(vis) === "1";
}
async function deleteCategory(id){
  if(!id) return;
  await jpost("/api/categories_delete", {id: parseInt(id,10)});
  await loadCategories();
}
async function loadBanners(){
  const data = await jget("/api/banners");
  const items = data.items || [];
  const rows = (items || []).map(x => {
    const id = x.id ?? "";
    const img = x.image_url ?? "";
    const link = x.link_url ?? "";
    const sort = x.sort_order ?? 0;
    const act = x.is_active ? 1 : 0;
    const thumb = img ? `<a href="${img}" target="_blank"><img src="${img}" style="width:84px;height:48px;object-fit:cover;border-radius:8px;border:1px solid #eee" /></a>` : "";
    return `
      <tr>
        <td>${id}</td>
        <td>${thumb}<div class="muted" style="max-width:420px;word-break:break-all">${img}</div></td>
        <td style="max-width:320px;word-break:break-all">${link}</td>
        <td>${sort}</td>
        <td>${act}</td>
        <td>
          <button onclick="editBanner('${id}','${String(img).replace(/'/g,'&#39;')}','${String(link).replace(/'/g,'&#39;')}','${sort}','${act}')">编辑</button>
          <button onclick="deleteBanner('${id}')">删除</button>
        </td>
      </tr>
    `;
  }).join("");
  const html = `
    <table>
      <thead><tr><th>id</th><th>image</th><th>link</th><th>sort_order</th><th>is_active</th><th>op</th></tr></thead>
      <tbody>${rows || ""}</tbody>
    </table>
  `;
  document.getElementById("banners").innerHTML = items.length ? html : "<div class='muted'>无数据</div>";
}
async function upsertBanner(){
  const body = {
    id: parseInt(document.getElementById("banId").value.trim()||"0",10),
    image_url: document.getElementById("banImg").value.trim(),
    link_url: document.getElementById("banLink").value.trim(),
    sort_order: parseInt(document.getElementById("banSort").value.trim()||"0",10),
    is_active: document.getElementById("banActive").checked
  };
  await jpost("/api/banners_upsert", body);
  document.getElementById("banId").value = "";
  await loadBanners();
}
function editBanner(id, img, link, sort, act){
  document.getElementById("banId").value = id || "";
  document.getElementById("banImg").value = img || "";
  document.getElementById("banLink").value = link || "";
  document.getElementById("banSort").value = sort || "0";
  document.getElementById("banActive").checked = String(act) === "1";
}
async function deleteBanner(id){
  if(!id) return;
  await jpost("/api/banners_delete", {id: parseInt(id,10)});
  await loadBanners();
}

async function uploadImageFromInput(inputId, folder){
  const el = document.getElementById(inputId);
  const f = el && el.files && el.files[0] ? el.files[0] : null;
  if(!f) throw new Error("no file");
  const fd = new FormData();
  fd.append("file", f);
  fd.append("folder", folder||"misc");
  const r = await fetch("/api/upload_image", {method:"POST", body: fd});
  if(!r.ok){ throw new Error(await r.text()); }
  return await r.json();
}

async function uploadBannerImage(){
  const r = await uploadImageFromInput("banFile", "banners");
  if(r && r.url) document.getElementById("banImg").value = r.url;
  await loadBanners();
}

async function uploadCoverImage(){
  const r = await uploadImageFromInput("vCoverFile", "covers");
  if(r && r.url) document.getElementById("vCover").value = r.url;
}

function useSelectedVideoName(){
  const el = document.getElementById("vVideoFile");
  const f = el && el.files && el.files[0] ? el.files[0] : null;
  if(!f) return;
  document.getElementById("vLocal").value = f.name || "";
}

async function loadVideosAdmin(){
  const q = document.getElementById("vQ").value.trim();
  const status = document.getElementById("vStatus").value.trim();
  const url = "/api/videos_admin?q=" + encodeURIComponent(q) + "&status=" + encodeURIComponent(status) + "&limit=200";
  const data = await jget(url);
  document.getElementById("videosAdmin").innerHTML = tableHtml(data.items||[]);
}

async function createVideoJob(){
  const body = {
    local_filename: document.getElementById("vLocal").value.trim(),
    category_id: parseInt((document.getElementById("vCategorySel").value||"0"),10),
    sort_order: parseInt(document.getElementById("vSort").value.trim()||"0",10),
    is_published: document.getElementById("vPub").checked,
    cover_url: document.getElementById("vCover").value.trim(),
    tags: document.getElementById("vTags").value.trim(),
    caption: document.getElementById("vCaption").value.trim()
  };
  const r = await jpost("/api/video_create", body);
  document.getElementById("vResult").innerText = "已创建任务 video_id=" + (r.id || "0");
  await loadVideosAdmin();
}

loadStats();
loadOrders();
loadCoupons();
loadAccessCodes();
loadBroadcasts();
loadReconcile();
loadCategories();
loadBanners();
initPage();
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    server_version = "PVAdmin/1.0"

    def _forbidden(self, msg: str = "forbidden"):
        self._send(HTTPStatus.FORBIDDEN, (msg or "forbidden").encode("utf-8"), "text/plain; charset=utf-8")

    def _send_headers_only(self, code: int, ctype: str, length: int):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(int(length)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

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
        ip = _client_ip(self)
        if not _ip_allowed(ip):
            self._forbidden("ip not allowed")
            return False
        ident = _basic_auth_identity(self.headers)
        if not ident:
            self._unauthorized()
            return False
        self._auth_user, self._auth_role = ident
        self._auth_ip = ip
        return True

    def _require_local_uploader(self, qs: dict | None = None) -> bool:
        token = (self.headers.get("X-Local-Uploader-Token") or "").strip()
        if not token and qs:
            token = (qs.get("token", [""])[0] or "").strip()
        if not LOCAL_UPLOADER_TOKEN:
            self._forbidden("local uploader disabled")
            return False
        if token != LOCAL_UPLOADER_TOKEN:
            self._forbidden("bad token")
            return False
        return True

    def _head_static_webapp(self, path: str):
        if ".." in path:
            return self._forbidden("invalid path")
        rel_path = path[len("/webapp/") :]
        if not rel_path or rel_path.endswith("/"):
            rel_path += "index.html"
        base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "webapp")
        full_path = os.path.join(base_dir, rel_path)
        if not os.path.abspath(full_path).startswith(base_dir):
            return self._forbidden("invalid path")
        if not os.path.exists(full_path) or not os.path.isfile(full_path):
            return self._send_headers_only(404, "text/plain", 0)
        ctype, _ = mimetypes.guess_type(full_path)
        if not ctype:
            ctype = "application/octet-stream"
        try:
            size = int(os.path.getsize(full_path))
        except Exception:
            size = 0
        return self._send_headers_only(200, ctype, size)

    def do_HEAD(self):
        u = urlparse(self.path)
        path = u.path

        if path.startswith("/uploads/"):
            rel = path[len("/uploads/") :]
            base = _uploads_dir()
            full = _safe_join(base, rel)
            if not full or not os.path.exists(full) or not os.path.isfile(full):
                return self._send_headers_only(404, "text/plain", 0)
            ctype, _ = mimetypes.guess_type(full)
            if not ctype:
                ctype = "application/octet-stream"
            try:
                size = int(os.path.getsize(full))
            except Exception:
                size = 0
            return self._send_headers_only(200, ctype, size)

        if path.startswith("/webapp/"):
            return self._head_static_webapp(path)

        if path == "/health":
            ip = _client_ip(self)
            if not _ip_allowed(ip):
                self.send_response(HTTPStatus.FORBIDDEN)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", "14")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                return
            body = _json_bytes({"ok": True, "ts": _utc_now().isoformat()})
            return self._send_headers_only(200, "application/json; charset=utf-8", len(body))

        if path.startswith("/api/webapp/"):
            qs = parse_qs(u.query)
            init_data = self.headers.get("X-Telegram-Init-Data") or (qs.get("initData", [""])[0] or "")
            user_data = _validate_webapp_init_data(init_data, BOT_TOKEN)

            if path == "/api/webapp/auth":
                if not user_data:
                    return self._send_headers_only(401, "text/plain", len(b"Invalid initData"))
                uid = int(user_data.get("id"))
                u = get_user(uid)
                is_vip = False
                if u and u.get("paid_until"):
                    if u["paid_until"] > _utc_now():
                        is_vip = True
                body = _json_bytes({"user": u, "is_vip": is_vip, "bot_username": BOT_USERNAME})
                return self._send_headers_only(200, "application/json; charset=utf-8", len(body))

            if path == "/api/webapp/config":
                body = _json_bytes({"categories": list_categories(visible_only=True), "banners": list_banners(active_only=True)})
                return self._send_headers_only(200, "application/json; charset=utf-8", len(body))

            if path == "/api/webapp/plans":
                body = _json_bytes({"plans": PLANS})
                return self._send_headers_only(200, "application/json; charset=utf-8", len(body))

            if path == "/api/webapp/videos":
                q = (qs.get("q", [""])[0] or "").strip()
                page = int((qs.get("page", ["1"])[0] or "1"))
                limit = int((qs.get("limit", ["20"])[0] or "20"))
                cat_id = int((qs.get("category_id", ["0"])[0] or "0"))
                sort = (qs.get("sort", ["latest"])[0] or "latest")
                is_vip = False
                if user_data:
                    uid = int(user_data.get("id"))
                    u = get_user(uid)
                    if u and u.get("paid_until") and u["paid_until"] > _utc_now():
                        is_vip = True
                data = list_videos(q=q, page=page, limit=limit, category_id=cat_id, sort=sort)
                for item in data["items"]:
                    paid_cid = str(item["channel_id"])
                    if paid_cid.startswith("-100"):
                        paid_cid = paid_cid[4:]
                    item["paid_link"] = f"https://t.me/c/{paid_cid}/{item['message_id']}"
                    item["free_link"] = None
                    if item.get("free_channel_id") and item.get("free_message_id"):
                        free_cid = str(item["free_channel_id"])
                        if free_cid.startswith("-100"):
                            free_cid = free_cid[4:]
                        item["free_link"] = f"https://t.me/c/{free_cid}/{item['free_message_id']}"
                    item["is_locked"] = not is_vip
                body = _json_bytes(data)
                return self._send_headers_only(200, "application/json; charset=utf-8", len(body))

            return self._send_headers_only(404, "text/plain", 0)

        if not self._require_auth():
            return
        return self._send_headers_only(200, "text/plain", 0)

    def do_GET(self):
        u = urlparse(self.path)
        path = u.path

        if path.startswith("/uploads/"):
            rel = path[len("/uploads/") :]
            base = _uploads_dir()
            full = _safe_join(base, rel)
            if not full or not os.path.exists(full) or not os.path.isfile(full):
                return self._send(404, b"Not Found", "text/plain; charset=utf-8")
            ctype, _ = mimetypes.guess_type(full)
            if not ctype:
                ctype = "application/octet-stream"
            try:
                with open(full, "rb") as f:
                    data = f.read()
            except Exception:
                return self._send(500, b"read failed", "text/plain; charset=utf-8")
            return self._send(200, data, ctype)

        if path.startswith("/webapp/"):
            return self._serve_static_webapp(path)

        if path.startswith("/api/webapp/"):
            qs = parse_qs(u.query)
            init_data = self.headers.get("X-Telegram-Init-Data") or (qs.get("initData", [""])[0] or "")
            user_data = _validate_webapp_init_data(init_data, BOT_TOKEN)

            if path == "/api/webapp/auth":
                if not user_data:
                    return self._send(401, b"Invalid initData", "text/plain")
                uid = int(user_data.get("id"))
                u = get_user(uid)
                is_vip = False
                if u and u.get("paid_until"):
                    if u["paid_until"] > _utc_now():
                        is_vip = True
                return self._send(200, _json_bytes({"user": u, "is_vip": is_vip, "bot_username": BOT_USERNAME}), "application/json; charset=utf-8")
            
            if path == "/api/webapp/config":
                return self._send(200, _json_bytes({
                    "categories": list_categories(visible_only=True),
                    "banners": list_banners(active_only=True)
                }), "application/json; charset=utf-8")

            if path == "/api/webapp/plans":
                return self._send(200, _json_bytes({"plans": PLANS}), "application/json; charset=utf-8")

            if path == "/api/webapp/videos":
                q = (qs.get("q", [""])[0] or "").strip()
                page = int((qs.get("page", ["1"])[0] or "1"))
                limit = int((qs.get("limit", ["20"])[0] or "20"))
                cat_id = int((qs.get("category_id", ["0"])[0] or "0"))
                sort = (qs.get("sort", ["latest"])[0] or "latest")
                
                is_vip = False
                if user_data:
                    uid = int(user_data.get("id"))
                    u = get_user(uid)
                    if u and u.get("paid_until") and u["paid_until"] > _utc_now():
                        is_vip = True
                
                data = list_videos(q=q, page=page, limit=limit, category_id=cat_id, sort=sort)
                for item in data["items"]:
                    paid_cid = str(item["channel_id"])
                    if paid_cid.startswith("-100"): paid_cid = paid_cid[4:]
                    item["paid_link"] = f"https://t.me/c/{paid_cid}/{item['message_id']}"
                    
                    item["free_link"] = None
                    if item.get("free_channel_id") and item.get("free_message_id"):
                         free_cid = str(item["free_channel_id"])
                         if free_cid.startswith("-100"): free_cid = free_cid[4:]
                         item["free_link"] = f"https://t.me/c/{free_cid}/{item['free_message_id']}"
                    
                    item["is_locked"] = not is_vip
                    
                return self._send(200, _json_bytes(data), "application/json; charset=utf-8")

            if path == "/api/webapp/track_view":
                if not user_data:
                    return self._send(401, b"Invalid initData", "text/plain")
                vid = int((qs.get("video_id", ["0"])[0] or "0"))
                if vid > 0:
                    try:
                        record_video_view(int(user_data.get("id")), vid)
                    except Exception:
                        pass
                return self._send(200, _json_bytes({"ok": True}), "application/json; charset=utf-8")
            
            return self._send(404, b"Not Found", "text/plain")

        if path.startswith("/api/local_uploader/"):
            qs = parse_qs(u.query)
            if not self._require_local_uploader(qs):
                return
            if path == "/api/local_uploader/claim":
                job = local_uploader_claim_next()
                return self._send(200, _json_bytes({"job": job}), "application/json; charset=utf-8")
            return self._send(404, b"Not Found", "text/plain")

        if path == "/health":
            ip = _client_ip(self)
            if not _ip_allowed(ip):
                return self._forbidden("ip not allowed")
            body = _json_bytes({"ok": True, "ts": _utc_now().isoformat()})
            return self._send(200, body, "application/json; charset=utf-8")

        if not self._require_auth():
            return

        if path == "/":
            return self._send(200, INDEX_HTML.encode("utf-8"), "text/html; charset=utf-8")

        if path == "/api/stats":
            body = _json_bytes(stats())
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/videos_admin":
            qs = parse_qs(u.query)
            q = (qs.get("q", [""])[0] or "").strip()
            status = (qs.get("status", [""])[0] or "").strip() or None
            limit = int((qs.get("limit", ["200"])[0] or "200"))
            body = _json_bytes({"items": list_videos_admin(q=q, limit=limit, status=status)})
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

        if path == "/api/reconcile":
            qs = parse_qs(u.query)
            pending_min = int((qs.get("pending_min", ["60"])[0] or "60"))
            limit = int((qs.get("limit", ["50"])[0] or "50"))
            body = _json_bytes(
                {
                    "unmatched_txs": list_unmatched_txs(limit=limit),
                    "pending_orders": list_pending_orders_older(minutes=pending_min, limit=limit),
                }
            )
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/categories":
            body = _json_bytes({"items": list_categories(visible_only=False)})
            return self._send(200, body, "application/json; charset=utf-8")
        
        if path == "/api/banners":
            body = _json_bytes({"items": list_banners(active_only=False)})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/export/users.csv":
            qs = parse_qs(u.query)
            q = (qs.get("q", [""])[0] or "").strip()
            limit = int((qs.get("limit", ["2000"])[0] or "2000"))
            rows = list_users(q=q, limit=limit)
            cols = ["telegram_id", "username", "paid_until", "total_received", "wallet_addr", "first_source", "last_source", "last_source_at", "is_blacklisted", "is_whitelisted", "note", "created_at"]
            body = _csv_bytes(rows, cols)
            return self._send_csv("users.csv", body)

        if path == "/api/export/coupons.csv":
            qs = parse_qs(u.query)
            limit = int((qs.get("limit", ["20000"])[0] or "20000"))
            rows = list_coupons(limit=limit)
            cols = ["code", "kind", "value", "plan_codes", "max_uses", "used_count", "expires_at", "active", "created_at"]
            body = _csv_bytes(rows, cols)
            return self._send_csv("coupons.csv", body)

        if path == "/api/export/access_codes.csv":
            qs = parse_qs(u.query)
            limit = int((qs.get("limit", ["20000"])[0] or "20000"))
            rows = list_access_codes(limit=limit)
            cols = ["code", "days", "plan_code", "max_uses", "used_count", "expires_at", "note", "created_by", "created_at", "last_used_at"]
            body = _csv_bytes(rows, cols)
            return self._send_csv("access_codes.csv", body)

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

        if path.startswith("/api/local_uploader/"):
            qs = parse_qs(u.query)
            if not self._require_local_uploader(qs):
                return
            try:
                n = int(self.headers.get("Content-Length") or "0")
            except Exception:
                n = 0
            raw = self.rfile.read(n) if n > 0 else b"{}"
            try:
                data = json.loads(raw.decode("utf-8", errors="ignore") or "{}")
            except Exception:
                data = {}

            if path == "/api/local_uploader/update":
                local_uploader_update(
                    video_id=int(data.get("video_id") or 0),
                    upload_status=(data.get("upload_status") or "").strip(),
                    channel_id=data.get("channel_id"),
                    message_id=data.get("message_id"),
                    free_channel_id=data.get("free_channel_id"),
                    free_message_id=data.get("free_message_id"),
                    file_id=data.get("file_id"),
                    error=data.get("error"),
                )
                return self._send(200, _json_bytes({"ok": True}), "application/json; charset=utf-8")
            return self._send(404, b"Not Found", "text/plain")

        if not self._require_auth():
            return
        if getattr(self, "_auth_role", "") != "admin":
            return self._send(403, b"readonly", "text/plain; charset=utf-8")
        if not ADMIN_WEB_ACTIONS_ENABLE:
            return self._send(403, b"actions disabled", "text/plain; charset=utf-8")

        if path == "/api/upload_image":
            ct = (self.headers.get("Content-Type") or "").strip()
            if "multipart/form-data" not in ct:
                return self._send(400, b"bad content-type", "text/plain; charset=utf-8")
            try:
                form = cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ={"REQUEST_METHOD": "POST", "CONTENT_TYPE": ct})
            except Exception:
                return self._send(400, b"bad multipart", "text/plain; charset=utf-8")
            ff = form["file"] if "file" in form else None
            if not ff or not getattr(ff, "file", None):
                return self._send(400, b"missing file", "text/plain; charset=utf-8")
            filename = (getattr(ff, "filename", "") or "").strip()
            ctype = (getattr(ff, "type", "") or "").strip().lower()
            folder = (form.getfirst("folder", "") or "").strip().lower()
            if folder not in ("banners", "covers", "misc"):
                folder = "misc"
            ext = ""
            fn_lower = filename.lower()
            for e in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
                if fn_lower.endswith(e):
                    ext = e
                    break
            if not ext:
                if ctype == "image/jpeg":
                    ext = ".jpg"
                elif ctype == "image/png":
                    ext = ".png"
                elif ctype == "image/webp":
                    ext = ".webp"
                elif ctype == "image/gif":
                    ext = ".gif"
            if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
                return self._send(400, b"bad file type", "text/plain; charset=utf-8")
            base = _uploads_dir()
            rel = f"{folder}/{datetime.utcnow().strftime('%Y%m%d')}"
            out_dir = _safe_join(base, rel)
            if not out_dir:
                return self._send(400, b"bad path", "text/plain; charset=utf-8")
            os.makedirs(out_dir, exist_ok=True)
            out_name = secrets.token_hex(16) + ext
            out_full = os.path.join(out_dir, out_name)
            try:
                with open(out_full, "wb") as f:
                    f.write(ff.file.read())
            except Exception:
                return self._send(500, b"write failed", "text/plain; charset=utf-8")
            url = _public_base_url(self) + "/uploads/" + rel + "/" + out_name
            return self._send(200, _json_bytes({"ok": True, "url": url}), "application/json; charset=utf-8")

        try:
            n = int(self.headers.get("Content-Length") or "0")
        except Exception:
            n = 0
        raw = self.rfile.read(n) if n > 0 else b"{}"
        try:
            data = json.loads(raw.decode("utf-8", errors="ignore") or "{}")
        except Exception:
            data = {}

        actor = getattr(self, "_auth_user", ADMIN_WEB_USER)
        if path == "/api/categories_upsert":
            upsert_category(
                id=int(data.get("id") or 0),
                name=data.get("name"),
                is_visible=bool(data.get("is_visible")),
                sort_order=int(data.get("sort_order") or 0)
            )
            return self._send(200, _json_bytes({"ok": True}), "application/json; charset=utf-8")
            
        if path == "/api/banners_upsert":
            upsert_banner(
                id=int(data.get("id") or 0),
                image_url=data.get("image_url"),
                link_url=data.get("link_url"),
                is_active=bool(data.get("is_active")),
                sort_order=int(data.get("sort_order") or 0)
            )
            return self._send(200, _json_bytes({"ok": True}), "application/json; charset=utf-8")

        if path == "/api/categories_delete":
            delete_category(int(data.get("id") or 0))
            return self._send(200, _json_bytes({"ok": True}), "application/json; charset=utf-8")

        if path == "/api/banners_delete":
            delete_banner(int(data.get("id") or 0))
            return self._send(200, _json_bytes({"ok": True}), "application/json; charset=utf-8")

        if path == "/api/video_create":
            sdt = (data.get("published_at") or "").strip()
            dt = None
            if sdt:
                try:
                    dt = datetime.fromisoformat(sdt.replace("Z", "+00:00"))
                    dt = dt.replace(tzinfo=None)
                except Exception:
                    dt = None
            vid = admin_create_video_job(
                local_filename=(data.get("local_filename") or "").strip(),
                caption=(data.get("caption") or "").strip(),
                cover_url=(data.get("cover_url") or "").strip(),
                tags=(data.get("tags") or "").strip(),
                category_id=int(data.get("category_id") or 0),
                sort_order=int(data.get("sort_order") or 0),
                is_published=bool(data.get("is_published")),
                published_at=dt,
            )
            return self._send(200, _json_bytes({"ok": True, "id": vid}), "application/json; charset=utf-8")

        if path == "/api/video_update":
            sdt = (data.get("published_at") or "").strip()
            dt = None
            if sdt:
                try:
                    dt = datetime.fromisoformat(sdt.replace("Z", "+00:00"))
                    dt = dt.replace(tzinfo=None)
                except Exception:
                    dt = None
            admin_update_video_meta(
                video_id=int(data.get("id") or 0),
                caption=(data.get("caption") or "").strip(),
                cover_url=(data.get("cover_url") or "").strip(),
                tags=(data.get("tags") or "").strip(),
                category_id=int(data.get("category_id") or 0),
                sort_order=int(data.get("sort_order") or 0),
                is_published=bool(data.get("is_published")),
                published_at=dt,
                local_filename=(data.get("local_filename") or "").strip(),
            )
            return self._send(200, _json_bytes({"ok": True}), "application/json; charset=utf-8")

        if path == "/api/video_publish":
            admin_set_video_publish(int(data.get("id") or 0), bool(data.get("is_published")))
            return self._send(200, _json_bytes({"ok": True}), "application/json; charset=utf-8")

        if path == "/api/video_sort":
            admin_set_video_sort(int(data.get("id") or 0), int(data.get("sort_order") or 0))
            return self._send(200, _json_bytes({"ok": True}), "application/json; charset=utf-8")

        if path == "/api/user_extend":
            telegram_id = int(str(data.get("telegram_id") or "0"))
            days = int(data.get("days") or 0)
            note = (data.get("note") or "").strip()
            paid_until = user_extend_days(telegram_id, days, actor=actor, note=note, ip=getattr(self, "_auth_ip", self.client_address[0]))
            body = _json_bytes({"ok": True, "paid_until": paid_until})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/user_resend_invite":
            telegram_id = int(str(data.get("telegram_id") or "0"))
            note = (data.get("note") or "").strip()
            ok, err = resend_invite_link(telegram_id, actor=actor, note=note, ip=getattr(self, "_auth_ip", self.client_address[0]))
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

        if path == "/api/coupons_generate":
            created = generate_coupons(
                kind=(data.get("kind") or "").strip(),
                value=(data.get("value") or "").strip(),
                plan_codes=(data.get("plan_codes") or "").strip(),
                max_uses=(data.get("max_uses") or ""),
                expire_hours=(data.get("expire_hours") or ""),
                count=int(data.get("count") or 0),
                prefix=(data.get("prefix") or "").strip(),
            )
            body = _json_bytes({"ok": True, "created": created})
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

        if path == "/api/access_codes_generate":
            created = generate_access_codes(
                days=int(data.get("days") or 0),
                max_uses=int(data.get("max_uses") or 1),
                expire_hours=(data.get("expire_hours") or ""),
                note=(data.get("note") or "").strip(),
                count=int(data.get("count") or 0),
                prefix=(data.get("prefix") or "").strip(),
                created_by=actor,
            )
            body = _json_bytes({"ok": True, "created": created})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/broadcast_create":
            bid = create_broadcast_job_v2(
                segment=(data.get("segment") or "").strip(),
                source=(data.get("source") or "").strip(),
                text=(data.get("text") or ""),
                parse_mode=(data.get("parse_mode") or "").strip(),
                media_type=(data.get("media_type") or "").strip(),
                media=(data.get("media") or "").strip(),
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
            res = user_toggle_flags(telegram_id, toggle=toggle, note=note, actor=actor, ip=getattr(self, "_auth_ip", self.client_address[0]))
            body = _json_bytes({"ok": True, **res})
            return self._send(200, body, "application/json; charset=utf-8")

        if path == "/api/reconcile_assign":
            tx_id = (data.get("tx_id") or "").strip()
            order_id = int(data.get("order_id") or 0)
            note = (data.get("note") or "").strip()
            ok, err = reconcile_assign(tx_id=tx_id, order_id=order_id, actor=actor, note=note, ip=getattr(self, "_auth_ip", self.client_address[0]))
            code = 200 if ok else 400
            body = _json_bytes({"ok": ok, "error": err})
            return self._send(code, body, "application/json; charset=utf-8")

        if path == "/api/reconcile_retry_tx":
            tx_id = (data.get("tx_id") or "").strip()
            note = (data.get("note") or "").strip()
            ok, err = reconcile_retry_tx(tx_id=tx_id, actor=actor, note=note, ip=getattr(self, "_auth_ip", self.client_address[0]))
            code = 200 if ok else 400
            body = _json_bytes({"ok": ok, "error": err})
            return self._send(code, body, "application/json; charset=utf-8")

        self._send(404, b"not found", "text/plain; charset=utf-8")

    def _serve_static_webapp(self, path: str):
        if ".." in path:
            return self._forbidden("invalid path")
        rel_path = path[len("/webapp/"):]
        if not rel_path or rel_path.endswith("/"):
            rel_path += "index.html"
        base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "webapp")
        full_path = os.path.join(base_dir, rel_path)
        if not os.path.abspath(full_path).startswith(base_dir):
            return self._forbidden("invalid path")
        if not os.path.exists(full_path) or not os.path.isfile(full_path):
            return self._send(404, b"Not Found", "text/plain")
        ctype, _ = mimetypes.guess_type(full_path)
        if not ctype:
            ctype = "application/octet-stream"
        try:
            with open(full_path, "rb") as f:
                content = f.read()
            self._send(200, content, ctype)
        except Exception as e:
            self._send(500, str(e).encode("utf-8"), "text/plain")

    def log_message(self, format, *args):
        return


def list_videos(q: str, page: int, limit: int, category_id: int = 0, sort: str = "latest", include_unpublished: bool = False) -> dict:
    page = max(1, page)
    limit = max(1, min(limit, 100))
    offset = (page - 1) * limit
    
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        where_clauses = ["1=1"]
        params = []

        if q:
            where_clauses.append("caption LIKE %s")
            params.append(f"%{q}%")

        if category_id > 0:
            where_clauses.append("category_id = %s")
            params.append(category_id)

        if not include_unpublished:
            where_clauses.append("upload_status='done'")
            where_clauses.append("is_published=1")
            where_clauses.append("(published_at IS NULL OR published_at <= UTC_TIMESTAMP())")
            
        where_str = " AND ".join(where_clauses)
        
        order_by = "sort_order DESC, published_at DESC, created_at DESC"
        if sort == "hot":
            order_by = "view_count DESC, sort_order DESC, published_at DESC, created_at DESC"
        
        # Get count
        cur.execute(f"SELECT COUNT(*) as cnt FROM videos WHERE {where_str}", tuple(params))
        total = cur.fetchone()["cnt"]
        
        # Get items
        sql = f"""
            SELECT id, channel_id, message_id, caption, view_count, category_id, free_channel_id, free_message_id, is_hot, created_at 
            FROM videos 
            WHERE {where_str} 
            ORDER BY {order_by} 
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        cur.execute(sql, tuple(params))
        items = cur.fetchall()
        
        return {
            "items": items,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


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


def _read_heartbeat(path: str) -> dict:
    p = (path or "").strip()
    if not p:
        return {"ok": False, "age_sec": None}
    try:
        if not os.path.exists(p):
            return {"ok": False, "age_sec": None}
        age = int(time.time() - os.path.getmtime(p))
        data = None
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        except Exception:
            data = {}
        return {"ok": age < 600, "age_sec": age, "ts": data.get("ts"), "iso": data.get("iso")}
    except Exception:
        return {"ok": False, "age_sec": None}


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
    hb_app = _read_heartbeat(HEARTBEAT_FILE)
    hb_userbot = _read_heartbeat(HEARTBEAT_USERBOT_FILE)

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
        "hb_app_age_sec": hb_app.get("age_sec"),
        "hb_userbot_age_sec": hb_userbot.get("age_sec"),
    }


def list_users(q: str, limit: int) -> list[dict]:
    limit = max(1, min(int(limit), 200))
    if not q:
        sql = """
            SELECT
              u.telegram_id,
              u.username,
              u.created_at,
              u.paid_until,
              (u.paid_until IS NOT NULL AND u.paid_until > UTC_TIMESTAMP()) AS is_member,
              (SELECT MIN(o.created_at) FROM orders o WHERE o.telegram_id=u.telegram_id AND o.status='success') AS member_since,
              u.last_plan,
              u.total_received,
              u.wallet_addr
            FROM users u
            ORDER BY u.created_at DESC
            LIMIT %s
        """
        return _q_all(sql, (limit,))

    if q.isdigit():
        sql = """
            SELECT
              u.telegram_id,
              u.username,
              u.created_at,
              u.paid_until,
              (u.paid_until IS NOT NULL AND u.paid_until > UTC_TIMESTAMP()) AS is_member,
              (SELECT MIN(o.created_at) FROM orders o WHERE o.telegram_id=u.telegram_id AND o.status='success') AS member_since,
              u.last_plan,
              u.total_received,
              u.wallet_addr
            FROM users u
            WHERE u.telegram_id=%s
            LIMIT %s
        """
        return _q_all(sql, (int(q), limit))

    sql = """
        SELECT
          u.telegram_id,
          u.username,
          u.created_at,
          u.paid_until,
          (u.paid_until IS NOT NULL AND u.paid_until > UTC_TIMESTAMP()) AS is_member,
          (SELECT MIN(o.created_at) FROM orders o WHERE o.telegram_id=u.telegram_id AND o.status='success') AS member_since,
          u.last_plan,
          u.total_received,
          u.wallet_addr
        FROM users u
        WHERE u.username LIKE %s
        ORDER BY u.created_at DESC
        LIMIT %s
    """
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


def list_unmatched_txs(limit: int) -> list[dict]:
    limit = max(1, min(int(limit), 2000))
    sql = """
        SELECT tx_id, telegram_id, addr, from_addr, amount, status, plan_code, credited_amount, processed_at, block_time, created_at
        FROM usdt_txs
        WHERE (telegram_id IS NULL OR telegram_id=0)
          AND status IN ('seen','unmatched')
        ORDER BY created_at DESC
        LIMIT %s
    """
    return _q_all(sql, (limit,))


def list_pending_orders_older(minutes: int, limit: int) -> list[dict]:
    minutes = max(1, min(int(minutes), 43200))
    limit = max(1, min(int(limit), 2000))
    sql = """
        SELECT id, telegram_id, addr, amount, base_amount, discount_amount, coupon_code, plan_code, status, created_at
        FROM orders
        WHERE status='pending'
          AND created_at <= (UTC_TIMESTAMP() - INTERVAL %s MINUTE)
        ORDER BY created_at ASC
        LIMIT %s
    """
    return _q_all(sql, (minutes, limit))


def _plan_by_code(code: str) -> dict | None:
    c = (code or "").strip()
    for p in PLANS:
        if (p.get("code") or "").strip() == c:
            return p
    return None


def reconcile_assign(tx_id: str, order_id: int, actor: str, note: str, ip: str) -> tuple[bool, str]:
    tx_id = (tx_id or "").strip()
    order_id = int(order_id or 0)
    if not tx_id or order_id <= 0:
        return False, "bad params"

    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM usdt_txs WHERE tx_id=%s LIMIT 1", (tx_id,))
        tx = cur.fetchone()
        cur.execute("SELECT * FROM orders WHERE id=%s LIMIT 1", (order_id,))
        order = cur.fetchone()
        if not tx or not order:
            return False, "tx/order not found"
        if (order.get("status") or "") != "pending":
            return False, "order not pending"
        if (tx.get("status") or "") not in ("seen", "unmatched"):
            return False, "tx status not eligible"
        if tx.get("telegram_id"):
            return False, "tx already assigned"

        addr = str(order.get("addr") or "")
        if addr and str(tx.get("addr") or "") and str(tx.get("addr") or "") != addr:
            return False, "addr mismatch"

        try:
            tx_amount = Decimal(str(tx.get("amount") or "0"))
            order_amount = Decimal(str(order.get("amount") or "0"))
        except Exception:
            return False, "bad amount"
        eps = Decimal(str(AMOUNT_EPS))
        if abs(tx_amount - order_amount) > eps:
            return False, f"amount mismatch tx={tx_amount} order={order_amount}"

        telegram_id = int(order.get("telegram_id") or 0)
        user = get_user(telegram_id) if telegram_id else None
        if not user:
            return False, "user not found"

        plan_code = str(order.get("plan_code") or "").strip()
        plan = _plan_by_code(plan_code)
        if not plan:
            return False, "plan not found"

        now = datetime.utcnow()
        old_paid_until = user.get("paid_until")
        new_paid_until = compute_new_paid_until(old_paid_until, [plan])
        total_old = Decimal(str(user.get("total_received") or 0))
        total_new = total_old + tx_amount

        cur2 = conn.cursor()
        cur2.execute(
            """
            UPDATE users
            SET paid_until=%s, total_received=%s, last_plan=%s
            WHERE telegram_id=%s
            """,
            (new_paid_until, str(total_new), plan_code, telegram_id),
        )
        cur2.execute("UPDATE orders SET status='success', tx_id=%s WHERE id=%s", (tx_id, order_id))
        cur2.execute(
            """
            UPDATE usdt_txs
            SET status='processed', telegram_id=%s, plan_code=%s, credited_amount=%s, processed_at=%s
            WHERE tx_id=%s
            """,
            (telegram_id, plan_code, str(Decimal(str(plan.get("price")))), now, tx_id),
        )
        cur2.execute("SELECT coupon_code, coupon_used FROM orders WHERE id=%s LIMIT 1", (order_id,))
        row = cur2.fetchone()
        if row:
            coupon_code = row[0]
            coupon_used = int(row[1] or 0)
            if coupon_code and coupon_used == 0:
                cur2.execute("UPDATE orders SET coupon_used=1 WHERE id=%s", (order_id,))
                cur2.execute("UPDATE coupons SET used_count=used_count+1 WHERE code=%s", (coupon_code,))
        conn.commit()
        try:
            _audit(actor, "reconcile_assign", telegram_id, {"tx_id": tx_id, "order_id": order_id, "note": note, "ip": ip})
        except Exception:
            pass
        return True, ""
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, f"{type(e).__name__}: {e}"
    finally:
        try:
            conn.close()
        except Exception:
            pass


def reconcile_retry_tx(tx_id: str, actor: str, note: str, ip: str) -> tuple[bool, str]:
    tx_id = (tx_id or "").strip()
    if not tx_id:
        return False, "tx_id required"
    row = _q_all("SELECT * FROM usdt_txs WHERE tx_id=%s LIMIT 1", (tx_id,))
    if not row:
        return False, "tx not found"
    tx = row[0]
    if tx.get("telegram_id"):
        return False, "tx already assigned"
    if (tx.get("status") or "") not in ("seen", "unmatched"):
        return False, "tx status not eligible"
    addr = str(tx.get("addr") or "")
    try:
        amount = Decimal(str(tx.get("amount") or "0"))
    except Exception:
        return False, "bad amount"
    eps = Decimal(str(AMOUNT_EPS))
    from core.models import match_pending_order_by_amount_v2
    from config import MATCH_ORDER_LOOKBACK_HOURS, MATCH_ORDER_PREFER_RECENT
    tx_time = tx.get("block_time") or tx.get("created_at") or None
    order = match_pending_order_by_amount_v2(
        addr=addr,
        amount=amount,
        eps=eps,
        tx_time=tx_time,
        lookback_hours=int(MATCH_ORDER_LOOKBACK_HOURS),
        prefer_recent=bool(MATCH_ORDER_PREFER_RECENT),
    )
    if not order:
        try:
            _audit(actor, "reconcile_retry_tx_no_match", None, {"tx_id": tx_id, "note": note, "ip": ip})
        except Exception:
            pass
        return False, "no pending order match"
    return reconcile_assign(tx_id=tx_id, order_id=int(order.get("id") or 0), actor=actor, note=note or "retry_tx", ip=ip)


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


def generate_coupons(kind: str, value: str, plan_codes: str, max_uses, expire_hours, count: int, prefix: str) -> int:
    kind = (kind or "").strip().lower()
    if kind not in ("percent", "fixed"):
        raise ValueError("kind must be percent/fixed")
    try:
        v = float(value)
    except Exception:
        raise ValueError("bad value")
    count = int(count or 0)
    if count <= 0 or count > 500:
        raise ValueError("bad count")
    prefix = (prefix or "").strip().upper()
    if prefix and not prefix.endswith("_"):
        prefix = prefix + "_"
    plan_codes = (plan_codes or "").strip() or None
    max_uses_v = None
    if str(max_uses).strip():
        max_uses_v = int(max_uses)
    expires_at = None
    if str(expire_hours).strip():
        expires_at = _utc_now() + timedelta(hours=int(expire_hours))
    created = 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        for _ in range(count * 3):
            if created >= count:
                break
            code = prefix + secrets.token_hex(4).upper()
            cur.execute(
                """
                INSERT IGNORE INTO coupons (code, kind, value, plan_codes, max_uses, used_count, expires_at, active)
                VALUES (%s,%s,%s,%s,%s,0,%s,1)
                """,
                (code, kind, str(v), plan_codes, max_uses_v, expires_at),
            )
            if cur.rowcount == 1:
                created += 1
        conn.commit()
        cur.close()
        return created
    finally:
        try:
            conn.close()
        except Exception:
            pass

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


def generate_access_codes(days: int, max_uses: int, expire_hours, note: str, count: int, prefix: str, created_by: str) -> int:
    days = int(days or 0)
    if days == 0:
        raise ValueError("days required")
    max_uses = max(1, int(max_uses or 1))
    count = int(count or 0)
    if count <= 0 or count > 500:
        raise ValueError("bad count")
    prefix = (prefix or "").strip().upper()
    if prefix and not prefix.endswith("_"):
        prefix = prefix + "_"
    expires_at = None
    if str(expire_hours).strip():
        expires_at = _utc_now() + timedelta(hours=int(expire_hours))
    note = (note or "").strip() or None
    created_by = (created_by or "").strip() or None
    created = 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        for _ in range(count * 3):
            if created >= count:
                break
            code = prefix + secrets.token_urlsafe(9).replace("-", "").replace("_", "").upper()
            cur.execute(
                """
                INSERT IGNORE INTO access_codes (code, days, plan_code, max_uses, used_count, expires_at, note, created_by)
                VALUES (%s,%s,NULL,%s,0,%s,%s,%s)
                """,
                (code, days, max_uses, expires_at, (note or "")[:256] if note else None, created_by),
            )
            if cur.rowcount == 1:
                created += 1
        conn.commit()
        cur.close()
        return created
    finally:
        try:
            conn.close()
        except Exception:
            pass

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
    media_type: str,
    media: str,
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
    media_type = (media_type or "").strip().lower() or None
    media = (media or "").strip() or None
    if media_type and media_type not in ("photo", "video"):
        raise ValueError("bad media_type")
    if media_type and not media:
        raise ValueError("media required")
    if media and not media_type:
        raise ValueError("media_type required")
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
            INSERT INTO broadcast_jobs (segment, source, text, parse_mode, media_type, media, button_text, button_url, disable_preview, status, created_by)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'created',%s)
            """,
            (segment, source, text, parse_mode, media_type, media, button_text, button_url, disable_preview, created_by),
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
        SELECT id, segment, source, status, media_type, media, parse_mode, button_url, created_by, created_at, started_at, finished_at, total, success, failed
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
    media_type = (job.get("media_type") or "").strip().lower() or None
    media = (job.get("media") or "").strip() or None
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
        payload = {"chat_id": str(uid)}
        method = "sendMessage"
        if media_type and media:
            if media_type == "photo":
                method = "sendPhoto"
                payload["photo"] = media
            elif media_type == "video":
                method = "sendVideo"
                payload["video"] = media
                payload["supports_streaming"] = "true"
            payload["caption"] = text
            if parse_mode:
                payload["parse_mode"] = parse_mode
        else:
            payload["text"] = text
            if parse_mode:
                payload["parse_mode"] = parse_mode
            if disable_preview:
                payload["disable_web_page_preview"] = "true"
        if button_url:
            bt = button_text or "打开"
            payload["reply_markup"] = json.dumps({"inline_keyboard": [[{"text": bt, "url": button_url}]]}, ensure_ascii=False)
        ok, res = _bot_api(method, payload)
        if not ok and isinstance(res, dict) and res.get("retry_after"):
            try:
                time.sleep(float(res.get("retry_after") or 1))
            except Exception:
                time.sleep(1.0)
            ok, res = _bot_api(method, payload)
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
    viewed_tags = []
    try:
        viewed_tags = user_viewed_tags(telegram_id)
    except Exception:
        viewed_tags = []
    return {"user": user, "orders": orders, "txs": txs, "viewed_tags": viewed_tags}


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

