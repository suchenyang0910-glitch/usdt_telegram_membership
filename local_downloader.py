import json
import os
import time
import urllib.request
from datetime import datetime, timedelta

_LOCAL_ENV_LOADED = False


def _maybe_load_local_env():
    global _LOCAL_ENV_LOADED
    if _LOCAL_ENV_LOADED:
        return
    base = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base, "local_userbot.env")
    if not os.path.exists(env_path):
        _LOCAL_ENV_LOADED = True
        return
    try:
        with open(env_path, "r", encoding="utf-8-sig") as f:
            for line in f:
                s = (line or "").strip()
                if not s or s.startswith("#"):
                    continue
                if "=" not in s:
                    continue
                k, v = s.split("=", 1)
                k = (k or "").strip().lstrip("\ufeff")
                if not k:
                    continue
                if os.getenv(k, "").strip():
                    continue
                os.environ[k] = (v or "").strip()
    except Exception:
        _LOCAL_ENV_LOADED = True
        return
    _LOCAL_ENV_LOADED = True


_maybe_load_local_env()


def _token() -> str:
    return (os.getenv("LOCAL_UPLOADER_TOKEN", "") or "").strip()


def _monitor_chat_id() -> int | None:
    v = (os.getenv("LOCAL_DOWNLOADER_MONITOR_CHAT", "") or os.getenv("LOCAL_USERBOT_MONITOR_CHAT", "") or "").strip()
    if not v:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _bot_token() -> str:
    return (os.getenv("BOT_TOKEN", "") or "").strip()


def _notify_text() -> str:
    return (os.getenv("LOCAL_DOWNLOADER_NOTIFY", "") or "1").strip()


def _notify_enabled() -> bool:
    return _notify_text() == "1"


def _tg_send_bot(text: str) -> bool:
    token = _bot_token()
    chat_id = _monitor_chat_id()
    if not token or chat_id is None:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        return True
    except Exception:
        return False


def _notify_done(job_id: int, filename: str, file_size: int):
    if not _notify_enabled():
        return
    chat_id = _monitor_chat_id()
    if chat_id is None:
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"✅ 下载完成\njob_id={job_id}\n文件：{filename}\n大小：{file_size} bytes\n时间：{ts}"
    _tg_send_bot(msg)


def _base_url() -> str:
    return (os.getenv("LOCAL_DOWNLOADER_BASE_URL", "") or os.getenv("LOCAL_USERBOT_BASE_URL", "") or "").strip().rstrip("/")


def _work_dir() -> str:
    return (os.getenv("LOCAL_DOWNLOADER_DIR", "") or os.getenv("LOCAL_USERBOT_ROOT", "") or "").strip() or "."


def _poll_sec() -> float:
    try:
        return max(1.0, float(os.getenv("LOCAL_DOWNLOADER_POLL_SEC", "10") or "10"))
    except Exception:
        return 10.0


def _chunk_size() -> int:
    try:
        return max(64 * 1024, int(os.getenv("LOCAL_DOWNLOADER_CHUNK_SIZE", str(1024 * 1024)) or str(1024 * 1024)))
    except Exception:
        return 1024 * 1024


def _http_json(method: str, url: str, headers: dict[str, str], body: dict | None = None) -> dict:
    data = None
    h = dict(headers or {})
    if body is not None:
        raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        data = raw
        h["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=h, method=method)
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    obj = json.loads(raw or "{}")
    return obj if isinstance(obj, dict) else {}


def _next_hour_utc(now: datetime | None = None) -> datetime:
    now = now or datetime.utcnow()
    nxt = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    return nxt


def _safe_filename(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return ""
    name = name.replace("\\", "/").split("/")[-1]
    name = name.replace("..", "_")
    return name[:180]


def _update_progress(base_url: str, job_id: int, status: str, progress: int | None, file_size: int | None, filename: str | None, error: str | None = None):
    _http_json(
        "POST",
        base_url + "/api/local_uploader/download_update",
        {"X-Local-Uploader-Token": _token()},
        {"job_id": int(job_id), "status": status, "progress": progress, "file_size": file_size, "filename": filename, "error": error},
    )


def _download_stream(url: str, dst_path: str, job_id: int, base_url: str, filename: str):
    os.makedirs(os.path.dirname(dst_path) or ".", exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "pvbot-local-downloader/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        total = int(resp.headers.get("Content-Length") or "0")
        done = 0
        next_tick = _next_hour_utc()
        _update_progress(base_url, job_id, "downloading", 0, total if total > 0 else None, filename, None)
        with open(dst_path, "wb") as f:
            while True:
                chunk = resp.read(_chunk_size())
                if not chunk:
                    break
                f.write(chunk)
                done += len(chunk)
                now = datetime.utcnow()
                if now >= next_tick:
                    if total > 0:
                        pct = int(min(99, (done * 100) // max(1, total)))
                    else:
                        pct = 0
                    _update_progress(base_url, job_id, "downloading", pct, total if total > 0 else done, filename, None)
                    next_tick = _next_hour_utc(now)
        if total > 0:
            _update_progress(base_url, job_id, "done", 100, total, filename, None)
        else:
            _update_progress(base_url, job_id, "done", 100, done, filename, None)
        try:
            real_size = int(os.path.getsize(dst_path))
        except Exception:
            real_size = int(total or done or 0)
        _notify_done(job_id, filename, real_size)


def main():
    base_url = _base_url()
    if not base_url:
        raise SystemExit("LOCAL_DOWNLOADER_BASE_URL missing")
    if not _token():
        raise SystemExit("LOCAL_UPLOADER_TOKEN missing")

    print(f"[local_downloader] start base_url={base_url} work_dir={_work_dir()}")
    last_idle = 0.0
    while True:
        try:
            data = _http_json(
                "GET",
                base_url + "/api/local_uploader/download_claim",
                {"X-Local-Uploader-Token": _token()},
                None,
            )
            job = (data.get("job") or None) if isinstance(data, dict) else None
        except Exception:
            job = None

        if not job:
            now = time.time()
            if now - last_idle >= 60:
                print("[local_downloader] idle (no pending download jobs)")
                last_idle = now
            time.sleep(_poll_sec())
            continue

        job_id = int(job.get("id") or 0)
        source_url = (job.get("source_url") or "").strip()
        filename = _safe_filename(job.get("filename") or "")
        if not filename:
            filename = f"download_{job_id}.bin"

        if job_id <= 0 or not source_url:
            try:
                _update_progress(base_url, job_id, "failed", 0, 0, filename, "bad job params")
            except Exception:
                pass
            time.sleep(1)
            continue

        dst_path = os.path.join(_work_dir(), filename)
        try:
            _download_stream(source_url, dst_path, job_id, base_url, filename)
        except Exception as e:
            try:
                _update_progress(base_url, job_id, "failed", 0, 0, filename, f"{type(e).__name__}: {e}")
            except Exception:
                pass
        time.sleep(1)


if __name__ == "__main__":
    main()

