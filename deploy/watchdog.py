import os
import subprocess
import time
import socket
import urllib.parse
import urllib.request
import json


def _abs(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return p
    if os.path.isabs(p):
        return p
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return os.path.join(base, p)


def _load_json_file(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _resolve_cfg_path(p: str, default_filename: str) -> str:
    p = _abs(p)
    if not p:
        return p
    try:
        if os.path.isdir(p):
            return os.path.join(p, default_filename)
    except Exception:
        pass
    return p


_cfg_default_path = _resolve_cfg_path(os.getenv("APP_CONFIG_DEFAULT_FILE", "config/app_config.defaults.json"), "app_config.defaults.json")
_cfg_path = _resolve_cfg_path(os.getenv("APP_CONFIG_FILE", "config/app_config.json"), "app_config.json")
_CFG = {**_load_json_file(_cfg_default_path), **_load_json_file(_cfg_path)}

_ENV_ONLY_KEYS = {
    "BOT_TOKEN",
    "BOT_USERNAME",
    "PAID_CHANNEL_ID",
    "HIGHLIGHT_CHANNEL_ID",
    "FREE_CHANNEL_ID_1",
    "FREE_CHANNEL_ID_2",
    "FREE_CHANNEL_IDS",
    "TRONGRID_API_KEY",
    "MIN_TX_AGE_SEC",
    "PAYMENT_MODE",
    "RECEIVE_ADDRESS",
    "PAYMENT_SUFFIX_ENABLE",
    "PAYMENT_SUFFIX_MIN",
    "PAYMENT_SUFFIX_MAX",
    "USDT_ADDRESS_POOL",
    "DB_HOST",
    "DB_PORT",
    "DB_USER",
    "DB_PASS",
    "DB_NAME",
    "MYSQL_ROOT_PASSWORD",
    "MYSQL_DATABASE",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
}


def _env(name: str, default: str = "") -> str:
    if name in os.environ:
        return (os.getenv(name, default) or "").strip()
    if name in _ENV_ONLY_KEYS:
        return (default or "").strip()
    v = _CFG.get(name, default)
    return ("" if v is None else str(v)).strip()


def _env_int(name: str, default: int) -> int:
    v = _env(name, "")
    if not v:
        return default
    return int(v)


def _project_dir(mode: str) -> str:
    explicit = _env("WATCHDOG_PROJECT_DIR", "")
    if explicit:
        return explicit
    if mode == "docker":
        compose_dir = _env("WATCHDOG_DOCKER_COMPOSE_DIR", "/opt/pvbot/usdt_telegram_membership/deploy")
        return os.path.abspath(os.path.join(compose_dir, ".."))
    return _env("WATCHDOG_PROJECT_DIR", "/opt/pvbot/usdt_telegram_membership")


def _abs_in_project(project_dir: str, path: str) -> str:
    path = (path or "").strip()
    if not path:
        return path
    if os.path.isabs(path):
        return path
    return os.path.join(project_dir, path)


def _parse_heartbeat_map(raw: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in (raw or "").split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        k, v = part.split(":", 1)
        k = k.strip()
        v = v.strip()
        if k and v:
            out[k] = v
    return out


def _check_heartbeat(path: str, max_age_sec: int) -> tuple[bool, str]:
    try:
        if not os.path.exists(path):
            return False, f"heartbeat missing: {path}"
        age = float(time.time() - os.path.getmtime(path))
        if age > float(max_age_sec):
            return False, f"heartbeat stale age={int(age)}s path={path}"
        return True, ""
    except Exception as e:
        return False, f"heartbeat check error: {type(e).__name__}: {e}"


def _pick_chat_id() -> int | None:
    for key in ("WATCHDOG_CHAT_ID", "SUPPORT_GROUP_ID", "ADMIN_REPORT_CHAT_ID"):
        v = _env(key, "")
        if v:
            try:
                return int(v)
            except Exception:
                pass
    raw = _env("ADMIN_USER_IDS", "")
    if raw:
        first = raw.split(",")[0].strip()
        try:
            return int(first)
        except Exception:
            return None
    return None


def _tg_send(text: str):
    token = _env("BOT_TOKEN", "")
    chat_id = _pick_chat_id()
    if not token or chat_id is None:
        print("[watchdog] skip notify (missing BOT_TOKEN or chat id)")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": str(chat_id), "text": text}).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            resp.read()
    except Exception:
        return


def _state_path() -> str:
    return _env("WATCHDOG_STATE_FILE", "/tmp/pvbot_watchdog_state.json")


def _load_state() -> dict:
    p = _state_path()
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save_state(state: dict):
    p = _state_path()
    try:
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception:
        return


def _maybe_notify_ok():
    if _env("WATCHDOG_NOTIFY_OK", "0") != "1":
        return
    every_min = _env_int("WATCHDOG_NOTIFY_OK_EVERY_MIN", 360)
    now = int(time.time())
    st = _load_state()
    last = int(st.get("last_ok_ts") or 0)
    if last and now - last < every_min * 60:
        return
    _tg_send("[watchdog] all services running")
    st["last_ok_ts"] = now
    _save_state(st)


def _run(cmd: list[str], cwd: str | None = None) -> tuple[int, str]:
    res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False)
    out = (res.stdout or "") + (("\n" + res.stderr) if res.stderr else "")
    return res.returncode, out.strip()


def _now_ts() -> int:
    return int(time.time())


def _state_get_target(st: dict, key: str) -> dict:
    t = (st.get("targets") or {}).get(key)
    return t if isinstance(t, dict) else {}


def _state_set_target(st: dict, key: str, data: dict):
    targets = st.get("targets")
    if not isinstance(targets, dict):
        targets = {}
        st["targets"] = targets
    targets[key] = data


def _record_success(st: dict, key: str):
    t = _state_get_target(st, key)
    t["fail_count"] = 0
    t["last_ok_ts"] = _now_ts()
    t["circuit_until_ts"] = 0
    t["unhealthy_count"] = 0
    t["first_unhealthy_ts"] = 0
    _state_set_target(st, key, t)


def _record_failure(st: dict, key: str, reason: str):
    t = _state_get_target(st, key)
    t["fail_count"] = int(t.get("fail_count") or 0) + 1
    t["last_fail_ts"] = _now_ts()
    t["last_reason"] = (reason or "")[:800]
    t["unhealthy_count"] = int(t.get("unhealthy_count") or 0) + 1
    if not int(t.get("first_unhealthy_ts") or 0):
        t["first_unhealthy_ts"] = _now_ts()
    _state_set_target(st, key, t)


def _record_restart(st: dict, key: str):
    t = _state_get_target(st, key)
    hist = t.get("restart_ts") or []
    if not isinstance(hist, list):
        hist = []
    now = _now_ts()
    hist.append(now)
    window_min = _env_int("WATCHDOG_CIRCUIT_WINDOW_MIN", 60)
    cutoff = now - int(window_min) * 60
    hist = [int(x) for x in hist if int(x) >= cutoff][-200:]
    t["restart_ts"] = hist
    _state_set_target(st, key, t)


def _circuit_allows_restart(st: dict, key: str) -> tuple[bool, str]:
    t = _state_get_target(st, key)
    now = _now_ts()
    until = int(t.get("circuit_until_ts") or 0)
    if until and now < until:
        left = until - now
        return False, f"circuit open ({left}s left)"
    hist = t.get("restart_ts") or []
    if not isinstance(hist, list):
        hist = []
    max_n = _env_int("WATCHDOG_CIRCUIT_MAX_RESTARTS", 5)
    window_min = _env_int("WATCHDOG_CIRCUIT_WINDOW_MIN", 60)
    cutoff = now - int(window_min) * 60
    recent = [int(x) for x in hist if int(x) >= cutoff]
    if len(recent) < int(max_n):
        return True, ""
    open_min = _env_int("WATCHDOG_CIRCUIT_OPEN_MIN", 15)
    t["circuit_until_ts"] = now + int(open_min) * 60
    _state_set_target(st, key, t)
    return False, f"circuit opened for {open_min}min (restarts={len(recent)}/{max_n} in {window_min}min)"


def _unhealthy_allows_restart(st: dict, key: str) -> tuple[bool, str]:
    t = _state_get_target(st, key)
    grace = _env_int("WATCHDOG_HEARTBEAT_GRACE_SEC", 180)
    confirm = _env_int("WATCHDOG_UNHEALTHY_CONFIRM", 2)
    now = _now_ts()
    first = int(t.get("first_unhealthy_ts") or 0)
    if first and grace and (now - first) < int(grace):
        return False, f"within grace ({now-first}s/{grace}s)"
    unhealthy = int(t.get("unhealthy_count") or 0)
    if unhealthy < int(confirm):
        return False, f"unhealthy confirm ({unhealthy}/{confirm})"
    return True, ""


def _tail_lines(text: str, max_chars: int) -> str:
    s = (text or "").strip()
    if not s:
        return ""
    if len(s) <= max_chars:
        return s
    return s[-max_chars:]


def _systemd_tail(unit: str) -> str:
    n = _env_int("WATCHDOG_LOG_TAIL_LINES", 200)
    rc, out = _run(["journalctl", "-u", unit, "-n", str(n), "--no-pager", "-o", "cat"])
    if rc != 0:
        return ""
    return _tail_lines(out, _env_int("WATCHDOG_LOG_TAIL_MAX_CHARS", 6000))


def _docker_tail(compose_dir: str, svc: str) -> str:
    n = _env_int("WATCHDOG_LOG_TAIL_LINES", 200)
    rc, out = _run(["docker", "compose", "logs", "--no-color", "--tail", str(n), svc], cwd=compose_dir)
    if rc != 0:
        return ""
    return _tail_lines(out, _env_int("WATCHDOG_LOG_TAIL_MAX_CHARS", 6000))


def _docker_health_ok(compose_dir: str, svc: str) -> bool | None:
    rc, cid = _run(["docker", "compose", "ps", "-q", svc], cwd=compose_dir)
    cid = (cid or "").strip()
    if rc != 0 or not cid:
        return None
    rc2, status = _run(["docker", "inspect", "-f", "{{.State.Health.Status}}", cid])
    status = (status or "").strip().lower()
    if rc2 != 0 or not status or status in ("<no value>", "null"):
        return None
    return status == "healthy"


def _parse_checks(raw: str) -> list[str]:
    return [x.strip() for x in (raw or "").split(",") if x.strip()]


def _http_check(name: str, url: str) -> tuple[bool, str]:
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=8) as resp:
            code = int(getattr(resp, "status", 0) or 0)
            if 200 <= code < 300:
                return True, ""
            return False, f"http {name} bad status={code} url={url}"
    except Exception as e:
        return False, f"http {name} error {type(e).__name__}: {e}"


def _tcp_check(name: str, host: str, port: int) -> tuple[bool, str]:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        try:
            s.connect((host, int(port)))
        finally:
            try:
                s.close()
            except Exception:
                pass
        return True, ""
    except Exception as e:
        return False, f"tcp {name} error {type(e).__name__}: {e}"


def _do_external_checks() -> tuple[bool, str]:
    fails: list[str] = []
    for item in _parse_checks(_env("WATCHDOG_HTTP_CHECKS", "")):
        if ":" not in item:
            continue
        name, url = item.split(":", 1)
        ok, reason = _http_check(name.strip(), url.strip())
        if not ok:
            fails.append(reason)
    for item in _parse_checks(_env("WATCHDOG_TCP_CHECKS", "")):
        parts = item.split(":")
        if len(parts) < 3:
            continue
        name = parts[0].strip()
        host = parts[1].strip()
        try:
            port = int(parts[2])
        except Exception:
            continue
        ok, reason = _tcp_check(name, host, port)
        if not ok:
            fails.append(reason)
    if _env("WATCHDOG_TG_CHECK", "0") == "1":
        token = _env("BOT_TOKEN", "")
        if token:
            ok, reason = _http_check("tg", f"https://api.telegram.org/bot{token}/getMe")
            if not ok:
                fails.append(reason)
        else:
            fails.append("tg check enabled but BOT_TOKEN missing")
    if _env("WATCHDOG_TRONGRID_CHECK", "0") == "1":
        key = _env("TRONGRID_API_KEY", "")
        addr = _env("RECEIVE_ADDRESS", "")
        if addr:
            try:
                req = urllib.request.Request(f"https://api.trongrid.io/v1/accounts/{addr}", method="GET")
                if key:
                    req.add_header("TRON-PRO-API-KEY", key)
                with urllib.request.urlopen(req, timeout=8) as resp:
                    code = int(getattr(resp, "status", 0) or 0)
                    if not (200 <= code < 300):
                        fails.append(f"trongrid bad status={code}")
            except Exception as e:
                fails.append(f"trongrid error {type(e).__name__}: {e}")
        else:
            fails.append("trongrid check enabled but RECEIVE_ADDRESS missing")
    if fails:
        return False, "\n".join(fails)[:1200]
    return True, ""


def _systemd_check_and_fix(units: list[str], heartbeat_map: dict[str, str], project_dir: str, max_age_sec: int):
    print(f"[watchdog] mode=systemd units={units}")
    st = _load_state()
    for unit in units:
        if not unit:
            continue
        key = f"systemd:{unit}"
        rc, _ = _run(["systemctl", "is-active", unit])
        need_restart = False
        reason = ""
        if rc == 0:
            hb_rel = heartbeat_map.get(unit, "")
            if hb_rel:
                hb = _abs_in_project(project_dir, hb_rel)
                ok_hb, reason = _check_heartbeat(hb, max_age_sec)
                if ok_hb:
                    print(f"[watchdog] {unit} active")
                    _record_success(st, key)
                    continue
                need_restart = True
            else:
                print(f"[watchdog] {unit} active")
                _record_success(st, key)
                continue
        else:
            need_restart = True
            reason = "not active"

        if not need_restart:
            continue

        ok_restart, cb_reason = _circuit_allows_restart(st, key)
        if not ok_restart:
            _record_failure(st, key, cb_reason or reason)
            tail = _systemd_tail(unit)
            msg = f"[watchdog] {unit} {cb_reason or 'circuit open'}\nreason={reason}"
            if tail:
                msg = msg + "\n\n" + tail
            _tg_send(msg[:3500])
            continue

        _record_failure(st, key, reason)
        tail = _systemd_tail(unit)
        msg = f"[watchdog] {unit} unhealthy, restarting...\nreason={reason}"
        if tail and _env("WATCHDOG_INCLUDE_LOGS_ON", "fail") in ("always", "pre", "fail"):
            msg = msg + "\n\n" + tail
        _tg_send(msg[:3500])
        rc2, out2 = _run(["systemctl", "restart", unit])
        _record_restart(st, key)
        time.sleep(2)
        rc3, _ = _run(["systemctl", "is-active", unit])
        if rc2 == 0 and rc3 == 0:
            print(f"[watchdog] {unit} restarted OK")
            _record_success(st, key)
            _tg_send(f"[watchdog] {unit} restarted OK")
        else:
            print(f"[watchdog] {unit} restart FAILED: {out2[:200]}")
            tail2 = _systemd_tail(unit)
            msg2 = f"[watchdog] {unit} restart FAILED\n{out2[:800]}"
            if tail2:
                msg2 = msg2 + "\n\n" + tail2
            _tg_send(msg2[:3500])
    _save_state(st)


def _docker_check_and_fix(compose_dir: str, services: list[str], heartbeat_map: dict[str, str], project_dir: str, max_age_sec: int):
    print(f"[watchdog] mode=docker compose_dir={compose_dir} services={services}")
    base = ["docker", "compose"]
    all_ok = True
    st = _load_state()
    for svc in services:
        if not svc:
            continue
        key = f"docker:{svc}"
        rc, out = _run(base + ["ps", svc, "--status", "running", "--services"], cwd=compose_dir)
        running = (rc == 0) and (svc in (out.splitlines() if out else []))
        if running:
            hb_rel = heartbeat_map.get(svc, "")
            if hb_rel:
                hb = _abs_in_project(project_dir, hb_rel)
                ok_hb, reason = _check_heartbeat(hb, max_age_sec)
                if ok_hb:
                    print(f"[watchdog] {svc} running")
                    _record_success(st, key)
                    continue
                if _env("WATCHDOG_DOCKER_TRUST_HEALTHCHECK", "1") == "1":
                    ok_health = _docker_health_ok(compose_dir, svc)
                    if ok_health is True:
                        print(f"[watchdog] {svc} healthcheck=healthy (ignore heartbeat file)")
                        _record_success(st, key)
                        continue
                all_ok = False
                print(f"[watchdog] {svc} unhealthy -> up -d --force-recreate ({reason})")
                ok_restart, cb_reason = _circuit_allows_restart(st, key)
                if not ok_restart:
                    _record_failure(st, key, cb_reason or reason)
                    tail = _docker_tail(compose_dir, svc)
                    msg = f"[watchdog] docker service {svc} {cb_reason or 'circuit open'}\nreason={reason}"
                    if tail:
                        msg = msg + "\n\n" + tail
                    _tg_send(msg[:3500])
                    continue
                _record_failure(st, key, reason)
                ok_h, h_reason = _unhealthy_allows_restart(st, key)
                if not ok_h:
                    tail = _docker_tail(compose_dir, svc)
                    msg = f"[watchdog] docker service {svc} unhealthy (no restart)\nreason={reason}\n{h_reason}"
                    if tail:
                        msg = msg + "\n\n" + tail
                    _tg_send(msg[:3500])
                    continue
                tail = _docker_tail(compose_dir, svc)
                msg = f"[watchdog] docker service {svc} unhealthy, restarting...\n{reason}"
                if tail and _env("WATCHDOG_INCLUDE_LOGS_ON", "fail") in ("always", "pre", "fail"):
                    msg = msg + "\n\n" + tail
                _tg_send(msg[:3500])
            else:
                print(f"[watchdog] {svc} running")
                _record_success(st, key)
                continue
        all_ok = False
        print(f"[watchdog] {svc} not running -> up -d --force-recreate")
        ok_restart, cb_reason = _circuit_allows_restart(st, key)
        if not ok_restart:
            _record_failure(st, key, cb_reason or "not running")
            tail = _docker_tail(compose_dir, svc)
            msg = f"[watchdog] docker service {svc} {cb_reason or 'circuit open'}\nreason=not running"
            if tail:
                msg = msg + "\n\n" + tail
            _tg_send(msg[:3500])
            continue
        _record_failure(st, key, "not running")
        tail = _docker_tail(compose_dir, svc)
        msg = f"[watchdog] docker service {svc} not running, restarting..."
        if tail and _env("WATCHDOG_INCLUDE_LOGS_ON", "fail") in ("always", "pre", "fail"):
            msg = msg + "\n\n" + tail
        _tg_send(msg[:3500])
        up_cmd = base + ["up", "-d", "--force-recreate"]
        if _env("WATCHDOG_DOCKER_RESTART_BUILD", "0") == "1":
            up_cmd.append("--build")
        up_cmd.append(svc)
        rc2, out2 = _run(up_cmd, cwd=compose_dir)
        _record_restart(st, key)
        time.sleep(2)
        rc3, out3 = _run(base + ["ps", svc, "--status", "running", "--services"], cwd=compose_dir)
        running2 = (rc3 == 0) and (svc in (out3.splitlines() if out3 else []))
        if rc2 == 0 and running2:
            print(f"[watchdog] {svc} restarted OK")
            _record_success(st, key)
            _tg_send(f"[watchdog] docker service {svc} restarted OK")
        else:
            print(f"[watchdog] {svc} restart FAILED: {out2[:200]}")
            tail2 = _docker_tail(compose_dir, svc)
            msg2 = f"[watchdog] docker service {svc} restart FAILED\n{out2[:800]}"
            if tail2:
                msg2 = msg2 + "\n\n" + tail2
            _tg_send(msg2[:3500])
            all_ok = False

    if all_ok:
        ok_ext, ext_reason = _do_external_checks()
        if not ok_ext:
            _tg_send(f"[watchdog] external checks FAILED\n{ext_reason}"[:3500])
        else:
            _maybe_notify_ok()
    _save_state(st)


def _auto_mode() -> str:
    explicit = _env("WATCHDOG_MODE", "").lower()
    if explicit:
        return explicit

    compose_dir = _env("WATCHDOG_DOCKER_COMPOSE_DIR", "/opt/pvbot/usdt_telegram_membership/deploy")
    for fname in ("docker-compose.yml", "compose.yml"):
        if os.path.exists(os.path.join(compose_dir, fname)):
            return "docker"
    return "systemd"


def main():
    if _env("WATCHDOG_ENABLE", "1") != "1":
        print("[watchdog] WATCHDOG_ENABLE!=1, exit")
        return

    mode = _auto_mode()
    print(f"[watchdog] selected_mode={mode}")
    project_dir = _project_dir(mode)
    max_age_sec = _env_int("WATCHDOG_HEARTBEAT_MAX_AGE_SEC", 300)
    heartbeat_map = _parse_heartbeat_map(_env("WATCHDOG_HEARTBEAT_MAP", ""))
    if mode == "docker":
        compose_dir = _env("WATCHDOG_DOCKER_COMPOSE_DIR", "/opt/pvbot/usdt_telegram_membership/deploy")
        services = [x.strip() for x in _env("WATCHDOG_DOCKER_SERVICES", "app,mysql").split(",") if x.strip()]
        _docker_check_and_fix(compose_dir, services, heartbeat_map, project_dir, max_age_sec)
        return

    units = [x.strip() for x in _env("WATCHDOG_SYSTEMD_UNITS", "pvbot.service").split(",") if x.strip()]
    _systemd_check_and_fix(units, heartbeat_map, project_dir, max_age_sec)


if __name__ == "__main__":
    main()

