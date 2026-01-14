import os
import subprocess
import time
import urllib.parse
import urllib.request
import json


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


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


def _systemd_check_and_fix(units: list[str], heartbeat_map: dict[str, str], project_dir: str, max_age_sec: int):
    print(f"[watchdog] mode=systemd units={units}")
    for unit in units:
        if not unit:
            continue
        rc, _ = _run(["systemctl", "is-active", unit])
        if rc == 0:
            hb_rel = heartbeat_map.get(unit, "")
            if hb_rel:
                hb = _abs_in_project(project_dir, hb_rel)
                ok_hb, reason = _check_heartbeat(hb, max_age_sec)
                if ok_hb:
                    print(f"[watchdog] {unit} active")
                    continue
                print(f"[watchdog] {unit} unhealthy -> restart ({reason})")
                _tg_send(f"[watchdog] {unit} unhealthy, restarting...\n{reason}")
            else:
                print(f"[watchdog] {unit} active")
                continue
        else:
            print(f"[watchdog] {unit} not active -> restart")
            _tg_send(f"[watchdog] {unit} not active, restarting...")
        rc2, out2 = _run(["systemctl", "restart", unit])
        time.sleep(2)
        rc3, _ = _run(["systemctl", "is-active", unit])
        if rc2 == 0 and rc3 == 0:
            print(f"[watchdog] {unit} restarted OK")
            _tg_send(f"[watchdog] {unit} restarted OK")
        else:
            print(f"[watchdog] {unit} restart FAILED: {out2[:200]}")
            _tg_send(f"[watchdog] {unit} restart FAILED\n{out2[:800]}")


def _docker_check_and_fix(compose_dir: str, services: list[str], heartbeat_map: dict[str, str], project_dir: str, max_age_sec: int):
    print(f"[watchdog] mode=docker compose_dir={compose_dir} services={services}")
    base = ["docker", "compose"]
    all_ok = True
    for svc in services:
        if not svc:
            continue
        rc, out = _run(base + ["ps", svc, "--status", "running", "--services"], cwd=compose_dir)
        running = (rc == 0) and (svc in (out.splitlines() if out else []))
        if running:
            hb_rel = heartbeat_map.get(svc, "")
            if hb_rel:
                hb = _abs_in_project(project_dir, hb_rel)
                ok_hb, reason = _check_heartbeat(hb, max_age_sec)
                if ok_hb:
                    print(f"[watchdog] {svc} running")
                    continue
                all_ok = False
                print(f"[watchdog] {svc} unhealthy -> up -d --force-recreate ({reason})")
                _tg_send(f"[watchdog] docker service {svc} unhealthy, restarting...\n{reason}")
            else:
                print(f"[watchdog] {svc} running")
                continue
        all_ok = False
        print(f"[watchdog] {svc} not running -> up -d --force-recreate")
        _tg_send(f"[watchdog] docker service {svc} not running, restarting...")
        rc2, out2 = _run(base + ["up", "-d", "--force-recreate", svc], cwd=compose_dir)
        time.sleep(2)
        rc3, out3 = _run(base + ["ps", svc, "--status", "running", "--services"], cwd=compose_dir)
        running2 = (rc3 == 0) and (svc in (out3.splitlines() if out3 else []))
        if rc2 == 0 and running2:
            print(f"[watchdog] {svc} restarted OK")
            _tg_send(f"[watchdog] docker service {svc} restarted OK")
        else:
            print(f"[watchdog] {svc} restart FAILED: {out2[:200]}")
            _tg_send(f"[watchdog] docker service {svc} restart FAILED\n{out2[:800]}")
            all_ok = False

    if all_ok:
        _maybe_notify_ok()


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
        services = [x.strip() for x in _env("WATCHDOG_DOCKER_SERVICES", "app,mysql,userbot").split(",") if x.strip()]
        _docker_check_and_fix(compose_dir, services, heartbeat_map, project_dir, max_age_sec)
        return

    units = [x.strip() for x in _env("WATCHDOG_SYSTEMD_UNITS", "pvbot.service").split(",") if x.strip()]
    _systemd_check_and_fix(units, heartbeat_map, project_dir, max_age_sec)


if __name__ == "__main__":
    main()

