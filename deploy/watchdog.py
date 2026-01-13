import os
import subprocess
import time
import urllib.parse
import urllib.request


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _env_int(name: str, default: int) -> int:
    v = _env(name, "")
    if not v:
        return default
    return int(v)


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


def _run(cmd: list[str], cwd: str | None = None) -> tuple[int, str]:
    res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False)
    out = (res.stdout or "") + (("\n" + res.stderr) if res.stderr else "")
    return res.returncode, out.strip()


def _systemd_check_and_fix(units: list[str]):
    print(f"[watchdog] mode=systemd units={units}")
    for unit in units:
        if not unit:
            continue
        rc, _ = _run(["systemctl", "is-active", unit])
        if rc == 0:
            print(f"[watchdog] {unit} active")
            continue
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


def _docker_check_and_fix(compose_dir: str, services: list[str]):
    print(f"[watchdog] mode=docker compose_dir={compose_dir} services={services}")
    base = ["docker", "compose"]
    for svc in services:
        if not svc:
            continue
        rc, out = _run(base + ["ps", svc, "--status", "running", "--services"], cwd=compose_dir)
        running = (rc == 0) and (svc in (out.splitlines() if out else []))
        if running:
            print(f"[watchdog] {svc} running")
            continue
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
    if mode == "docker":
        compose_dir = _env("WATCHDOG_DOCKER_COMPOSE_DIR", "/opt/pvbot/usdt_telegram_membership/deploy")
        services = [x.strip() for x in _env("WATCHDOG_DOCKER_SERVICES", "app,mysql,userbot").split(",") if x.strip()]
        _docker_check_and_fix(compose_dir, services)
        return

    units = [x.strip() for x in _env("WATCHDOG_SYSTEMD_UNITS", "pvbot.service").split(",") if x.strip()]
    _systemd_check_and_fix(units)


if __name__ == "__main__":
    main()

