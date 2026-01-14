import os
import sys


def _read_lines(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read().splitlines()


def _parse(lines: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = (k or "").strip()
        if not k:
            continue
        v2 = (v or "").strip()
        if v2.startswith("=") and line.startswith(k + "=="):
            v2 = v2[1:].lstrip()
        out[k] = v2
    return out


def _set_line(lines: list[str], key: str, value: str) -> tuple[list[str], bool]:
    updated = False
    out: list[str] = []
    found = False
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            out.append(raw)
            continue
        k, _ = line.split("=", 1)
        k = (k or "").strip()
        if k != key:
            out.append(raw)
            continue
        if not found:
            out.append(f"{key}={value}")
            found = True
            updated = True
        else:
            updated = True
            continue
    if not found:
        out.append(f"{key}={value}")
        updated = True
    return out, updated


def _needs_sync(v: str) -> bool:
    s = (v or "").strip()
    return (not s) or (s == "REPLACE_ME") or (s == "Txxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")


def main():
    root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    env_path = os.path.join(root, ".env")
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        env_path = os.path.abspath(sys.argv[1].strip())
    if not os.path.exists(env_path):
        print(f"missing: {env_path}")
        return 2

    lines = _read_lines(env_path)
    vals = _parse(lines)

    db_user = (vals.get("DB_USER") or "").strip()
    db_pass = (vals.get("DB_PASS") or "").strip()
    db_name = (vals.get("DB_NAME") or "").strip()

    changed_keys: list[str] = []
    if db_user and _needs_sync(vals.get("MYSQL_USER", "")):
        lines, _ = _set_line(lines, "MYSQL_USER", db_user)
        changed_keys.append("MYSQL_USER")
    if db_pass and _needs_sync(vals.get("MYSQL_PASSWORD", "")):
        lines, _ = _set_line(lines, "MYSQL_PASSWORD", db_pass)
        changed_keys.append("MYSQL_PASSWORD")
    if db_name and _needs_sync(vals.get("MYSQL_DATABASE", "")):
        lines, _ = _set_line(lines, "MYSQL_DATABASE", db_name)
        changed_keys.append("MYSQL_DATABASE")
    if db_pass and _needs_sync(vals.get("MYSQL_ROOT_PASSWORD", "")):
        lines, _ = _set_line(lines, "MYSQL_ROOT_PASSWORD", db_pass)
        changed_keys.append("MYSQL_ROOT_PASSWORD")

    if changed_keys:
        with open(env_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines).rstrip() + "\n")
        print("updated keys:")
        for k in changed_keys:
            print(k)
    else:
        print("no changes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

