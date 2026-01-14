import os
import sys


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def _parse_env_lines(text: str) -> tuple[dict[str, str], dict[str, int]]:
    values: dict[str, str] = {}
    counts: dict[str, int] = {}
    for raw in (text or "").splitlines():
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
        values[k] = v2
        counts[k] = int(counts.get(k, 0)) + 1
    return values, counts


def _write_text(path: str, text: str):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def _build_cleaned(example_text: str, env_values: dict[str, str]) -> tuple[str, list[str], list[str]]:
    used: set[str] = set()
    missing: list[str] = []
    out_lines: list[str] = []

    for raw in (example_text or "").splitlines():
        if not raw.strip():
            out_lines.append("")
            continue
        if raw.lstrip().startswith("#"):
            out_lines.append(raw.rstrip())
            continue
        if "=" not in raw:
            out_lines.append(raw.rstrip())
            continue
        k, v = raw.split("=", 1)
        key = (k or "").strip()
        if not key:
            out_lines.append(raw.rstrip())
            continue
        used.add(key)
        if key in env_values:
            out_lines.append(f"{key}={env_values[key]}")
        else:
            missing.append(key)
            out_lines.append(f"{key}={(v or '').strip()}")

    extra = sorted([k for k in env_values.keys() if k not in used])
    if extra:
        out_lines.append("")
        out_lines.append("# extra keys from .env")
        for k in extra:
            out_lines.append(f"{k}={env_values[k]}")
    out_lines.append("")
    return "\n".join(out_lines), missing, extra


def main():
    root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    env_path = os.path.join(root, ".env")
    example_path = os.path.join(root, ".env.example")
    out_path = os.path.join(root, ".env.cleaned")
    missing_path = os.path.join(root, ".env.missing.keys")
    dupe_path = os.path.join(root, ".env.duplicate.keys")

    if len(sys.argv) >= 2 and sys.argv[1].strip():
        env_path = os.path.abspath(sys.argv[1].strip())
    if len(sys.argv) >= 3 and sys.argv[2].strip():
        example_path = os.path.abspath(sys.argv[2].strip())
    if len(sys.argv) >= 4 and sys.argv[3].strip():
        out_path = os.path.abspath(sys.argv[3].strip())

    if not os.path.exists(env_path):
        print(f"missing: {env_path}")
        return 2
    if not os.path.exists(example_path):
        print(f"missing: {example_path}")
        return 2

    env_values, env_counts = _parse_env_lines(_read_text(env_path))
    example_text = _read_text(example_path)
    cleaned, missing, extra = _build_cleaned(example_text, env_values)

    dupes = sorted([k for k, c in env_counts.items() if int(c) > 1])
    _write_text(out_path, cleaned)
    _write_text(missing_path, "\n".join(missing) + ("\n" if missing else ""))
    _write_text(dupe_path, "\n".join(dupes) + ("\n" if dupes else ""))

    print("written:")
    print(f"- {out_path}")
    print(f"- {missing_path}")
    print(f"- {dupe_path}")
    print("")
    print("summary:")
    print(f"- missing keys filled from example: {len(missing)}")
    print(f"- extra keys appended: {len(extra)}")
    print(f"- duplicate keys detected in input env: {len(dupes)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

