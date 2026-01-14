import os
import sys


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def _parse_env(text: str) -> tuple[list[str], dict[str, str], dict[str, int]]:
    keys_in_order: list[str] = []
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
        keys_in_order.append(k)
        values[k] = v
        counts[k] = int(counts.get(k, 0)) + 1
    return keys_in_order, values, counts


def _print_list(title: str, items: list[str]):
    print(f"\n== {title} ({len(items)}) ==")
    for x in items:
        print(x)


def main():
    root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    env_path = os.path.join(root, ".env")
    example_path = os.path.join(root, ".env.example")
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        env_path = os.path.abspath(sys.argv[1].strip())
    if len(sys.argv) >= 3 and sys.argv[2].strip():
        example_path = os.path.abspath(sys.argv[2].strip())
    if not os.path.exists(example_path):
        print(f"missing: {example_path}")
        return 2
    if not os.path.exists(env_path):
        print(f"missing: {env_path}")
        return 2

    env_keys, _, env_counts = _parse_env(_read_text(env_path))
    ex_keys, _, ex_counts = _parse_env(_read_text(example_path))
    env_set = set(env_keys)
    ex_set = set(ex_keys)

    missing = sorted(list(ex_set - env_set))
    extra = sorted(list(env_set - ex_set))
    env_dupes = sorted([k for k, c in env_counts.items() if int(c) > 1])
    ex_dupes = sorted([k for k, c in ex_counts.items() if int(c) > 1])

    _print_list("missing in .env (present in .env.example)", missing)
    _print_list("extra in .env (not in .env.example)", extra)
    _print_list("duplicate keys in .env", env_dupes)
    _print_list("duplicate keys in .env.example", ex_dupes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

