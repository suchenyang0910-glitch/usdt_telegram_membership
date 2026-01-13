import argparse
import base64
import json
import random
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED


def _auth_header(user: str, password: str) -> str:
    raw = f"{user}:{password}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _req(url: str, method: str, headers: dict[str, str], body: bytes | None, timeout: float) -> tuple[int, int, str]:
    req = urllib.request.Request(url, data=body, method=method)
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            return resp.status, len(data), ""
    except urllib.error.HTTPError as e:
        try:
            payload = e.read()
        except Exception:
            payload = b""
        return int(getattr(e, "code", 0) or 0), len(payload), f"HTTPError: {e}"
    except Exception as e:
        return 0, 0, f"{type(e).__name__}: {e}"


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = int(round((len(s) - 1) * p))
    k = max(0, min(len(s) - 1, k))
    return float(s[k])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="e.g. http://127.0.0.1:8080")
    ap.add_argument("--user", required=True)
    ap.add_argument("--pass", dest="password", required=True)
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--concurrency", type=int, default=30)
    ap.add_argument("--timeout", type=float, default=8.0)
    ap.add_argument("--mix", default="health,stats,users,detail", help="comma list: health,stats,users,detail")
    ap.add_argument("--telegram-id", type=int, default=0, help="optional fixed telegram_id for detail")
    ap.add_argument("--q", default="", help="optional username query")
    args = ap.parse_args()

    base = (args.base or "").strip()
    if base.startswith("`") and base.endswith("`") and len(base) >= 2:
        base = base[1:-1].strip()
    base = base.rstrip("/")
    if not (base.startswith("http://") or base.startswith("https://")):
        raise SystemExit(f"bad --base: {base!r} (expect http://host:port)")
    headers = {"Authorization": _auth_header(args.user, args.password)}
    mix = [x.strip() for x in (args.mix or "").split(",") if x.strip()]
    if not mix:
        mix = ["health", "stats", "users", "detail"]

    def make_task_url(kind: str) -> tuple[str, str, bytes | None, dict[str, str]]:
        if kind == "health":
            return f"{base}/health", "GET", None, {}
        if kind == "stats":
            return f"{base}/api/stats", "GET", None, {}
        if kind == "users":
            q = args.q or ("user" + str(random.randint(1, 9999)))
            qs = urllib.parse.urlencode({"q": q, "limit": "20"})
            return f"{base}/api/users?{qs}", "GET", None, {}
        if kind == "detail":
            tid = args.telegram_id or random.randint(10000, 99999)
            qs = urllib.parse.urlencode({"telegram_id": str(tid)})
            return f"{base}/api/user_detail?{qs}", "GET", None, {}
        return f"{base}/health", "GET", None, {}

    end_at = time.time() + max(1, int(args.duration))
    latencies: list[float] = []
    codes: dict[int, int] = {}
    errs: dict[str, int] = {}
    bytes_total = 0
    total = 0

    def worker_once() -> tuple[float, int, int, str]:
        kind = random.choice(mix)
        url, method, body, extra_headers = make_task_url(kind)
        h = dict(headers)
        for k, v in extra_headers.items():
            h[k] = v
        t0 = time.time()
        code, size, err = _req(url, method, h, body, args.timeout)
        dt = time.time() - t0
        return dt, code, size, err

    with ThreadPoolExecutor(max_workers=max(1, int(args.concurrency))) as ex:
        inflight = set()
        for _ in range(max(1, int(args.concurrency))):
            inflight.add(ex.submit(worker_once))

        while inflight:
            done, _pending = wait(inflight, timeout=1, return_when=FIRST_COMPLETED)
            if not done:
                if time.time() >= end_at:
                    break
                continue
            for f in list(done):
                inflight.remove(f)
                try:
                    dt, code, size, err = f.result()
                except Exception as e:
                    dt, code, size, err = 0.0, 0, 0, f"{type(e).__name__}: {e}"
                total += 1
                latencies.append(float(dt))
                bytes_total += int(size)
                codes[int(code)] = int(codes.get(int(code), 0)) + 1
                if err:
                    errs[err] = int(errs.get(err, 0)) + 1

                if time.time() < end_at:
                    inflight.add(ex.submit(worker_once))

        for f in list(inflight):
            try:
                f.cancel()
            except Exception:
                pass

    ok = sum(v for k, v in codes.items() if 200 <= k < 300)
    fail = total - ok
    p50 = _percentile(latencies, 0.50)
    p95 = _percentile(latencies, 0.95)
    p99 = _percentile(latencies, 0.99)
    avg = statistics.mean(latencies) if latencies else 0.0
    rps = (total / max(1.0, float(args.duration)))

    out = {
        "base": base,
        "duration_sec": int(args.duration),
        "concurrency": int(args.concurrency),
        "requests": int(total),
        "ok": int(ok),
        "fail": int(fail),
        "rps": round(float(rps), 2),
        "avg_ms": round(float(avg) * 1000.0, 2),
        "p50_ms": round(float(p50) * 1000.0, 2),
        "p95_ms": round(float(p95) * 1000.0, 2),
        "p99_ms": round(float(p99) * 1000.0, 2),
        "bytes_total": int(bytes_total),
        "codes": dict(sorted(codes.items(), key=lambda x: x[0])),
        "top_errors": sorted(errs.items(), key=lambda x: x[1], reverse=True)[:5],
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

