import argparse
import os
import sys

from social.publisher import SocialConfig, SocialPublisher


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--text", required=True)
    ap.add_argument("--link", default="")
    ap.add_argument("--image", default="")
    ap.add_argument("--media", action="append", default=[])
    args = ap.parse_args(argv)

    cfg = SocialConfig(
        facebook_page_id=os.getenv("FACEBOOK_PAGE_ID", "").strip(),
        facebook_access_token=os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN", "").strip(),
        x_bearer_token=os.getenv("X_BEARER_TOKEN", "").strip(),
        x_api_key=os.getenv("X_API_KEY", "").strip(),
        x_api_key_secret=os.getenv("X_API_KEY_SECRET", "").strip(),
        x_access_token=os.getenv("X_ACCESS_TOKEN", "").strip(),
        x_access_token_secret=os.getenv("X_ACCESS_TOKEN_SECRET", "").strip(),
    )
    pub = SocialPublisher(cfg)
    media_paths = [p for p in (args.media or []) if (p or "").strip()]
    if (args.image or "").strip():
        media_paths.append(args.image.strip())
    results = pub.publish_all(text=args.text, link=(args.link or None), image_path=(args.image or None), media_paths=media_paths or None)
    if not results:
        print("no platforms configured (FACEBOOK_PAGE_ID/FACEBOOK_PAGE_ACCESS_TOKEN or X_BEARER_TOKEN)")
        return 2
    ok = True
    for r in results:
        if r.ok:
            print(f"{r.platform}: ok post_id={r.post_id or ''}")
        else:
            ok = False
            print(f"{r.platform}: failed err={r.error}", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

