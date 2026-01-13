from config import BOT_USERNAME


def highlight_caption() -> str:
    return (
        "🎬 试看 30 秒高能片段\n\n"
        "想看完整版？点击进入 Bot 开通会员：\n"
        f"https://t.me/{BOT_USERNAME}?start=from_highlight\n\n"
        "📌 会员价格 & 时长\n"
        "⚡️ 月费会员：9.99 USDT（30天）\n"
        "⚡️ 季度会员：19.99 USDT（90天）\n"
        "⚡️ 年费会员：79.99 USDT（365天）\n"
    )


def compose_free_caption(original_caption: str, max_len: int = 1024) -> str:
    base = highlight_caption()
    orig = (original_caption or "").strip()
    if not orig:
        return base[:max_len]

    sep = "\n\n"
    keep = max_len - len(base) - len(sep)
    if keep <= 0:
        return base[:max_len]
    if len(orig) > keep:
        ell = "…"
        head = int(keep * 0.7)
        tail = keep - head - len(ell)
        if tail <= 0:
            orig = orig[:keep].rstrip()
        else:
            left = orig[:head].rstrip()
            right = orig[-tail:].lstrip()
            orig = (left + ell + right).strip()
    return (orig + sep + base)[:max_len]

