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

