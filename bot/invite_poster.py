# bot/invite_poster.py
import os
from io import BytesIO

import qrcode
from PIL import Image, ImageDraw, ImageFont

from config import BOT_USERNAME
from core.utils import b58encode
from bot.i18n import t, normalize_lang

POSTER_WIDTH = 1080
POSTER_HEIGHT = 1920
DEFAULT_FONTS = [
    os.getenv("POSTER_FONT_PATH", ""),
    r"C:\Windows\Fonts\msyh.ttc",
    r"C:\Windows\Fonts\msyh.ttf",
    r"C:\Windows\Fonts\arial.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
]


def _pick_font_path() -> str | None:
    for p in DEFAULT_FONTS:
        if p and os.path.exists(p):
            return p
    return None

def generate_invite_link(telegram_id: int) -> (str, str):
    code = b58encode(telegram_id)
    link = f"https://t.me/{BOT_USERNAME}?start=ref_{code}"
    return code, link

def generate_invite_poster(telegram_id: int, username: str | None, lang_code: str = "en") -> BytesIO:
    lang = normalize_lang(lang_code)
    code, link = generate_invite_link(telegram_id)

    img = Image.new("RGB", (POSTER_WIDTH, POSTER_HEIGHT), (10, 10, 14))
    draw = ImageDraw.Draw(img)

    font_path = _pick_font_path()
    if font_path:
        try:
            title_font = ImageFont.truetype(font_path, 64)
            big_font = ImageFont.truetype(font_path, 48)
            small_font = ImageFont.truetype(font_path, 32)
        except Exception:
            title_font = big_font = small_font = ImageFont.load_default()
    else:
        title_font = big_font = small_font = ImageFont.load_default()

    title_text = t(lang, "poster_title")
    tw, th = draw.textsize(title_text, font=title_font)
    draw.text(((POSTER_WIDTH - tw) / 2, 120), title_text, fill=(255, 255, 255), font=title_font)

    slogan = t(lang, "poster_slogan")
    sw, sh = draw.textsize(slogan, font=big_font)
    draw.text(((POSTER_WIDTH - sw) / 2, 260), slogan, fill=(230, 230, 230), font=big_font)

    # QR
    qr = qrcode.QRCode(
        version=2,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=10,
        border=2,
    )
    qr.add_data(link)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    qr_size = 520
    qr_img = qr_img.resize((qr_size, qr_size), Image.LANCZOS)
    qr_x = (POSTER_WIDTH - qr_size) // 2
    qr_y = 420
    img.paste(qr_img, (qr_x, qr_y))

    # 文本
    user_display = username or str(telegram_id)
    line1 = t(lang, "poster_line1", code=code)
    line2 = t(lang, "poster_line2", user=user_display)
    line3 = t(lang, "poster_line3")

    base_y = qr_y + qr_size + 80
    for i, text in enumerate([line1, line2, line3]):
        lw, lh = draw.textsize(text, font=small_font)
        draw.text(((POSTER_WIDTH - lw) / 2, base_y + i * 46),
                  text, fill=(220, 220, 220), font=small_font)

    footer = t(lang, "poster_footer", bot=BOT_USERNAME)
    fw, fh = draw.textsize(footer, font=small_font)
    draw.text(((POSTER_WIDTH - fw) / 2, POSTER_HEIGHT - fh - 80),
              footer, fill=(120, 120, 120), font=small_font)

    buf = BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf
