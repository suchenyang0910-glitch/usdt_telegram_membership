# bot/i18n.py
from typing import Dict

def normalize_lang(code: str) -> str:
    if not code:
        return "en"
    code = code.lower()
    if code.startswith("zh"):
        return "zh"
    if code.startswith("en"):
        return "en"
    if code.startswith("km") or code.startswith("kh"):
        return "km"
    if code.startswith("vi"):
        return "vi"
    return "en"


TEXTS: Dict[str, Dict[str, str]] = {
    # --- /start 欢迎 ---
    "welcome_title": {
        "zh": "欢迎来到 PV Premium 付费频道 🔥",
        "en": "Welcome to PV Premium Channel 🔥",
        "km": "សូមស្វាគមន៍មកកាន់ PV Premium Channel 🔥",
        "vi": "Chào mừng đến kênh PV Premium 🔥",
    },
    "welcome_body": {
        "zh": (
            "这里不是泛滥资源，而是 *高质量内容集中营*。\n\n"
            "你将获得：\n"
            "• 每周 100+ 条精选完整视频\n"
            "• 按主题分类的系列合集，节省你大量时间\n"
            "• 持续更新，历史内容长期保留，随时回看\n"
            "• 仅限会员访问，不会在公开频道出现\n"
        ),
        "en": (
            "This is not random content – it's a *curated premium library*.\n\n"
            "You get:\n"
            "• 100+ new full videos every week\n"
            "• Organized collections by theme to save your time\n"
            "• Continuous updates with long-term access to history\n"
            "• Members-only, not shared in public channels\n"
        ),
        "km": (
            "នេះមិនមែនជាមាតិកាចៃដន្យទេ ប៉ុន្តែជា *បណ្ណាល័យមាតិកាគុណភាពខ្ពស់*។\n\n"
            "អ្នកនឹងទទួលបាន៖\n"
            "• វីដេអូពេញលេញ 100+ រៀងរាល់សប្ដាហ៍\n"
            "• ចែកជាប្រភេទ/ប្រធានបទ ដើម្បីសន្សំម៉ោង\n"
            "• អាប់ដេតជាបន្តបន្ទាប់ និងអាចមើលថយក្រោយបាន\n"
            "• សម្រាប់សមាជិកបង់ប្រាក់ប៉ុណ្ណោះ មិនចែករំលែកសាធារណៈទេ\n"
        ),
        "vi": (
            "Đây không phải kho video rác, mà là *thư viện nội dung chọn lọc*.\n\n"
            "Bạn sẽ nhận được:\n"
            "• Hơn 100 video full mới mỗi tuần\n"
            "• Các bộ sưu tập theo chủ đề, tiết kiệm thời gian tìm kiếm\n"
            "• Cập nhật đều, nội dung cũ vẫn có thể xem lại\n"
            "• Chỉ dành cho thành viên, không phát tán công khai\n"
        ),
    },

    "plans_title": {
        "zh": "当前可选会员套餐：",
        "en": "Available membership plans:",
        "km": "កញ្ចប់សមាជិកដែលអាចជ្រើសរើស៖",
        "vi": "Các gói thành viên hiện có:",
    },
    "plan_line": {
        "zh": "{name}：{price} USDT / {days} 天",
        "en": "{name}: {price} USDT / {days} days",
        "km": "{name}: {price} USDT / {days} ថ្ងៃ",
        "vi": "{name}: {price} USDT / {days} ngày",
    },

    "current_status": {
        "zh": "当前会员有效期至：{until} (UTC)",
        "en": "Your membership is valid until: {until} (UTC)",
        "km": "សិទ្ធិសមាជិករបស់អ្នកមានរហូតដល់: {until} (UTC)",
        "vi": "Gói thành viên của bạn có hiệu lực đến: {until} (UTC)",
    },
    "no_membership": {
        "zh": "你目前还没有开通会员，可以随时充值开通。",
        "en": "You don't have an active membership yet. You can activate it anytime.",
        "km": "បច្ចុប្បន្នអ្នកមិនទាន់មានសមាជិកកម្មទេ អ្នកអាចបើកបានគ្រប់ពេល។",
        "vi": "Hiện tại bạn chưa có gói thành viên. Bạn có thể kích hoạt bất cứ lúc nào.",
    },

    "pay_instructions": {
        "zh": (
            "请使用 TRON（USDT-TRC20）向以下地址转账：\n"
            "`{addr}`\n\n"
            "系统每分钟自动检测到账，识别成功后将自动开通或续费你的频道访问权限，并私信你入群邀请链接。"
        ),
        "en": (
            "Please send *USDT-TRC20* to this address:\n"
            "`{addr}`\n\n"
            "The system checks payments every minute. Once detected, your access will be activated or extended automatically."
        ),
        "km": (
            "សូមផ្ទេរ *USDT-TRC20* ទៅអាសយដ្ឋាននេះ៖\n"
            "`{addr}`\n\n"
            "ប្រព័ន្ធនឹងពិនិត្យការទូទាត់រៀងរាល់១នាទី ហើយបើរកឃើញប្រាក់ចូល នឹងបើក ឬបន្តសិទ្ធិឱ្យអ្នកស្វ័យប្រវត្តិ។"
        ),
        "vi": (
            "Vui lòng chuyển *USDT-TRC20* tới địa chỉ sau:\n"
            "`{addr}`\n\n"
            "Hệ thống kiểm tra thanh toán mỗi phút. Khi nhận được, quyền truy cập của bạn sẽ được kích hoạt hoặc gia hạn tự động."
        ),
    },

    "pricing_block": {
        "zh": (
            "📌 会员价格 & 时长\n"
            "⚡️ 月费会员：9.99 USDT（30天）\n"
            "⚡️ 季度会员：19.99 USDT（90天）\n"
            "⚡️ 年费会员：79.99 USDT（365天）"
        ),
        "en": (
            "Membership prices & duration\n"
            "Monthly: 9.99 USDT (30 days)\n"
            "Quarter: 19.99 USDT (90 days)\n"
            "Yearly: 79.99 USDT (365 days)"
        ),
        "km": (
            "តម្លៃ និងរយៈពេលសមាជិកភាព\n"
            "Monthly: 9.99 USDT (30 days)\n"
            "Quarter: 19.99 USDT (90 days)\n"
            "Yearly: 79.99 USDT (365 days)"
        ),
        "vi": (
            "Giá & thời hạn gói thành viên\n"
            "Tháng: 9.99 USDT (30 ngày)\n"
            "Quý: 19.99 USDT (90 ngày)\n"
            "Năm: 79.99 USDT (365 ngày)"
        ),
    },
    "contact_hint": {
        "zh": "如有问题，你可以随时私信 @{bot} 咨询。",
        "en": "If you have any questions, feel free to DM @{bot}.",
        "km": "បើមានសំណួរ អ្នកអាចផ្ញើសារ​មក @{bot} បានគ្រប់ពេល។",
        "vi": "Nếu có bất kỳ câu hỏi nào, hãy inbox @{bot} để được hỗ trợ.",
    },

    "plans_command_title": {
        "zh": "会员价格与时长：",
        "en": "Membership prices & duration:",
        "km": "តម្លៃ និងរយៈពេលសមាជិកភាព៖",
        "vi": "Giá & thời hạn gói thành viên:",
    },

    # --- 支付成功 / 到期 ---
    "success_payment": {
        "zh": (
            "✅ 已检测到你的充值：{amount} USDT\n"
            "系统已为你开通/续费会员至：{until}\n\n"
            "点击下面链接加入或重新加入付费频道：\n{link}"
        ),
        "en": (
            "✅ Payment received: {amount} USDT\n"
            "Your premium access is now valid until: {until}\n\n"
            "Tap the link below to join or rejoin the premium channel:\n{link}"
        ),
        "km": (
            "✅ បានរកឃើញការទូទាត់របស់អ្នក៖ {amount} USDT\n"
            "សិទ្ធិចូលឆានែលបង់ប្រាក់មានរហូតដល់៖ {until}\n\n"
            "ចុចតំណខាងក្រោមដើម្បីចូល ឬចូលម្តងទៀត៖\n{link}"
        ),
        "vi": (
            "✅ Đã nhận thanh toán: {amount} USDT\n"
            "Quyền truy cập trả phí của bạn có hiệu lực đến: {until}\n\n"
            "Nhấn vào link bên dưới để vào hoặc vào lại kênh premium:\n{link}"
        ),
    },

    "expired_notice": {
        "zh": (
            "⛔ 你的付费频道访问权限已到期，系统已将你移出完整视频频道。\n"
            "如果想恢复访问，可以随时通过 USDT 充值再次开通。"
        ),
        "en": (
            "⛔ Your premium access has expired and you’ve been removed from the full-content channel.\n"
            "You can top up with USDT anytime to restore access."
        ),
        "km": (
            "⛔ សិទ្ធិចូលឆានែលបង់ប្រាក់របស់អ្នកបានផុតកំណត់ ហើយប្រព័ន្ធបានដកអ្នកចេញពីឆានែលវីដេអូពេញលេញ។\n"
            "បើចង់ចូលម្តងទៀត អ្នកអាចបញ្ចូល USDT ដើម្បីបើកសិទ្ធិឡើងវិញ។"
        ),
        "vi": (
            "⛔ Quyền truy cập trả phí của bạn đã hết hạn và bạn đã được hệ thống đưa ra khỏi kênh full video.\n"
            "Nếu muốn vào lại, bạn có thể nạp USDT và kích hoạt lại bất cứ lúc nào."
        ),
    },

    "expiring_soon_notice": {
        "zh": (
            "⏳ 会员即将到期提醒\n\n"
            "你的会员将在 {days} 天内到期（{until}）。\n"
            "到期后系统会自动将你移出会员频道。\n\n"
            "需要续费：直接继续向你专属 USDT-TRC20 收款地址转账即可，然后系统会自动续期。"
        ),
        "en": (
            "⏳ Membership expiring soon\n\n"
            "Your membership will expire within {days} day(s) ({until}).\n"
            "After expiration, you will be removed from the premium channel.\n\n"
            "To renew, simply send USDT-TRC20 to your assigned address again and the system will extend automatically."
        ),
        "km": (
            "⏳ សមាជិកភាពជិតផុតកំណត់\n\n"
            "សមាជិកភាពរបស់អ្នកនឹងផុតកំណត់ក្នុងរយៈពេល {days} ថ្ងៃ ({until})។\n"
            "បន្ទាប់ពីផុតកំណត់ ប្រព័ន្ធនឹងដកអ្នកចេញពីឆានែលបង់ប្រាក់។\n\n"
            "ដើម្បីបន្ត សូមផ្ទេរ USDT-TRC20 ទៅអាសយដ្ឋានរបស់អ្នកម្តងទៀត ហើយប្រព័ន្ធនឹងបន្តស្វ័យប្រវត្តិ។"
        ),
        "vi": (
            "⏳ Sắp hết hạn\n\n"
            "Gói thành viên của bạn sẽ hết hạn trong {days} ngày ({until}).\n"
            "Sau khi hết hạn, bạn sẽ bị đưa ra khỏi kênh premium.\n\n"
            "Để gia hạn, chỉ cần chuyển USDT-TRC20 tới địa chỉ đã được cấp và hệ thống sẽ tự gia hạn."
        ),
    },

    # --- 邀请中心 / 裂变 ---
    "invite_panel_intro": {
        "zh": (
            "📢 这是你的专属邀请中心。\n\n"
            "每成功邀请 1 位完成付费的新用户，你将获得额外会员时长奖励。\n"
            "邀请越多，看得越久，成本越低。"
        ),
        "en": (
            "📢 This is your personal invite center.\n\n"
            "For every new paying user you invite, you earn extra membership days.\n"
            "The more you invite, the longer you watch almost for free."
        ),
        "km": (
            "📢 នេះគឺជាមជ្ឈមណ្ឌលអញ្ជើញផ្ទាល់ខ្លួនរបស់អ្នក។\n\n"
            "រាល់ការអញ្ជើញមិត្តថ្មីម្នាក់ឱ្យបង់ប្រាក់ អ្នកនឹងទទួលបានថ្ងៃសមាជិកបន្ថែម។\n"
            "អញ្ជើញ càng ច្រើន càng មើលបានយូរ។"
        ),
        "vi": (
            "📢 Đây là trung tâm giới thiệu cá nhân của bạn.\n\n"
            "Mỗi người dùng mới bạn mời và thanh toán thành công, bạn sẽ được thưởng thêm ngày thành viên.\n"
            "Mời càng nhiều, xem càng lâu với chi phí gần như bằng 0."
        ),
    },
    "invite_panel_stats": {
        "zh": "📊 当前数据：\n• 邀请人数：{count} 人\n• 累计获得奖励：{days} 天会员时长\n",
        "en": "📊 Your stats:\n• Invited users: {count}\n• Total bonus: {days} extra membership days\n",
        "km": "📊 ស្ថិតិរបស់អ្នក៖\n• ចំនួនមិត្តដែលបានអញ្ជើញ៖ {count}\n• សរុបថ្ងៃបន្ថែមដែលទទួលបាន៖ {days} ថ្ងៃ\n",
        "vi": "📊 Thống kê của bạn:\n• Số người đã mời: {count}\n• Tổng số ngày thưởng: {days} ngày thành viên\n",
    },
    "invite_panel_link_block": {
        "zh": "🔗 你的专属邀请链接：\n{link}\n\n📎 邀请码：{code}\n",
        "en": "🔗 Your personal invite link:\n{link}\n\n📎 Invite code: {code}\n",
        "km": "🔗 តំណអញ្ជើញផ្ទាល់ខ្លួនរបស់អ្នក៖\n{link}\n\n📎 កូដអញ្ជើញ៖ {code}\n",
        "vi": "🔗 Link giới thiệu riêng của bạn:\n{link}\n\n📎 Mã mời: {code}\n",
    },
    "invite_panel_copy_hint": {
        "zh": (
            "你可以复制下面这段文字发给好友 / 群：\n\n"
            "“我在看一个 Telegram 付费频道，内容更新很快、质量也不错。\n"
            "现在它有邀请奖励活动：每邀请 1 位完成付费，就送我 3 天会员时长。\n"
            "你也可以一起来看看，用我的链接注册：{link}”"
        ),
        "en": (
            "You can copy & share this text to friends or groups:\n\n"
            "\"I'm using a Telegram premium channel with fast updates and good-quality content.\n"
            "They now have an invite bonus: for every paying user I invite, I get 3 extra days of access.\n"
            "You can join too using my link: {link}\""
        ),
        "km": (
            "អ្នកអាចចម្លងអត្ថបទខាងក្រោមនេះទៅផ្ញើ给មិត្តភក្តិ ឬក្រុម៖\n\n"
            "“ខ្ញុំកំពុងប្រើឆានែល Telegram បង់ប្រាក់មួយ ដែលមានការអាប់ដេតលឿន និងមាតិកាគុណភាពល្អ។\n"
            "ឥឡូវមានកម្មវិធីរង្វាន់អញ្ជើញ៖ អញ្ជើញមិត្តបង់ប្រាក់ ១ នាក់ = បន្ថែមសិទ្ធិ ៣ ថ្ងៃ។\n"
            "អ្នកអាចចូលរួមតាមតំណរបស់ខ្ញុំ៖ {link}”"
        ),
        "vi": (
            "Bạn có thể copy đoạn này để gửi cho bạn bè / group:\n\n"
            "“Mình đang xem một kênh Telegram trả phí, nội dung cập nhật nhanh và khá chất lượng.\n"
            "Hiện họ có chương trình thưởng giới thiệu: mỗi người mình mời và thanh toán, mình được +3 ngày xem.\n"
            "Bạn có thể vào thử bằng link của mình: {link}”"
        ),
    },
    "invite_reward_message": {
        "zh": "🎉 你成功邀请用户 {uid} 完成首次付费，系统已奖励你 +{days} 天会员时长！",
        "en": "🎉 You successfully invited user {uid} for their first payment. You’ve been rewarded +{days} days of membership!",
        "km": "🎉 អ្នកបានអញ្ជើញអ្នកប្រើ {uid} បង់ប្រាក់ជាលើកដំបូងបានជោគជ័យ ប្រព័ន្ធបានផ្តល់រង្វាន់ +{days} ថ្ងៃសមាជិកភាព!",
        "vi": "🎉 Bạn đã mời thành công người dùng {uid} thanh toán lần đầu. Hệ thống đã thưởng cho bạn +{days} ngày thành viên!",
    },

    # --- 海报 ---
    "poster_title": {
        "zh": "PV 付费频道邀请卡",
        "en": "PV Premium Invite Card",
        "km": "កាតអញ្ជើញ PV Premium",
        "vi": "Thẻ mời kênh PV Premium",
    },
    "poster_slogan": {
        "zh": "邀请 1 位付费会员 = 奖励 3 天观看权限",
        "en": "Invite 1 paying user = +3 days premium access",
        "km": "អញ្ជើញមិត្តបង់ប្រាក់ ១ នាក់ = បន្ថែមសិទ្ធិ ៣ ថ្ងៃ",
        "vi": "Mời 1 người trả phí = +3 ngày xem premium",
    },
    "poster_line1": {
        "zh": "邀请码：{code}",
        "en": "Invite code: {code}",
        "km": "កូដអញ្ជើញ៖ {code}",
        "vi": "Mã mời: {code}",
    },
    "poster_line2": {
        "zh": "专属推广人：{user}",
        "en": "Referrer: {user}",
        "km": "អ្នកអញ្ជើញ៖ {user}",
        "vi": "Người giới thiệu: {user}",
    },
    "poster_line3": {
        "zh": "扫码或长按二维码，打开 Telegram 自动跳转。",
        "en": "Scan or long press the QR to open Telegram and join.",
        "km": "ស្កេន ឬចុចលើ QR ឲ្យយូរ ដើម្បីបើក Telegram និងចូលភ្ជាប់។",
        "vi": "Quét hoặc giữ lâu mã QR để mở Telegram và tham gia.",
    },
    "poster_footer": {
        "zh": "官方入口：t.me/{bot}",
        "en": "Official entry: t.me/{bot}",
        "km": "ច្រកផ្លូវផ្លូវការ៖ t.me/{bot}",
        "vi": "Cổng vào chính thức: t.me/{bot}",
    },
}

def t(lang_code: str, key: str, **kwargs) -> str:
    lang = normalize_lang(lang_code)
    text = TEXTS.get(key, {}).get(lang) or TEXTS.get(key, {}).get("en", "")
    if kwargs:
        try:
            text = text.format(**kwargs)
        except Exception:
            pass
    return text
