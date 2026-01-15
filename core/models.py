
# ------------ Mini App / CMS ------------

def update_video_free_link(paid_channel_id: int, paid_message_id: int, free_channel_id: int, free_message_id: int):
    conn = get_conn(); cur = conn.cursor()
    cur.execute(
        """
        UPDATE videos
        SET free_channel_id=%s, free_message_id=%s
        WHERE channel_id=%s AND message_id=%s
        """,
        (int(free_channel_id), int(free_message_id), int(paid_channel_id), int(paid_message_id)),
    )
    conn.commit()
    cur.close(); conn.close()

def list_categories(visible_only: bool = True) -> List[Dict]:
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    if visible_only:
        cur.execute("SELECT * FROM categories WHERE is_visible=1 ORDER BY sort_order ASC, id DESC")
    else:
        cur.execute("SELECT * FROM categories ORDER BY sort_order ASC, id DESC")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def upsert_category(id: int | None, name: str, is_visible: bool, sort_order: int):
    name = (name or "").strip()
    if not name: return
    conn = get_conn(); cur = conn.cursor()
    if id and id > 0:
        cur.execute("UPDATE categories SET name=%s, is_visible=%s, sort_order=%s WHERE id=%s", (name, 1 if is_visible else 0, sort_order, id))
    else:
        cur.execute("INSERT INTO categories (name, is_visible, sort_order) VALUES (%s, %s, %s)", (name, 1 if is_visible else 0, sort_order))
    conn.commit()
    cur.close(); conn.close()

def delete_category(id: int):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM categories WHERE id=%s", (id,))
    conn.commit()
    cur.close(); conn.close()

def list_banners(active_only: bool = True) -> List[Dict]:
    conn = get_conn(); cur = conn.cursor(dictionary=True)
    if active_only:
        cur.execute("SELECT * FROM banners WHERE is_active=1 ORDER BY sort_order ASC, id DESC")
    else:
        cur.execute("SELECT * FROM banners ORDER BY sort_order ASC, id DESC")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows

def upsert_banner(id: int | None, image_url: str, link_url: str, is_active: bool, sort_order: int):
    image_url = (image_url or "").strip()
    if not image_url: return
    conn = get_conn(); cur = conn.cursor()
    if id and id > 0:
        cur.execute("UPDATE banners SET image_url=%s, link_url=%s, is_active=%s, sort_order=%s WHERE id=%s", 
            (image_url, link_url, 1 if is_active else 0, sort_order, id))
    else:
        cur.execute("INSERT INTO banners (image_url, link_url, is_active, sort_order) VALUES (%s, %s, %s, %s)",
            (image_url, link_url, 1 if is_active else 0, sort_order))
    conn.commit()
    cur.close(); conn.close()

def delete_banner(id: int):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM banners WHERE id=%s", (id,))
    conn.commit()
    cur.close(); conn.close()

def set_video_category(video_id: int, category_id: int):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE videos SET category_id=%s WHERE id=%s", (category_id, video_id))
    conn.commit()
    cur.close(); conn.close()
