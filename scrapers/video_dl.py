import os
import json
import re
import requests
from dataclasses import dataclass
from urllib.parse import urlparse
from typing import Optional, List

try:
    import yt_dlp
except ImportError:
    yt_dlp = None


@dataclass
class VideoInfo:
    title: str
    description: str
    tags: List[str]
    categories: List[str]
    duration: int
    view_count: int
    uploader: str
    upload_date: str
    thumbnail: Optional[str] = None
    file_path: Optional[str] = None
    meta_path: Optional[str] = None
    folder_path: Optional[str] = None
    webpage_url: Optional[str] = None


class VideoDownloader:
    def __init__(self, output_dir: str = "downloads", proxy: Optional[str] = None):
        self.base_output_dir = output_dir
        self.proxy = proxy
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

    def _sanitize_filename(self, s: str) -> str:
        # 1. 简单过滤非法字符，保留中文
        s = re.sub(r'[\\/*?:"<>|]', '_', s).strip()
        # 2. 避免以点结尾（Windows 也不允许）
        while s.endswith('.'):
            s = s[:-1]
        return s

    def _fetch_page_meta(self, url: str) -> dict:
        """
        使用 requests 获取页面 HTML，并解析:
        1. head 中的 title, description, og:title
        2. body 中的 .video-tags-list 下的所有标签
        """
        meta = {
            "head_title": "",
            "head_description": "",
            "og_title": "",
            "page_tags": []
        }
        try:
            proxies = None
            if self.proxy:
                proxies = {"http": self.proxy, "https": self.proxy}
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            resp = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            if resp.status_code != 200:
                print(f"Fetch meta failed: {resp.status_code}")
                return meta
            
            html = resp.text
            
            # 1. <title>...</title>
            m_title = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
            if m_title:
                meta["head_title"] = m_title.group(1).strip()
                
            # 2. <meta name="description" content="...">
            meta_tags = re.findall(r'<meta\s+[^>]*>', html, re.IGNORECASE)
            for tag in meta_tags:
                if re.search(r'name=["\']?description["\']?', tag, re.IGNORECASE):
                    m_content = re.search(r'content=["\'](.*?)["\']', tag, re.IGNORECASE)
                    if not m_content:
                        m_content = re.search(r'content=([^"\'>\s]+)', tag, re.IGNORECASE)
                    if m_content:
                        meta["head_description"] = m_content.group(1).strip()
                        break
            
            # 3. <meta property="og:title" content="...">
            m_og = re.search(r'<meta\s+property=["\']og:title["\']\s+content=["\'](.*?)["\']', html, re.IGNORECASE)
            if m_og:
                meta["og_title"] = m_og.group(1).strip()

            # 4. 解析 video-tags-list 中的标签
            tags_div_match = re.search(r'<div[^>]*class=["\'][^"\']*video-tags-list[^"\']*["\'][^>]*>(.*?)</div>', html, re.IGNORECASE | re.DOTALL)
            
            if tags_div_match:
                tags_content = tags_div_match.group(1)
                tag_matches = re.findall(r'<a[^>]+href=["\']([^"\']*)["\'][^>]*>(.*?)</a>', tags_content, re.IGNORECASE | re.DOTALL)
                clean_tags = []
                for href, content in tag_matches:
                    text = re.sub(r'<[^>]+>', '', content).strip()
                    if not text: continue
                    if text in ['+', 'Edit tags and models']: continue
                    if re.match(r'^\d+$', text): continue
                    if '/profiles/' in href or '/channels/' in href or '/users/' in href: continue
                    if text not in clean_tags:
                        clean_tags.append(text)
                if clean_tags:
                    meta["page_tags"] = clean_tags

            for k in meta:
                if isinstance(meta[k], str):
                    meta[k] = meta[k].replace("&amp;", "&").replace("&quot;", '"').replace("&#39;", "'").replace("&lt;", "<").replace("&gt;", ">")
                elif isinstance(meta[k], list):
                    meta[k] = [t.replace("&amp;", "&").replace("&quot;", '"').replace("&#39;", "'").replace("&lt;", "<").replace("&gt;", ">") for t in meta[k]]
                
        except Exception as e:
            print(f"Warning: Failed to fetch page meta: {e}")
        
        return meta

    def _get_folder_name_from_url(self, url: str, fallback_title: str) -> str:
        """
        尝试从 URL 提取文件夹名：
        1. 获取 URL Path 最后一段非空片段
        2. 如果包含 '.'，取 '.' 之后的部分
        3. 如果不包含 '.'，取整段
        4. 如果结果无效，回退到 safe_title
        """
        try:
            path = urlparse(url).path
            # 去除末尾 /
            if path.endswith('/'):
                path = path[:-1]
            
            # 取最后一段
            segments = path.split('/')
            last_segment = segments[-1] if segments else ""
            
            if not last_segment:
                return fallback_title
                
            # 处理 '.'
            if '.' in last_segment:
                # 取最后一个 . 之后的内容 (通常是 ID 或 slug)
                # 例如 video.123 -> 123
                # video.katmbcf254a -> katmbcf254a
                parts = last_segment.split('.')
                candidate = parts[-1]
            else:
                candidate = last_segment
            
            # 清洗 candidate
            candidate = self._sanitize_filename(candidate)
            
            # 如果 candidate 太短或无效 (例如只有 "_")，回退
            if not candidate or len(candidate) < 2 or candidate == "_":
                return fallback_title
                
            return candidate
            
        except Exception:
            return fallback_title

    def download(self, url: str) -> Optional[VideoInfo]:
        if not yt_dlp:
            raise ImportError("yt-dlp not installed. Please run `pip install yt-dlp`")

        try:
            with yt_dlp.YoutubeDL({'quiet': True, 'proxy': self.proxy}) as ydl:
                print(f"Fetching metadata for: {url}")
                info_dict = ydl.extract_info(url, download=False)
                
            title = info_dict.get('title', 'video')
            video_id = info_dict.get('id', 'unknown')
            
            # 创建独立文件夹
            # 策略：优先从 URL 提取，提取失败则用 Title + ID
            max_folder_len = 80
            safe_title = self._sanitize_filename(title)
            if len(safe_title) > max_folder_len:
                safe_title = safe_title[:max_folder_len].strip()
            
            fallback_name = f"{safe_title} [{video_id}]"
            
            folder_name = self._get_folder_name_from_url(url, fallback_name)
            
            # 再次确保长度安全
            if len(folder_name) > max_folder_len:
                 folder_name = folder_name[:max_folder_len].strip()
                 
            save_dir = os.path.join(self.base_output_dir, folder_name)
            
            if not os.path.exists(save_dir):
                os.makedirs(save_dir, exist_ok=True)

            # 2. 正式下载配置
            ydl_opts = {
                'outtmpl': f'{save_dir}/video.%(ext)s', 
                'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                'writethumbnail': True,
                'noplaylist': True,
                'quiet': True,
                'no_warnings': True,
            }

            if self.proxy:
                ydl_opts['proxy'] = self.proxy

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                print(f"Downloading to: {save_dir}")
                info = ydl.extract_info(url, download=True)
                
                filename = ydl.prepare_filename(info)
                if not os.path.exists(filename):
                    base = os.path.splitext(filename)[0]
                    for ext in ['.mkv', '.webm', '.mp4']:
                        if os.path.exists(base + ext):
                            filename = base + ext
                            break

                thumb_path = None
                base_name = os.path.splitext(filename)[0]
                for ext in [".jpg", ".jpeg", ".png", ".webp"]:
                    if os.path.exists(base_name + ext):
                        thumb_path = base_name + ext
                        break
                
                page_meta = self._fetch_page_meta(url)

                tags = info.get('tags', []) or []
                categories = info.get('categories', []) or []
                desc = info.get('description', '') or ''
                uploader = info.get('uploader', '')
                view_count = info.get('view_count', 0)
                duration = info.get('duration', 0)
                webpage_url = info.get('webpage_url', url)
                upload_date = info.get('upload_date', '')

                meta_content = [
                    f"Title: {title}",
                    f"URL: {webpage_url}",
                    f"Uploader: {uploader}",
                    f"Date: {upload_date}",
                    f"Duration: {duration}s",
                    f"Views: {view_count}",
                    f"Categories: {', '.join(categories)}",
                    f"Tags: {', '.join(tags)}",
                    "-" * 20,
                    "HEAD Meta Info:",
                    f"Page Title: {page_meta['head_title']}",
                    f"OG Title:   {page_meta['og_title']}",
                    f"Page Desc:  {page_meta['head_description']}",
                    "-" * 20,
                    "Page Body Tags:",
                    f"{', '.join(page_meta['page_tags'])}",
                    "-" * 20,
                    "Description:",
                    desc
                ]
                
                meta_path = os.path.join(save_dir, "meta.txt")
                with open(meta_path, "w", encoding="utf-8") as f:
                    f.write("\n".join(meta_content))

                return VideoInfo(
                    title=title,
                    description=desc,
                    tags=tags,
                    categories=categories,
                    duration=duration,
                    view_count=view_count,
                    uploader=uploader,
                    upload_date=upload_date,
                    thumbnail=thumb_path,
                    file_path=filename,
                    meta_path=meta_path,
                    folder_path=save_dir,
                    webpage_url=webpage_url
                )

        except Exception as e:
            print(f"Error downloading {url}: {e}")
            return None
