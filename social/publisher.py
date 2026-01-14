from __future__ import annotations

import base64
import hashlib
import hmac
import mimetypes
import os
import time
import uuid
from dataclasses import dataclass
from urllib.parse import quote

import requests


@dataclass(frozen=True)
class PublishResult:
    ok: bool
    platform: str
    post_id: str | None = None
    url: str | None = None
    error: str | None = None


def _guess_mime(path: str) -> str:
    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"


def _read_file(path: str) -> tuple[str, bytes, str]:
    name = os.path.basename(path)
    with open(path, "rb") as f:
        data = f.read()
    return name, data, _guess_mime(path)


class FacebookPublisher:
    def __init__(self, page_id: str, access_token: str):
        self._page_id = (page_id or "").strip()
        self._token = (access_token or "").strip()

    def enabled(self) -> bool:
        return bool(self._page_id and self._token)

    def publish(self, message: str, link: str | None = None, image_path: str | None = None) -> PublishResult:
        if not self.enabled():
            return PublishResult(ok=False, platform="facebook", error="facebook not configured")
        message = (message or "").strip()
        if not message and not link and not image_path:
            return PublishResult(ok=False, platform="facebook", error="empty payload")

        base = f"https://graph.facebook.com/v20.0/{self._page_id}"
        try:
            if image_path:
                name, data, mt = _read_file(image_path)
                url = base + "/photos"
                resp = requests.post(
                    url,
                    data={"access_token": self._token, "caption": message, "published": "true"},
                    files={"source": (name, data, mt)},
                    timeout=30,
                )
            else:
                url = base + "/feed"
                payload = {"access_token": self._token}
                if message:
                    payload["message"] = message
                if link:
                    payload["link"] = link
                resp = requests.post(url, data=payload, timeout=30)

            if resp.status_code >= 400:
                return PublishResult(ok=False, platform="facebook", error=f"HTTP {resp.status_code}: {resp.text[:800]}")
            js = resp.json()
            pid = str(js.get("id") or "")
            return PublishResult(ok=True, platform="facebook", post_id=pid or None)
        except Exception as e:
            return PublishResult(ok=False, platform="facebook", error=f"{type(e).__name__}: {e}")


class XPublisher:
    def __init__(
        self,
        bearer_token: str,
        api_key: str,
        api_key_secret: str,
        access_token: str,
        access_token_secret: str,
    ):
        self._bearer = (bearer_token or "").strip()
        self._api_key = (api_key or "").strip()
        self._api_key_secret = (api_key_secret or "").strip()
        self._access_token = (access_token or "").strip()
        self._access_token_secret = (access_token_secret or "").strip()

    def enabled(self) -> bool:
        if self._api_key and self._api_key_secret and self._access_token and self._access_token_secret:
            return True
        return bool(self._bearer)

    def _oauth1_header(self, method: str, url: str, params: dict | None = None) -> str:
        nonce = uuid.uuid4().hex
        ts = str(int(time.time()))
        oauth_params = {
            "oauth_consumer_key": self._api_key,
            "oauth_nonce": nonce,
            "oauth_signature_method": "HMAC-SHA1",
            "oauth_timestamp": ts,
            "oauth_token": self._access_token,
            "oauth_version": "1.0",
        }

        def _enc(s: str) -> str:
            return quote(str(s), safe="~")

        flat: list[tuple[str, str]] = []
        for k in sorted(oauth_params.keys()):
            flat.append((k, str(oauth_params[k])))
        if params:
            for k in sorted(params.keys()):
                v = params[k]
                if v is None:
                    continue
                if isinstance(v, (list, tuple)):
                    for x in v:
                        if x is None:
                            continue
                        flat.append((str(k), str(x)))
                else:
                    flat.append((str(k), str(v)))
        flat.sort(key=lambda kv: (_enc(kv[0]), _enc(kv[1])))

        items = flat
        param_str = "&".join([f"{_enc(k)}={_enc(v)}" for k, v in items])
        base_str = "&".join([_enc(method.upper()), _enc(url), _enc(param_str)])
        signing_key = f"{_enc(self._api_key_secret)}&{_enc(self._access_token_secret)}"
        sig = base64.b64encode(hmac.new(signing_key.encode("utf-8"), base_str.encode("utf-8"), hashlib.sha1).digest()).decode(
            "utf-8"
        )
        oauth_params["oauth_signature"] = sig
        header = "OAuth " + ", ".join([f'{_enc(k)}="{_enc(oauth_params[k])}"' for k in sorted(oauth_params.keys())])
        return header

    def _oauth1_enabled(self) -> bool:
        return bool(self._api_key and self._api_key_secret and self._access_token and self._access_token_secret)

    def _upload_media(self, path: str) -> tuple[bool, str | None, str | None]:
        if not self._oauth1_enabled():
            return False, None, "media upload requires OAuth1 user tokens"
        if not path or not os.path.exists(path):
            return False, None, "media file missing"

        media_type = _guess_mime(path)
        is_video = media_type.startswith("video/")
        is_gif = media_type == "image/gif"
        if is_video:
            category = "tweet_video"
        elif is_gif:
            category = "tweet_gif"
        else:
            category = "tweet_image"

        total_bytes = int(os.path.getsize(path) or 0)
        if total_bytes <= 0:
            return False, None, "empty media file"

        upload_url = "https://upload.twitter.com/1.1/media/upload.json"
        init_params = {"command": "INIT", "total_bytes": total_bytes, "media_type": media_type, "media_category": category}
        try:
            init_headers = {"Authorization": self._oauth1_header("POST", upload_url, init_params)}
            init_resp = requests.post(upload_url, headers=init_headers, data=init_params, timeout=60)
            if init_resp.status_code >= 400:
                return False, None, f"INIT HTTP {init_resp.status_code}: {init_resp.text[:800]}"
            init_js = init_resp.json() or {}
            media_id = str(init_js.get("media_id_string") or init_js.get("media_id") or "")
            if not media_id:
                return False, None, "INIT missing media_id"

            segment_index = 0
            chunk_size = 4 * 1024 * 1024
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    append_params = {"command": "APPEND", "media_id": media_id, "segment_index": segment_index}
                    append_headers = {"Authorization": self._oauth1_header("POST", upload_url, append_params)}
                    files = {"media": ("blob", chunk, "application/octet-stream")}
                    append_resp = requests.post(upload_url, headers=append_headers, data=append_params, files=files, timeout=300)
                    if append_resp.status_code >= 400:
                        return False, None, f"APPEND HTTP {append_resp.status_code}: {append_resp.text[:800]}"
                    segment_index += 1

            fin_params = {"command": "FINALIZE", "media_id": media_id}
            fin_headers = {"Authorization": self._oauth1_header("POST", upload_url, fin_params)}
            fin_resp = requests.post(upload_url, headers=fin_headers, data=fin_params, timeout=60)
            if fin_resp.status_code >= 400:
                return False, None, f"FINALIZE HTTP {fin_resp.status_code}: {fin_resp.text[:800]}"
            fin_js = fin_resp.json() or {}
            proc = fin_js.get("processing_info") if isinstance(fin_js, dict) else None
            if isinstance(proc, dict):
                state = str(proc.get("state") or "").lower()
                while state in ("pending", "in_progress"):
                    wait_s = int(proc.get("check_after_secs") or 2)
                    time.sleep(max(1, min(30, wait_s)))
                    status_params = {"command": "STATUS", "media_id": media_id}
                    status_headers = {"Authorization": self._oauth1_header("GET", upload_url, status_params)}
                    status_resp = requests.get(upload_url, headers=status_headers, params=status_params, timeout=60)
                    if status_resp.status_code >= 400:
                        return False, None, f"STATUS HTTP {status_resp.status_code}: {status_resp.text[:800]}"
                    status_js = status_resp.json() or {}
                    proc = status_js.get("processing_info") if isinstance(status_js, dict) else None
                    if not isinstance(proc, dict):
                        break
                    state = str(proc.get("state") or "").lower()
                if state == "failed":
                    err = (proc.get("error") or {}) if isinstance(proc.get("error"), dict) else {}
                    msg = str(err.get("message") or "processing failed")
                    return False, None, msg[:800]

            return True, media_id, None
        except Exception as e:
            return False, None, f"{type(e).__name__}: {e}"

    def publish(self, text: str, media_paths: list[str] | None = None) -> PublishResult:
        if not self.enabled():
            return PublishResult(ok=False, platform="x", error="x not configured")
        text = (text or "").strip()
        if not text:
            return PublishResult(ok=False, platform="x", error="empty text")
        try:
            url = "https://api.x.com/2/tweets"
            headers = {"Content-Type": "application/json"}
            if self._oauth1_enabled():
                headers["Authorization"] = self._oauth1_header("POST", url)
            else:
                headers["Authorization"] = f"Bearer {self._bearer}"

            media_ids: list[str] = []
            for p in (media_paths or [])[:4]:
                ok_up, mid, err = self._upload_media(p)
                if not ok_up or not mid:
                    return PublishResult(ok=False, platform="x", error=f"media upload failed: {err or 'unknown'}")
                media_ids.append(mid)

            payload: dict = {"text": text}
            if media_ids:
                payload["media"] = {"media_ids": media_ids}

            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            if resp.status_code >= 400:
                hint = ""
                if self._bearer and not (self._api_key and self._access_token):
                    hint = " (Bearer token 通常无法发推，需要 OAuth1 用户令牌)"
                return PublishResult(ok=False, platform="x", error=f"HTTP {resp.status_code}: {resp.text[:800]}{hint}")
            js = resp.json() or {}
            pid = (((js.get("data") or {}) if isinstance(js, dict) else {}) or {}).get("id")
            return PublishResult(ok=True, platform="x", post_id=str(pid) if pid else None)
        except Exception as e:
            return PublishResult(ok=False, platform="x", error=f"{type(e).__name__}: {e}")


@dataclass(frozen=True)
class SocialConfig:
    facebook_page_id: str
    facebook_access_token: str
    x_bearer_token: str
    x_api_key: str
    x_api_key_secret: str
    x_access_token: str
    x_access_token_secret: str


class SocialPublisher:
    def __init__(self, cfg: SocialConfig):
        self.facebook = FacebookPublisher(cfg.facebook_page_id, cfg.facebook_access_token)
        self.x = XPublisher(cfg.x_bearer_token, cfg.x_api_key, cfg.x_api_key_secret, cfg.x_access_token, cfg.x_access_token_secret)

    def publish_all(
        self, text: str, link: str | None = None, image_path: str | None = None, media_paths: list[str] | None = None
    ) -> list[PublishResult]:
        out: list[PublishResult] = []
        if self.facebook.enabled():
            fb_img = image_path
            if not fb_img and media_paths:
                for p in media_paths:
                    mt = _guess_mime(p)
                    if mt.startswith("image/") and mt != "image/gif":
                        fb_img = p
                        break
            out.append(self.facebook.publish(message=text, link=link, image_path=fb_img))
        if self.x.enabled():
            out.append(self.x.publish(text=text, media_paths=media_paths))
        return out

