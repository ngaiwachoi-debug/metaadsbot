"""facebook-python-business-sdk wrappers with normalized error shape."""

from __future__ import annotations

import logging
import os
from typing import Any


def normalize_sdk_error(error: Exception | Any) -> dict[str, Any]:
    """
    Normalize SDK/transport exceptions into legacy-compatible error payload.

    Mandatory keys:
    - message
    - code
    - error_subcode
    - fbtrace_id
    - is_transient
    """
    out: dict[str, Any] = {
        "message": str(error) if error is not None else "Unknown SDK error",
        "code": 0,
        "error_subcode": 0,
        "fbtrace_id": "",
        "is_transient": False,
    }
    if error is None:
        return out
    # FacebookRequestError-like APIs
    get = getattr
    try:
        msg = get(error, "api_error_message", lambda: "")() or get(error, "get_message", lambda: "")()
        if msg:
            out["message"] = str(msg)
    except Exception:
        pass
    try:
        out["code"] = int(get(error, "api_error_code", lambda: 0)() or 0)
    except Exception:
        pass
    try:
        out["error_subcode"] = int(get(error, "api_error_subcode", lambda: 0)() or 0)
    except Exception:
        pass
    try:
        out["fbtrace_id"] = str(get(error, "api_blame_field_specs", lambda: "")() or "")
    except Exception:
        pass
    try:
        tr = get(error, "api_transient_error", lambda: False)()
        out["is_transient"] = bool(tr)
    except Exception:
        pass
    try:
        u_title = get(error, "api_error_user_title", lambda: "")()
        if u_title:
            out["error_user_title"] = str(u_title)
    except Exception:
        pass
    try:
        u_msg = get(error, "api_error_user_msg", lambda: "")()
        if u_msg:
            out["error_user_msg"] = str(u_msg)
    except Exception:
        pass
    try:
        et = get(error, "api_error_type", lambda: "")()
        if et:
            out["type"] = str(et)
    except Exception:
        pass
    return out


class SdkGraphClient:
    """Thin SDK-backed Graph caller used by MetaWriteAdapter."""

    def __init__(self, *, version: str, token: str, logger: logging.Logger | None = None) -> None:
        self.version = version
        self.token = token
        self.logger = logger or logging.getLogger(__name__)
        self._ready = False
        self._api = None
        self._init_error: dict[str, Any] | None = None
        self._init_sdk()

    def _init_sdk(self) -> None:
        try:
            from facebook_business.api import FacebookAdsApi  # type: ignore

            FacebookAdsApi.init(access_token=self.token, api_version=self.version, debug=True)
            self._api = FacebookAdsApi.get_default_api()
            self._ready = self._api is not None
        except Exception as e:
            self._ready = False
            self._init_error = normalize_sdk_error(e)
            self._init_error["message"] = f"sdk_init_failed: {self._init_error.get('message', str(e))}"

    def _not_ready(self) -> dict[str, Any]:
        err = self._init_error or normalize_sdk_error(RuntimeError("sdk_not_ready"))
        return {"error": err}

    def graph_get(self, node_id: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self._ready or not self._api:
            return self._not_ready()
        try:
            resp = self._api.call("GET", (node_id.lstrip("/"),), params=dict(params or {}))
            body = resp.json() if hasattr(resp, "json") else {}
            return body if isinstance(body, dict) else {}
        except Exception as e:
            return {"error": normalize_sdk_error(e)}

    def graph_post(self, node_id: str, data: dict[str, Any]) -> dict[str, Any]:
        if not self._ready or not self._api:
            return self._not_ready()
        try:
            resp = self._api.call("POST", (node_id.lstrip("/"),), params=dict(data or {}))
            body = resp.json() if hasattr(resp, "json") else {}
            return body if isinstance(body, dict) else {}
        except Exception as e:
            return {"error": normalize_sdk_error(e)}

    def upload_ad_image_jpeg(self, ad_account_id: str, jpeg_bytes: bytes, *, filename: str = "creative.jpg") -> str:
        if not self._ready or not self._api:
            return ""
        if not ad_account_id or len(jpeg_bytes) < 100:
            return ""
        try:
            from facebook_business.adobjects.adaccount import AdAccount  # type: ignore

            account = AdAccount(ad_account_id)
            images = account.create_ad_image(
                fields=[],
                params={"filename": filename},
                files={"filename": (filename, jpeg_bytes, "image/jpeg")},
            )
            if isinstance(images, dict):
                imgs = images.get("images")
                if isinstance(imgs, dict) and imgs:
                    first = next(iter(imgs.values()))
                    if isinstance(first, dict):
                        return str(first.get("hash") or "").strip()
            return ""
        except Exception:
            return ""

