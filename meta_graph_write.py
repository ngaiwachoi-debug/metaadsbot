"""Meta Graph API client for Action Plan executors: GET/POST, throttle, 429 backoff, logging."""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Any

import httpx
from dotenv import load_dotenv

from meta_actions_logging import log_graph_error_payload
from meta_utils import to_float_minor

load_dotenv()

_ROOT = os.path.dirname(os.path.abspath(__file__))


class GraphAuthError(RuntimeError):
    """Token invalid/expired or OAuth error (e.g. code 190). Abort batch."""


class GraphThrottleError(RuntimeError):
    """Exceeded retries after throttling."""


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip() or default)
    except ValueError:
        return default


class GraphClient:
    """
    Synchronous Graph client. Mutations sleep META_ACTION_DELAY_MS between calls.
    Retries on HTTP 429 and transient Graph errors with exponential backoff.
    """

    def __init__(
        self,
        *,
        delay_ms: int | None = None,
        max_retries: int | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.token = (os.getenv("META_ACCESS_TOKEN") or "").strip()
        self.version = (os.getenv("META_GRAPH_API_VERSION", "v18.0") or "v18.0").strip()
        if not self.version.startswith("v"):
            self.version = "v" + self.version.lstrip("v")
        raw_acct = (os.getenv("AD_ACCOUNT_ID") or "").strip()
        self.ad_account_id = raw_acct if raw_acct.startswith("act_") else f"act_{raw_acct}" if raw_acct else ""
        self.delay_ms = delay_ms if delay_ms is not None else _env_int("META_ACTION_DELAY_MS", 300)
        self.max_retries = max_retries if max_retries is not None else _env_int("META_ACTION_MAX_RETRIES", 5)
        self.logger = logger or logging.getLogger(__name__)
        self._last_mutation_ts = 0.0
        self._client = httpx.Client(timeout=60.0)

    def base_url(self) -> str:
        return f"https://graph.facebook.com/{self.version}"

    def _throttle_mutation_gap(self) -> None:
        now = time.monotonic()
        gap = self.delay_ms / 1000.0
        elapsed = now - self._last_mutation_ts
        if self._last_mutation_ts > 0 and elapsed < gap:
            time.sleep(gap - elapsed)
        self._last_mutation_ts = time.monotonic()

    def _sleep_backoff(self, attempt: int, retry_after: float | None = None) -> None:
        if retry_after is not None and retry_after > 0:
            time.sleep(min(retry_after, 120.0))
            return
        base = min(2**attempt, 60)
        jitter = random.uniform(0, 0.5 * base)
        time.sleep(base + jitter)

    def graph_get(self, node_id: str, params: dict[str, Any] | None = None, *, is_mutation: bool = False) -> dict[str, Any]:
        if is_mutation:
            self._throttle_mutation_gap()
        params = dict(params or {})
        params["access_token"] = self.token
        url = f"{self.base_url()}/{node_id.lstrip('/')}"
        attempt = 0
        while True:
            try:
                r = self._client.get(url, params=params)
            except httpx.RequestError as e:
                attempt += 1
                if attempt > self.max_retries:
                    raise
                self.logger.warning("GET transport error %s, retry %s", e, attempt)
                self._sleep_backoff(attempt)
                continue

            # Usage headers (throttle hints)
            for hk in ("x-ad-account-usage", "x-business-use-case-usage", "x-app-usage"):
                hv = r.headers.get(hk)
                if hv:
                    self.logger.debug("header %s=%s", hk, hv)

            if r.status_code == 429:
                attempt += 1
                if attempt > self.max_retries:
                    raise GraphThrottleError("Too many 429 responses on GET")
                ra = r.headers.get("retry-after")
                self.logger.warning("429 on GET, retry %s Retry-After=%s", attempt, ra)
                self._sleep_backoff(attempt, float(ra) if ra and ra.isdigit() else None)
                continue

            data = r.json() if r.content else {}
            err = data.get("error") if isinstance(data, dict) else None
            if isinstance(err, dict):
                code = int(err.get("code", 0) or 0)
                if code == 190:
                    log_graph_error_payload(self.logger, err, prefix="auth ")
                    raise GraphAuthError(err.get("message", "OAuth error"))
                if err.get("is_transient") and attempt < self.max_retries:
                    attempt += 1
                    self.logger.warning("Transient Graph error on GET, retry %s: %s", attempt, err.get("message"))
                    self._sleep_backoff(attempt)
                    continue

            if r.status_code >= 400:
                log_graph_error_payload(self.logger, err if isinstance(err, dict) else None)
                r.raise_for_status()

            return data if isinstance(data, dict) else {}

    def graph_post(self, node_id: str, data: dict[str, Any]) -> dict[str, Any]:
        self._throttle_mutation_gap()
        payload = {**data, "access_token": self.token}
        url = f"{self.base_url()}/{node_id.lstrip('/')}"
        attempt = 0
        while True:
            try:
                r = self._client.post(url, data=payload)
            except httpx.RequestError as e:
                attempt += 1
                if attempt > self.max_retries:
                    raise
                self.logger.warning("POST transport error %s, retry %s", e, attempt)
                self._sleep_backoff(attempt)
                continue

            if r.status_code == 429:
                attempt += 1
                if attempt > self.max_retries:
                    raise GraphThrottleError("Too many 429 responses on POST")
                ra = r.headers.get("retry-after")
                self.logger.warning("429 on POST, retry %s Retry-After=%s", attempt, ra)
                self._sleep_backoff(attempt, float(ra) if ra and ra.isdigit() else None)
                continue

            body = r.json() if r.content else {}
            err = body.get("error") if isinstance(body, dict) else None
            if isinstance(err, dict):
                code = int(err.get("code", 0) or 0)
                if code == 190:
                    log_graph_error_payload(self.logger, err, prefix="auth ")
                    raise GraphAuthError(err.get("message", "OAuth error"))
                # Throttle-style user errors
                if code in (4, 17, 613) and attempt < self.max_retries:
                    attempt += 1
                    self.logger.warning("Throttle-like Graph error on POST, retry %s: %s", attempt, err.get("message"))
                    self._sleep_backoff(attempt)
                    continue
                if err.get("is_transient") and attempt < self.max_retries:
                    attempt += 1
                    self._sleep_backoff(attempt)
                    continue

            if r.status_code >= 400:
                log_graph_error_payload(self.logger, err if isinstance(err, dict) else None)
                # Return body for caller to log user_msg without raising raw httpx
                return body if isinstance(body, dict) else {"error": {"message": r.text}}

            return body if isinstance(body, dict) else {}

    def create_adset(self, body: dict[str, Any]) -> dict[str, Any]:
        """POST ``{ad_account_id}/adsets`` (form body; use json.dumps for targeting etc.)."""
        if not self.ad_account_id:
            return {"error": {"message": "empty ad_account_id"}}
        return self.graph_post(f"{self.ad_account_id.lstrip('/')}/adsets", body)

    def upload_ad_image_jpeg(self, jpeg_bytes: bytes, *, filename: str = "creative.jpg") -> str:
        """POST ``act_*/adimages`` (multipart). Returns ``image_hash`` or empty string."""
        if not self.ad_account_id or len(jpeg_bytes) < 100:
            return ""
        self._throttle_mutation_gap()
        url = f"{self.base_url()}/{self.ad_account_id.lstrip('/')}/adimages"
        attempt = 0
        while True:
            try:
                r = self._client.post(
                    url,
                    data={"access_token": self.token},
                    files={"filename": (filename, jpeg_bytes, "image/jpeg")},
                )
            except httpx.RequestError as e:
                attempt += 1
                if attempt > self.max_retries:
                    self.logger.warning("adimages POST transport error: %s", e)
                    return ""
                self._sleep_backoff(attempt)
                continue

            if r.status_code == 429:
                attempt += 1
                if attempt > self.max_retries:
                    return ""
                ra = r.headers.get("retry-after")
                self._sleep_backoff(attempt, float(ra) if ra and ra.isdigit() else None)
                continue

            body = r.json() if r.content else {}
            err = body.get("error") if isinstance(body, dict) else None
            if isinstance(err, dict):
                code = int(err.get("code", 0) or 0)
                if code == 190:
                    log_graph_error_payload(self.logger, err, prefix="auth ")
                    raise GraphAuthError(err.get("message", "OAuth error"))

            if r.status_code >= 400:
                log_graph_error_payload(self.logger, err if isinstance(err, dict) else None)
                return ""

            images = body.get("images") if isinstance(body, dict) else None
            if isinstance(images, dict) and images:
                first = next(iter(images.values()))
                if isinstance(first, dict):
                    return str(first.get("hash") or "").strip()
            return ""

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> GraphClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


def get_account_min_budget_minor(client: GraphClient) -> tuple[float, str]:
    """Mirror callfrommeta: min daily budget in minor units + currency."""
    if not client.ad_account_id:
        return 0.0, "HKD"
    data = client.graph_get(client.ad_account_id, {"fields": "min_daily_budget,currency"})
    min_minor = to_float_minor(data.get("min_daily_budget", 0))
    currency = str(data.get("currency", "HKD") or "HKD")
    return min_minor, currency


def hkd_display_string_to_minor(s: str, *, currency: str = "HKD") -> int:
    """Parse display budget like '$1,234' or '1234' to minor units (cents for HKD)."""
    t = (s or "").strip().replace(",", "").replace("$", "").replace("HKD", "").strip()
    if not t:
        return 0
    try:
        v = float(t)
    except ValueError:
        return 0
    # Meta HKD minor = cents
    if currency.upper() == "HKD":
        return int(round(v * 100))
    return int(round(v * 100))
