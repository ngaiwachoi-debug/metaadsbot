"""
Phase 4: For every shop in SHOP_CONFIGS, fetch Facebook Page posts and compare to
promoted `effective_object_story_id` from the ad account. Writes pending_tests.json
when a recent page post has no matching ad creative story id.

專頁貼文必須使用「專頁 access_token」（由用戶 META_ACCESS_TOKEN 呼叫 /me/accounts 取得），
不可直接用用戶 Token 呼叫 /{page-id}/posts。
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any

import httpx
from dotenv import load_dotenv

if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

load_dotenv()

ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "") or ""
AD_ACCOUNT_ID = os.getenv("AD_ACCOUNT_ID", "") or ""
API_VERSION = os.getenv("META_GRAPH_API_VERSION", "v18.0").strip() or "v18.0"

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_PATH = os.path.join(ROOT, "pending_tests.json")

# How many newest posts to consider per actor/page.
POSTS_PER_PAGE = 5
HTTP_TIMEOUT = 60.0


def _parse_json_env(name: str, default: Any) -> Any:
    raw = os.getenv(name, "")
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        print(f"⚠️ 環境變數 {name} 的 JSON 無法解析，使用預設。")
        return default


def load_local_config() -> tuple[dict[str, str], dict[str, dict]]:
    """Load SHOP_NAME_MAP + SHOP_CONFIGS from .env (same as shop_mapping / engine)."""
    name_map = _parse_json_env("SHOP_NAME_MAP", {})
    shop_configs = _parse_json_env("SHOP_CONFIGS", {})
    if not isinstance(name_map, dict):
        name_map = {}
    if not isinstance(shop_configs, dict):
        shop_configs = {}
    return name_map, shop_configs


def _is_numeric_graph_id(key: str) -> bool:
    """Only numeric keys are valid {id}/posts Graph node ids for this script."""
    return str(key or "").strip().isdigit()


def _norm_story_key(s: str) -> str:
    return str(s or "").strip()


def _parse_meta_error(resp: httpx.Response) -> dict[str, Any]:
    try:
        payload = resp.json()
    except Exception:
        return {"message": resp.text[:500], "type": "unknown", "code": "", "error_subcode": ""}
    err = payload.get("error", payload if isinstance(payload, dict) else {})
    if not isinstance(err, dict):
        err = {}
    return {
        "message": str(err.get("message", "")),
        "type": str(err.get("type", "")),
        "code": err.get("code", ""),
        "error_subcode": err.get("error_subcode", ""),
    }


def _print_meta_error(prefix: str, resp: httpx.Response) -> dict[str, Any]:
    err = _parse_meta_error(resp)
    print(
        f"❌ Meta API 錯誤 [代碼: {err['code']}][子代碼: {err['error_subcode']}]: {err['message']} "
        f"(type={err['type']}, http={resp.status_code}, scope={prefix})"
    )
    return err


def _should_try_media_fallback(err: dict[str, Any]) -> bool:
    msg = str(err.get("message", "")).lower()
    return (
        "object does not exist" in msg
        or "unsupported get request" in msg
        or "does not support this operation" in msg
        or "unknown path components" in msg
        or "object with id" in msg
    )


def debug_token_scopes(client: httpx.Client) -> None:
    """
    Print token scopes to quickly diagnose missing permissions.
    Note: debug_token traditionally expects app access token; we still probe with current token
    for transparent diagnostics.
    """
    url = f"https://graph.facebook.com/{API_VERSION}/debug_token"
    params = {
        "input_token": ACCESS_TOKEN,
        "access_token": ACCESS_TOKEN,
    }
    r = client.get(url, params=params, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        _print_meta_error("debug_token", r)
        return
    try:
        data = (r.json() or {}).get("data", {})
    except Exception:
        print("⚠️ debug_token 回應無法解析。")
        return
    scopes = data.get("scopes") or []
    if not isinstance(scopes, list):
        scopes = []
    print(f"🔐 Token Scopes ({len(scopes)}): {', '.join(str(x) for x in scopes) if scopes else '(empty)'}")


def fetch_all_promoted_story_ids(client: httpx.Client) -> set[str]:
    """Collect effective_object_story_id from all ads in the account."""
    promoted: set[str] = set()
    base = f"https://graph.facebook.com/{API_VERSION}/{AD_ACCOUNT_ID}/ads"
    params = {
        "fields": "creative{effective_object_story_id}",
        "limit": 100,
        "access_token": ACCESS_TOKEN,
    }
    url: str | None = base
    first_page = True
    while url:
        r = client.get(url, params=params if first_page else None, timeout=HTTP_TIMEOUT)
        first_page = False
        if r.status_code != 200:
            _print_meta_error("ads/effective_object_story_id", r)
            break
        data = r.json()
        for ad in data.get("data", []) or []:
            cr = ad.get("creative") or {}
            if isinstance(cr, str):
                continue
            sid = cr.get("effective_object_story_id")
            if sid:
                s = _norm_story_key(sid)
                promoted.add(s)
                if "_" in s:
                    promoted.add(s.split("_", 1)[-1])
        url = (data.get("paging") or {}).get("next")
    return promoted


def fetch_page_access_tokens(client: httpx.Client) -> dict[str, str]:
    """
    以用戶 Token 呼叫 GET /me/accounts，取得所管理專頁的 id 與專頁專用 access_token。
    回傳 page_id -> page_access_token（僅含 Facebook Page，不含 IG 使用者節點）。
    """
    mapping: dict[str, str] = {}
    base = f"https://graph.facebook.com/{API_VERSION}/me/accounts"
    params = {
        "fields": "id,name,access_token",
        "limit": 100,
        "access_token": ACCESS_TOKEN,
    }
    url: str | None = base
    first_page = True
    while url:
        r = client.get(url, params=params if first_page else None, timeout=HTTP_TIMEOUT)
        first_page = False
        if r.status_code != 200:
            _print_meta_error("me/accounts", r)
            break
        data = r.json()
        for row in data.get("data", []) or []:
            if not isinstance(row, dict):
                continue
            pid = str(row.get("id") or "").strip()
            tok = str(row.get("access_token") or "").strip()
            if pid and tok:
                mapping[pid] = tok
        url = (data.get("paging") or {}).get("next")
    return mapping


def _fetch_node_feed(
    client: httpx.Client, actor_id: str, edge: str, page_token: str
) -> tuple[list[dict], bool]:
    """Returns (data, ok). page_token must be the Page access token for this actor_id."""
    if not page_token:
        return [], False
    url = f"https://graph.facebook.com/{API_VERSION}/{actor_id}/{edge}"
    params = {
        "fields": "id,created_time,message",
        "limit": POSTS_PER_PAGE,
        "access_token": page_token,
    }
    r = client.get(url, params=params, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        err = _print_meta_error(f"{actor_id}/{edge}", r)
        # Permission / oauth wall: skip this actor without raising.
        if r.status_code in (400, 401, 403):
            return [], False
        if _should_try_media_fallback(err):
            return [], False
        return [], False
    data = r.json()
    rows = data.get("data", []) if isinstance(data, dict) else []
    return (list(rows) if isinstance(rows, list) else []), True


def fetch_actor_posts(client: httpx.Client, actor_id: str, page_token: str) -> list[dict]:
    """Fetch recent posts (or IG /media fallback) using the Page-scoped token for this id."""
    if not page_token:
        return []
    # Primary: Facebook page-style posts
    posts, ok = _fetch_node_feed(client, actor_id, "posts", page_token)
    if ok and posts:
        return posts
    if ok and not posts:
        return []

    # Fallback: Instagram business/media style (同樣帶入專頁 Token；若 actor 為純 IG id 可能仍失敗)
    media, ok2 = _fetch_node_feed(client, actor_id, "media", page_token)
    if ok2:
        if media:
            print(f"ℹ️ actor_id={actor_id} 使用 /media fallback 成功。")
        return media
    return []


def _post_is_promoted(post_id: str, promoted: set[str]) -> bool:
    pid = _norm_story_key(post_id)
    if not pid:
        return False
    if pid in promoted:
        return True
    if "_" in pid:
        tail = pid.split("_", 1)[-1]
        if tail in promoted:
            return True
    for p in promoted:
        if p.endswith(pid) or pid.endswith(p):
            return True
    return False


def get_actor_ids_for_shop(target_shop_name: str, shop_name_map: dict[str, str]) -> list[str]:
    """
    Reverse lookup from SHOP_NAME_MAP: numeric keys are Graph page/IG ids;
    values are internal 店名 (same labels as SHOP_CONFIGS keys), not necessarily the Page's public name.
    """
    target = str(target_shop_name or "").strip()
    out: list[str] = []
    if not target:
        return out
    for actor_id, mapped_shop in shop_name_map.items():
        if str(mapped_shop or "").strip() != target:
            continue
        aid = str(actor_id or "").strip()
        if aid and _is_numeric_graph_id(aid):
            out.append(aid)
    return sorted(set(out))


def run() -> list[dict]:
    if not ACCESS_TOKEN or not AD_ACCOUNT_ID:
        print("❌ META_ACCESS_TOKEN / AD_ACCOUNT_ID 未設定，略過 pending_tests 檢查。")
        return []

    name_map, configs = load_local_config()
    if not configs:
        print("⚠️ SHOP_CONFIGS 為空，無店鋪可檢查。")
        return []

    if not name_map:
        print("⚠️ SHOP_NAME_MAP 為空，無法對應專頁 ID。")
        return []

    pending: list[dict] = []

    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    with httpx.Client(limits=limits) as client:
        debug_token_scopes(client)
        page_tokens = fetch_page_access_tokens(client)
        print(f"📋 /me/accounts：已載入 {len(page_tokens)} 個專頁 access_token。")
        promoted = fetch_all_promoted_story_ids(client)
        print(f"📌 帳戶內已收集 promoted story id 約 {len(promoted)} 筆（去重後）。")

        for shop in sorted(configs.keys()):
            actor_ids = get_actor_ids_for_shop(shop, name_map)
            print(f"🏪 掃描店鋪：{shop} | actor_ids={actor_ids}")
            if not actor_ids:
                print(f"⚠️ 店鋪「{shop}」在 SHOP_NAME_MAP 無對應 actor/page id，略過。")
                continue

            for actor_id in actor_ids:
                page_token = page_tokens.get(actor_id)
                if not page_token:
                    print(
                        f"⚠️ actor_id={actor_id} 在 /me/accounts 無對應專頁 Token（可能非目前用戶管理的 FB 專頁，或為 IG 帳號 id），略過。"
                    )
                    continue
                posts = fetch_actor_posts(client, actor_id, page_token)
                if not posts:
                    continue
                posts.sort(key=lambda x: str(x.get("created_time", "")), reverse=True)

                for post in posts:
                    pid = str(post.get("id", "") or "")
                    if not pid:
                        continue
                    if _post_is_promoted(pid, promoted):
                        continue
                    pending.append(
                        {
                            "shop": shop,
                            "actor_id": actor_id,
                            "post_id": pid,
                            "created_time": post.get("created_time", ""),
                        }
                    )
                    print(
                        f"🧪 待測：{shop} | actor_id={actor_id} | post={pid}（最新貼文尚未對應廣告 story）"
                    )
                    break

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(pending, f, ensure_ascii=False, indent=2)
    print(f"✅ 已寫入 {OUT_PATH}（{len(pending)} 筆）。")
    return pending


if __name__ == "__main__":
    run()
