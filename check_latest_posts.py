"""
Phase 4: For every shop in SHOP_CONFIGS, fetch Facebook Page posts and compare to
promoted `effective_object_story_id` from the ad account. Writes pending_tests.json
when a recent page post has no matching ad creative story id.
"""
from __future__ import annotations

import json
import os
import sys

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
CONFIG_PATH = os.path.join(ROOT, "config.json")

# How many newest posts to consider per actor/page.
POSTS_PER_PAGE = 5
HTTP_TIMEOUT = 60.0

def load_local_config() -> tuple[dict[str, str], dict[str, dict]]:
    """Load SHOP_NAME_MAP + SHOP_CONFIGS from local config.json."""
    if not os.path.isfile(CONFIG_PATH):
        print(f"⚠️ 找不到設定檔：{CONFIG_PATH}")
        return {}, {}
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            cfg = json.load(f)
    except json.JSONDecodeError:
        print(f"⚠️ 設定檔 JSON 格式錯誤：{CONFIG_PATH}")
        return {}, {}
    except Exception as e:
        print(f"⚠️ 讀取設定檔失敗：{e}")
        return {}, {}

    if not isinstance(cfg, dict):
        print(f"⚠️ 設定檔內容不是 JSON 物件：{CONFIG_PATH}")
        return {}, {}

    name_map = cfg.get("SHOP_NAME_MAP", {})
    shop_configs = cfg.get("SHOP_CONFIGS", {})
    if not isinstance(name_map, dict):
        name_map = {}
    if not isinstance(shop_configs, dict):
        shop_configs = {}
    return name_map, shop_configs


def _norm_story_key(s: str) -> str:
    return str(s or "").strip()


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
            print(f"⚠️ ads list HTTP {r.status_code}: {r.text[:500]}")
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


def fetch_actor_posts(client: httpx.Client, actor_id: str) -> list[dict]:
    url = f"https://graph.facebook.com/{API_VERSION}/{actor_id}/posts"
    params = {
        "fields": "id,created_time,message",
        "limit": POSTS_PER_PAGE,
        "access_token": ACCESS_TOKEN,
    }
    r = client.get(url, params=params, timeout=HTTP_TIMEOUT)
    if r.status_code != 200:
        # OAuth / permission errors should be skipped without stopping the script.
        if r.status_code in (400, 401, 403):
            print(f"⚠️ actor_id={actor_id} 權限不足或 OAuth 錯誤，已略過。HTTP {r.status_code}")
        else:
            print(f"⚠️ actor_id={actor_id} posts HTTP {r.status_code}: {r.text[:400]}")
        return []
    data = r.json()
    return list(data.get("data", []) or [])


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
    Reverse lookup from SHOP_NAME_MAP:
    keys are actor_ids, values are shop names.
    """
    target = str(target_shop_name or "").strip()
    out: list[str] = []
    if not target:
        return out
    for actor_id, mapped_shop in shop_name_map.items():
        if str(mapped_shop or "").strip() == target:
            aid = str(actor_id or "").strip()
            if aid:
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

    with httpx.Client() as client:
        promoted = fetch_all_promoted_story_ids(client)
        print(f"📌 帳戶內已收集 promoted story id 約 {len(promoted)} 筆（去重後）。")

        for shop in sorted(configs.keys()):
            actor_ids = get_actor_ids_for_shop(shop, name_map)
            if not actor_ids:
                print(f"⚠️ 店鋪「{shop}」在 SHOP_NAME_MAP 無對應 actor/page id，略過。")
                continue

            for actor_id in actor_ids:
                posts = fetch_actor_posts(client, actor_id)
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
