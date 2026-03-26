"""
讀取 Raw 工作表（callfrommeta 同步結果），寫入 Refined 工作表後，輸出「動態日預算配速 + 操作清單」至 AI_操作清單。
"""
from __future__ import annotations

import difflib
import json
import math
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import gspread
from collections import defaultdict
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

from engine import (
    NEW_AD_TEST_BUDGET,
    SHOP_CONFIGS,
    _is_new_ad,
    aggregate_by_adset,
    aggregate_shop_spend_from_rows,
    build_pool_items_by_shop,
    classify_strategy,
    daily_targets,
    get_dynamic_target_cpc,
    load_pending_tests_entries,
    new_post_budget_reserve_for_shop,
    weighted_pool_allocation,
)
from meta_targeting import parse_targeting_details
from meta_utils import to_float_minor, to_hkd_from_meta_minor
from shop_mapping import SHOP_NAME_MAP, map_shop_name

if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

load_dotenv()
SHEET_NAME = os.getenv("SHEET_NAME", "AdSurvivor_Report")
RAW_SHEET_TAB = os.getenv("RAW_SHEET_TAB", "Sheet1")
REFINED_SHEET_TAB = os.getenv("REFINED_SHEET_TAB", "Sheet2")
OUTPUT_TAB = "AI_操作清單"

MIN_ACTIVE_ADS_PER_SHOP = int(os.getenv("MIN_ACTIVE_ADS_PER_SHOP", "3") or 3)
MIN_POOL_SIZE = int(os.getenv("MIN_POOL_SIZE", "3") or 3)
MAX_RUNNING_ADS = int(os.getenv("MAX_RUNNING_ADS", "15") or 15)
META_GRAPH_API_VERSION = os.getenv("META_GRAPH_API_VERSION", "v18.0").strip() or "v18.0"
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "") or ""
AI_API_KEY = os.getenv("AI_API_KEY", "") or ""
AI_BASE_URL = (os.getenv("AI_BASE_URL", "") or "").rstrip("/")
AI_MODEL_NAME = os.getenv("AI_MODEL_NAME", "MiniMax-M2.7") or "MiniMax-M2.7"

REFINED_COLUMNS = [
    "synced_at",
    "廣告ID",
    "actor_id",
    "instagram_actor_id",
    "店名",
    "來源專頁",
    "fb_page_name",
    "Campaign Name",
    "Campaign ID",
    "AdSet ID",
    "AdSet Name",
    "7日平均 CPC",
    "本月平均 CPC",
    "現有日預算",
    "帳戶最低日預算(API)",
    "今日 CPC",
    "今日花費",
    "7日花費",
    "7日點擊",
    "30日平均 CPC",
    "本月累積花費",
    "本月點擊",
    "廣告名稱",
    "created_time",
    "廣告文案",
    "詳細目標設定",
    "targeting_json",
    "分類",
]


def get_google_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME)


def _to_float(v) -> float:
    s = str(v or "").strip().replace(",", "").replace("$", "")
    m = re.search(r"-?\d+(\.\d+)?", s)
    return float(m.group(0)) if m else 0.0


def _normalize_header_key(name: str) -> str:
    """Strip spaces, BOM, and non-printable junk from sheet header cells."""
    s = (name or "").replace("\ufeff", "").strip()
    return "".join(ch for ch in s if ch.isprintable())


def _norm_actor_id(v) -> str:
    """Normalize Meta IDs for SHOP_NAME_MAP keys (string digits)."""
    if v is None or v == "":
        return ""
    s = str(v).strip()
    if s.replace(".", "", 1).isdigit():
        try:
            return str(int(float(s)))
        except Exception:
            return s
    return s


def _read_raw_rows(ss):
    try:
        ws = ss.worksheet(RAW_SHEET_TAB)
    except Exception:
        ws = ss.get_worksheet(0)
    values = ws.get_all_values()
    if not values:
        return []
    headers = [_normalize_header_key(c) for c in values[0]]
    rows: list[dict] = []
    for row in values[1:]:
        if not any(str(c).strip() for c in row):
            continue
        d: dict = {}
        for i, key in enumerate(headers):
            if not key:
                continue
            d[key] = row[i] if i < len(row) else ""
        rows.append(d)
    return rows


def _get_or_create_worksheet(ss, title: str):
    try:
        return ss.worksheet(title)
    except Exception:
        return ss.add_worksheet(title=title, rows=1000, cols=30)


def _is_cbo(val) -> bool:
    if isinstance(val, bool):
        return val
    s = str(val or "").strip().upper()
    return s in ("TRUE", "1", "YES")


def _existing_daily_budget_hkd(r: dict) -> float:
    adset_minor = to_float_minor(r.get("adset_daily_budget_minor"))
    campaign_minor = to_float_minor(r.get("campaign_daily_budget_minor"))
    api_minor = to_float_minor(r.get("adset_daily_budget_api_minor"))
    if _is_cbo(r.get("CBO")) and campaign_minor > 0:
        return to_hkd_from_meta_minor(campaign_minor)
    if adset_minor > 0:
        return to_hkd_from_meta_minor(adset_minor)
    if api_minor > 0:
        return to_hkd_from_meta_minor(api_minor)
    return 0.0


def _category_label(strategy: str) -> str:
    return "菲律賓(香港廣告)" if strategy == "BUN" else "Hong Kong"


def _resolve_shop_from_raw(r: dict) -> str:
    """ID-first: SHOP_NAME_MAP[actor_id / instagram_actor_id], then fb_page_name fuzzy (map_shop_name)."""
    aid = _norm_actor_id(r.get("actor_id"))
    iid = _norm_actor_id(r.get("instagram_actor_id"))
    fb_page_name = str(r.get("fb_page_name", "") or "")

    if aid:
        if aid in SHOP_NAME_MAP:
            return str(SHOP_NAME_MAP[aid]).strip()
    if iid:
        if iid in SHOP_NAME_MAP:
            return str(SHOP_NAME_MAP[iid]).strip()

    fuzzy = map_shop_name(fb_page_name)
    if fuzzy and fuzzy != "其他":
        return fuzzy
    if (aid or iid) and fuzzy == "其他":
        return "Unknown"
    return fuzzy if fuzzy else "Unknown"


def refine_raw_rows(rows_raw: list[dict]) -> list[dict]:
    """Map Raw API rows → engine 期望欄位（HKD、店名、詳細目標文字等）。"""
    out: list[dict] = []
    for r in rows_raw:
        ad_id = str(r.get("廣告ID", "") or "").strip()
        if not ad_id:
            continue
        if str(r.get("synced_at", "")).strip() == "總消耗統計":
            continue

        fb_page_name = str(r.get("fb_page_name", "") or "")
        actor_id_s = _norm_actor_id(r.get("actor_id"))
        instagram_actor_id_s = _norm_actor_id(r.get("instagram_actor_id"))
        shop = _resolve_shop_from_raw(r)
        ad_name = str(r.get("廣告名稱", "") or "")
        body = str(r.get("廣告文案", "") or "")
        created_time = str(r.get("created_time", "") or "")
        campaign_name = str(r.get("Campaign Name", "") or "")

        strategy = classify_strategy(ad_name, body, created_time, campaign_name, shop)
        category = _category_label(strategy)

        targ_raw = r.get("targeting_json", "")
        if not isinstance(targ_raw, str):
            targ_raw = json.dumps(targ_raw, ensure_ascii=False) if targ_raw else ""
        try:
            targ = json.loads(targ_raw) if isinstance(targ_raw, str) and targ_raw.strip() else {}
        except json.JSONDecodeError:
            targ = {}
        details = parse_targeting_details(targ)

        month_spend = _to_float(r.get("本月累積花費", 0))
        month_clicks = _to_float(r.get("本月點擊", 0))
        month_avg_cpc = (month_spend / month_clicks) if month_clicks > 0 else 0.0

        min_hkd = to_hkd_from_meta_minor(r.get("帳戶最低日預算_minor", 0))

        row_out = {
            "synced_at": str(r.get("synced_at", "") or ""),
            "廣告ID": ad_id,
            "actor_id": actor_id_s,
            "instagram_actor_id": instagram_actor_id_s,
            "店名": shop,
            "來源專頁": fb_page_name,
            "fb_page_name": fb_page_name,
            "Campaign Name": campaign_name,
            "Campaign ID": str(r.get("Campaign ID", "") or ""),
            "AdSet ID": str(r.get("AdSet ID", "") or ""),
            "AdSet Name": str(r.get("AdSet Name", "") or ""),
            "7日平均 CPC": round(_to_float(r.get("7日平均 CPC", 0)), 4),
            "本月平均 CPC": round(month_avg_cpc, 4),
            "現有日預算": round(_existing_daily_budget_hkd(r), 2),
            "帳戶最低日預算(API)": round(min_hkd, 2),
            "今日 CPC": round(_to_float(r.get("今日 CPC", 0)), 4),
            "今日花費": round(_to_float(r.get("今日花費", 0)), 2),
            "7日花費": round(_to_float(r.get("7日花費", 0)), 2),
            "7日點擊": round(_to_float(r.get("7日點擊", 0)), 2),
            "30日平均 CPC": round(_to_float(r.get("30日平均 CPC", 0)), 4),
            "本月累積花費": round(month_spend, 2),
            "本月點擊": round(month_clicks, 2),
            "廣告名稱": ad_name,
            "created_time": created_time,
            "廣告文案": body,
            "詳細目標設定": details,
            "targeting_json": targ_raw if isinstance(targ_raw, str) else "",
            "分類": category,
        }
        out.append(row_out)
    return out


def _refined_to_grid(rows: list[dict]) -> list[list]:
    header = REFINED_COLUMNS
    grid = [header]
    for d in rows:
        grid.append([d.get(k, "") for k in header])
    return grid


_INTEREST_LIB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "meta_interests_lib.json")
_interest_lib_cache: dict[str, str] | None = None

_SKIP_TARGETING_KEYS = frozenset({"custom_audiences", "excluded_custom_audiences"})


def _normalize_lib_key(name: str) -> str:
    return (name or "").strip().lower()


def _load_interest_lib() -> dict[str, str]:
    global _interest_lib_cache
    if _interest_lib_cache is not None:
        return _interest_lib_cache
    if not os.path.isfile(_INTEREST_LIB_PATH):
        _interest_lib_cache = {}
        return _interest_lib_cache
    try:
        with open(_INTEREST_LIB_PATH, encoding="utf-8") as f:
            raw = json.load(f)
        _interest_lib_cache = {str(k).lower(): str(v) for k, v in raw.items()} if isinstance(raw, dict) else {}
    except Exception:
        _interest_lib_cache = {}
    return _interest_lib_cache


def _save_interest_lib(data: dict[str, str]) -> None:
    global _interest_lib_cache
    _interest_lib_cache = dict(data)
    os.makedirs(os.path.dirname(_INTEREST_LIB_PATH), exist_ok=True)
    tmp = _INTEREST_LIB_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_interest_lib_cache, f, ensure_ascii=False, indent=2)
    os.replace(tmp, _INTEREST_LIB_PATH)


def _collect_targeting_names_and_lib(
    obj: Any,
    names: set[str],
    lib_delta: dict[str, str],
) -> None:
    if isinstance(obj, dict):
        if "name" in obj and isinstance(obj.get("name"), str):
            nm = obj["name"].strip()
            if nm and not nm.isdigit():
                names.add(nm)
            tid = obj.get("id")
            if nm and tid is not None and str(tid).strip():
                k = _normalize_lib_key(nm)
                if k:
                    lib_delta[k] = str(tid).strip()
        for k, v in obj.items():
            if k in _SKIP_TARGETING_KEYS:
                continue
            _collect_targeting_names_and_lib(v, names, lib_delta)
    elif isinstance(obj, list):
        for item in obj:
            _collect_targeting_names_and_lib(item, names, lib_delta)


def _extract_audience_tags(targeting_json_str: str) -> str:
    """Parse Meta targeting JSON → comma-separated audience names; upsert id+name into meta_interests_lib.json."""
    s = (targeting_json_str or "").strip()
    if not s:
        return "Custom/Lookalike Audience"
    try:
        root = json.loads(s)
    except json.JSONDecodeError:
        return "Custom/Lookalike Audience"
    if not isinstance(root, dict):
        return "Custom/Lookalike Audience"

    names: set[str] = set()
    lib_delta: dict[str, str] = {}
    for key, val in root.items():
        if key in _SKIP_TARGETING_KEYS:
            continue
        _collect_targeting_names_and_lib(val, names, lib_delta)

    if lib_delta:
        lib = _load_interest_lib()
        changed = False
        for k, v in lib_delta.items():
            if lib.get(k) != v:
                lib[k] = v
                changed = True
        if changed:
            _save_interest_lib(lib)

    if not names:
        return "Custom/Lookalike Audience"
    return ", ".join(sorted(names))


def _graph_batch_search_interests(queries: list[str]) -> dict[str, str]:
    """Layer 3: one HTTP round-trip. Maps normalized query → Meta interest id (first hit)."""
    if not queries or not META_ACCESS_TOKEN:
        return {}
    batch = [
        {
            "method": "GET",
            "relative_url": (
                "search?type=adinterest&q="
                + urllib.parse.quote(q, safe="")
                + "&limit=1"
            ),
        }
        for q in queries
    ]
    url = f"https://graph.facebook.com/{META_GRAPH_API_VERSION}/"
    body = urllib.parse.urlencode(
        {"batch": json.dumps(batch), "access_token": META_ACCESS_TOKEN}
    ).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    out: dict[str, str] = {}
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="replace"))
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError):
        return out
    if not isinstance(body, list):
        return out
    for q, item in zip(queries, body):
        if not isinstance(item, dict):
            continue
        if int(item.get("code", 0) or 0) != 200:
            continue
        raw_body = item.get("body")
        if not raw_body:
            continue
        try:
            inner = json.loads(raw_body) if isinstance(raw_body, str) else raw_body
        except json.JSONDecodeError:
            continue
        data = inner.get("data") if isinstance(inner, dict) else None
        if not data or not isinstance(data, list) or not data:
            continue
        first = data[0]
        if isinstance(first, dict) and first.get("id"):
            out[_normalize_lib_key(q)] = str(first["id"])
    return out


def validate_and_get_ids(tags_list: list[str]) -> tuple[list[str], list[str], str]:
    """
    Resolve tag names → Meta interest IDs (Layers 1–2 local, Layer 3 batched Graph).
    Returns (ordered_ids_for_resolved_tags, ordered_original_tags_kept, status).
    """
    lib = _load_interest_lib()
    keys = list(lib.keys())
    resolved: list[str] = []
    kept_tags: list[str] = []
    need_api: list[str] = []
    used_api = False

    for tag in tags_list:
        t = (tag or "").strip()
        if not t:
            continue
        nk = _normalize_lib_key(t)
        tid = lib.get(nk)
        if tid:
            resolved.append(tid)
            kept_tags.append(t)
            continue
        fuzzy = difflib.get_close_matches(nk, keys, n=1, cutoff=0.90)
        if fuzzy:
            mk = fuzzy[0]
            tid2 = lib.get(mk)
            if tid2:
                resolved.append(tid2)
                kept_tags.append(t)
                continue
        need_api.append(t)

    if need_api and META_ACCESS_TOKEN:
        found = _graph_batch_search_interests(need_api)
        used_api = True
        lib_changed = False
        for t in need_api:
            nk = _normalize_lib_key(t)
            tid = found.get(nk)
            if tid:
                lib[nk] = tid
                lib_changed = True
                resolved.append(tid)
                kept_tags.append(t)
        if lib_changed:
            _save_interest_lib(lib)

    n_in = len([x for x in tags_list if (x or "").strip()])
    if n_in > len(resolved):
        status = "partial"
    elif used_api:
        status = "verified_api"
    else:
        status = "verified_local"
    return resolved, kept_tags, status


def _http_post_json(url: str, headers: dict[str, str], payload: dict) -> dict | None:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception:
        return None


def _generate_new_explore_tags(champion_tags: str, loser_tags: str) -> list[str]:
    """Subtractive prompt: pick 3 interests from top-20 library keys, excluding champion/loser tokens."""
    lib = _load_interest_lib()
    pool = sorted(lib.keys())[:20]
    banned = set()
    for part in re.split(r"[,，、\n]+", f"{champion_tags},{loser_tags}"):
        p = _normalize_lib_key(part)
        if p:
            banned.add(p)

    candidates = [k for k in pool if k not in banned]
    if not candidates:
        candidates = [k for k in sorted(lib.keys()) if k not in banned][:20]

    if AI_API_KEY and AI_BASE_URL and candidates:
        url = f"{AI_BASE_URL}/chat/completions"
        hdr = {"Authorization": f"Bearer {AI_API_KEY}", "Content-Type": "application/json"}
        prompt = (
            "請從以下已驗證清單中挑選恰好 3 個興趣（只輸出名稱，逗號分隔，勿加說明）：\n"
            + "、".join(candidates[:20])
            + "\n\n必須使用清單內詞彙，嚴禁自創拼寫。若清單不足 3 個則盡量輸出。"
        )
        js = _http_post_json(
            url,
            hdr,
            {
                "model": AI_MODEL_NAME,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.2,
            },
        )
        if js and isinstance(js.get("choices"), list) and js["choices"]:
            msg = js["choices"][0].get("message", {}) or {}
            text = str(msg.get("content", "") or "").strip()
            picked: list[str] = []
            for part in re.split(r"[,，、\n]+", text):
                p = part.strip()
                if p and _normalize_lib_key(p) in lib:
                    picked.append(p)
            if picked:
                return picked[:3]

    out: list[str] = []
    for k in candidates:
        out.append(k)
        if len(out) >= 3:
            break
    return out[:3]


def _is_pending_placeholder_row(r: dict) -> bool:
    name = str(r.get("廣告名稱", "") or "")
    return "PENDING" in name.upper()


def _audience_isolation_hint(strategy: str) -> str:
    if strategy == "BUN":
        return "BUN: Interests + EXCLUDE Traditional Chinese"
    return "HK: Lifestyle/Beauty + EXCLUDE Philippines/Tagalog/Expats"


def _percentile_labels_for_rank(x: int, y: int) -> tuple[str, bool, bool, bool]:
    """Returns (band_label, is_bottom5, is_explore, is_exploit). y = pool size, x = rank 1..y best..worst."""
    if y <= 0:
        return ("", False, False, False)
    top15_cut = max(1, math.ceil(0.15 * y))
    bottom5_n = max(1, math.ceil(0.05 * y))
    bottom20_n = max(1, math.ceil(0.20 * y))
    bottom40_n = max(1, math.ceil(0.40 * y))

    is_top15 = x <= top15_cut
    is_bottom5 = x > y - bottom5_n
    in_bottom20 = x > y - bottom20_n
    in_bottom40 = x > y - bottom40_n

    is_explore = in_bottom20 and (not is_bottom5)
    is_exploit = in_bottom40 and (not in_bottom20)

    if is_top15:
        band = "Top 15%"
    elif is_bottom5:
        band = "Bottom 5%"
    elif is_explore:
        band = "Explore 6–20%"
    elif is_exploit:
        band = "Exploit 21–40%"
    else:
        band = "中性區間"
    return (band, is_bottom5, is_explore, is_exploit)


UNKNOWN_SHOP = "Unknown"
MAP_FAIL_NOTE = "⚠️ 店名映射失敗，請檢查 SHOP_NAME_MAP"

OPERATION_HEADER = [
    "店名",
    "策略",
    "廣告名稱",
    "今日/7日/月均 CPC",
    "💰 今日已花 / 類別日限額",
    "店內 CPC 排名",
    "百分位區間",
    "決策優先級",
    "決策指令碼",
    "💡 指令原因",
    "現有日預算(來自Meta)",
    "📊 建議日預算(歸一化結果)",
    "冠軍標籤",
    "落後標籤",
    "探索新標籤",
    "AI_受眾建議_JSON",
    "受眾隔離標籤",
]


def _build_p00_rows_for_pending() -> list[list]:
    """Phase 4: one summary row per shop with entries in pending_tests.json (top of sheet)."""
    entries = load_pending_tests_entries()
    if not entries:
        return []
    n_cols = len(OPERATION_HEADER)
    by_shop: dict[str, list] = defaultdict(list)
    for e in entries:
        s = str(e.get("shop", "")).strip()
        if s:
            by_shop[s].append(e)
    rows: list[list] = []
    for shop in sorted(by_shop.keys()):
        items = by_shop[shop]
        parts = [f"post_id={e.get('post_id', '')} page={e.get('page_id', '')}" for e in items[:5]]
        suffix = f" …共 {len(items)} 筆" if len(items) > 5 else ""
        reason = (
            f"🧪 P00 待測新貼文：日池已預留 ${NEW_AD_TEST_BUDGET:.0f} 供測試。 "
            + ", ".join(parts)
            + suffix
        )
        row = [""] * n_cols
        row[0] = shop
        row[1] = "[P00]"
        row[2] = "（系統）待測新貼文 / 預算預留"
        row[3] = "- / - / -"
        row[4] = "-"
        row[5] = "N/A"
        row[6] = "N/A"
        row[7] = "P00"
        row[8] = "[指令: NO_ACTION]"
        row[9] = reason
        row[10] = "-"
        row[11] = "-"
        rows.append(row)
    return rows


def _build_action_rows(rows: list[dict]) -> list[list]:
    # 白名單：SHOP_CONFIGS 內店鋪；另保留 店名=="Unknown" 以利除錯（對照 Raw actor_id）
    whitelist = set(SHOP_CONFIGS.keys())
    filtered_rows = [
        r
        for r in rows
        if str(r.get("店名", "") or "").strip() in whitelist
        or str(r.get("店名", "") or "").strip() == UNKNOWN_SHOP
    ]
    rows = filtered_rows

    _, today_bun, today_hk = aggregate_shop_spend_from_rows(rows)
    out: list[list] = []
    account_min_budget = _to_float(rows[0].get("帳戶最低日預算(API)", 0)) if rows else 0.0
    tag_cache: dict[str, str] = {}

    def _tags_cached(tj: str) -> str:
        if tj not in tag_cache:
            tag_cache[tj] = _extract_audience_tags(tj)
        return tag_cache[tj]

    # B.1：店鋪活躍廣告數（7 日花費 > 0，排除 PENDING）
    shop_active_counts: dict[str, int] = defaultdict(int)
    for r in rows:
        if _is_pending_placeholder_row(r):
            continue
        if _to_float(r.get("7日花費", 0)) <= 0:
            continue
        shop = str(r.get("店名", "") or r.get("來源專頁", "") or "").strip() or "[其他]"
        shop_active_counts[shop] += 1

    # (店名, 策略) 池：僅 7 日花費 > 0 的列參與排名
    pool_rows: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for idx, r in enumerate(rows):
        if _is_pending_placeholder_row(r):
            continue
        if _to_float(r.get("7日花費", 0)) <= 0:
            continue
        shop = str(r.get("店名", "") or r.get("來源專頁", "") or "").strip() or "[其他]"
        ad_name = str(r.get("廣告名稱", ""))
        body = str(r.get("廣告文案", ""))
        created_time = str(r.get("created_time", ""))
        campaign_name = str(r.get("Campaign Name", ""))
        strategy = classify_strategy(ad_name, body, created_time, campaign_name, shop)
        ad_id = str(r.get("廣告ID", "") or "").strip()
        tj = str(r.get("targeting_json", "") or "")
        cpc_row = _to_float(r.get("7日平均 CPC", 0))
        spend_7 = _to_float(r.get("7日花費", 0))
        pool_rows[(shop, strategy)].append(
            {
                "idx": idx,
                "ad_id": ad_id,
                "cpc": cpc_row,
                "spend_7": spend_7,
                "targeting_json": tj,
            }
        )

    # 排名與冠軍標籤（僅店通過 B.1 且池大小 >= MIN_POOL_SIZE）
    rank_by_ad_id: dict[str, dict[str, Any]] = {}
    champion_tags_by_pool: dict[tuple[str, str], str] = {}

    for (shop, strategy), members in pool_rows.items():
        if shop_active_counts.get(shop, 0) < MIN_ACTIVE_ADS_PER_SHOP:
            continue
        y = len(members)
        if y < MIN_POOL_SIZE:
            continue
        members_sorted = sorted(
            members,
            key=lambda m: (m["cpc"], -m["spend_7"], m["ad_id"]),
        )
        champ_tj = str(members_sorted[0].get("targeting_json", "") or "")
        champion_tags_by_pool[(shop, strategy)] = _tags_cached(champ_tj)

        for rank_i, m in enumerate(members_sorted, start=1):
            band, is_b5, is_ex, is_exploit = _percentile_labels_for_rank(rank_i, y)
            aid = m["ad_id"]
            rank_by_ad_id[aid] = {
                "rank": rank_i,
                "pool_size": y,
                "band": band,
                "is_bottom5": is_b5,
                "is_explore": is_ex,
                "is_exploit": is_exploit,
            }

    adset_meta = aggregate_by_adset(rows)
    items_by_shop = build_pool_items_by_shop(adset_meta)

    suggested_by_adset: dict[str, float] = {}
    shop_checks: dict[str, dict] = {}
    for shop, items in items_by_shop.items():
        reserve = new_post_budget_reserve_for_shop(shop)
        s_map, check = weighted_pool_allocation(
            shop, items, account_min_budget=account_min_budget, new_post_budget_reserve=reserve
        )
        suggested_by_adset.update(s_map)
        shop_checks[shop] = check

    seen_budget_adsets: set[str] = set()
    expected_cols = len(OPERATION_HEADER)

    for idx, r in enumerate(rows):
        shop = str(r.get("店名", "") or r.get("來源專頁", "") or "").strip() or "[其他]"
        ad_name = str(r.get("廣告名稱", ""))
        body = str(r.get("廣告文案", ""))
        created_time = str(r.get("created_time", ""))
        campaign_name = str(r.get("Campaign Name", ""))
        adset_id = str(r.get("AdSet ID", "") or r.get("廣告ID", "") or f"adset_fallback_{idx}")
        ad_id = str(r.get("廣告ID", "") or "").strip()
        adset = adset_meta.get(adset_id)
        if adset is None:
            continue

        today_cpc = _to_float(r.get("今日 CPC", r.get("今 CPC", 0)))
        cpc_7d_adset = adset.weighted_cpc_7d
        cpc_month = adset.month_cpc
        today_spend = _to_float(r.get("今日花費", 0))
        current_daily_budget = adset.current_budget
        strategy = classify_strategy(ad_name, body, created_time, campaign_name, shop)
        target_cpc = get_dynamic_target_cpc(strategy)
        tj = str(r.get("targeting_json", "") or "")
        loser_tags = _tags_cached(tj)
        champ_tags = champion_tags_by_pool.get((shop, strategy), "")

        targets = daily_targets(shop)
        class_limit = targets["bun_target"] if strategy == "BUN" else targets["hk_target"]
        class_spend = today_bun.get(shop, 0.0) if strategy == "BUN" else today_hk.get(shop, 0.0)
        spend_vs_target = f"${class_spend:.0f} / ${class_limit:.0f}"
        suggested_budget = suggested_by_adset.get(adset_id, max(current_daily_budget, 50.0))

        curr_for_compare = current_daily_budget if current_daily_budget > 0 else suggested_budget
        budget_delta_ratio = abs(suggested_budget - curr_for_compare) / max(1.0, curr_for_compare)
        budget_reason = "7日權重歸一化分配結果，建議於 4 AM 統一調整。"
        if budget_delta_ratio < 0.10:
            suggested_budget = curr_for_compare
            budget_reason = "調整幅度低於 10%，維持原預算。"

        shop_check = shop_checks.get(shop, {})
        adset_min_floor = _to_float(shop_check.get("adset_min_floor", account_min_budget + 1.0))
        if strategy == "BUN" and shop_check.get("bun_underfunded_warning"):
            floor_warn = "⚠️ 門店總預算過低，不足以支撐所有廣告組合的最低運行門檻。"
        elif strategy != "BUN" and shop_check.get("hk_underfunded_warning"):
            floor_warn = "⚠️ 門店總預算過低，不足以支撐所有廣告組合的最低運行門檻。"
        else:
            floor_warn = ""
        floor_auto_adjust_note = (
            "🛡️ 已自動根據 API 調整至平台安全最低預算 (+1元)。"
            if curr_for_compare + 0.01 < adset_min_floor
            else ""
        )

        if adset_id in seen_budget_adsets:
            current_budget_display = "Shared"
            suggested_budget_display = "Shared"
        else:
            current_budget_display = f"${curr_for_compare:.0f}"
            suggested_budget_display = f"${suggested_budget:.0f}"
            seen_budget_adsets.add(adset_id)

        active_shop_n = shop_active_counts.get(shop, 0)
        pool_key = (shop, strategy)
        pool_n = len(pool_rows.get(pool_key, []))
        meta = rank_by_ad_id.get(ad_id) if ad_id else None

        if shop_active_counts.get(shop, 0) < MIN_ACTIVE_ADS_PER_SHOP:
            rank_cell = "N/A"
            band_cell = "N/A (樣本不足)"
            priority = "P4"
            cmd = "[指令: NO_ACTION]"
            reason_core = (
                f"🛡️ 整店活躍廣告不足 ({active_shop_n} < {MIN_ACTIVE_ADS_PER_SHOP})，跳過 P0–P2。"
            )
            new_explore_tags: list[str] = []
            ai_json_cell = ""
        elif pool_n < MIN_POOL_SIZE:
            rank_cell = "N/A"
            band_cell = "N/A (樣本不足)"
            priority = "P4"
            cmd = "[指令: NO_ACTION]"
            reason_core = f"🛡️ 樣本不足保護：({shop},{strategy}) 池僅 {pool_n} 支活躍廣告 (< {MIN_POOL_SIZE})，跳過 P0–P2。"
            new_explore_tags = []
            ai_json_cell = ""
        elif _is_new_ad(created_time):
            if meta:
                rank_cell = f"{meta['rank']}/{meta['pool_size']}"
                band_cell = meta["band"]
            elif _to_float(r.get("7日花費", 0)) <= 0:
                rank_cell = "N/A"
                band_cell = "N/A (無7日花費)"
            else:
                rank_cell = "N/A"
                band_cell = "N/A (無池內排名)"
            priority = "P4"
            cmd = "[指令: NO_ACTION]"
            reason_core = "🛡️ 新廣告保護期內，暫不觸發 P0–P2。"
            new_explore_tags = []
            ai_json_cell = ""
        else:
            if meta:
                rank_cell = f"{meta['rank']}/{meta['pool_size']}"
                band_cell = meta["band"]
            elif _to_float(r.get("7日花費", 0)) <= 0:
                rank_cell = "N/A"
                band_cell = "N/A (無7日花費)"
            else:
                rank_cell = "N/A"
                band_cell = "N/A (無池內排名)"

            cpc_breach = cpc_7d_adset > target_cpc
            bottom5 = bool(meta and meta.get("is_bottom5"))
            p0_triggers: list[str] = []
            if cpc_breach:
                p0_triggers.append(
                    f"[止損] 7日加權 CPC (${cpc_7d_adset:.2f}) 超過目標 (${target_cpc:.2f})"
                )
            if bottom5:
                p0_triggers.append("[汰換] 池內排名墊底 (Bottom 5%)")

            if p0_triggers:
                priority = "P0"
                cmd = "[指令: PAUSE_ONLY]"
                reason_core = "；".join(p0_triggers)
                new_explore_tags = []
                ai_json_cell = ""
            elif meta and meta.get("is_explore"):
                priority = "P1"
                cmd = "[指令: PAUSE_AND_DUPLICATE_AUDIENCE]"
                new_explore_tags = _generate_new_explore_tags(champ_tags, loser_tags)
                tag_ids, kept, st = validate_and_get_ids(new_explore_tags)
                payload = {
                    "action": "duplicate",
                    "tags": kept,
                    "tag_ids": tag_ids,
                    "status": st,
                }
                ai_json_cell = cmd + "\n" + json.dumps(payload, ensure_ascii=False)
                reason_core = "池內 Explore 區間：以冠軍池為參考產出探索受眾。"
            elif meta and meta.get("is_exploit"):
                priority = "P2"
                cmd = "[指令: PAUSE_AND_DUPLICATE_AUDIENCE]"
                raw_champ = [x.strip() for x in re.split(r"[,，、\n]+", champ_tags) if x.strip()]
                new_explore_tags = []
                tag_ids, kept, st = validate_and_get_ids(raw_champ)
                payload = {
                    "action": "duplicate",
                    "tags": kept,
                    "tag_ids": tag_ids,
                    "status": st,
                }
                ai_json_cell = cmd + "\n" + json.dumps(payload, ensure_ascii=False)
                reason_core = "池內 Exploit 區間：複製並採用冠軍標籤。"
            else:
                priority = "P4"
                cmd = "[指令: NO_ACTION]"
                reason_core = "維持觀察（未命中結構化止損／探索／開採條件）。"
                new_explore_tags = []
                ai_json_cell = ""

        # §B.3：活躍上限 — P1/P2 複製降級為僅暫停
        if cmd == "[指令: PAUSE_AND_DUPLICATE_AUDIENCE]" and active_shop_n >= MAX_RUNNING_ADS:
            cmd = "[指令: PAUSE_ONLY]"
            ai_json_cell = ""
            reason_core += f" 已達活躍上限 ({MAX_RUNNING_ADS})，只出不進。"

        if shop == UNKNOWN_SHOP:
            reason_core = f"{MAP_FAIL_NOTE} {reason_core}"

        reason_full = (
            " ".join(x for x in [floor_warn, floor_auto_adjust_note, reason_core, budget_reason] if x)
            + " 建議於 4 AM 執行，以免驚動學習階段。"
        )

        new_explore_cell = ", ".join(new_explore_tags) if new_explore_tags else ""

        row_out = [
            shop,
            f"[{strategy}]",
            ad_name,
            f"{today_cpc:.2f} / {cpc_7d_adset:.2f} / {cpc_month:.2f}",
            spend_vs_target,
            rank_cell,
            band_cell,
            priority,
            cmd,
            reason_full,
            current_budget_display,
            suggested_budget_display,
            champ_tags,
            loser_tags,
            new_explore_cell,
            ai_json_cell,
            _audience_isolation_hint(strategy),
            adset_id,
            today_spend,
        ]
        out.append(row_out)

    out.sort(key=lambda x: float(x[-1]) if x else 0.0, reverse=True)

    check_rows: list[list] = []
    shop_to_adset_ids: dict[str, set[str]] = defaultdict(set)
    for aid, meta in adset_meta.items():
        shop_to_adset_ids[meta.shop].add(aid)

    for shop, aids in shop_to_adset_ids.items():
        suggested_sum = sum(_to_float(suggested_by_adset.get(aid, 0.0)) for aid in aids)
        cfg = shop_checks.get(shop, {})
        target_total = float(cfg.get("total_target", suggested_sum))
        deviation = suggested_sum - target_total
        reserve = float(cfg.get("new_post_budget_reserve", 0) or 0)
        extra = f" | 新貼文預留 ${reserve:.0f}" if reserve > 0.01 else ""
        cr = [""] * expected_cols
        cr[0] = f"[核對] {shop}"
        cr[9] = f"建議總和 ${suggested_sum:.0f} / 設定目標 ${target_total:.0f} / 偏差 {deviation:+.1f}{extra}"
        check_rows.append(cr)

    cleaned = [row[:-2] for row in out]
    for data_row in cleaned:
        data_row.extend([""] * (expected_cols - len(data_row)))
    p00_rows = _build_p00_rows_for_pending()
    for pr in p00_rows:
        pr.extend([""] * (expected_cols - len(pr)))
    cleaned = p00_rows + cleaned
    cleaned.extend(check_rows)
    return cleaned


def main():
    ss = get_google_sheet()
    rows_raw = _read_raw_rows(ss)
    rows_refined = refine_raw_rows(rows_raw)
    ws_ref = _get_or_create_worksheet(ss, REFINED_SHEET_TAB)
    ws_ref.clear()
    ws_ref.update(range_name="A1", values=_refined_to_grid(rows_refined))
    print(f"✅ Refined 已更新: {REFINED_SHEET_TAB} ({len(rows_refined)} 筆)")

    action_rows = _build_action_rows(rows_refined)
    ws = _get_or_create_worksheet(ss, OUTPUT_TAB)
    ws.clear()
    ws.update(range_name="A1", values=[OPERATION_HEADER] + action_rows)
    print(f"✅ 操作清單已更新: {OUTPUT_TAB} ({len(action_rows)} 筆)")


if __name__ == "__main__":
    main()
