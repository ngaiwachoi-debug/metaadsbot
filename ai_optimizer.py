"""
讀取 Raw 工作表（callfrommeta 同步結果），寫入 Refined 工作表後，輸出：
- `AI_操作清單`：動態日預算與決策欄位；
- `AI_Action_Plan`（可 `ACTION_PLAN_TAB` 覆寫）：四區塊執行清單（新建／暫停／預算／受眾）。
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
from dataclasses import dataclass, field
from typing import Any

import gspread
from collections import defaultdict
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

from engine import (
    ENGINE_MIN_DAILY_BUDGET,
    GOOGLE_CREDENTIALS_PATH,
    NEW_AD_TEST_BUDGET,
    SHOP_CONFIGS,
    AdsetAggregate,
    _is_new_ad,
    _pool_name,
    adset_tier_key_for_rank,
    aggregate_by_adset,
    aggregate_shop_spend_from_rows,
    best_p00_template_adset_id,
    build_pool_items_by_shop,
    classify_strategy,
    detect_fatigue,
    effective_pool_limits,
    get_dynamic_target_cpc,
    get_tier_cuts,
    load_pending_tests_entries,
    pending_post_reserves_by_pool,
    trend_ratio,
    weighted_pool_allocation,
)
from meta_targeting import parse_targeting_details
from meta_utils import norm_meta_graph_id, to_float_minor, to_hkd_from_meta_minor
from shop_mapping import SHOP_NAME_MAP, map_shop_name

if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

_ROOT = os.path.dirname(os.path.abspath(__file__))


def _google_credentials_path() -> str:
    p = (GOOGLE_CREDENTIALS_PATH or "credentials.json").strip()
    return p if os.path.isabs(p) else os.path.join(_ROOT, p)


load_dotenv()
SHEET_NAME = os.getenv("SHEET_NAME", "AdSurvivor_Report")
RAW_SHEET_TAB = os.getenv("RAW_SHEET_TAB", "Sheet1")
REFINED_SHEET_TAB = os.getenv("REFINED_SHEET_TAB", "Sheet2")
OUTPUT_TAB = "AI_操作清單"
ACTION_PLAN_TAB = (os.getenv("ACTION_PLAN_TAB", "") or "AI_Action_Plan").strip() or "AI_Action_Plan"

MIN_ACTIVE_ADS_PER_SHOP = int(os.getenv("MIN_ACTIVE_ADS_PER_SHOP", "3") or 3)
MIN_POOL_SIZE = int(os.getenv("MIN_POOL_SIZE", "3") or 3)
MAX_RUNNING_ADS = int(os.getenv("MAX_RUNNING_ADS", "15") or 15)
try:
    BUDGET_NO_OP_THRESHOLD_DEFAULT = float(os.getenv("BUDGET_NO_OP_THRESHOLD_DEFAULT", "0.10") or 0.10)
except Exception:
    BUDGET_NO_OP_THRESHOLD_DEFAULT = 0.10
try:
    BUDGET_NO_OP_THRESHOLD_TOP = float(os.getenv("BUDGET_NO_OP_THRESHOLD_TOP", "0.15") or 0.15)
except Exception:
    BUDGET_NO_OP_THRESHOLD_TOP = 0.15


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if raw == "":
        return default
    return raw in ("1", "true", "yes", "on")


BLOCK_DOWNGRADE_CHAMPION_STRONG = _env_bool("BLOCK_DOWNGRADE_CHAMPION_STRONG", True)

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

UNKNOWN_SHOP = "Unknown"


def _is_unknown_shop_label(name: str) -> bool:
    s = (name or "").strip()
    return s == UNKNOWN_SHOP or s.startswith(f"{UNKNOWN_SHOP} (")


def _format_unknown_shop(aid: str, iid: str) -> str:
    parts: list[str] = []
    if aid:
        parts.append(f"page={aid}")
    if iid:
        parts.append(f"ig={iid}")
    if parts:
        return f"{UNKNOWN_SHOP} ({', '.join(parts)})"
    return UNKNOWN_SHOP


def get_google_sheet():
    cred_path = _google_credentials_path()
    if not os.path.isfile(cred_path):
        print(f"❌ Google 憑證檔不存在: {cred_path}")
        print("   請將 GCP 服務帳號 JSON 放到專案目錄並命名為 credentials.json，或在 .env 設定 GOOGLE_CREDENTIALS_PATH（可為絕對路徑）。")
        sys.exit(1)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(cred_path, scope)
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
    return norm_meta_graph_id(v)


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
        return _format_unknown_shop(aid, iid)
    return fuzzy if fuzzy else _format_unknown_shop(aid, iid)


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


# Pool-level copy for 受眾建議 / 受眾隔離 (do not concatenate; use separate columns + JSON).
_AUDIENCE_RECOMMENDATION: dict[str, str] = {
    "BUN": "BUN: Interests",
    "HK": "HK: Lifestyle/Beauty",
}
_AUDIENCE_EXCLUSION: dict[str, str] = {
    "BUN": "EXCLUDE Traditional Chinese",
    "HK": "EXCLUDE Philippines/Tagalog/Expats",
}


def _audience_hint_pool_key(strategy: str) -> str:
    """Map lane strategy (BUN / GENERAL / LTV / …) to BUN vs HK hint keys."""
    return "BUN" if strategy == "BUN" else "HK"


def _audience_recommendation_hint(strategy: str) -> str:
    return _AUDIENCE_RECOMMENDATION.get(_audience_hint_pool_key(strategy), "")


def _audience_exclusion_hint(strategy: str) -> str:
    return _AUDIENCE_EXCLUSION.get(_audience_hint_pool_key(strategy), "")


def _p00_champion_tags_for_pool(
    champion_tags_by_pool: dict[tuple[str, str], str] | None,
    shop: str,
    pool: str,
) -> str:
    """Champion interest labels for P00: BUN lane or HK (GENERAL then LTV)."""
    c = champion_tags_by_pool or {}
    pl = str(pool or "hk").lower()
    if pl == "bun":
        return (c.get((shop, "BUN"), "") or "").strip()
    g = (c.get((shop, "GENERAL"), "") or "").strip()
    if g:
        return g
    return (c.get((shop, "LTV"), "") or "").strip()


def _action_plan_audience_hint_json(
    strategy: str,
    *,
    exclusion: str,
    tags: str = "",
    note: str = "",
) -> str:
    """Structured audience hints for Action Plan / ops (executor ignores this column)."""
    obj: dict[str, str] = {
        "suggestion": _audience_recommendation_hint(strategy),
        "exclusion": exclusion,
    }
    if (tags or "").strip():
        obj["tags"] = tags.strip()
    if (note or "").strip():
        obj["note"] = note.strip()
    return json.dumps(obj, ensure_ascii=False)


def _tier_band_label_zh(tier_key: str) -> str:
    labels = {
        "champion": "冠軍組 Top 5%",
        "strong": "強勢組 Top 6–15%",
        "middle": "中性區間",
        "explore": "Explore 61–80%",
        "tail": "Tail 81–95%",
        "bottom": "Bottom 5%",
    }
    return labels.get((tier_key or "middle").lower(), "中性區間")


MAP_FAIL_NOTE = "⚠️ 店名映射失敗，請檢查 SHOP_NAME_MAP_PATH 或 SHOP_NAME_MAP"


@dataclass
class AdDecision:
    """Single ad row after engine rules (shared by AI_操作清單 + AI_Action_Plan)."""

    shop: str
    strategy: str
    ad_name: str
    ad_id: str
    adset_id: str
    today_cpc: float
    cpc_7d_adset: float
    cpc_month: float
    today_spend: float
    spend_vs_target: str
    rank_cell: str
    band_cell: str
    priority: str
    cmd: str
    reason_core: str
    reason_full: str
    current_budget_display: str
    suggested_budget_display: str
    curr_for_compare: float
    suggested_budget: float
    champ_tags: str
    loser_tags: str
    new_explore_tags: list[str] = field(default_factory=list)
    ai_json_cell: str = ""
    isolation: str = ""
    duplicate_intent: bool = False
    duplicate_kind: str = ""
    budget_reason: str = ""
    floor_warn: str = ""
    floor_auto_adjust_note: str = ""
    target_cpc: float = 0.0
    tier_key: str = "middle"
    page_id: str = ""
    campaign_id: str = ""


ACTION_PLAN_TITLE_NEW = "📋 新建廣告清單 (New Ads to Create)"
ACTION_PLAN_TITLE_PAUSE = "⏸️ 暫停廣告清單 (Ads to Pause)"
ACTION_PLAN_TITLE_BUDGET = "💰 預算調整清單 (Budget Adjustments)"
ACTION_PLAN_TITLE_AUDIENCE = "🏷️ 受眾標籤置換清單 (Audience Label Replacement)"

ACTION_PLAN_HEADER_NEW_ADS = [
    "店名",
    "策略",
    "複製自（原始Ad名稱）",
    "複製自AdSet ID",
    "目標廣告組合 ID",
    "Post或Object Story ID",
    "專頁 ID",
    "WhatsApp號碼",
    "宣傳活動 ID",
    "最後錯誤",
    "新廣告名稱建議",
    "新受眾標籤",
    "受眾隔離標籤",
    "AI_受眾建議_JSON",
    "預算建議",
    "創建方式",
]
ACTION_PLAN_HEADER_PAUSE = [
    "店名",
    "策略",
    "廣告名稱",
    "AdSet ID",
    "廣告 ID",
    "暫停原因",
    "停預算回收",
    "是否刪除素材",
]
ACTION_PLAN_HEADER_BUDGET = [
    "店名",
    "策略",
    "廣告名稱",
    "AdSet ID",
    "現有預算",
    "建議預算",
    "調整幅度",
    "調整原因",
    "優先級",
]
ACTION_PLAN_HEADER_AUDIENCE = [
    "店名",
    "策略",
    "廣告名稱",
    "AdSet ID",
    "舊受眾標籤",
    "新受眾標籤",
    "受眾隔離標籤",
    "受眾規模驗證(MAU)",
    "備註",
]


def _shorten_title_fragment(shop: str, ad_name: str, max_len: int = 36) -> str:
    s = (ad_name or "").strip()
    shop = (shop or "").strip()
    if shop and s.startswith(shop):
        s = s[len(shop) :].lstrip("-_/ \t")
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^\w\u4e00-\u9fff\-]+", "", s)
    s = s.strip("-") or "ad"
    if len(s) > max_len:
        s = s[: max_len - 1].rstrip("-") + "…"
    return s


def _suggested_duplicate_ad_name(shop: str, strategy: str, source_ad_name: str, kind: str) -> str:
    mid = "BUN" if strategy == "BUN" else "HK"
    frag = _shorten_title_fragment(shop, source_ad_name)
    suf = "-e" if kind == "explore" else "-x" if kind == "exploit" else ""
    return f"{shop}-{mid}-{frag}{suf}"


def _suggested_p00_ad_name(shop: str, post_id: str, pool: str = "hk") -> str:
    mid = "BUN" if str(pool).lower() == "bun" else "HK"
    tail = str(post_id or "").strip()
    tail = tail[-8:] if len(tail) > 8 else tail or "post"
    return f"{shop}-{mid}-new-{tail}"


def _p00_target_adset_id_for_shop_pool(shop: str, pool: str) -> str:
    """目標廣告組合 ID for P00 when not duplicated from an existing ad set (see SHOP_CONFIGS / config.json)."""
    conf = SHOP_CONFIGS.get(shop) or {}
    pl = str(pool or "hk").lower()
    if pl == "bun":
        raw = conf.get("p00_bun_adset_id") or conf.get("p00_target_adset_id")
    else:
        raw = conf.get("p00_hk_adset_id") or conf.get("p00_target_adset_id")
    return norm_meta_graph_id(str(raw or ""))


def _mau_cell_for_tags(tags: list[str]) -> str:
    if not tags:
        return "—"
    _tag_ids, _kept, st = validate_and_get_ids(tags)
    return f"{st} ({len(_tag_ids)} interest IDs)"


def _pad_grid_row(cells: list[str], width: int) -> list[str]:
    out = list(cells)
    while len(out) < width:
        out.append("")
    return out[:width]


def _action_plan_max_width() -> int:
    return max(
        len(ACTION_PLAN_HEADER_NEW_ADS),
        len(ACTION_PLAN_HEADER_PAUSE),
        len(ACTION_PLAN_HEADER_BUDGET),
        len(ACTION_PLAN_HEADER_AUDIENCE),
    )


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


def _build_p00_rows_for_pending(
    champion_tags_by_pool: dict[tuple[str, str], str] | None = None,
) -> list[list]:
    """Phase 4: one row per pending_tests.json entry (aligned with Action Plan + reserves)."""
    entries = load_pending_tests_entries(restrict_to_shop_configs=True)
    if not entries:
        return []
    n_cols = len(OPERATION_HEADER)
    by_shop_pool: dict[tuple[str, str], list] = defaultdict(list)
    for e in entries:
        s = str(e.get("shop", "")).strip()
        pool = str(e.get("pool", "hk") or "hk").lower()
        if s:
            by_shop_pool[(s, pool)].append(e)
    rows: list[list] = []
    for (shop, pool) in sorted(by_shop_pool.keys()):
        items = sorted(
            by_shop_pool[(shop, pool)],
            key=lambda x: str(x.get("created_time", "") or ""),
            reverse=True,
        )
        pool_label = "[BUN]" if pool == "bun" else "[HK]"
        strategy_for_hint = "BUN" if pool == "bun" else "GENERAL"
        for e in items:
            pid = str(e.get("post_id", "") or "")
            reason = (
                f"🧪 P00 待測新貼文 {pool_label}：{pool}池此筆預留 ${NEW_AD_TEST_BUDGET:.0f}。 post_id={pid}"
            )
            champ = _p00_champion_tags_for_pool(champion_tags_by_pool, shop, pool)
            excl = _audience_exclusion_hint(strategy_for_hint)
            hint_json = _action_plan_audience_hint_json(
                strategy_for_hint,
                exclusion=excl,
                tags=champ,
                note="繼承冠軍受眾 (Champion Inheritance)",
            )
            row = [""] * n_cols
            row[0] = shop
            row[1] = f"[P00] {pool_label}"
            row[2] = "（系統）待測新貼文 / 預算預留"
            row[3] = "- / - / -"
            row[4] = "-"
            row[5] = "N/A"
            row[6] = "N/A"
            row[7] = "P00"
            row[8] = "[指令: NO_ACTION]"
            row[9] = reason
            row[10] = "-"
            row[11] = f"${NEW_AD_TEST_BUDGET:.0f}"
            row[12] = champ or "—"
            row[15] = hint_json
            row[16] = excl
            rows.append(row)
    return rows


def _compute_ad_decisions(
    rows: list[dict],
) -> tuple[
    list[AdDecision],
    dict[str, AdsetAggregate],
    dict[str, float],
    dict[str, dict],
    float,
    dict[tuple[str, str], str],
]:
    whitelist = set(SHOP_CONFIGS.keys())
    filtered_rows = [
        r
        for r in rows
        if str(r.get("店名", "") or "").strip() in whitelist
        or _is_unknown_shop_label(str(r.get("店名", "") or ""))
    ]
    rows = filtered_rows

    _, today_bun, today_hk = aggregate_shop_spend_from_rows(rows)
    decisions: list[AdDecision] = []
    account_min_budget = _to_float(rows[0].get("帳戶最低日預算(API)", 0)) if rows else 0.0
    tag_cache: dict[str, str] = {}

    def _tags_cached(tj: str) -> str:
        if tj not in tag_cache:
            tag_cache[tj] = _extract_audience_tags(tj)
        return tag_cache[tj]

    shop_active_counts: dict[str, int] = defaultdict(int)
    for r in rows:
        if _is_pending_placeholder_row(r):
            continue
        if _to_float(r.get("7日花費", 0)) <= 0:
            continue
        shop = str(r.get("店名", "") or r.get("來源專頁", "") or "").strip() or "[其他]"
        shop_active_counts[shop] += 1

    adset_meta = aggregate_by_adset(rows)
    lane_adsets: dict[tuple[str, str], list[AdsetAggregate]] = defaultdict(list)
    for agg in adset_meta.values():
        if agg.spend_7d <= 0:
            continue
        lane_adsets[(agg.shop, agg.strategy)].append(agg)

    tier_by_adset: dict[str, str] = {}
    rank_meta_by_adset: dict[str, dict[str, Any]] = {}
    champion_tags_by_pool: dict[tuple[str, str], str] = {}
    lane_pool_size: dict[tuple[str, str], int] = {}
    tier_cuts = get_tier_cuts()

    for (shop, strategy), members in lane_adsets.items():
        if shop_active_counts.get(shop, 0) < MIN_ACTIVE_ADS_PER_SHOP:
            continue
        y = len(members)
        if y < MIN_POOL_SIZE:
            continue
        sorted_aggs = sorted(
            members,
            key=lambda a: (a.weighted_cpc_7d, -a.spend_7d, a.adset_id),
        )
        lane_pool_size[(shop, strategy)] = y
        champ_id = sorted_aggs[0].adset_id
        champ_tj = ""
        for r in rows:
            rid = str(r.get("AdSet ID", "") or r.get("廣告ID", "") or "")
            if rid != champ_id:
                continue
            tj = str(r.get("targeting_json", "") or "")
            if tj:
                champ_tj = tj
                break
        champion_tags_by_pool[(shop, strategy)] = _tags_cached(champ_tj)

        for rank_i, agg in enumerate(sorted_aggs, start=1):
            tk = adset_tier_key_for_rank(rank_i, y, tier_cuts)
            tier_by_adset[agg.adset_id] = tk
            rank_meta_by_adset[agg.adset_id] = {
                "rank": rank_i,
                "pool_size": y,
                "band": _tier_band_label_zh(tk),
                "tier_key": tk,
                "is_bottom5": tk == "bottom",
                "is_explore": tk == "explore",
                "is_exploit": tk == "tail",
            }

    items_by_shop = build_pool_items_by_shop(adset_meta, tier_by_adset)

    pause_only_adset_ids: set[str] = set()
    for agg in adset_meta.values():
        if agg.spend_7d <= 0:
            continue
        shop_n = shop_active_counts.get(agg.shop, 0)
        if shop_n < MIN_ACTIVE_ADS_PER_SHOP:
            continue
        pool_key = (agg.shop, agg.strategy)
        pool_sz = lane_pool_size.get(pool_key, 0)
        if pool_sz < MIN_POOL_SIZE:
            continue
        first_row = next(
            (r for r in rows
             if str(r.get("AdSet ID", "") or r.get("廣告ID", "") or "") == agg.adset_id),
            None,
        )
        if first_row and _is_new_ad(str(first_row.get("created_time", ""))):
            continue
        tier = tier_by_adset.get(agg.adset_id, "middle")
        target_cpc = get_dynamic_target_cpc(agg.strategy)
        cpc_breach = agg.weighted_cpc_7d > target_cpc
        is_bottom = tier == "bottom"
        if cpc_breach or is_bottom:
            pause_only_adset_ids.add(agg.adset_id)

    for shop in items_by_shop:
        items_by_shop[shop] = [
            x for x in items_by_shop[shop] if x.adset_id not in pause_only_adset_ids
        ]

    suggested_by_adset: dict[str, float] = {}
    shop_checks: dict[str, dict] = {}
    for shop, items in items_by_shop.items():
        rb, rh = pending_post_reserves_by_pool(shop)
        s_map, check = weighted_pool_allocation(
            shop, items, account_min_budget=account_min_budget,
            reserve_bun=rb, reserve_hk=rh,
        )
        suggested_by_adset.update(s_map)
        shop_checks[shop] = check

    for aid in pause_only_adset_ids:
        suggested_by_adset[aid] = 0.0

    seen_budget_adsets: set[str] = set()

    for idx, r in enumerate(rows):
        shop = str(r.get("店名", "") or r.get("來源專頁", "") or "").strip() or "[其他]"
        ad_name = str(r.get("廣告名稱", ""))
        body = str(r.get("廣告文案", ""))
        created_time = str(r.get("created_time", ""))
        campaign_name = str(r.get("Campaign Name", ""))
        adset_id = str(r.get("AdSet ID", "") or r.get("廣告ID", "") or f"adset_fallback_{idx}")
        ad_id = str(r.get("廣告ID", "") or "").strip()
        page_id_row = str(r.get("actor_id", "") or "").strip()
        campaign_id_row = str(r.get("Campaign ID", "") or "").strip()
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

        pool_limits = effective_pool_limits(shop)
        class_limit = pool_limits["bun_limit"] if strategy == "BUN" else pool_limits["hk_limit"]
        class_spend = today_bun.get(shop, 0.0) if strategy == "BUN" else today_hk.get(shop, 0.0)
        spend_vs_target = f"${class_spend:.0f} / ${class_limit:.0f}"
        suggested_budget = suggested_by_adset.get(
            adset_id, max(current_daily_budget, ENGINE_MIN_DAILY_BUDGET)
        )
        curr_for_compare = current_daily_budget if current_daily_budget > 0 else suggested_budget
        budget_reason = ""

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

        active_shop_n = shop_active_counts.get(shop, 0)
        pool_key = (shop, strategy)
        pool_n = len(lane_adsets.get(pool_key, []))
        meta = rank_meta_by_adset.get(adset_id)

        duplicate_intent = False
        duplicate_kind = ""

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
                duplicate_intent = True
                duplicate_kind = "explore"
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
                duplicate_intent = True
                duplicate_kind = "exploit"
                new_explore_tags = []
                raw_champ = [x.strip() for x in re.split(r"[,，、\n]+", champ_tags) if x.strip()]
                tag_ids, kept, st = validate_and_get_ids(raw_champ)
                payload = {
                    "action": "duplicate",
                    "tags": kept,
                    "tag_ids": tag_ids,
                    "status": st,
                }
                ai_json_cell = cmd + "\n" + json.dumps(payload, ensure_ascii=False)
                reason_core = "池內 Tail 區間：複製並採用冠軍標籤。"
            elif meta and meta.get("tier_key") in ("champion", "strong"):
                priority = "P3"
                cmd = "[指令: NO_ACTION]"
                reason_core = (
                    "冠軍組保護，建議維持或上調預算。"
                    if meta.get("tier_key") == "champion"
                    else "強勢組，建議維持預算。"
                )
                new_explore_tags = []
                ai_json_cell = ""
            else:
                priority = "P4"
                cmd = "[指令: NO_ACTION]"
                reason_core = "維持觀察（中性區間）。"
                new_explore_tags = []
                ai_json_cell = ""

        if cmd == "[指令: PAUSE_AND_DUPLICATE_AUDIENCE]" and active_shop_n >= MAX_RUNNING_ADS:
            cmd = "[指令: PAUSE_ONLY]"
            ai_json_cell = ""
            duplicate_intent = False
            duplicate_kind = ""
            reason_core += f" 已達活躍上限 ({MAX_RUNNING_ADS})，只出不進。"

        if _is_unknown_shop_label(shop):
            reason_core = f"{MAP_FAIL_NOTE} {reason_core}"

        if BLOCK_DOWNGRADE_CHAMPION_STRONG and meta and meta.get("tier_key") in ("champion", "strong"):
            if cmd != "[指令: PAUSE_ONLY]":
                suggested_budget = max(suggested_budget, curr_for_compare)

        th = (
            BUDGET_NO_OP_THRESHOLD_TOP
            if (meta and meta.get("tier_key") in ("champion", "strong"))
            else BUDGET_NO_OP_THRESHOLD_DEFAULT
        )
        budget_delta_ratio = abs(suggested_budget - curr_for_compare) / max(1.0, curr_for_compare)
        if budget_delta_ratio < th:
            suggested_budget = curr_for_compare
            budget_reason = f"調整幅度低於 {th:.0%}，維持原預算。"
        else:
            budget_reason = "7日權重歸一化分配結果（分層上限），建議於 4 AM 統一調整。"

        if adset_id in seen_budget_adsets:
            current_budget_display = "Shared"
            suggested_budget_display = "Shared" if adset_id not in pause_only_adset_ids else "將暫停"
        else:
            current_budget_display = f"${curr_for_compare:.0f}"
            if adset_id in pause_only_adset_ids:
                suggested_budget_display = "將暫停"
            elif abs(suggested_budget - curr_for_compare) < 0.01:
                suggested_budget_display = "維持"
            else:
                suggested_budget_display = f"${suggested_budget:.0f}"
            seen_budget_adsets.add(adset_id)

        if cmd == "[指令: PAUSE_ONLY]":
            reason_full = (
                " ".join(x for x in [floor_warn, floor_auto_adjust_note, reason_core] if x)
                + " 建議於 4 AM 執行，以免驚動學習階段。"
            )
        else:
            reason_full = (
                " ".join(x for x in [floor_warn, floor_auto_adjust_note, reason_core, budget_reason] if x)
                + " 建議於 4 AM 執行，以免驚動學習階段。"
            )

        isolation = _audience_exclusion_hint(strategy)
        _hints = json.dumps(
            {
                "suggestion": _audience_recommendation_hint(strategy),
                "exclusion": isolation,
            },
            ensure_ascii=False,
        )
        if (ai_json_cell or "").strip():
            ai_json_cell = f"{_hints}\n{ai_json_cell}"
        else:
            ai_json_cell = _hints
        tier_key_val = (meta or {}).get("tier_key", "middle")
        decisions.append(
            AdDecision(
                shop=shop,
                strategy=strategy,
                ad_name=ad_name,
                ad_id=ad_id,
                adset_id=adset_id,
                today_cpc=today_cpc,
                cpc_7d_adset=cpc_7d_adset,
                cpc_month=cpc_month,
                today_spend=today_spend,
                spend_vs_target=spend_vs_target,
                rank_cell=rank_cell,
                band_cell=band_cell,
                priority=priority,
                cmd=cmd,
                reason_core=reason_core,
                reason_full=reason_full,
                current_budget_display=current_budget_display,
                suggested_budget_display=suggested_budget_display,
                curr_for_compare=curr_for_compare,
                suggested_budget=suggested_budget,
                champ_tags=champ_tags,
                loser_tags=loser_tags,
                new_explore_tags=list(new_explore_tags),
                ai_json_cell=ai_json_cell,
                isolation=isolation,
                duplicate_intent=duplicate_intent,
                duplicate_kind=duplicate_kind,
                budget_reason=budget_reason,
                floor_warn=floor_warn,
                floor_auto_adjust_note=floor_auto_adjust_note,
                target_cpc=target_cpc,
                tier_key=str(tier_key_val),
                page_id=page_id_row,
                campaign_id=campaign_id_row,
            )
        )

    decisions.sort(key=lambda d: d.today_spend, reverse=True)
    return decisions, adset_meta, suggested_by_adset, shop_checks, account_min_budget, champion_tags_by_pool


def _build_action_plan_grid(
    refined_rows: list[dict],
    decisions: list[AdDecision],
    adset_meta: dict[str, AdsetAggregate],
    suggested_by_adset: dict[str, float],
    champion_tags_by_pool: dict[tuple[str, str], str] | None = None,
) -> list[list[str]]:
    w = _action_plan_max_width()
    grid: list[list[str]] = []

    entries = load_pending_tests_entries(restrict_to_shop_configs=True)
    by_shop_pool: dict[tuple[str, str], list] = defaultdict(list)
    for e in entries:
        s = str(e.get("shop", "") or "").strip()
        pool = str(e.get("pool", "hk") or "hk").lower()
        if s:
            by_shop_pool[(s, pool)].append(e)

    rows_new: list[list[str]] = []
    for (shop, pool) in sorted(by_shop_pool.keys()):
        items = sorted(
            by_shop_pool[(shop, pool)],
            key=lambda x: str(x.get("created_time", "") or ""),
            reverse=True,
        )
        pool_label = "[BUN]" if pool == "bun" else "[HK]"
        strategy_for_hint = "BUN" if pool == "bun" else "GENERAL"
        for e in items:
            pid = str(e.get("post_id", "") or "")
            actor = str(e.get("actor_id", "") or "").strip()
            page_for_tpl = norm_meta_graph_id(actor)
            if not page_for_tpl and "_" in pid:
                page_for_tpl = norm_meta_graph_id(pid.split("_", 1)[0])
            p00_template = best_p00_template_adset_id(
                refined_rows, adset_meta, shop, pool, page_for_tpl
            )
            champ = _p00_champion_tags_for_pool(champion_tags_by_pool, shop, pool)
            excl = _audience_exclusion_hint(strategy_for_hint)
            hint_json = _action_plan_audience_hint_json(
                strategy_for_hint,
                exclusion=excl,
                tags=champ,
                note="繼承冠軍受眾 (Champion Inheritance)",
            )
            rows_new.append(
                [
                    shop,
                    f"[P00] {pool_label}",
                    "—",
                    p00_template,
                    "",
                    pid,
                    actor,
                    "",
                    "",
                    "",
                    _suggested_p00_ad_name(shop, pid, pool),
                    champ or "—",
                    excl,
                    hint_json,
                    f"${NEW_AD_TEST_BUDGET:.0f}",
                    "NEW_FROM_POST",
                ]
            )

    for d in decisions:
        if not d.duplicate_intent or not d.duplicate_kind:
            continue
        new_tags = ", ".join(d.new_explore_tags) if d.duplicate_kind == "explore" else d.champ_tags
        budget_suggest = f"${NEW_AD_TEST_BUDGET:.0f}" if d.duplicate_kind == "explore" else d.suggested_budget_display
        dup_hint_json = _action_plan_audience_hint_json(
            d.strategy,
            exclusion=d.isolation,
            tags=new_tags,
        )
        rows_new.append(
            [
                d.shop,
                f"[{d.strategy}]",
                d.ad_name,
                d.adset_id,
                d.adset_id,
                "",
                d.page_id or "",
                "",
                d.campaign_id or "",
                "",
                _suggested_duplicate_ad_name(d.shop, d.strategy, d.ad_name, d.duplicate_kind),
                new_tags,
                d.isolation,
                dup_hint_json,
                budget_suggest,
                "DUPLICATE_WITH_NEW_AUDIENCE",
            ]
        )

    rows_pause: list[list[str]] = []
    for d in decisions:
        if d.cmd != "[指令: PAUSE_ONLY]":
            continue
        reclaim = f"日預算 {d.current_budget_display} | 今日 ${d.today_spend:.0f}"
        rows_pause.append(
            [
                d.shop,
                f"[{d.strategy}]",
                d.ad_name,
                d.adset_id,
                norm_meta_graph_id(d.ad_id) if d.ad_id else "",
                d.reason_core,
                reclaim,
                "否（保留素材）",
            ]
        )

    pause_ids = {d.adset_id for d in decisions if d.cmd == "[指令: PAUSE_ONLY]"}
    rep_ad: dict[str, AdDecision] = {}
    for d in decisions:
        if d.adset_id not in rep_ad:
            rep_ad[d.adset_id] = d

    rows_budget: list[list[str]] = []
    for adset_id, meta in adset_meta.items():
        if adset_id in pause_ids:
            continue
        curr = float(meta.current_budget or 0.0)
        suggested = float(suggested_by_adset.get(adset_id, curr))
        if curr <= 0:
            curr = suggested
        ratio = abs(suggested - curr) / max(1.0, curr)
        if ratio < BUDGET_NO_OP_THRESHOLD_DEFAULT:
            continue
        d0 = rep_ad.get(adset_id)
        shop = meta.shop
        strat = meta.strategy
        ad_name = d0.ad_name if d0 else ""
        delta = suggested - curr
        sign = "+" if delta >= 0 else ""
        rows_budget.append(
            [
                shop,
                f"[{strat}]",
                ad_name,
                adset_id,
                f"${curr:.0f}",
                f"${suggested:.0f}",
                f"{sign}${delta:.0f}",
                "7日權重歸一化",
                "P3",
            ]
        )

    rows_aud: list[list[str]] = []
    for d in decisions:
        if d.cmd == "[指令: PAUSE_ONLY]":
            continue
        if d.duplicate_intent and d.duplicate_kind == "explore":
            note = "Explore 複製：新受眾"
            rows_aud.append(
                [
                    d.shop,
                    f"[{d.strategy}]",
                    d.ad_name,
                    d.adset_id,
                    d.loser_tags,
                    ", ".join(d.new_explore_tags),
                    d.isolation,
                    _mau_cell_for_tags(d.new_explore_tags),
                    note,
                ]
            )
        elif d.duplicate_intent and d.duplicate_kind == "exploit":
            note = "Exploit 複製：冠軍標籤"
            raw_champ = [x.strip() for x in re.split(r"[,，、\n]+", d.champ_tags) if x.strip()]
            rows_aud.append(
                [
                    d.shop,
                    f"[{d.strategy}]",
                    d.ad_name,
                    d.adset_id,
                    d.loser_tags,
                    d.champ_tags,
                    d.isolation,
                    _mau_cell_for_tags(raw_champ),
                    note,
                ]
            )

        fatigue_label, _ = detect_fatigue(d.strategy, d.cpc_7d_adset, d.cpc_month, d.target_cpc)
        if "素材衰退" in fatigue_label and trend_ratio(d.cpc_7d_adset, d.cpc_month) > 1.25:
            alt = _generate_new_explore_tags(d.champ_tags, d.loser_tags)
            rows_aud.append(
                [
                    d.shop,
                    f"[{d.strategy}]",
                    d.ad_name,
                    d.adset_id,
                    d.loser_tags,
                    ", ".join(alt),
                    d.isolation,
                    _mau_cell_for_tags(alt),
                    "素材衰退（7日vs月均）：建議換素材並重測受眾",
                ]
            )

    sections: list[tuple[str, list[str], list[list[str]]]] = [
        (ACTION_PLAN_TITLE_NEW, ACTION_PLAN_HEADER_NEW_ADS, rows_new),
        (ACTION_PLAN_TITLE_PAUSE, ACTION_PLAN_HEADER_PAUSE, rows_pause),
        (ACTION_PLAN_TITLE_BUDGET, ACTION_PLAN_HEADER_BUDGET, rows_budget),
        (ACTION_PLAN_TITLE_AUDIENCE, ACTION_PLAN_HEADER_AUDIENCE, rows_aud),
    ]
    for title, header, data in sections:
        if not data:
            continue
        grid.append(_pad_grid_row([title] + [""] * (w - 1), w))
        grid.append(_pad_grid_row(header, w))
        for row in data:
            grid.append(_pad_grid_row(row, w))
        grid.append([""] * w)

    if grid and grid[-1] == [""] * w:
        grid.pop()
    return grid


def _build_operation_rows_from_decisions(
    decisions: list[AdDecision],
    adset_meta: dict[str, AdsetAggregate],
    suggested_by_adset: dict[str, float],
    shop_checks: dict[str, dict],
    *,
    champion_tags_by_pool: dict[tuple[str, str], str] | None = None,
) -> list[list]:
    expected_cols = len(OPERATION_HEADER)
    out: list[list] = []
    for d in decisions:
        new_explore_cell = ", ".join(d.new_explore_tags) if d.new_explore_tags else ""
        out.append(
            [
                d.shop,
                f"[{d.strategy}]",
                d.ad_name,
                f"{d.today_cpc:.2f} / {d.cpc_7d_adset:.2f} / {d.cpc_month:.2f}",
                d.spend_vs_target,
                d.rank_cell,
                d.band_cell,
                d.priority,
                d.cmd,
                d.reason_full,
                d.current_budget_display,
                d.suggested_budget_display,
                d.champ_tags,
                d.loser_tags,
                new_explore_cell,
                d.ai_json_cell,
                d.isolation,
                d.adset_id,
                d.today_spend,
            ]
        )

    check_rows: list[list] = []
    shop_to_adset_ids: dict[str, set[str]] = defaultdict(set)
    for aid, meta in adset_meta.items():
        shop_to_adset_ids[meta.shop].add(aid)

    for shop, aids in shop_to_adset_ids.items():
        suggested_sum = sum(_to_float(suggested_by_adset.get(aid, 0.0)) for aid in aids)
        cfg = shop_checks.get(shop, {})
        target_gross = float(cfg.get("total_target", suggested_sum))
        reserve = float(cfg.get("new_post_budget_reserve", 0) or 0)
        deviation = suggested_sum + reserve - target_gross
        alloc = float(cfg.get("total_allocatable", suggested_sum))
        bun_s = float(cfg.get("bun_suggested", 0) or 0)
        hk_s = float(cfg.get("hk_suggested", 0) or 0)
        engine_sum = float(cfg.get("total_suggested", bun_s + hk_s) or 0)
        extra_reserve = f" | 新貼文預留 ${reserve:.0f}" if reserve > 0.01 else ""
        mismatch_warn = (
            f" ⚠與分池合 ${engine_sum:.0f} 差>1.5"
            if abs(suggested_sum - engine_sum) > 1.5
            else ""
        )
        cr = [""] * expected_cols
        cr[0] = f"[核對] {shop}"
        cr[9] = (
            f"廣告組加總 ${suggested_sum:.0f} (BUN ${bun_s:.0f} + HK ${hk_s:.0f}) | "
            f"可分配 ${alloc:.0f} | 店日總 ${target_gross:.0f}{extra_reserve} | "
            f"偏差(加總+預留-店總) {deviation:+.1f}{mismatch_warn} — "
            f"勿對「📊建議」逐列SUM(同廣告組多列僅首列為金額)"
        )
        check_rows.append(cr)

    cleaned = [row[:-2] for row in out]
    for data_row in cleaned:
        data_row.extend([""] * (expected_cols - len(data_row)))
    p00_rows = _build_p00_rows_for_pending(champion_tags_by_pool)
    for pr in p00_rows:
        pr.extend([""] * (expected_cols - len(pr)))
    cleaned = p00_rows + cleaned
    cleaned.extend(check_rows)
    return cleaned


def _build_action_rows(rows: list[dict]) -> list[list]:
    d, meta, sug, chk, _, champ = _compute_ad_decisions(rows)
    return _build_operation_rows_from_decisions(
        d, meta, sug, chk, champion_tags_by_pool=champ
    )


def main():
    ss = get_google_sheet()
    rows_raw = _read_raw_rows(ss)
    rows_refined = refine_raw_rows(rows_raw)
    ws_ref = _get_or_create_worksheet(ss, REFINED_SHEET_TAB)
    ws_ref.clear()
    ws_ref.update(range_name="A1", values=_refined_to_grid(rows_refined))
    print(f"✅ Refined 已更新: {REFINED_SHEET_TAB} ({len(rows_refined)} 筆)")

    (
        decisions,
        adset_meta,
        suggested_by_adset,
        shop_checks,
        _,
        champion_tags_by_pool,
    ) = _compute_ad_decisions(rows_refined)
    action_rows = _build_operation_rows_from_decisions(
        decisions,
        adset_meta,
        suggested_by_adset,
        shop_checks,
        champion_tags_by_pool=champion_tags_by_pool,
    )
    ws = _get_or_create_worksheet(ss, OUTPUT_TAB)
    ws.clear()
    ws.update(range_name="A1", values=[OPERATION_HEADER] + action_rows)
    print(f"✅ 操作清單已更新: {OUTPUT_TAB} ({len(action_rows)} 筆)")

    plan_grid = _build_action_plan_grid(
        rows_refined,
        decisions,
        adset_meta,
        suggested_by_adset,
        champion_tags_by_pool,
    )
    ws_plan = _get_or_create_worksheet(ss, ACTION_PLAN_TAB)
    ws_plan.clear()
    if plan_grid:
        ws_plan.update(range_name="A1", values=plan_grid)
        print(f"✅ Action Plan 已更新: {ACTION_PLAN_TAB} ({len(plan_grid)} 列)")
    else:
        print(f"ℹ️ Action Plan 無資料區塊，已清空: {ACTION_PLAN_TAB}")


if __name__ == "__main__":
    main()
