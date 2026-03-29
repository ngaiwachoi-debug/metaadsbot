from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv

load_dotenv()


def _parse_json_env(name: str, default: Any) -> Any:
    raw = os.getenv(name, "")
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


STRATEGY_TARGET_CPC = _parse_json_env(
    "STRATEGY_TARGET_CPC",
    {"LTV": 35.0, "BUN": 1.5, "GENERAL": 18.0, "NEW": 20.0},
)
SHOP_CONFIGS = _parse_json_env("SHOP_CONFIGS", {})
LTV_KEYWORDS = [x.strip().lower() for x in os.getenv("LTV_KEYWORDS", "Gentlelase,755,脫毛").split(",") if x.strip()]
NEW_AD_PROTECTION_HOURS = int(os.getenv("NEW_AD_PROTECTION_HOURS", "72") or 72)
try:
    LTV_VALUE_MULTIPLIER = float(os.getenv("LTV_VALUE_MULTIPLIER", "2.0") or 2.0)
except Exception:
    LTV_VALUE_MULTIPLIER = 2.0
try:
    _min_budget_env = float(os.getenv("MIN_DAILY_BUDGET", "8") or 8)
except Exception:
    _min_budget_env = 8.0
DEFAULT_ADSET_MIN_FLOOR = _min_budget_env + 1.0
ENGINE_MIN_DAILY_BUDGET = _min_budget_env

DEFAULT_TIER_CUTS: dict[str, float] = {
    "champion": 0.05,
    "strong": 0.15,
    "middle": 0.60,
    "explore": 0.80,
    "tail": 0.95,
}

try:
    LTV_BUDGET_WEIGHT = float(os.getenv("LTV_BUDGET_WEIGHT", "1.5") or 1.5)
except Exception:
    LTV_BUDGET_WEIGHT = 1.5
try:
    BUDGET_CAP_MIDDLE = float(os.getenv("BUDGET_CAP_MIDDLE", "1.2") or 1.2)
except Exception:
    BUDGET_CAP_MIDDLE = 1.2
try:
    BUDGET_CAP_CHAMPION_STRONG = float(os.getenv("BUDGET_CAP_CHAMPION_STRONG", "1.5") or 1.5)
except Exception:
    BUDGET_CAP_CHAMPION_STRONG = 1.5
try:
    ALLOC_RESIDUAL_TOP_FRACTION = float(os.getenv("ALLOC_RESIDUAL_TOP_FRACTION", "0.5") or 0.5)
except Exception:
    ALLOC_RESIDUAL_TOP_FRACTION = 0.5
ALLOC_RESIDUAL_TOP_FRACTION = min(1.0, max(0.01, ALLOC_RESIDUAL_TOP_FRACTION))


def get_tier_cuts() -> dict[str, float]:
    merged = dict(DEFAULT_TIER_CUTS)
    raw = _parse_json_env("TIER_CUTS", {})
    if isinstance(raw, dict):
        for k, v in raw.items():
            try:
                merged[str(k)] = float(v)
            except (TypeError, ValueError):
                pass
    return merged


def adset_tier_key_for_rank(rank: int, y: int, cuts: dict[str, float] | None = None) -> str:
    """rank 1 = best (lowest CPC). Returns tier key: champion|strong|middle|explore|tail|bottom."""
    if y <= 0 or rank < 1:
        return "middle"
    c = cuts if cuts is not None else get_tier_cuts()
    r_ch = max(1, math.ceil(y * float(c.get("champion", 0.05))))
    r_st = max(r_ch, math.ceil(y * float(c.get("strong", 0.15))))
    r_mid = max(r_st, math.ceil(y * float(c.get("middle", 0.60))))
    r_exp = max(r_mid, math.ceil(y * float(c.get("explore", 0.80))))
    r_tail = max(r_exp, math.ceil(y * float(c.get("tail", 0.95))))
    if rank <= r_ch:
        return "champion"
    if rank <= r_st:
        return "strong"
    if rank <= r_mid:
        return "middle"
    if rank <= r_exp:
        return "explore"
    if rank <= r_tail:
        return "tail"
    return "bottom"

try:
    NEW_AD_TEST_BUDGET = float(os.getenv("NEW_AD_TEST_BUDGET", "150") or 150)
except Exception:
    NEW_AD_TEST_BUDGET = 150.0

_ENGINE_DIR = os.path.dirname(os.path.abspath(__file__))
PENDING_TESTS_JSON_PATH = os.path.join(_ENGINE_DIR, "pending_tests.json")


def load_pending_tests_entries() -> list[dict[str, Any]]:
    """Phase 4: `pending_tests.json` from `check_latest_posts.py` — shops with unpromoted recent posts."""
    if not os.path.isfile(PENDING_TESTS_JSON_PATH):
        return []
    try:
        with open(PENDING_TESTS_JSON_PATH, encoding="utf-8") as f:
            raw = json.load(f)
        entries = raw if isinstance(raw, list) else []
    except Exception:
        return []
    for e in entries:
        if not e.get("pool"):
            msg = str(e.get("message", "") or "")
            ct = str(e.get("created_time", "") or "")
            shop = str(e.get("shop", "") or "")
            if msg:
                strategy = classify_strategy("", msg, ct, "", shop)
                e["pool"] = _pool_name(strategy)
            else:
                e["pool"] = "hk"
    return entries


def shop_has_pending_post_test(shop_name: str) -> bool:
    sn = str(shop_name or "").strip()
    if not sn:
        return False
    for e in load_pending_tests_entries():
        if str(e.get("shop", "")).strip() == sn:
            return True
    return False


def new_post_budget_reserve_for_shop(shop_name: str) -> float:
    """Reserve one block of NEW_AD_TEST_BUDGET per shop while pending post tests exist."""
    return NEW_AD_TEST_BUDGET if shop_has_pending_post_test(shop_name) else 0.0


def pending_post_reserves_by_pool(shop_name: str) -> tuple[float, float]:
    """Return (reserve_bun, reserve_hk) for a shop based on pending_tests.json entries.

    Each pending entry contributes NEW_AD_TEST_BUDGET to its pool.
    """
    sn = str(shop_name or "").strip()
    if not sn:
        return 0.0, 0.0
    reserve_bun = 0.0
    reserve_hk = 0.0
    for e in load_pending_tests_entries():
        if str(e.get("shop", "")).strip() != sn:
            continue
        pool = str(e.get("pool", "hk") or "hk").lower()
        if pool == "bun":
            reserve_bun += NEW_AD_TEST_BUDGET
        else:
            reserve_hk += NEW_AD_TEST_BUDGET
    return reserve_bun, reserve_hk


def _to_float(v) -> float:
    s = str(v or "").strip().replace(",", "").replace("$", "")
    try:
        return float(s)
    except Exception:
        try:
            return float("".join(ch for ch in s if ch in "0123456789.-"))
        except Exception:
            return 0.0


def classify_strategy(
    ad_name: str, body: str, created_time: str, campaign_name: str = "", shop_name: str = ""
) -> str:
    name = (ad_name or "").upper()
    campaign = (campaign_name or "").upper()
    text_l = f"{(ad_name or '').lower()} {(body or '').lower()}"
    bun_enabled = True
    if shop_name:
        bun_enabled = daily_targets(shop_name).get("bun_target", 0.0) > 0
    # BUN 優先：Campaign 或 Ad 名稱只要命中即歸 BUN
    if bun_enabled and ("BUN" in campaign or "BUN" in name):
        return "BUN"
    # LTV 只在非 BUN 下生效
    if any(k in text_l for k in LTV_KEYWORDS):
        return "LTV"
    # 全英文文案（不含中文）作為 BUN 輔助條件
    if bun_enabled and (body or "").strip() and not re.search(r"[\u4e00-\u9fff]", body or ""):
        return "BUN"
    if _is_new_ad(created_time):
        return "NEW"
    return "GENERAL"


def _is_new_ad(created_time: str) -> bool:
    if not created_time:
        return False
    try:
        dt = datetime.strptime(created_time, "%Y-%m-%dT%H:%M:%S%z")
        delta_hours = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 3600
        return delta_hours < NEW_AD_PROTECTION_HOURS
    except Exception:
        return False


def _shop_config(shop_name: str) -> tuple[float, float]:
    conf = SHOP_CONFIGS.get(shop_name, {"total": 500, "bun_ratio": 0.2})
    total = _to_float(conf.get("total", 500)) or 500.0
    bun_ratio = _to_float(conf.get("bun_ratio", 0.2))
    bun_ratio = min(1.0, max(0.0, bun_ratio))
    return total, bun_ratio


def daily_targets(shop_name: str) -> dict[str, float]:
    total, bun_ratio = _shop_config(shop_name)
    return {
        "total": total,
        "bun_target": total * bun_ratio,
        "hk_target": total * (1 - bun_ratio),
    }


def effective_pool_limits(shop_name: str) -> dict[str, float]:
    """Gross pool caps from SHOP_CONFIGS minus P00 reserves (per pending_tests.json pool)."""
    t = daily_targets(shop_name)
    rb, rh = pending_post_reserves_by_pool(shop_name)
    rb = max(0.0, _to_float(rb))
    rh = max(0.0, _to_float(rh))
    return {
        "shop_daily_cap": t["total"],
        "bun_limit": max(0.0, t["bun_target"] - rb),
        "hk_limit": max(0.0, t["hk_target"] - rh),
        "reserve_bun": rb,
        "reserve_hk": rh,
    }


def aggregate_shop_spend_from_rows(rows: list[dict]) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    today_total: dict[str, float] = {}
    today_bun: dict[str, float] = {}
    today_hk: dict[str, float] = {}
    for r in rows:
        shop = str(r.get("店名", "") or r.get("來源專頁", "") or "").strip() or "[其他]"
        spend = _to_float(r.get("今日花費", 0))
        strategy = classify_strategy(
            str(r.get("廣告名稱", "")),
            str(r.get("廣告文案", "")),
            str(r.get("created_time", "")),
            str(r.get("Campaign Name", "")),
            shop,
        )
        today_total[shop] = today_total.get(shop, 0.0) + spend
        if strategy == "BUN":
            today_bun[shop] = today_bun.get(shop, 0.0) + spend
        else:
            today_hk[shop] = today_hk.get(shop, 0.0) + spend
    return today_total, today_bun, today_hk


def get_dynamic_target_cpc(strategy: str) -> float:
    general_target = _to_float(STRATEGY_TARGET_CPC.get("GENERAL", 18.0))
    if strategy == "LTV":
        return general_target * max(1.0, LTV_VALUE_MULTIPLIER)
    return _to_float(STRATEGY_TARGET_CPC.get(strategy, general_target))


def trend_ratio(cpc_7d: float, cpc_month: float) -> float:
    return cpc_7d / max(0.1, cpc_month)


def detect_fatigue(strategy: str, cpc_7d: float, cpc_month: float, target: float) -> tuple[str, str]:
    ratio = trend_ratio(cpc_7d, cpc_month)
    # 所有診斷以 7日 vs 月均 趨勢為核心
    if ratio <= 1.1 and cpc_7d <= target:
        return "✅ 表現穩定", "此建議基於月度趨勢對比，建議於 4 AM 統一調整。"
    if ratio > 1.25:
        reason = "近一週成本明顯高於本月平均，建議更換新素材以重啟點擊率。此建議基於月度趨勢對比，建議於 4 AM 統一調整。"
        return "📝 素材衰退 (建議更新圖文)", reason
    if ratio > 1.1 and cpc_7d > target:
        return "🎭 建議 A/B Test (受眾可能不準)", "此建議基於月度趨勢對比，建議於 4 AM 統一調整。"
    return "🔽 下調日預算設定", "此建議基於月度趨勢對比，建議於 4 AM 統一調整。"


@dataclass
class AdsetPoolItem:
    adset_id: str
    shop: str
    strategy: str
    current_budget: float
    cpc_7d: float
    tier: str = "middle"


@dataclass
class AdsetAggregate:
    adset_id: str
    shop: str
    strategy: str
    current_budget: float
    spend_7d: float
    clicks_7d: float
    weighted_cpc_7d: float
    month_cpc: float
    today_spend: float


def aggregate_by_adset(rows: list[dict]) -> dict[str, AdsetAggregate]:
    grouped: dict[str, list[dict]] = {}
    for idx, r in enumerate(rows):
        adset_id = str(r.get("AdSet ID", "") or r.get("廣告ID", "") or f"adset_fallback_{idx}")
        grouped.setdefault(adset_id, []).append(r)

    result: dict[str, AdsetAggregate] = {}
    for adset_id, bucket in grouped.items():
        first = bucket[0]
        shop = str(first.get("店名", "") or first.get("來源專頁", "") or "").strip() or "[其他]"
        strategies = [
            classify_strategy(
                str(x.get("廣告名稱", "")),
                str(x.get("廣告文案", "")),
                str(x.get("created_time", "")),
                str(x.get("Campaign Name", "")),
                shop,
            )
            for x in bucket
        ]
        strategy = "BUN" if "BUN" in strategies else ("LTV" if "LTV" in strategies else "GENERAL")

        spend_7d = sum(_to_float(x.get("7日花費", 0)) for x in bucket)
        clicks_7d = sum(_to_float(x.get("7日點擊", 0)) for x in bucket)
        weighted_cpc_7d = (spend_7d / clicks_7d) if clicks_7d > 0 else _to_float(first.get("7日平均 CPC", 0))
        month_vals = [_to_float(x.get("本月平均 CPC", 0)) for x in bucket]
        month_cpc = sum(month_vals) / max(1, len(month_vals))
        current_budget = _to_float(first.get("現有日預算", 0))
        if current_budget <= 0:
            current_budget = 100.0
        today_spend = sum(_to_float(x.get("今日花費", 0)) for x in bucket)

        result[adset_id] = AdsetAggregate(
            adset_id=adset_id,
            shop=shop,
            strategy=strategy,
            current_budget=current_budget,
            spend_7d=spend_7d,
            clicks_7d=clicks_7d,
            weighted_cpc_7d=max(0.01, weighted_cpc_7d),
            month_cpc=month_cpc,
            today_spend=today_spend,
        )
    return result


def _pool_name(strategy: str) -> str:
    return "bun" if strategy == "BUN" else "hk"


def build_pool_items_by_shop(
    adsets: dict[str, AdsetAggregate],
    tier_by_adset: dict[str, str] | None = None,
) -> dict[str, list[AdsetPoolItem]]:
    tier_by_adset = tier_by_adset or {}
    items_by_shop: dict[str, list[AdsetPoolItem]] = {}
    for adset in adsets.values():
        tier = str(tier_by_adset.get(adset.adset_id, "middle") or "middle").lower()
        items_by_shop.setdefault(adset.shop, []).append(
            AdsetPoolItem(
                adset_id=adset.adset_id,
                shop=adset.shop,
                strategy=adset.strategy,
                current_budget=adset.current_budget,
                cpc_7d=adset.weighted_cpc_7d,
                tier=tier,
            )
        )
    return items_by_shop


def weighted_pool_allocation(
    shop_name: str,
    items: list[AdsetPoolItem],
    account_min_budget: float | None = None,
    reserve_bun: float = 0.0,
    reserve_hk: float = 0.0,
) -> tuple[dict[str, float], dict[str, Any]]:
    """
    AdSet Budget Allocation with Floor Guarantee and tier-aware caps/floors.
    HK pool: LTV AdSets get LTV_BUDGET_WEIGHT on 1/cpc scores.
    Residual fill/trim uses ALLOC_RESIDUAL_TOP_FRACTION.
    Reserves are subtracted from the correct pool target (BUN or HK).
    """
    total, bun_ratio = _shop_config(shop_name)
    rb = max(0.0, _to_float(reserve_bun))
    rh = max(0.0, _to_float(reserve_hk))
    bun_alloc = max(0.0, total * bun_ratio - rb)
    hk_alloc = max(0.0, total * (1.0 - bun_ratio) - rh)
    net_for_single_pool = max(0.0, total - rb - rh)
    adset_min_floor = max(
        1.0,
        (_to_float(account_min_budget) + 1.0) if account_min_budget is not None else DEFAULT_ADSET_MIN_FLOOR,
    )
    by_pool = {"bun": [], "hk": []}
    for x in items:
        by_pool[_pool_name(x.strategy)].append(x)

    suggestions: dict[str, float] = {}

    def _tier_cap_mult(tier: str) -> float:
        tnorm = (tier or "middle").lower()
        if tnorm in ("champion", "strong"):
            if BUDGET_CAP_CHAMPION_STRONG <= 0.01:
                return 1e9
            return max(1.0, BUDGET_CAP_CHAMPION_STRONG)
        if tnorm == "middle":
            return max(1.0, BUDGET_CAP_MIDDLE)
        return 1.0

    def alloc(pool_items: list[AdsetPoolItem], pool_total: float, is_hk_pool: bool) -> bool:
        if not pool_items:
            return False

        for i in pool_items:
            if i.current_budget <= 0:
                i.current_budget = 100.0

        min_budget: dict[str, float] = {}
        for x in pool_items:
            aid = x.adset_id
            tier = (x.tier or "middle").lower()
            if tier in ("champion", "strong"):
                min_budget[aid] = max(adset_min_floor, x.current_budget)
            else:
                min_budget[aid] = adset_min_floor

        floor_total = sum(min_budget[x.adset_id] for x in pool_items)
        underfunded_warning = pool_total < floor_total - 0.01

        for x in pool_items:
            suggestions[x.adset_id] = min_budget[x.adset_id]

        remaining = pool_total - floor_total

        scores: dict[str, float] = {}
        for x in pool_items:
            base = 1.0 / max(0.1, x.cpc_7d)
            if is_hk_pool and x.strategy == "LTV":
                base *= LTV_BUDGET_WEIGHT
            scores[x.adset_id] = base
        sum_scores = sum(scores.values()) or 1.0

        upper: dict[str, float] = {}
        for x in pool_items:
            aid = x.adset_id
            upper[aid] = x.current_budget * _tier_cap_mult(x.tier)

        if remaining > 0.01 and not underfunded_warning:
            for x in pool_items:
                aid = x.adset_id
                raw = remaining * (scores[aid] / sum_scores)
                suggestions[aid] += raw

        for x in pool_items:
            aid = x.adset_id
            suggestions[aid] = min(suggestions[aid], upper[aid])

        current_sum = sum(suggestions[x.adset_id] for x in pool_items)
        delta = pool_total - current_sum

        ranked_best = sorted(pool_items, key=lambda z: z.cpc_7d)
        ranked_worst = list(reversed(ranked_best))
        frac = ALLOC_RESIDUAL_TOP_FRACTION

        if delta > 0.01:
            top_n = max(1, math.ceil(len(ranked_best) * frac))
            top = ranked_best[:top_n]
            for _ in range(30):
                if delta <= 0.01:
                    break
                progressed = False
                share = delta / max(1, len(top))
                for z in top:
                    aid = z.adset_id
                    cap = upper[aid] - suggestions[aid]
                    if cap <= 0:
                        continue
                    add = min(share, cap)
                    if add > 0:
                        suggestions[aid] += add
                        delta -= add
                        progressed = True
                if not progressed:
                    break
        elif delta < -0.01:
            need_cut = -delta
            tail_n = max(1, math.ceil(len(ranked_worst) * frac))
            tail = ranked_worst[:tail_n]
            for _ in range(30):
                if need_cut <= 0.01:
                    break
                progressed = False
                share = need_cut / max(1, len(tail))
                for z in tail:
                    aid = z.adset_id
                    floor_i = min_budget[aid]
                    room = suggestions[aid] - floor_i
                    if room <= 0:
                        continue
                    cut = min(share, room)
                    if cut > 0:
                        suggestions[aid] -= cut
                        need_cut -= cut
                        progressed = True
                if not progressed:
                    break
        if delta > 0.01:
            top_n = max(1, math.ceil(len(ranked_best) * frac))
            top = ranked_best[:top_n]
            for _ in range(30):
                if delta <= 0.01:
                    break
                share = delta / max(1, len(top))
                for z in top:
                    aid = z.adset_id
                    suggestions[aid] += share
                    delta -= share
                    if delta <= 0.01:
                        break

        return underfunded_warning

    bun_pool_total = bun_alloc
    hk_pool_total = hk_alloc
    if not by_pool["bun"]:
        bun_pool_total = 0.0
        hk_pool_total = net_for_single_pool

    bun_under = alloc(by_pool["bun"], bun_pool_total, False)
    hk_under = alloc(by_pool["hk"], hk_pool_total, True)

    bun_s = sum(suggestions[x.adset_id] for x in by_pool["bun"]) if by_pool["bun"] else 0.0
    hk_s = sum(suggestions[x.adset_id] for x in by_pool["hk"]) if by_pool["hk"] else 0.0
    reserve_total = rb + rh
    total_allocatable = bun_pool_total + hk_pool_total
    check: dict[str, Any] = {
        "bun_suggested": bun_s,
        "bun_target": bun_pool_total,
        "hk_suggested": hk_s,
        "hk_target": hk_pool_total,
        "total_suggested": bun_s + hk_s,
        "total_target": total,
        "total_target_before_reserve": total,
        "total_allocatable": total_allocatable,
        "new_post_budget_reserve": reserve_total,
        "reserve_bun": rb,
        "reserve_hk": rh,
        "adset_min_floor": adset_min_floor,
        "bun_underfunded_warning": bun_under,
        "hk_underfunded_warning": hk_under,
    }
    return suggestions, check
