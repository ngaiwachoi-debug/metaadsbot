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
    _min_budget_env = float(os.getenv("MIN_DAILY_BUDGET", "50") or 50)
except Exception:
    _min_budget_env = 50.0
DEFAULT_ADSET_MIN_FLOOR = _min_budget_env + 1.0

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
        return raw if isinstance(raw, list) else []
    except Exception:
        return []


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


def build_pool_items_by_shop(adsets: dict[str, AdsetAggregate]) -> dict[str, list[AdsetPoolItem]]:
    items_by_shop: dict[str, list[AdsetPoolItem]] = {}
    for adset in adsets.values():
        items_by_shop.setdefault(adset.shop, []).append(
            AdsetPoolItem(
                adset_id=adset.adset_id,
                shop=adset.shop,
                strategy=adset.strategy,
                current_budget=adset.current_budget,
                cpc_7d=adset.weighted_cpc_7d,
            )
        )
    return items_by_shop


def weighted_pool_allocation(
    shop_name: str,
    items: list[AdsetPoolItem],
    account_min_budget: float | None = None,
    new_post_budget_reserve: float = 0.0,
) -> tuple[dict[str, float], dict[str, float]]:
    """
    AdSet Budget Allocation with Floor Guarantee.
    1. Reserve ADSET_MIN_FLOOR per active AdSet.
    2. Distribute remaining pool budget by 1/cpc_7d weight.
    3. Apply upper cap (current_budget * 1.2).
    4. Residual fill to top performers if under-allocated.
    5. Trim from worst if over-allocated.

    new_post_budget_reserve: subtract from shop daily total (Phase 4 pending post tests) before BUN/HK split.
    """
    total, bun_ratio = _shop_config(shop_name)
    reserve = max(0.0, _to_float(new_post_budget_reserve))
    total_eff = max(0.0, total - reserve)
    t = {
        "total": total_eff,
        "bun_target": total_eff * bun_ratio,
        "hk_target": total_eff * (1.0 - bun_ratio),
    }
    adset_min_floor = max(
        1.0,
        (_to_float(account_min_budget) + 1.0) if account_min_budget is not None else DEFAULT_ADSET_MIN_FLOOR,
    )
    by_pool = {"bun": [], "hk": []}
    for x in items:
        by_pool[_pool_name(x.strategy)].append(x)

    suggestions: dict[str, float] = {}

    def alloc(pool_items: list[AdsetPoolItem], pool_total: float):
        if not pool_items:
            return

        n = len(pool_items)
        floor_total = adset_min_floor * n

        # 冷啟動：current_budget=0 → 100，避免後續運算問題（不用於計算，只墊基礎值）
        for i in pool_items:
            if i.current_budget <= 0:
                i.current_budget = 100.0

        # -------- 門檻不足警告 --------
        if pool_total < floor_total - 0.01:
            underfunded_warning = True
        else:
            underfunded_warning = False

        # -------- 先每人一個 floor --------
        for x in pool_items:
            suggestions[x.adset_id] = adset_min_floor

        # -------- 計算餘額 --------
        remaining = pool_total - floor_total

        # -------- 餘額按 1/cpc_7d 分配 --------
        scores = {x.adset_id: 1.0 / max(0.1, x.cpc_7d) for x in pool_items}
        sum_scores = sum(scores.values()) or 1.0
        upper = {x.adset_id: x.current_budget * 1.2 for x in pool_items}

        if remaining > 0.01 and not underfunded_warning:
            for x in pool_items:
                raw = remaining * (scores[x.adset_id] / sum_scores)
                suggestions[x.adset_id] += raw

        # -------- 套用 upper cap --------
        for x in pool_items:
            aid = x.adset_id
            suggestions[aid] = min(suggestions[aid], upper[aid])

        # -------- 歸一化：補足總額 --------
        current_sum = sum(suggestions[x.adset_id] for x in pool_items)
        delta = pool_total - current_sum

        ranked_best = sorted(pool_items, key=lambda z: z.cpc_7d)
        ranked_worst = list(reversed(ranked_best))

        if delta > 0.01:
            # 分配不足 -> 補貼前 50% 高表現者
            top_n = max(1, math.ceil(len(ranked_best) * 0.5))
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
            # 分配超額 -> 從後 50% 回收
            need_cut = -delta
            tail_n = max(1, math.ceil(len(ranked_worst) * 0.5))
            tail = ranked_worst[:tail_n]
            for _ in range(30):
                if need_cut <= 0.01:
                    break
                progressed = False
                share = need_cut / max(1, len(tail))
                for z in tail:
                    aid = z.adset_id
                    room = suggestions[aid] - adset_min_floor
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
            # 若上限卡死仍不足，為了逼近店鋪總額，解除上限繼續補至高表現 AdSet
            top_n = max(1, math.ceil(len(ranked_best) * 0.5))
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

    bun_pool_total = t["bun_target"]
    hk_pool_total = t["hk_target"]
    if not by_pool["bun"]:
        bun_pool_total = 0.0
        hk_pool_total = t["total"]

    bun_under = alloc(by_pool["bun"], bun_pool_total)
    hk_under = alloc(by_pool["hk"], hk_pool_total)

    bun_s = sum(suggestions[x.adset_id] for x in by_pool["bun"]) if by_pool["bun"] else 0.0
    hk_s = sum(suggestions[x.adset_id] for x in by_pool["hk"]) if by_pool["hk"] else 0.0
    check = {
        "bun_suggested": bun_s,
        "bun_target": bun_pool_total,
        "hk_suggested": hk_s,
        "hk_target": hk_pool_total,
        "total_suggested": bun_s + hk_s,
        "total_target": t["total"],
        "total_target_before_reserve": total,
        "new_post_budget_reserve": reserve,
        "adset_min_floor": adset_min_floor,
        "bun_underfunded_warning": bun_under,
        "hk_underfunded_warning": hk_under,
    }
    return suggestions, check
