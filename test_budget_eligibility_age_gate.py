from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from ai_optimizer import _compute_ad_decisions
from engine import AdsetPoolItem, weighted_pool_allocation


def _ts(hours_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).strftime("%Y-%m-%dT%H:%M:%S+0000")


def _row(
    *,
    shop: str,
    adset_id: str,
    ad_id: str,
    strategy_tag: str,
    created_time: str,
    current_budget: float,
    spend_7d: float,
    clicks_7d: float,
    cpc_7d: float,
) -> dict:
    ad_name = f"{strategy_tag} creative"
    return {
        "店名": shop,
        "來源專頁": shop,
        "廣告名稱": ad_name,
        "廣告文案": "body",
        "Campaign Name": "camp",
        "Campaign ID": "cmp_1",
        "AdSet ID": adset_id,
        "廣告ID": ad_id,
        "created_time": created_time,
        "7日花費": spend_7d,
        "7日點擊": clicks_7d,
        "7日平均 CPC": cpc_7d,
        "本月平均 CPC": cpc_7d,
        "今日 CPC": cpc_7d,
        "今日花費": 0.0,
        "現有日預算": current_budget,
        "帳戶最低日預算(API)": 8,
        "optimization_goal": "POST_ENGAGEMENT",
        "destination_type": "ON_POST",
        "campaign_objective": "OUTCOME_ENGAGEMENT",
        "targeting_json": "{}",
        "actor_id": "645677431964443",
    }


class TestWeightedPoolAllocationOccupiedBudget(unittest.TestCase):
    def test_pool_targets_deduct_occupied_budget(self) -> None:
        # default _shop_config for unknown shop => total=500, bun_ratio=0.2
        items = [
            AdsetPoolItem("bun_1", "Unknown Shop", "BUN", 100, 2.0, "middle"),
            AdsetPoolItem("hk_1", "Unknown Shop", "GENERAL", 100, 2.0, "middle"),
        ]
        s_map, check = weighted_pool_allocation(
            "Unknown Shop",
            items,
            account_min_budget=8.0,
            reserve_bun=0.0,
            reserve_hk=0.0,
            occupied_bun=30.0,
            occupied_hk=50.0,
        )
        self.assertIn("bun_1", s_map)
        self.assertIn("hk_1", s_map)
        self.assertAlmostEqual(float(check["bun_target"]), 70.0, places=3)  # 500*0.2 - 30
        self.assertAlmostEqual(float(check["hk_target"]), 350.0, places=3)  # 500*0.8 - 50
        self.assertAlmostEqual(float(check["occupied_budget_reserve"]), 80.0, places=3)


class TestAgeEligibilityGate(unittest.TestCase):
    def test_under_48h_adset_locked_and_deducted_from_pool(self) -> None:
        shop = "Olase 旺角店"  # config.json total=700, bun_ratio=0.0 => all HK pool
        rows = [
            _row(
                shop=shop,
                adset_id="old_hk_1",
                ad_id="ad_old_1",
                strategy_tag="GEN",
                created_time=_ts(96),
                current_budget=120.0,
                spend_7d=100.0,
                clicks_7d=20.0,
                cpc_7d=5.0,
            ),
            _row(
                shop=shop,
                adset_id="new_hk_1",
                ad_id="ad_new_1",
                strategy_tag="GEN",
                created_time=_ts(12),
                current_budget=80.0,
                spend_7d=15.0,
                clicks_7d=3.0,
                cpc_7d=5.0,
            ),
        ]
        _, _, suggested_by_adset, shop_checks, _, _ = _compute_ad_decisions(rows)
        # New adset (<48h) is locked to current budget and excluded from normalized pool allocation.
        self.assertAlmostEqual(float(suggested_by_adset["new_hk_1"]), 80.0, places=3)
        chk = shop_checks[shop]
        self.assertAlmostEqual(float(chk.get("occupied_hk", 0.0)), 80.0, places=3)
        # Shop total 700 with hk-only pool: allocatable target should be 700 - occupied_hk.
        self.assertAlmostEqual(float(chk.get("hk_target", 0.0)), 620.0, places=3)

    def test_locked_budget_pool_uses_deterministic_hk_bun_mapping(self) -> None:
        shop = "Olase 旺角店"  # HK-only cap in config, but we still verify occupied pool attribution.
        rows = [
            # Same adset has one BUN-like row and one non-BUN row; tie broken by newest row.
            _row(
                shop=shop,
                adset_id="mix_pool_1",
                ad_id="ad_mix_old",
                strategy_tag="BUN",
                created_time=_ts(30),
                current_budget=90.0,
                spend_7d=10.0,
                clicks_7d=2.0,
                cpc_7d=5.0,
            ),
            _row(
                shop=shop,
                adset_id="mix_pool_1",
                ad_id="ad_mix_new",
                strategy_tag="GEN",
                created_time=_ts(12),
                current_budget=90.0,
                spend_7d=10.0,
                clicks_7d=2.0,
                cpc_7d=5.0,
            ),
            _row(
                shop=shop,
                adset_id="old_hk_2",
                ad_id="ad_old_2",
                strategy_tag="GEN",
                created_time=_ts(96),
                current_budget=110.0,
                spend_7d=100.0,
                clicks_7d=20.0,
                cpc_7d=5.0,
            ),
        ]
        _, _, suggested_by_adset, shop_checks, _, _ = _compute_ad_decisions(rows)
        chk = shop_checks[shop]
        # mix_pool_1 is ineligible (<48h), locked to current budget.
        self.assertAlmostEqual(float(suggested_by_adset["mix_pool_1"]), 90.0, places=3)
        # Tie-break uses newest row (GEN => hk), so occupied_hk gets 90 and occupied_bun stays 0.
        self.assertAlmostEqual(float(chk.get("occupied_hk", 0.0)), 90.0, places=3)
        self.assertAlmostEqual(float(chk.get("occupied_bun", 0.0)), 0.0, places=3)


if __name__ == "__main__":
    unittest.main()

