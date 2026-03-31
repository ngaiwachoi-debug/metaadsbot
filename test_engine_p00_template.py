"""Tests for P00 template selection including zero-spend seed fallback."""
from __future__ import annotations

import unittest

from engine import AdsetAggregate, aggregate_by_adset, best_p00_template_adset_id


def _base_row(
    *,
    page: str,
    adset_id: str,
    ad_name: str = "Ad",
    created_time: str = "2025-01-01T00:00:00+0000",
    spend_7d: float = 0.0,
    clicks_7d: float = 0.0,
    og: str = "POST_ENGAGEMENT",
    dest: str = "ON_POST",
    camp_obj: str = "OUTCOME_ENGAGEMENT",
    shop: str = "TestShop",
) -> dict:
    return {
        "actor_id": page,
        "instagram_actor_id": "",
        "AdSet ID": adset_id,
        "廣告名稱": ad_name,
        "廣告文案": "正文",
        "created_time": created_time,
        "Campaign Name": "Camp",
        "店名": shop,
        "optimization_goal": og,
        "destination_type": dest,
        "campaign_objective": camp_obj,
        "7日花費": spend_7d,
        "7日點擊": clicks_7d,
        "7日平均 CPC": 1.0,
        "廣告ID": f"ad_{adset_id}_{created_time}",
    }


class TestBestP00TemplateAdsetId(unittest.TestCase):
    def test_spend_positive_wins_over_zero_spend_seed(self) -> None:
        page = "645677431964443"
        rows = [
            _base_row(page=page, adset_id="old_seed", created_time="2025-06-01T00:00:00+0000", spend_7d=0.0),
            _base_row(
                page=page,
                adset_id="winner",
                created_time="2025-01-01T00:00:00+0000",
                spend_7d=20.0,
                clicks_7d=10.0,
            ),
        ]
        meta = aggregate_by_adset(rows)
        got = best_p00_template_adset_id(rows, meta, "TestShop", "hk", page)
        self.assertEqual(got, "winner")

    def test_zero_spend_seed_newest_created_time(self) -> None:
        page = "645677431964443"
        rows = [
            _base_row(page=page, adset_id="older", created_time="2025-01-01T00:00:00+0000", spend_7d=0.0),
            _base_row(page=page, adset_id="newer", created_time="2025-08-01T00:00:00+0000", spend_7d=0.0),
        ]
        meta = aggregate_by_adset(rows)
        got = best_p00_template_adset_id(rows, meta, "TestShop", "hk", page)
        self.assertEqual(got, "newer")

    def test_all_positive_spend_messaging_falls_back_to_zero_spend_engagement(self) -> None:
        page = "645677431964443"
        rows = [
            _base_row(
                page=page,
                adset_id="messaging",
                created_time="2025-05-01T00:00:00+0000",
                spend_7d=30.0,
                clicks_7d=10.0,
                og="CONVERSATIONS",
                dest="MESSENGER",
                camp_obj="OUTCOME_ENGAGEMENT",
            ),
            _base_row(
                page=page,
                adset_id="seed610",
                created_time="2025-09-01T00:00:00+0000",
                spend_7d=0.0,
                og="POST_ENGAGEMENT",
                dest="ON_POST",
                camp_obj="OUTCOME_ENGAGEMENT",
            ),
        ]
        meta = aggregate_by_adset(rows)
        got = best_p00_template_adset_id(rows, meta, "TestShop", "hk", page)
        self.assertEqual(got, "seed610")

    def test_zero_spend_messaging_only_tier3_seed(self) -> None:
        """When only zero-spend sets are messaging, tier-3 seed still returns newest (executor safe_mode)."""
        page = "645677431964443"
        rows = [
            _base_row(
                page=page,
                adset_id="msg_seed",
                created_time="2025-09-01T00:00:00+0000",
                spend_7d=0.0,
                og="CONVERSATIONS",
                dest="MESSENGER",
                camp_obj="OUTCOME_ENGAGEMENT",
            ),
        ]
        meta = aggregate_by_adset(rows)
        got = best_p00_template_adset_id(rows, meta, "TestShop", "hk", page)
        self.assertEqual(got, "msg_seed")

    def test_explicit_adset_meta_missing_bucket_skipped_for_seed(self) -> None:
        """If adset_id is in pool_ok but aggregate lacks row, seed path skips missing agg."""
        page = "645677431964443"
        rows = [
            _base_row(page=page, adset_id="only", spend_7d=0.0),
        ]
        meta: dict[str, AdsetAggregate] = {}
        got = best_p00_template_adset_id(rows, meta, "TestShop", "hk", page)
        self.assertEqual(got, "")


if __name__ == "__main__":
    unittest.main()
