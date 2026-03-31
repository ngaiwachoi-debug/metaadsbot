"""Tests: Action Plan header → template/target ad set ids (new ads section).

Run: python test_action_plan_new_ads_ids.py
Or:  python -m unittest test_action_plan_new_ads_ids -v
"""

from __future__ import annotations

import logging
import unittest
from unittest.mock import MagicMock

from ai_optimizer import ACTION_PLAN_TITLE_NEW

from action_plan_parse import _norm_header_key, parse_action_plan_grid
from meta_utils import normalize_object_story_id
from execute_action_plan_new_ads import (
    _PROACTIVE_COMPAT_PROFILES_ORDERED,
    _apply_proactive_compat_profile,
    _check_post_has_messaging_cta,
    _create_adset_native_cloning_fallback,
    _fetch_post_routing_hints,
    _filter_proactive_profiles_for_post_format,
    _infer_post_format_kind,
    _parse_positive_min_bid_minor,
    _resolve_template_and_target,
    _template_row_is_messaging,
    _try_proactive_adset_ladder,
)


class TestActionPlanNewAdsIds(unittest.TestCase):
    def test_normalize_object_story_id_sheet_numeric_concat(self) -> None:
        page = "645677431964443"
        merged = "645677431964443122169824240851740"
        self.assertEqual(
            normalize_object_story_id(merged, page),
            "645677431964443_122169824240851740",
        )

    def test_norm_header_key_collapses_spaces(self) -> None:
        self.assertEqual(
            _norm_header_key("複製自 AdSet ID"),
            _norm_header_key("複製自AdSet ID"),
        )
        self.assertEqual(_norm_header_key("複製自AdSet ID"), "複製自AdSetID")

    def test_parse_grid_exact_header_still_maps_template(self) -> None:
        title = ACTION_PLAN_TITLE_NEW
        header = [
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
        row = [
            "Shop",
            "[P00] GENERAL",
            "ad",
            "120228829427350067",
            "",
            "123_456",
            "789",
            "",
            "",
            "",
            "Name",
            "",
            "",
            "",
            "100",
            "NEW_FROM_POST",
        ]
        grid = [[title], header, row, []]
        out = parse_action_plan_grid(grid)
        rows = out[title]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("template_adset_id"), "120228829427350067")

    def test_parse_grid_spaced_header_variant_maps_template(self) -> None:
        """Sheets often insert a space: 複製自 AdSet ID vs 複製自AdSet ID."""
        title = ACTION_PLAN_TITLE_NEW
        header = [
            "店名",
            "策略",
            "複製自（原始Ad名稱）",
            "複製自 AdSet ID",
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
        row = [
            "Shop",
            "[P00] GENERAL",
            "ad",
            "120228829427350067",
            "",
            "123_456",
            "789",
            "",
            "",
            "",
            "Name",
            "",
            "",
            "",
            "100",
            "NEW_FROM_POST",
        ]
        grid = [[title], header, row, []]
        out = parse_action_plan_grid(grid)
        rows = out[title]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get("template_adset_id"), "120228829427350067")
        self.assertEqual(rows[0].get("target_adset_id"), "")

    def test_resolve_create_mode_when_template_set(self) -> None:
        row = {
            "strategy": "[P00] GENERAL",
            "create_mode": "NEW_FROM_POST",
            "target_adset_id": "",
            "template_adset_id": "120225972555890067",
            "post_or_object_story_id": "123_456",
            "page_id": "789",
        }
        res = _resolve_template_and_target(row)
        self.assertEqual(res.mode, "create")
        self.assertEqual(res.template_adset_id, "120225972555890067")

    def test_resolve_legacy_when_target_set(self) -> None:
        row = {
            "strategy": "[P00] GENERAL",
            "create_mode": "NEW_FROM_POST",
            "target_adset_id": "120225972555890067",
            "template_adset_id": "",
            "post_or_object_story_id": "x",
            "page_id": "y",
        }
        res = _resolve_template_and_target(row)
        self.assertEqual(res.mode, "legacy")
        self.assertEqual(res.target_adset_id, "120225972555890067")

    def test_resolve_missing_p00_new_from_post_without_template(self) -> None:
        row = {
            "strategy": "[P00] GENERAL",
            "create_mode": "NEW_FROM_POST",
            "target_adset_id": "",
            "template_adset_id": "",
            "post_or_object_story_id": "x",
            "page_id": "y",
        }
        res = _resolve_template_and_target(row)
        self.assertEqual(res.mode, "missing")


class TestSmartRoutingP00(unittest.TestCase):
    """P00 smart routing: template messaging DNA + post call_to_action (mocked Graph)."""

    def test_template_row_messaging_conversations(self) -> None:
        self.assertTrue(
            _template_row_is_messaging({"optimization_goal": "CONVERSATIONS", "destination_type": ""})
        )

    def test_template_row_messaging_destination(self) -> None:
        self.assertTrue(
            _template_row_is_messaging(
                {"optimization_goal": "LINK_CLICKS", "destination_type": "WHATSAPP_MESSAGE"}
            )
        )

    def test_template_row_not_messaging(self) -> None:
        self.assertFalse(
            _template_row_is_messaging(
                {"optimization_goal": "POST_ENGAGEMENT", "destination_type": "WEBSITE"}
            )
        )

    def test_check_post_has_messaging_cta_whatsapp(self) -> None:
        client = MagicMock()
        client.graph_get.return_value = {"call_to_action": {"type": "WHATSAPP_MESSAGE"}}
        log = logging.getLogger("test_smart_routing")
        self.assertTrue(_check_post_has_messaging_cta(client, "123_456789", log, "99"))
        client.graph_get.assert_called_once()

    def test_check_post_no_cta_when_missing_type(self) -> None:
        client = MagicMock()
        client.graph_get.return_value = {"call_to_action": {"type": "SHOP_NOW"}}
        log = logging.getLogger("test_smart_routing")
        self.assertFalse(_check_post_has_messaging_cta(client, "123_456789", log, "99"))

    def test_check_post_no_cta_on_graph_error(self) -> None:
        client = MagicMock()
        client.graph_get.return_value = {"error": {"message": "not found"}}
        log = logging.getLogger("test_smart_routing")
        self.assertFalse(_check_post_has_messaging_cta(client, "123_456789", log, "99"))

    def test_check_post_empty_id(self) -> None:
        client = MagicMock()
        log = logging.getLogger("test_smart_routing")
        self.assertFalse(_check_post_has_messaging_cta(client, "", log, "99"))
        client.graph_get.assert_not_called()

    def test_graph_get_includes_attachments_for_routing_hints(self) -> None:
        client = MagicMock()
        client.graph_get.return_value = {
            "call_to_action": {"type": "SHOP_NOW"},
            "attachments": {"data": [{"media_type": "photo"}]},
        }
        log = logging.getLogger("test_smart_routing")
        h = _fetch_post_routing_hints(client, "123_456789", log, "99")
        self.assertFalse(h.has_messaging_cta)
        self.assertEqual(h.format_kind, "image")
        _f = client.graph_get.call_args[0][1]["fields"]
        self.assertIn("attachments", _f)
        self.assertIn("call_to_action", _f)


class TestProactiveCompatLadder(unittest.TestCase):
    def _minimal_adset_base(self) -> dict:
        return {
            "name": "tmp",
            "campaign_id": "120111",
            "optimization_goal": "CONVERSATIONS",
            "billing_event": "IMPRESSIONS",
            "bid_strategy": "LOWEST_COST_WITH_BID_CAP",
            "bid_amount": "999",
            "targeting": "{}",
            "status": "PAUSED",
            "promoted_object": '{"page_id":"645677431964443"}',
            "daily_budget": "5000",
        }

    def test_parse_positive_min_bid_minor(self) -> None:
        self.assertEqual(_parse_positive_min_bid_minor("100"), 100)
        self.assertIsNone(_parse_positive_min_bid_minor("0"))
        self.assertIsNone(_parse_positive_min_bid_minor("x"))

    def test_infer_post_format_kind_video(self) -> None:
        self.assertEqual(
            _infer_post_format_kind({"attachments": {"data": [{"media_type": "video"}]}}),
            "video",
        )

    def test_filter_drops_video_only_for_image(self) -> None:
        filtered = _filter_proactive_profiles_for_post_format(_PROACTIVE_COMPAT_PROFILES_ORDERED, "image")
        ids = [p.profile_id for p in filtered]
        self.assertNotIn("p9_on_video_thruplay", ids)
        self.assertNotIn("p10_on_video_vv", ids)
        self.assertIn("p1_on_post_pe", ids)

    def test_filter_keeps_video_profiles_for_video(self) -> None:
        filtered = _filter_proactive_profiles_for_post_format(_PROACTIVE_COMPAT_PROFILES_ORDERED, "video")
        ids = [p.profile_id for p in filtered]
        self.assertIn("p9_on_video_thruplay", ids)

    def test_apply_without_cap_strips_bid_amount(self) -> None:
        log = logging.getLogger("t")
        p = _PROACTIVE_COMPAT_PROFILES_ORDERED[0]
        out = _apply_proactive_compat_profile(
            self._minimal_adset_base(), "Ad Name", p, log, "1", "100"
        )
        self.assertIsNotNone(out)
        assert out is not None
        self.assertEqual(out["bid_strategy"], "LOWEST_COST_WITHOUT_CAP")
        self.assertNotIn("bid_amount", out)
        self.assertEqual(out["optimization_goal"], "POST_ENGAGEMENT")

    def test_apply_with_cap_skips_invalid_min_bid(self) -> None:
        log = logging.getLogger("t")
        cap_prof = next(x for x in _PROACTIVE_COMPAT_PROFILES_ORDERED if x.bid_mode == "with_bid_cap")
        out = _apply_proactive_compat_profile(
            self._minimal_adset_base(), "Ad Name", cap_prof, log, "1", "0"
        )
        self.assertIsNone(out)

    def test_try_proactive_ladder_second_profile_succeeds(self) -> None:
        client = MagicMock()
        client.create_adset.side_effect = [
            {"error": {"code": 100, "error_subcode": 2490408}},
            {"id": "120245554605660067"},
        ]
        log = logging.getLogger("t")
        nid, err = _try_proactive_adset_ladder(
            client,
            self._minimal_adset_base(),
            enforced_name="X",
            logger=log,
            sheet_row="1",
            min_bid_minor="100",
            post_format_kind="image",
        )
        self.assertEqual(nid, "120245554605660067")
        self.assertIsNone(err)
        self.assertEqual(client.create_adset.call_count, 2)

    def test_native_fallback_passes_proactive_routing_false_on_safe_mode(self) -> None:
        """Reactive safe_mode must use ENGAGED_USERS + UNDEFINED (proactive_routing=False)."""
        client = MagicMock()

        def _se(body: dict):
            og = body.get("optimization_goal")
            if og == "CONVERSATIONS":
                return {"error": {"code": 100, "error_subcode": 2490408}}
            if og == "ENGAGED_USERS":
                return {"id": "999"}
            return {"error": {"code": 100}}

        client.create_adset.side_effect = _se
        log = logging.getLogger("t")
        base = self._minimal_adset_base()
        nid, _ = _create_adset_native_cloning_fallback(
            client,
            base,
            log,
            enforced_name="Y",
            sheet_row="1",
            min_bid_minor="100",
            proactive_safe=False,
        )
        self.assertEqual(nid, "999")


if __name__ == "__main__":
    unittest.main()
