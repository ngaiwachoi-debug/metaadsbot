#!/usr/bin/env python3
"""
Check whether a post id / ad set id appear in local pending_tests.json and optionally Google Refined sheet.

Usage:
  python check_p00_dataset_presence.py --post-id 645677431964443_122169824240851740
  python check_p00_dataset_presence.py --adset-id 120245552984610067 --page-id 645677431964443

Requires credentials for sheet path (same as ai_optimizer / callfrommeta).
"""
from __future__ import annotations

import argparse
import json
import os

_ROOT = os.path.dirname(os.path.abspath(__file__))
_PENDING = os.path.join(_ROOT, "pending_tests.json")


def _load_pending() -> list[dict]:
    if not os.path.isfile(_PENDING):
        return []
    try:
        with open(_PENDING, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def _scan_pending(entries: list[dict], post_id: str, page_id: str) -> None:
    post_id = (post_id or "").strip()
    page_id = (page_id or "").strip()
    print(f"pending_tests.json: {len(entries)} entries, path={_PENDING}")
    if post_id:
        ph = [e for e in entries if str(e.get("post_id", "")) == post_id]
        print(f"  exact post_id match: {len(ph)}")
        for e in ph[:5]:
            print(f"    shop={e.get('shop')} actor_id={e.get('actor_id')} pool={e.get('pool')}")
    if page_id and not post_id:
        ph = [e for e in entries if str(e.get("actor_id", "")).strip() == page_id]
        print(f"  entries for actor_id==page_id: {len(ph)}")


def _scan_refined(adset_id: str, page_id: str, story_substring: str) -> None:
    try:
        from ai_optimizer import load_refined_rows_from_sheet
        from engine import aggregate_by_adset
    except ImportError as e:
        print(f"Refined sheet: skip import error {e}")
        return
    rows = load_refined_rows_from_sheet()
    if not rows:
        print("Refined sheet: no rows (missing tab, credentials, or empty worksheet)")
        return
    print(f"Refined sheet: {len(rows)} rows")
    adset_id = (adset_id or "").strip()
    page_id = (page_id or "").strip()
    story_substring = (story_substring or "").strip()

    if adset_id:
        match = [r for r in rows if str(r.get("AdSet ID", "")).strip() == adset_id]
        print(f"  rows with AdSet ID == {adset_id}: {len(match)}")
        for r in match[:3]:
            print(
                f"    ad={r.get('廣告ID')} created_time={r.get('created_time')} "
                f"7d_spend={r.get('7日花費')} og={r.get('optimization_goal')} "
                f"dest={r.get('destination_type')} camp_obj={r.get('campaign_objective')}"
            )
    if page_id:
        match = [
            r
            for r in rows
            if str(r.get("actor_id", "")).strip() == page_id
            or str(r.get("instagram_actor_id", "")).strip() == page_id
        ]
        print(f"  rows for page actor_id / instagram_actor_id == {page_id}: {len(match)}")
    if story_substring:
        # Refined grid may not include story id; report if any column contains it
        n = 0
        for r in rows:
            blob = json.dumps(r, ensure_ascii=False)
            if story_substring in blob:
                n += 1
        print(f"  rows with substring in any field (story search): {n}")

    if adset_id:
        meta = aggregate_by_adset(rows)
        agg = meta.get(adset_id)
        if agg:
            print(
                f"  aggregate_by_adset[{adset_id}]: spend_7d={agg.spend_7d} "
                f"weighted_cpc_7d={agg.weighted_cpc_7d} og={agg.optimization_goal} "
                f"dest={agg.destination_type} camp_obj={agg.campaign_objective}"
            )
        else:
            print(f"  aggregate_by_adset: no bucket for AdSet ID {adset_id} (check id / raw sync)")


def main() -> None:
    ap = argparse.ArgumentParser(description="P00 data presence: pending_tests + Refined sheet")
    ap.add_argument("--post-id", default="", help="Full object_story_id or post id")
    ap.add_argument("--adset-id", default="", help="Ad set id to find in Refined")
    ap.add_argument("--page-id", default="", help="Page actor id (prefix of story id)")
    args = ap.parse_args()

    pending = _load_pending()
    _scan_pending(pending, args.post_id, args.page_id)
    _scan_refined(args.adset_id, args.page_id, args.post_id)
    print("Done.")


if __name__ == "__main__":
    main()
