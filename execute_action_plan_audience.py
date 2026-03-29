#!/usr/bin/env python3
"""Execute 🏷️ 受眾標籤置換清單: merge resolved interest IDs into ad set targeting (dry-run default)."""

from __future__ import annotations

import argparse
import json
import logging
import time

from action_plan_parse import parse_action_plan, section_by_short_name
from ai_optimizer import validate_and_get_ids
from meta_actions_common import add_common_args, init_cli
from meta_graph_write import GraphAuthError, GraphClient
from meta_targeting_merge import merge_interests_into_targeting, split_tags
from meta_utils import norm_meta_graph_id

log = logging.getLogger("audience")

def main() -> None:
    p = argparse.ArgumentParser(description="Merge audience interest tags into ad set targeting.")
    add_common_args(p)
    args = p.parse_args()
    dry = init_cli(args)

    client = GraphClient(delay_ms=args.delay_ms, logger=log)

    try:
        parsed = parse_action_plan(args.tab)
        rows = section_by_short_name(parsed, "audience")
    except Exception as e:
        log.exception("Failed to load/parse sheet: %s", e)
        raise SystemExit(1) from e

    if args.skip:
        rows = rows[args.skip :]
    if args.limit:
        rows = rows[: args.limit]

    stats = {"rows_total": len(rows), "would_execute": 0, "ok": 0, "skipped": 0, "failed": 0}
    t0 = time.monotonic()

    for row in rows:
        sr = row.get("_sheet_row", "?")
        adset_id = norm_meta_graph_id(row.get("adset_id", ""))
        new_tags = row.get("new_tags", "")
        if not adset_id:
            log.info("section=audience row=%s status=skip reason=no_adset", sr)
            stats["skipped"] += 1
            continue
        tags = split_tags(new_tags)
        if not tags:
            log.info("section=audience row=%s adset=%s status=skip reason=no_new_tags", sr, adset_id)
            stats["skipped"] += 1
            continue

        ids, _kept, st = validate_and_get_ids(tags)
        if not ids:
            log.warning("section=audience row=%s adset=%s status=skip reason=no_interest_ids status=%s", sr, adset_id, st)
            stats["skipped"] += 1
            continue

        try:
            cur = client.graph_get(adset_id, {"fields": "targeting"})
        except GraphAuthError:
            log.error("auth_failure")
            raise SystemExit(1) from None
        tgt = cur.get("targeting")
        if isinstance(tgt, str):
            try:
                tgt = json.loads(tgt)
            except json.JSONDecodeError:
                tgt = {}
        if not isinstance(tgt, dict):
            tgt = {}
        merged = merge_interests_into_targeting(tgt, ids)
        diff_preview = json.dumps({"before_keys": list(tgt.keys()), "after_flex": merged.get("flexible_spec")}, ensure_ascii=False)[:2000]
        if dry:
            log.info(
                "section=audience row=%s adset=%s op=merge_targeting status=would_execute preview=%s",
                sr,
                adset_id,
                diff_preview,
            )
            stats["would_execute"] += 1
            continue

        try:
            body = client.graph_post(adset_id, {"targeting": json.dumps(merged)})
            err = body.get("error") if isinstance(body, dict) else None
            if isinstance(err, dict):
                log.error("section=audience row=%s adset=%s status=fail", sr, adset_id)
                stats["failed"] += 1
                continue
            log.info("section=audience row=%s adset=%s status=ok", sr, adset_id)
            stats["ok"] += 1
        except GraphAuthError:
            log.error("auth_failure")
            raise SystemExit(1) from None
        except Exception as e:
            log.exception("row=%s err=%s", sr, e)
            stats["failed"] += 1

    elapsed = time.monotonic() - t0
    log.info(
        "summary rows_total=%s dry_run=%s ok=%s would=%s skipped=%s failed=%s elapsed_sec=%.2f",
        stats["rows_total"],
        dry,
        stats["ok"],
        stats["would_execute"],
        stats["skipped"],
        stats["failed"],
        elapsed,
    )
    client.close()
    if stats["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
