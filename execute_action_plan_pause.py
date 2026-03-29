#!/usr/bin/env python3
"""Execute ⏸️ 暫停廣告清單 from AI_Action_Plan: POST /{ad-id} status=PAUSED."""

from __future__ import annotations

import argparse
import logging
import time

from action_plan_parse import parse_action_plan, section_by_short_name
from meta_actions_common import add_common_args, init_cli
from meta_graph_write import GraphAuthError, GraphClient
from meta_utils import norm_meta_graph_id

log = logging.getLogger("pause")


def main() -> None:
    p = argparse.ArgumentParser(description="Pause ads from AI_Action_Plan (dry-run by default).")
    add_common_args(p)
    args = p.parse_args()
    dry = init_cli(args)

    delay_ms = args.delay_ms
    client = GraphClient(delay_ms=delay_ms if delay_ms is not None else None, logger=log)
    try:
        parsed = parse_action_plan(args.tab)
        rows = section_by_short_name(parsed, "pause")
    except Exception as e:
        log.exception("Failed to load/parse sheet: %s", e)
        raise SystemExit(1) from e

    if args.skip:
        rows = rows[args.skip :]
    if args.limit:
        rows = rows[: args.limit]

    stats = {
        "rows_total": len(rows),
        "would_execute": 0,
        "ok": 0,
        "skipped": 0,
        "failed": 0,
        "auth_abort": False,
    }
    t0 = time.monotonic()

    for row in rows:
        sr = row.get("_sheet_row", "?")
        aid = norm_meta_graph_id(row.get("ad_id", ""))
        if not aid:
            log.info(
                "section=pause row=%s op=pause_ad id=— status=skip reason=no_ad_id",
                sr,
            )
            stats["skipped"] += 1
            continue
        if dry:
            log.info(
                "section=pause row=%s op=pause_ad id=%s status=would_execute",
                sr,
                aid,
            )
            stats["would_execute"] += 1
            continue
        try:
            body = client.graph_post(aid, {"status": "PAUSED"})
            err = body.get("error") if isinstance(body, dict) else None
            if isinstance(err, dict):
                log.error("section=pause row=%s op=pause_ad id=%s status=fail", sr, aid)
                stats["failed"] += 1
                continue
            log.info("section=pause row=%s op=pause_ad id=%s status=ok", sr, aid)
            stats["ok"] += 1
        except GraphAuthError:
            log.error(
                "auth_failure after processing — set META_ACCESS_TOKEN and META_ACTION_EXECUTE_CONFIRM=YES; "
                "use --skip to resume."
            )
            stats["auth_abort"] = True
            break
        except Exception as e:
            log.exception("section=pause row=%s id=%s err=%s", sr, aid, e)
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
    if stats["failed"] or stats["auth_abort"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
