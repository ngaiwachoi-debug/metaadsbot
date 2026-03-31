#!/usr/bin/env python3
"""Execute 💰 預算調整清單: POST ad set daily_budget (minor units).

CBO (campaign-budget) rows: budget is applied at the parent ``campaign_id`` via ``daily_budget`` (same minor units).
"""

from __future__ import annotations

import argparse
import logging
import time

from action_plan_parse import parse_action_plan, section_by_short_name
from meta_actions_common import add_common_args, init_cli
from meta_graph_write import GraphAuthError, GraphClient, get_account_min_budget_minor, hkd_display_string_to_minor
from meta_utils import norm_meta_graph_id

log = logging.getLogger("budget")


def main() -> None:
    p = argparse.ArgumentParser(description="Adjust ad set budgets from AI_Action_Plan (dry-run by default).")
    add_common_args(p)
    args = p.parse_args()
    dry = init_cli(args)

    client = GraphClient(delay_ms=args.delay_ms, logger=log)
    min_minor, currency = get_account_min_budget_minor(client)

    try:
        parsed = parse_action_plan(args.tab)
        rows = section_by_short_name(parsed, "budget")
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
        "cbo_redirect": 0,
    }
    t0 = time.monotonic()

    for row in rows:
        sr = row.get("_sheet_row", "?")
        adset_id = norm_meta_graph_id(row.get("adset_id", ""))
        if not adset_id:
            log.info("section=budget row=%s status=skip reason=no_adset_id", sr)
            stats["skipped"] += 1
            continue
        sug = row.get("suggested_budget", "")
        minor = hkd_display_string_to_minor(sug, currency=currency)
        if minor <= 0:
            log.info("section=budget row=%s adset=%s status=skip reason=bad_budget %r", sr, adset_id, sug)
            stats["skipped"] += 1
            continue
        if minor < int(min_minor):
            log.warning(
                "section=budget row=%s adset=%s bump minor %s -> min %s",
                sr,
                adset_id,
                minor,
                int(min_minor),
            )
            minor = int(min_minor)

        # CBO check: campaign has daily_budget and ad set has 0
        try:
            aset = client.graph_get(
                adset_id,
                {"fields": "daily_budget,campaign{id,daily_budget,name}"},
            )
        except GraphAuthError:
            log.error("auth_failure — abort batch")
            raise SystemExit(1) from None
        camp = aset.get("campaign") or {}
        camp_id = norm_meta_graph_id(str(camp.get("id") or ""))
        cdb = float(camp.get("daily_budget") or 0)
        adb = float(aset.get("daily_budget") or 0)
        is_cbo = cdb > 0 and adb <= 0

        if is_cbo:
            if not camp_id:
                log.error(
                    "section=budget row=%s adset=%s status=fail reason=CBO but campaign id missing",
                    sr,
                    adset_id,
                )
                stats["failed"] += 1
                continue
            log.warning(
                "section=budget row=%s 🔄 Ad Set %s is CBO. Redirecting budget update to Campaign %s.",
                sr,
                adset_id,
                camp_id,
            )
            stats["cbo_redirect"] += 1
            if dry:
                log.info(
                    "section=budget row=%s op=set_campaign_daily_budget campaign=%s minor=%s status=would_execute",
                    sr,
                    camp_id,
                    minor,
                )
                stats["would_execute"] += 1
                continue
            try:
                body = client.graph_post(camp_id, {"daily_budget": str(minor)})
                err = body.get("error") if isinstance(body, dict) else None
                if isinstance(err, dict):
                    log.error("section=budget row=%s campaign=%s status=fail", sr, camp_id)
                    stats["failed"] += 1
                    continue
                log.info(
                    "section=budget row=%s op=set_campaign_daily_budget campaign=%s minor=%s status=ok",
                    sr,
                    camp_id,
                    minor,
                )
                stats["ok"] += 1
            except GraphAuthError:
                log.error("auth_failure — abort")
                raise SystemExit(1) from None
            except Exception as e:
                log.exception("row=%s err=%s", sr, e)
                stats["failed"] += 1
            continue

        if dry:
            log.info(
                "section=budget row=%s op=set_daily_budget adset=%s minor=%s status=would_execute",
                sr,
                adset_id,
                minor,
            )
            stats["would_execute"] += 1
            continue

        try:
            body = client.graph_post(adset_id, {"daily_budget": str(minor)})
            err = body.get("error") if isinstance(body, dict) else None
            if isinstance(err, dict):
                log.error("section=budget row=%s adset=%s status=fail", sr, adset_id)
                stats["failed"] += 1
                continue
            log.info("section=budget row=%s adset=%s minor=%s status=ok", sr, adset_id, minor)
            stats["ok"] += 1
        except GraphAuthError:
            log.error("auth_failure — abort")
            raise SystemExit(1) from None
        except Exception as e:
            log.exception("row=%s err=%s", sr, e)
            stats["failed"] += 1

    elapsed = time.monotonic() - t0
    log.info(
        "summary rows_total=%s dry_run=%s ok=%s would=%s skipped=%s failed=%s cbo_redirect=%s elapsed_sec=%.2f",
        stats["rows_total"],
        dry,
        stats["ok"],
        stats["would_execute"],
        stats["skipped"],
        stats["failed"],
        stats["cbo_redirect"],
        elapsed,
    )
    client.close()
    if stats["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
