#!/usr/bin/env python3
"""Execute 🏷️ 受眾標籤置換清單: merge resolved interest IDs into ad set targeting (dry-run default).

Short-term: PATCH ad set ``targeting`` with the same merge order as ``execute_action_plan_new_ads``
(HK-only geo, pool EXCLUDE countries, interests, champion fallback).

Mid-term: if Meta UI cannot reliably apply audience tag changes via PATCH, prefer
clone ad set → adjust budget/tags → activate → pause/delete old (extend executor).
"""

from __future__ import annotations

import argparse
import json
import logging
import time

from action_plan_parse import parse_action_plan, section_by_short_name
from ai_optimizer import (
    compute_champion_tags_by_pool,
    load_refined_rows_from_sheet,
    validate_and_get_ids,
    _p00_champion_tags_for_pool,
)
from meta_actions_common import add_common_args, init_cli
from meta_audience_hints import (
    collect_exclude_raw_segments_from_cells,
    is_sheet_copy_not_interest_label,
    parse_exclude_segments,
)
from meta_graph_write import GraphAuthError, GraphClient
from meta_targeting_merge import (
    drop_root_id_key,
    ensure_hong_kong_geo,
    merge_excluded_geo_into_targeting,
    merge_interests_into_targeting,
    merge_locale_exclusions_into_targeting,
    split_tags,
)
from meta_utils import norm_meta_graph_id

log = logging.getLogger("audience")


def _interest_labels_from_cells(*cell_strs: str) -> list[str]:
    _skip = frozenset({"—", "-", "n/a", "na", "none", ""})
    out: list[str] = []
    seen: set[str] = set()
    for cell in cell_strs:
        for p in split_tags(cell or ""):
            if p.strip().lower() in _skip or is_sheet_copy_not_interest_label(p):
                continue
            if p not in seen:
                seen.add(p)
                out.append(p)
    return out


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

    champion_tags_by_pool: dict[tuple[str, str], str] = {}
    try:
        rref = load_refined_rows_from_sheet()
        if rref:
            champion_tags_by_pool = compute_champion_tags_by_pool(rref)
    except Exception as e:
        log.warning("champion_tags_by_pool unavailable (refined sheet): %s", e)

    stats = {"rows_total": len(rows), "would_execute": 0, "ok": 0, "skipped": 0, "failed": 0}
    t0 = time.monotonic()

    for row in rows:
        sr = row.get("_sheet_row", "?")
        adset_id = norm_meta_graph_id(row.get("adset_id", ""))
        new_tags = row.get("new_tags", "")
        isolation = row.get("isolation", "")
        if not adset_id:
            log.info("section=audience row=%s status=skip reason=no_adset", sr)
            stats["skipped"] += 1
            continue

        if not str(new_tags or "").strip() and not str(isolation or "").strip():
            log.info(
                "section=audience row=%s adset=%s status=skip reason=empty_new_tags_and_isolation",
                sr,
                adset_id,
            )
            stats["skipped"] += 1
            continue

        interest_labels = _interest_labels_from_cells(str(new_tags or ""), str(isolation or ""))
        exclude_raw = collect_exclude_raw_segments_from_cells(str(new_tags or ""), str(isolation or ""))
        parsed_excl = parse_exclude_segments(exclude_raw)

        ids, _kept, st = validate_and_get_ids(interest_labels)
        shop = (row.get("shop") or "").strip()
        strat = (row.get("strategy") or "").upper().replace(" ", "")
        pool_key = "bun" if "BUN" in strat else "hk"
        if interest_labels and not ids:
            champ_line = _p00_champion_tags_for_pool(champion_tags_by_pool, shop, pool_key)
            champ_labels = split_tags(champ_line or "")
            ids, _kept, st = validate_and_get_ids(champ_labels)
            if not ids:
                log.warning(
                    "section=audience row=%s adset=%s status=skip reason=no_interest_ids_after_champion_fallback "
                    "sheet_tags=%s status=%s",
                    sr,
                    adset_id,
                    interest_labels,
                    st,
                )
                stats["skipped"] += 1
                continue
            log.info(
                "section=audience row=%s adset=%s audience_tags resolved=%s status=%s source=champion_fallback",
                sr,
                adset_id,
                len(ids),
                st,
            )
        elif interest_labels:
            log.info(
                "section=audience row=%s adset=%s audience_tags resolved=%s status=%s",
                sr,
                adset_id,
                len(ids),
                st,
            )

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
        merged = ensure_hong_kong_geo(tgt)
        merged = merge_interests_into_targeting(merged, ids)
        merged = merge_excluded_geo_into_targeting(merged, parsed_excl.country_codes)
        merged = merge_locale_exclusions_into_targeting(merged, parsed_excl.locale_keys)
        if parsed_excl.locale_keys:
            log.warning(
                "section=audience row=%s adset=%s targeting=locale_exclusion_not_sent_to_graph keys=%s",
                sr,
                adset_id,
                parsed_excl.locale_keys,
            )
        for w in parsed_excl.warnings:
            log.warning("section=audience row=%s adset=%s exclusion_parse=%s", sr, adset_id, w)
        merged = drop_root_id_key(merged)
        diff_preview = json.dumps(
            {"before_keys": list(tgt.keys()), "after_flex": merged.get("flexible_spec")},
            ensure_ascii=False,
        )[:2000]
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
