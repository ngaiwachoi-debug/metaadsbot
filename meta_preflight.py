"""Preflight GETs for Action Plan executors: ad set / campaign / page checks with caching."""

from __future__ import annotations

import logging
from typing import Any

from meta_graph_write import GraphClient

# Kept for other modules / docs that reference destination taxonomy.
WHATSAPP_DESTINATION_TYPES = frozenset(
    {
        "WHATSAPP",
        "MESSAGING_MESSENGER_WHATSAPP",
        "MESSAGING_INSTAGRAM_DIRECT_WHATSAPP",
        "MESSAGING_INSTAGRAM_DIRECT_MESSENGER_WHATSAPP",
    }
)

MESSAGING_DESTINATION_TYPES = WHATSAPP_DESTINATION_TYPES | frozenset(
    {
        "MESSENGER",
        "MESSAGING_INSTAGRAM_DIRECT_MESSENGER",
    }
)

MESSAGING_NO_WHATSAPP_CREATIVE_TYPES = frozenset(
    {
        "MESSENGER",
        "MESSAGING_INSTAGRAM_DIRECT_MESSENGER",
    }
)

ENGAGEMENT_DESTINATION_TYPES = frozenset(
    {
        "UNDEFINED",
        "ON_POST",
        "ON_VIDEO",
        "ON_PAGE",
        "ON_EVENT",
    }
)

ADSET_FIELDS = (
    "id,name,status,effective_status,campaign_id,destination_type,optimization_goal,"
    "billing_event,bid_strategy,bid_amount,daily_budget,lifetime_budget,promoted_object,targeting,attribution_spec"
)


def get_adset_cached(client: GraphClient, adset_id: str, cache: dict[str, dict]) -> dict[str, Any]:
    aid = (adset_id or "").strip()
    if not aid:
        return {}
    if aid in cache:
        return cache[aid]
    data = client.graph_get(aid, {"fields": ADSET_FIELDS})
    cache[aid] = data if isinstance(data, dict) else {}
    return cache[aid]


def get_campaign_cached(client: GraphClient, campaign_id: str, cache: dict[str, dict]) -> dict[str, Any]:
    cid = (campaign_id or "").strip()
    if not cid:
        return {}
    if cid in cache:
        return cache[cid]
    data = client.graph_get(
        cid,
        {"fields": "id,name,status,objective,special_ad_categories,buying_type,daily_budget"},
    )
    cache[cid] = data if isinstance(data, dict) else {}
    return cache[cid]


def get_page_cached(client: GraphClient, page_id: str, cache: dict[str, dict]) -> dict[str, Any]:
    pid = (page_id or "").strip()
    if not pid:
        return {}
    if pid in cache:
        return cache[pid]
    data = client.graph_get(pid, {"fields": "id,name"})
    cache[pid] = data if isinstance(data, dict) else {}
    return cache[pid]


def preflight_new_ad_row(
    client: GraphClient,
    *,
    target_adset_id: str,
    sheet_campaign_id: str,
    sheet_page_id: str,
    adset_cache: dict[str, dict],
    campaign_cache: dict[str, dict],
    page_cache: dict[str, dict],
    logger: logging.Logger,
) -> tuple[str, list[str]]:
    """
    Returns (status, messages) where status is PASS | WARN | FAIL.
    FAIL blocks creative/ad POST for new ads executor.

    Native Cloning uses ``object_story_id`` only; destination-specific CTWA checks are not required here.
    Validates promoted page and optional sheet campaign alignment.
    """
    msgs: list[str] = []
    adset = get_adset_cached(client, target_adset_id, adset_cache)
    if not adset.get("id"):
        return "FAIL", ["empty_or_invalid_adset_id"]

    po = adset.get("promoted_object") or {}
    if not isinstance(po, dict):
        po = {}
    api_page = str(po.get("page_id") or "").strip()
    if not api_page:
        msgs.append("promoted_object.page_id missing on ad set")
        return "FAIL", msgs

    sheet_page = (sheet_page_id or "").strip()
    if sheet_page and sheet_page != api_page:
        msgs.append(f"sheet 專頁 ID {sheet_page} != promoted_object.page_id {api_page}")
        return "FAIL", msgs

    cid = str(adset.get("campaign_id") or "").strip()
    sheet_camp = (sheet_campaign_id or "").strip()
    if sheet_camp and cid and sheet_camp != cid:
        msgs.append(f"sheet 宣傳活動 ID {sheet_camp} != ad set campaign_id {cid}")
        return "FAIL", msgs

    dst = str(adset.get("destination_type") or "").upper() or "UNDEFINED"
    if dst in ENGAGEMENT_DESTINATION_TYPES:
        camp = get_campaign_cached(client, cid, campaign_cache) if cid else {}
        obj = str(camp.get("objective") or "")
        logger.info(
            "preflight new_ad: destination_type=%s campaign_objective=%s",
            dst,
            obj or "unknown",
        )

    if sheet_page:
        pg = get_page_cached(client, sheet_page, page_cache)
        if not pg.get("id"):
            msgs.append(f"could not read Page {sheet_page} (token scope?) — verify in Business Suite")
            return "WARN", msgs

    return "PASS", msgs


def preflight_new_ad_whatsapp(
    client: GraphClient,
    *,
    target_adset_id: str,
    sheet_campaign_id: str,
    sheet_page_id: str,
    adset_cache: dict[str, dict],
    campaign_cache: dict[str, dict],
    page_cache: dict[str, dict],
    logger: logging.Logger,
) -> tuple[str, list[str]]:
    """Backward-compatible alias for :func:`preflight_new_ad_row`."""
    return preflight_new_ad_row(
        client,
        target_adset_id=target_adset_id,
        sheet_campaign_id=sheet_campaign_id,
        sheet_page_id=sheet_page_id,
        adset_cache=adset_cache,
        campaign_cache=campaign_cache,
        page_cache=page_cache,
        logger=logger,
    )
