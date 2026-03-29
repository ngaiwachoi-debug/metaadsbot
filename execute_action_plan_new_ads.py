#!/usr/bin/env python3
"""Execute 📋 新建廣告清單: preflight WhatsApp, NEW_FROM_POST → adcreative + ad (dry-run default).

**P00 create path:** If **目標** is empty and **複製自AdSet ID** is set (same-page template from
``ai_optimizer`` — not ``SHOP_CONFIGS`` brand defaults), the script **GET**s the template ad set,
verifies ``promoted_object.page_id`` matches **專頁 ID**, then **POST** ``act_*/adsets`` (new ad set,
not ``/copies``). Targeting merges **新受眾標籤** and **受眾隔離標籤** via ``validate_and_get_ids`` +
``merge_interests_into_targeting``; ``attribution_spec`` is **1-day click-through** (Graph: ``CLICK_THROUGH``). ``promoted_object``
and targeting are JSON-string fields; root-level Graph ``id`` keys are stripped before POST. Under CBO
(``campaign.daily_budget`` > 0), ``daily_budget`` is omitted on the new ad set (with a warning); else
``daily_budget`` comes from **預算建議**. Status **PAUSED**. Preflight/creative/ad run on the new id.
Logged **new_adset_id** can be pasted into **目標** to re-run without creating again.
If **目標** is non-empty, that ad set is used directly (no create). Dry-run may **GET** the template to
build a redacted ``would_post_adsets`` preview; it does not POST ad sets or ads.

Messaging creatives: **Automatic** multidest (OSS + ``asset_feed_spec``, three CTAs without hardcoded
links) by default. Sheet **手动/手動/manual** + **WhatsApp** requests manual OSS-only when the ad set is
not hybrid (no ``MESSENGER`` / ``INSTAGRAM_DIRECT`` in ``destination_type``).

**Fallback:** creative POST errors **1885374** / **2446125**, or ad POST **2446125** after a manual
creative, trigger automatic multidest retry so delivery is not blocked by WABA / hybrid constraints.

Copy/text/image align with pending_tests + sheet; image is uploaded to the ad account (image_hash).
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

import httpx

from action_plan_parse import parse_action_plan, section_by_short_name
from ai_optimizer import validate_and_get_ids
from engine import SHOP_CONFIGS, load_pending_tests_entries
from meta_actions_common import add_common_args, init_cli
from meta_actions_logging import log_graph_error_payload
from meta_graph_write import (
    GraphAuthError,
    GraphClient,
    get_account_min_budget_minor,
    hkd_display_string_to_minor,
)
from meta_preflight import (
    MESSAGING_NO_WHATSAPP_CREATIVE_TYPES,
    WHATSAPP_DESTINATION_TYPES,
    get_adset_cached,
    get_campaign_cached,
    preflight_new_ad_whatsapp,
)
from meta_targeting_merge import drop_root_id_key, merge_interests_into_targeting, split_tags
from meta_utils import norm_meta_graph_id

log = logging.getLogger("new_ads")

_ATTRIBUTION_SPEC_1D_CLICK: list[dict[str, Any]] = [
    {"event_type": "CLICK_THROUGH", "window_days": 1},
]


def _looks_like_meta_id(s: str) -> bool:
    t = norm_meta_graph_id(s)
    return bool(t) and t.isdigit() and len(t) >= 8


def _p00_is_bun_pool(row: dict) -> bool:
    strat = (row.get("strategy") or "").upper().replace(" ", "")
    return "P00" in strat and "BUN" in strat


def _row_is_p00_new_from_post(row: dict) -> bool:
    strat = (row.get("strategy") or "").upper().replace(" ", "")
    mode = (row.get("create_mode") or "").strip().upper()
    return "P00" in strat and "NEW_FROM_POST" in mode


def _p00_template_adset_from_config(row: dict) -> str:
    """SHOP_CONFIGS p00_*_template_adset_id for P00 rows (no HTTP)."""
    strat = (row.get("strategy") or "").upper().replace(" ", "")
    if "P00" not in strat:
        return ""
    shop = (row.get("shop") or "").strip()
    conf = SHOP_CONFIGS.get(shop) or {}
    if _p00_is_bun_pool(row):
        raw = conf.get("p00_bun_template_adset_id") or ""
    else:
        raw = conf.get("p00_hk_template_adset_id") or ""
    return norm_meta_graph_id(str(raw or ""))


@dataclass(frozen=True)
class _AdsetResolution:
    mode: Literal["legacy", "create", "missing"]
    target_adset_id: str = ""
    template_adset_id: str = ""


def _resolve_template_and_target(row: dict) -> _AdsetResolution:
    """
    HTTP-free: legacy if 目標 filled; else create if 複製自 AdSet ID valid.
    Does not use p00_hk_adset_id as template (those are legacy placement ids, not template sources).

    P00 + ``NEW_FROM_POST``: never use ``SHOP_CONFIGS`` ``p00_*_template_adset_id`` (brand-level;
    wrong 分店 page → Meta Invalid Parameter). Sheet **複製自** must be the same-page best-CPC ad set.
    """
    target = norm_meta_graph_id(row.get("target_adset_id", "") or "")
    if _looks_like_meta_id(target):
        return _AdsetResolution(mode="legacy", target_adset_id=target)
    tpl = norm_meta_graph_id(row.get("template_adset_id", "") or "")
    if _looks_like_meta_id(tpl):
        return _AdsetResolution(mode="create", template_adset_id=tpl)
    if _row_is_p00_new_from_post(row):
        return _AdsetResolution(mode="missing")
    cfg_tpl = _p00_template_adset_from_config(row)
    if _looks_like_meta_id(cfg_tpl):
        return _AdsetResolution(mode="create", template_adset_id=cfg_tpl)
    return _AdsetResolution(mode="missing")


def _promoted_page_id_from_adset(adset_row: dict) -> str:
    po = adset_row.get("promoted_object") or {}
    if not isinstance(po, dict):
        return ""
    return norm_meta_graph_id(str(po.get("page_id") or ""))


def _parse_targeting_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return copy.deepcopy(raw)
    if isinstance(raw, str):
        try:
            d = json.loads(raw)
            return copy.deepcopy(d) if isinstance(d, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _promoted_object_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return copy.deepcopy(raw)
    if isinstance(raw, str):
        try:
            d = json.loads(raw)
            return d if isinstance(d, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _validate_template_page_for_create(
    client: GraphClient,
    template_id: str,
    row_page_norm: str,
    adset_cache: dict[str, dict],
    logger: logging.Logger,
    sheet_row: str,
) -> bool:
    """GET template; log critical and return False if page_id != row page."""
    row_p = norm_meta_graph_id(row_page_norm)
    if not row_p:
        logger.critical(
            "section=new_ads row=%s create=abort reason=missing_row_page_id for template guard",
            sheet_row,
        )
        return False
    tpl_row = get_adset_cached(client, template_id, adset_cache)
    tpl_p = _promoted_page_id_from_adset(tpl_row)
    if tpl_p != row_p:
        logger.critical(
            "Template Page ID mismatch: Row expects %s but Template uses %s (row=%s template=%s)",
            row_p,
            tpl_p or "(empty)",
            sheet_row,
            template_id,
        )
        return False
    return True


def _p00_new_adset_display_name(ad_name: str, post_id: str) -> str:
    date_s = datetime.now(timezone.utc).strftime("%Y%m%d")
    tail = norm_meta_graph_id(post_id) or "post"
    if len(tail) > 24:
        tail = tail[-24:]
    base = (ad_name or "").strip() or "New ad"
    chunk = f" [P00_NEW]_{tail}_{date_s}"
    out = f"{base}{chunk}"
    return out[:255] if len(out) > 255 else out


def _build_new_adset_payload(
    client: GraphClient,
    template_data: dict[str, Any],
    *,
    display_name: str,
    interest_ids: list[str],
    budget_suggest_cell: str,
    currency: str,
    min_minor: int,
    campaign_cache: dict[str, dict],
    logger: logging.Logger,
    sheet_row: str,
) -> dict[str, Any]:
    campaign_id = norm_meta_graph_id(str(template_data.get("campaign_id") or ""))
    if not campaign_id:
        logger.error("section=new_ads row=%s create=abort template missing campaign_id", sheet_row)
        return {}

    og = str(template_data.get("optimization_goal") or "").strip()
    be = str(template_data.get("billing_event") or "").strip()
    if not og or not be:
        logger.error(
            "section=new_ads row=%s create=abort template missing optimization_goal or billing_event",
            sheet_row,
        )
        return {}

    tgt = _parse_targeting_dict(template_data.get("targeting"))
    merged = merge_interests_into_targeting(tgt, interest_ids)
    merged = drop_root_id_key(merged)

    po_raw = copy.deepcopy(_promoted_object_dict(template_data.get("promoted_object")))
    po_clean = drop_root_id_key(po_raw)

    payload: dict[str, Any] = {
        "name": display_name[:255],
        "campaign_id": campaign_id,
        "optimization_goal": og,
        "billing_event": be,
        "targeting": json.dumps(merged, separators=(",", ":")),
        "attribution_spec": json.dumps(_ATTRIBUTION_SPEC_1D_CLICK, separators=(",", ":")),
        "status": "PAUSED",
    }
    if po_clean:
        payload["promoted_object"] = json.dumps(po_clean, separators=(",", ":"))

    bs = str(template_data.get("bid_strategy") or "").strip()
    if bs:
        payload["bid_strategy"] = bs
    dt = str(template_data.get("destination_type") or "").strip()
    if dt:
        payload["destination_type"] = dt

    camp = get_campaign_cached(client, campaign_id, campaign_cache)
    cdb = float((camp.get("daily_budget") or 0) or 0)
    if cdb > 0:
        logger.warning(
            "section=new_ads row=%s create=omit_daily_budget CBO campaign owns budget campaign_id=%s",
            sheet_row,
            campaign_id,
        )
    else:
        minor = hkd_display_string_to_minor(budget_suggest_cell, currency=currency)
        if minor < int(min_minor):
            logger.warning(
                "section=new_ads row=%s create=bump_budget minor %s -> min %s",
                sheet_row,
                minor,
                int(min_minor),
            )
            minor = int(min_minor)
        if minor <= 0:
            logger.warning(
                "section=new_ads row=%s create=skip_daily_budget parsed_zero from %r",
                sheet_row,
                budget_suggest_cell,
            )
        else:
            payload["daily_budget"] = str(minor)

    return payload


def _redact_adset_create_body(body: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in body.items():
        if k in ("targeting", "promoted_object", "attribution_spec") and isinstance(v, str):
            out[k] = f"<redacted len={len(v)}>"
        else:
            out[k] = v
    return out


def _new_adset_id_from_create_response(body: dict[str, Any]) -> str:
    if not isinstance(body, dict) or body.get("error"):
        return ""
    return norm_meta_graph_id(str(body.get("id") or ""))


def _is_sheet_audience_copy_not_interest_label(t: str) -> bool:
    """
    Human hints from ai_optimizer (受眾建議 / 受眾隔離) are not Meta Targeting Search labels.
    Skip them so P00 create is not blocked when isolation is e.g. EXCLUDE Philippines/...
    or copy lines look like HK: Lifestyle/Beauty.
    """
    u = (t or "").strip()
    if not u:
        return True
    if u.upper().startswith("EXCLUDE"):
        return True
    return bool(re.match(r"^[A-Z]{2,3}:\s*", u))


def _row_audience_tag_labels_unique(row: dict) -> list[str]:
    _skip = frozenset({"—", "-", "n/a", "na", "none", ""})

    def _norm_cell(s: str) -> list[str]:
        parts = split_tags(s)
        return [
            p
            for p in parts
            if p.strip().lower() not in _skip and not _is_sheet_audience_copy_not_interest_label(p)
        ]

    raw_new = _norm_cell(str(row.get("new_audience_tags") or ""))
    raw_iso = _norm_cell(str(row.get("isolation_tags") or ""))
    seen: set[str] = set()
    out: list[str] = []
    for t in raw_new + raw_iso:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def _normalize_whatsapp_e164(raw: str) -> str:
    digits = "".join(c for c in (raw or "") if c.isdigit())
    return f"+{digits}" if digits else ""


def _row_wants_manual_whatsapp_only(row: dict) -> bool:
    """Sheet intent: 手动/手動/manual + WhatsApp in 策略 / 創建方式 → prefer manual OSS creative."""
    s = f"{row.get('strategy', '')} {row.get('create_mode', '')}"
    s_lower = s.lower()
    manual_kw = "手動" in s or "手动" in s or "manual" in s_lower
    if not manual_kw:
        return False
    return "whatsapp" in s_lower


def _graph_error_subcode(err: Any) -> int:
    if not isinstance(err, dict):
        return 0
    try:
        return int(err.get("error_subcode") or 0)
    except (TypeError, ValueError):
        return 0


def _is_whatsapp_link_error(err: Any) -> bool:
    """Graph e.g. 2446886 — page not linked to WhatsApp Business."""
    return _graph_error_subcode(err) == 2446886


def _is_instagram_destination_error(err: Any) -> bool:
    """Heuristic for Attempt 2 (Messenger-only); substrings per product spec."""
    if not isinstance(err, dict):
        return False
    blob = " ".join(
        [
            str(err.get("message") or ""),
            str(err.get("error_user_msg") or ""),
            str(err.get("error_user_title") or ""),
        ]
    ).lower()
    for sub in ("instagram", "ig", "linked", "connected"):
        if sub in blob:
            return True
    return False


def _create_adset_messaging_fallback(
    client: GraphClient,
    base_payload: dict[str, Any],
    logger: logging.Logger,
    sheet_row: str | None = None,
) -> tuple[str | None, dict[str, Any] | None]:
    """
    Try WHATSAPP → MESSAGING_INSTAGRAM_DIRECT_MESSENGER → MESSENGER on create errors.
    Returns (new_adset_id, last_error_dict) — id set only on success.
    """
    sr = sheet_row if sheet_row is not None else "?"

    p0 = copy.deepcopy(base_payload)
    p0["destination_type"] = "WHATSAPP"
    body0 = client.create_adset(p0)
    err0 = body0.get("error") if isinstance(body0.get("error"), dict) else None
    if not err0:
        nid = _new_adset_id_from_create_response(body0)
        if nid:
            return nid, None
        logger.error("section=new_ads row=%s create_adset empty id after WHATSAPP attempt body=%s", sr, body0)
        return None, None
    if _is_whatsapp_link_error(err0):
        logger.warning(
            "section=new_ads row=%s create=messaging_fallback ⚠️ WhatsApp failed (2446886), trying Messenger + Instagram Direct...",
            sr,
        )
    else:
        log_graph_error_payload(logger, err0, prefix="create_adset ")
        return None, err0

    p1 = copy.deepcopy(base_payload)
    p1["destination_type"] = "MESSAGING_INSTAGRAM_DIRECT_MESSENGER"
    body1 = client.create_adset(p1)
    err1 = body1.get("error") if isinstance(body1.get("error"), dict) else None
    if not err1:
        nid = _new_adset_id_from_create_response(body1)
        if nid:
            return nid, None
        logger.error("section=new_ads row=%s create_adset empty id after M+IG attempt body=%s", sr, body1)
        return None, None
    if _is_instagram_destination_error(err1):
        logger.warning(
            "section=new_ads row=%s create=messaging_fallback ⚠️ IG-related error, falling back to Messenger only...",
            sr,
        )
    else:
        log_graph_error_payload(logger, err1, prefix="create_adset ")
        return None, err1

    p2 = copy.deepcopy(base_payload)
    p2["destination_type"] = "MESSENGER"
    body2 = client.create_adset(p2)
    err2 = body2.get("error") if isinstance(body2.get("error"), dict) else None
    if not err2:
        nid = _new_adset_id_from_create_response(body2)
        if nid:
            return nid, None
        logger.error("section=new_ads row=%s create_adset empty id after MESSENGER attempt body=%s", sr, body2)
        return None, None
    log_graph_error_payload(logger, err2, prefix="create_adset ")
    return None, err2


def _adset_destination_requires_automatic_multidest(destination_type: str) -> bool:
    """
    Hybrid messaging ad sets (Messenger and/or Instagram Direct in the type string) cannot use
    OSS-only manual creatives at ad placement (2446493).
    """
    u = (destination_type or "").strip().upper()
    return "MESSENGER" in u or "INSTAGRAM_DIRECT" in u


def _creative_payload_is_manual_oss_only(payload: dict[str, Any]) -> bool:
    return "asset_feed_spec" not in payload


def _pending_message_picture_for_post(post_id: str) -> tuple[str, str | None]:
    pid = (post_id or "").strip()
    for e in load_pending_tests_entries(restrict_to_shop_configs=True):
        if str(e.get("post_id") or "").strip() != pid:
            continue
        msg = str(e.get("message") or "").strip()
        pic = str(e.get("full_picture") or "").strip() or None
        return msg, pic
    return "", None


def _multi_destination_messaging_creative_payload(
    ad_name: str,
    page_id: str,
    message: str,
    image_hash: str,
    whatsapp_e164: str,
    *,
    manual_whatsapp_only: bool,
    creative_name: str | None = None,
) -> dict[str, Any]:
    """
    Messaging creatives as JSON-string form fields where required by Graph.

    * **manual_whatsapp_only** (sheet: 手动/手動/manual + WhatsApp): WhatsApp-only ``object_story_spec``.
    * **Automatic** (default): ``object_story_spec`` + ``asset_feed_spec`` with
      ``DOF_MESSAGING_DESTINATION`` and three CTAs (no hardcoded ``link`` on CTA values).
    """
    cname = (creative_name or "").strip() or f"{ad_name}_creative"
    digits = "".join(c for c in whatsapp_e164 if c.isdigit())
    wa_api_link = f"https://api.whatsapp.com/send?phone={digits}" if digits else "https://api.whatsapp.com/"
    messenger_link = f"https://m.me/{page_id}"

    if manual_whatsapp_only:
        # Meta rejects a single-entry asset_feed_spec with DOF (1885374). WhatsApp-only = OSS only.
        oss_manual: dict[str, Any] = {
            "page_id": page_id,
            "link_data": {
                "name": (ad_name or "Promo")[:255],
                "message": (message or "")[:2000],
                "image_hash": image_hash,
                "link": wa_api_link,
                "call_to_action": {
                    "type": "WHATSAPP_MESSAGE",
                    "value": {
                        "whatsapp_number": whatsapp_e164,
                        "app_destination": "WHATSAPP",
                    },
                },
            },
        }
        return {
            "name": cname,
            "object_story_spec": json.dumps(oss_manual),
        }

    oss_auto: dict[str, Any] = {
        "page_id": page_id,
        "link_data": {
            "name": (ad_name or "Promo")[:255],
            "message": (message or "")[:2000],
            "image_hash": image_hash,
            "link": messenger_link,
            "call_to_action": {
                "type": "MESSAGE_PAGE",
                "value": {"app_destination": "MESSENGER"},
            },
        },
    }
    afs_auto: dict[str, Any] = {
        "optimization_type": "DOF_MESSAGING_DESTINATION",
        "call_to_actions": [
            {"type": "MESSAGE_PAGE", "value": {"app_destination": "MESSENGER"}},
            {"type": "WHATSAPP_MESSAGE", "value": {"app_destination": "WHATSAPP"}},
            {"type": "INSTAGRAM_MESSAGE", "value": {"app_destination": "INSTAGRAM_DIRECT"}},
        ],
    }

    return {
        "name": cname,
        "object_story_spec": json.dumps(oss_auto),
        "asset_feed_spec": json.dumps(afs_auto),
    }


def _no_whatsapp_messaging_creative_payload(
    ad_name: str,
    page_id: str,
    message: str,
    image_hash: str,
    destination_type: str,
    *,
    creative_name: str | None = None,
) -> dict[str, Any]:
    """
    Click-to-Messenger (and optional IG Direct) without WhatsApp CTAs — for ad sets
    created under MESSENGER or MESSAGING_INSTAGRAM_DIRECT_MESSENGER fallback.
    """
    cname = (creative_name or "").strip() or f"{ad_name}_creative"
    messenger_link = f"https://m.me/{page_id}"
    oss: dict[str, Any] = {
        "page_id": page_id,
        "link_data": {
            "name": (ad_name or "Promo")[:255],
            "message": (message or "")[:2000],
            "image_hash": image_hash,
            "link": messenger_link,
            "call_to_action": {
                "type": "MESSAGE_PAGE",
                "value": {"app_destination": "MESSENGER"},
            },
        },
    }
    dt = (destination_type or "").strip().upper()
    if dt == "MESSAGING_INSTAGRAM_DIRECT_MESSENGER":
        afs: dict[str, Any] = {
            "optimization_type": "DOF_MESSAGING_DESTINATION",
            "call_to_actions": [
                {"type": "MESSAGE_PAGE", "value": {"app_destination": "MESSENGER"}},
                {"type": "INSTAGRAM_MESSAGE", "value": {"app_destination": "INSTAGRAM_DIRECT"}},
            ],
        }
        return {
            "name": cname,
            "object_story_spec": json.dumps(oss),
            "asset_feed_spec": json.dumps(afs),
        }
    return {
        "name": cname,
        "object_story_spec": json.dumps(oss),
    }


def _source_jpeg_bytes_for_ctwa(picture_url: str | None, logger: logging.Logger) -> tuple[bytes, str]:
    """Prefer pending ``full_picture``; fallback placeholder if CDN blocks anonymous fetch."""
    ua = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    if picture_url:
        try:
            r = httpx.get(picture_url, timeout=45.0, follow_redirects=True, headers=ua)
            if r.status_code == 200 and len(r.content) > 500:
                return r.content, "pending_full_picture"
        except httpx.HTTPError as e:
            logger.debug("full_picture download failed: %s", e)
    try:
        r = httpx.get("https://picsum.photos/800/800", timeout=45.0, follow_redirects=True)
        if r.status_code == 200 and len(r.content) > 500:
            return r.content, "placeholder_image"
    except httpx.HTTPError:
        pass
    return b"", ""


def _resolve_messaging_creative_assets(
    client: GraphClient,
    *,
    row: dict,
    target_adset: str,
    story: str,
    ad_name: str,
    page_sheet: str,
    adset_cache: dict[str, dict],
    logger: logging.Logger,
    require_whatsapp: bool = True,
) -> dict[str, str] | None:
    """Resolve page_id, optional WhatsApp E.164, image_hash, primary text for messaging creatives."""
    adset_row = get_adset_cached(client, target_adset, adset_cache)
    po = adset_row.get("promoted_object") or {}
    page_promo = str(po.get("page_id") or page_sheet or "").strip()
    if not page_promo:
        return None
    wa_raw = (row.get("whatsapp_number") or "").strip()
    if not wa_raw and page_promo and require_whatsapp:
        pg = client.graph_get(page_promo, {"fields": "id,phone"})
        wa_raw = str(pg.get("phone") or "").strip()
    wa_e164 = _normalize_whatsapp_e164(wa_raw) if require_whatsapp else ""
    if require_whatsapp and not wa_e164:
        return None
    msg_hint, pic_url = _pending_message_picture_for_post(story)
    jpeg, src = _source_jpeg_bytes_for_ctwa(pic_url, logger)
    if not jpeg:
        return None
    if src == "placeholder_image":
        logger.warning(
            "section=new_ads post=%s CTWA image: could not fetch full_picture; using placeholder (replace in Ads Manager if needed)",
            story,
        )
    img_hash = client.upload_ad_image_jpeg(jpeg, filename="p00_ctwa.jpg")
    if not img_hash:
        return None
    return {
        "page_id": page_promo,
        "wa_e164": wa_e164,
        "image_hash": img_hash,
        "message": msg_hint,
    }


def _post_messaging_adcreative_with_auto_fallback(
    client: GraphClient,
    acct: str,
    *,
    assets: dict[str, str],
    ad_name: str,
    manual_effective: bool,
    sheet_row: str,
    logger: logging.Logger,
) -> tuple[dict[str, Any], bool]:
    """
    POST adcreative; on manual path, retry with automatic multidest if Graph returns 1885374 or 2446125.

    Returns (response dict, manual_oss_succeeded): manual_oss_succeeded True if the creative in use
    is OSS-only WhatsApp (used for ad-level 2446125 retry).
    """
    fb_msg = (
        "Manual WhatsApp failed (likely no WABA linked). "
        "Falling back to automatic multi-destination to ensure ad delivery."
    )
    fallback_codes = frozenset({1885374, 2446125})

    def _build_payload(manual: bool, name_suffix: str = "") -> dict[str, Any]:
        cname = f"{ad_name}_creative{name_suffix}" if name_suffix else None
        return _multi_destination_messaging_creative_payload(
            ad_name,
            assets["page_id"],
            assets["message"],
            assets["image_hash"],
            assets["wa_e164"],
            manual_whatsapp_only=manual,
            creative_name=cname,
        )

    if manual_effective:
        payload_m = _build_payload(True)
        try:
            cr = client.graph_post(f"{acct}/adcreatives", payload_m)
        except GraphAuthError:
            raise
        err = cr.get("error")
        sc = _graph_error_subcode(err)
        if cr.get("id") and _creative_payload_is_manual_oss_only(payload_m):
            return cr, True
        if sc in fallback_codes:
            logger.warning("section=new_ads row=%s %s", sheet_row, fb_msg)
            payload_a = _build_payload(False, name_suffix="_auto_fallback")
            cr2 = client.graph_post(f"{acct}/adcreatives", payload_a)
            return cr2, False
        return cr, False

    payload_a = _build_payload(False)
    cr = client.graph_post(f"{acct}/adcreatives", payload_a)
    return cr, False


def _post_ad_with_optional_creative_retry(
    client: GraphClient,
    acct: str,
    *,
    ad_name: str,
    target_adset: str,
    cid: str,
    assets: dict[str, str],
    sheet_row: str,
    logger: logging.Logger,
    manual_oss_delivered: bool,
) -> tuple[dict[str, Any], str]:
    """POST ad; if 2446125 and last creative was manual OSS, rebuild automatic creative and retry once."""
    fb_msg = (
        "Manual WhatsApp failed (likely no WABA linked). "
        "Falling back to automatic multi-destination to ensure ad delivery."
    )
    body = {
        "name": ad_name,
        "adset_id": target_adset,
        "creative": json.dumps({"creative_id": cid}),
        "status": "PAUSED",
    }
    ad_res = client.graph_post(f"{acct}/ads", body)
    err = ad_res.get("error")
    sc = _graph_error_subcode(err) if isinstance(err, dict) else 0
    if sc == 2446125 and manual_oss_delivered:
        logger.warning("section=new_ads row=%s %s", sheet_row, fb_msg)
        payload_a = _multi_destination_messaging_creative_payload(
            ad_name,
            assets["page_id"],
            assets["message"],
            assets["image_hash"],
            assets["wa_e164"],
            manual_whatsapp_only=False,
            creative_name=f"{ad_name}_creative_ad_fb",
        )
        cr2 = client.graph_post(f"{acct}/adcreatives", payload_a)
        cid2 = str(cr2.get("id", "") or "")
        if cid2 and not cr2.get("error"):
            body["creative"] = json.dumps({"creative_id": cid2})
            ad_res2 = client.graph_post(f"{acct}/ads", body)
            return ad_res2, cid2
    return ad_res, cid


def main() -> None:
    p = argparse.ArgumentParser(
        description="Create ads from AI_Action_Plan (WhatsApp preflight; NEW_FROM_POST only in v1)."
    )
    add_common_args(p)
    p.add_argument(
        "--i-know-what-im-doing",
        action="store_true",
        help="Allow POST even if preflight FAIL (dangerous)",
    )
    args = p.parse_args()
    dry = init_cli(args)

    client = GraphClient(delay_ms=args.delay_ms, logger=log)
    acct = client.ad_account_id
    if not acct:
        log.error("AD_ACCOUNT_ID missing in environment")
        raise SystemExit(1)

    try:
        parsed = parse_action_plan(args.tab)
        rows = section_by_short_name(parsed, "new_ads")
    except Exception as e:
        log.exception("Failed to load/parse sheet: %s", e)
        raise SystemExit(1) from e

    if args.skip:
        rows = rows[args.skip :]
    if args.limit:
        rows = rows[: args.limit]

    adset_cache: dict[str, dict] = {}
    campaign_cache: dict[str, dict] = {}
    page_cache: dict[str, dict] = {}

    acct_currency = "HKD"
    min_budget_minor_i = 0
    if not dry:
        min_budget_minor_f, acct_currency = get_account_min_budget_minor(client)
        min_budget_minor_i = int(min_budget_minor_f)

    stats = {
        "rows_total": len(rows),
        "would_execute": 0,
        "ok": 0,
        "skipped": 0,
        "failed": 0,
        "preflight_fail": 0,
        "dup_skipped": 0,
    }
    t0 = time.monotonic()

    for row in rows:
        sr = row.get("_sheet_row", "?")
        mode = (row.get("create_mode") or "").strip().upper()
        if "DUPLICATE" in mode:
            log.warning(
                "section=new_ads row=%s status=skip reason=DUPLICATE_WITH_NEW_AUDIENCE not_implemented_v1",
                sr,
            )
            stats["dup_skipped"] += 1
            stats["skipped"] += 1
            continue

        if "NEW_FROM_POST" not in mode and mode:
            log.info("section=new_ads row=%s status=skip reason=unknown_mode %s", sr, mode)
            stats["skipped"] += 1
            continue

        story = (row.get("post_or_object_story_id") or "").strip()
        page_sheet = (row.get("page_id") or "").strip()
        camp_sheet = (row.get("campaign_id") or "").strip()
        ad_name = (row.get("suggested_ad_name") or "New ad").strip() or "New ad"

        if not story:
            log.info("section=new_ads row=%s status=skip reason=missing_post_or_object_story_id", sr)
            stats["skipped"] += 1
            continue

        res = _resolve_template_and_target(row)
        if res.mode == "missing":
            log.info("section=new_ads row=%s status=skip reason=missing_template_adset_id", sr)
            stats["skipped"] += 1
            continue

        target_adset = ""
        if res.mode == "legacy":
            target_adset = res.target_adset_id
        else:
            tpl_id = res.template_adset_id
            new_display_name = _p00_new_adset_display_name(ad_name, story)
            budget_cell = str(row.get("budget_suggest") or "")
            tag_labels = _row_audience_tag_labels_unique(row)
            interest_ids, _kept, tag_status = validate_and_get_ids(tag_labels)
            if tag_labels and not interest_ids:
                log.warning(
                    "section=new_ads row=%s create=skip reason=no_interest_ids tags=%s status=%s",
                    sr,
                    tag_labels,
                    tag_status,
                )
                stats["skipped"] += 1
                continue
            if tag_labels:
                log.info(
                    "section=new_ads row=%s audience_tags resolved=%s status=%s",
                    sr,
                    len(interest_ids),
                    tag_status,
                )

            def _preview_payload() -> dict[str, Any]:
                tpl_row = get_adset_cached(client, tpl_id, adset_cache)
                return _build_new_adset_payload(
                    client,
                    tpl_row,
                    display_name=new_display_name,
                    interest_ids=interest_ids,
                    budget_suggest_cell=budget_cell,
                    currency=acct_currency,
                    min_minor=min_budget_minor_i,
                    campaign_cache=campaign_cache,
                    logger=log,
                    sheet_row=str(sr),
                )

            if dry:
                try:
                    if not _validate_template_page_for_create(
                        client, tpl_id, page_sheet, adset_cache, log, str(sr)
                    ):
                        stats["failed"] += 1
                        continue
                    payload_preview = _preview_payload()
                    if not payload_preview:
                        stats["failed"] += 1
                        continue
                    red = _redact_adset_create_body(payload_preview)
                    log.info(
                        "section=new_ads row=%s dry_run would_get_template=%s",
                        sr,
                        tpl_id,
                    )
                    log.info(
                        "section=new_ads row=%s dry_run would_post_adsets redacted=%s",
                        sr,
                        json.dumps(red, ensure_ascii=False, default=str),
                    )
                    log.info(
                        "section=new_ads row=%s dry_run note=on_execute create_adset may auto-retry "
                        "destination_type WHATSAPP -> MESSAGING_INSTAGRAM_DIRECT_MESSENGER -> MESSENGER "
                        "if Graph returns 2446886 (no WhatsApp) or IG-related errors",
                        sr,
                    )
                    log.info(
                        "section=new_ads row=%s dry_run would_preflight would_post_creative+ad adset=<after_create> story=%s",
                        sr,
                        story,
                    )
                    stats["would_execute"] += 1
                except GraphAuthError:
                    log.error("auth_failure during dry_run template/preview")
                    raise SystemExit(1) from None
                continue

            try:
                if not _validate_template_page_for_create(client, tpl_id, page_sheet, adset_cache, log, str(sr)):
                    stats["failed"] += 1
                    continue
                tpl_row = get_adset_cached(client, tpl_id, adset_cache)
                payload = _build_new_adset_payload(
                    client,
                    tpl_row,
                    display_name=new_display_name,
                    interest_ids=interest_ids,
                    budget_suggest_cell=budget_cell,
                    currency=acct_currency,
                    min_minor=min_budget_minor_i,
                    campaign_cache=campaign_cache,
                    logger=log,
                    sheet_row=str(sr),
                )
                if not payload:
                    stats["failed"] += 1
                    continue
                new_id, _last_create_err = _create_adset_messaging_fallback(
                    client, payload, log, sheet_row=str(sr)
                )
                if not new_id:
                    stats["failed"] += 1
                    continue
                adset_cache.pop(new_id, None)
                log.info(
                    "section=new_ads row=%s create=ok new_adset_id=%s — paste into 目標廣告組合 ID to re-run without create",
                    sr,
                    new_id,
                )
                target_adset = new_id
            except GraphAuthError:
                log.error("auth_failure during create")
                raise SystemExit(1) from None

        if not target_adset:
            log.info("section=new_ads row=%s status=skip reason=missing_target_adset_id", sr)
            stats["skipped"] += 1
            continue

        if dry:
            log.info(
                "section=new_ads row=%s dry_run would_preflight target_adset=%s page_id=%s (no Graph)",
                sr,
                target_adset,
                norm_meta_graph_id(page_sheet),
            )
            log.info(
                "section=new_ads row=%s dry_run would_post_adcreative+ad adset=%s story=%s",
                sr,
                target_adset,
                story,
            )
            stats["would_execute"] += 1
            continue

        try:
            status, pmsgs = preflight_new_ad_whatsapp(
                client,
                target_adset_id=target_adset,
                sheet_campaign_id=camp_sheet,
                sheet_page_id=page_sheet,
                adset_cache=adset_cache,
                campaign_cache=campaign_cache,
                page_cache=page_cache,
                logger=log,
            )
        except GraphAuthError:
            log.error("auth_failure during preflight")
            raise SystemExit(1) from None

        log.info(
            "section=new_ads row=%s op=preflight adset=%s status=%s detail=%s",
            sr,
            target_adset,
            status,
            "; ".join(pmsgs) if pmsgs else "ok",
        )

        if status == "FAIL" and not getattr(args, "i_know_what_im_doing", False):
            log.error("section=new_ads row=%s preflight=FAIL — fix sheet or use backup plan", sr)
            stats["preflight_fail"] += 1
            stats["failed"] += 1
            continue
        if status == "WARN":
            log.warning("section=new_ads row=%s preflight=WARN proceeding with caution", sr)

        adset_row = get_adset_cached(client, target_adset, adset_cache)
        dst = str(adset_row.get("destination_type") or "").strip().upper()

        messaging_assets: dict[str, str] | None = None
        creative_payload: dict[str, Any] | None = None
        creative_kind = ""
        manual_effective = False
        messaging_no_wa_dst: str | None = None

        if dst in WHATSAPP_DESTINATION_TYPES:
            manual_requested = _row_wants_manual_whatsapp_only(row)
            hybrid = _adset_destination_requires_automatic_multidest(dst)
            manual_effective = manual_requested and not hybrid
            if hybrid and manual_requested:
                log.warning(
                    "Warning: Ad Set is Hybrid. Forcing automatic multi-destination payload to avoid Error 2446493."
                )
            log.info(
                "section=new_ads row=%s messaging_creative intent=%s",
                sr,
                "manual_whatsapp_only"
                if manual_effective
                else (
                    "automatic_destinations_forced"
                    if manual_requested
                    else "automatic_destinations"
                ),
            )
            messaging_assets = _resolve_messaging_creative_assets(
                client,
                row=row,
                target_adset=target_adset,
                story=story,
                ad_name=ad_name,
                page_sheet=page_sheet,
                adset_cache=adset_cache,
                logger=log,
            )
            if not messaging_assets:
                log.error(
                    "section=new_ads row=%s CTWA creative needs 專頁 ID + WhatsApp (sheet 欄 or Page phone Graph)",
                    sr,
                )
                stats["failed"] += 1
                continue
            if manual_effective:
                creative_kind = "oss_whatsapp_only"
            elif manual_requested:
                creative_kind = "multidest_automatic_forced"
            else:
                creative_kind = "multidest_automatic"
        elif dst in MESSAGING_NO_WHATSAPP_CREATIVE_TYPES:
            messaging_no_wa_dst = dst
            log.info(
                "section=new_ads row=%s messaging_creative intent=no_whatsapp_ctas destination_type=%s",
                sr,
                dst,
            )
            messaging_assets = _resolve_messaging_creative_assets(
                client,
                row=row,
                target_adset=target_adset,
                story=story,
                ad_name=ad_name,
                page_sheet=page_sheet,
                adset_cache=adset_cache,
                logger=log,
                require_whatsapp=False,
            )
            if not messaging_assets:
                log.error(
                    "section=new_ads row=%s no-WA messaging creative needs 專頁 ID + image "
                    "(pending full_picture or placeholder upload)",
                    sr,
                )
                stats["failed"] += 1
                continue
            creative_kind = f"no_whatsapp_{dst.lower()}"
        else:
            creative_payload = {
                "name": f"{ad_name}_creative",
                "object_story_id": story,
            }
            creative_kind = "object_story_id"

        log.info(
            "section=new_ads row=%s op=post_adcreative+ad adset=%s story=%s kind=%s",
            sr,
            target_adset,
            story,
            creative_kind,
        )

        try:
            manual_oss_for_ad = False
            cid = ""
            if messaging_no_wa_dst:
                payload_nw = _no_whatsapp_messaging_creative_payload(
                    ad_name,
                    messaging_assets["page_id"] if messaging_assets else "",
                    messaging_assets["message"] if messaging_assets else "",
                    messaging_assets["image_hash"] if messaging_assets else "",
                    messaging_no_wa_dst,
                    creative_name=f"{ad_name}_creative",
                )
                cr = client.graph_post(f"{acct}/adcreatives", payload_nw)
            elif messaging_assets is not None:
                cr, manual_oss_for_ad = _post_messaging_adcreative_with_auto_fallback(
                    client,
                    acct,
                    assets=messaging_assets,
                    ad_name=ad_name,
                    manual_effective=manual_effective,
                    sheet_row=str(sr),
                    logger=log,
                )
            else:
                cr = client.graph_post(f"{acct}/adcreatives", creative_payload or {})
            if cr.get("error"):
                log_graph_error_payload(log, cr.get("error") if isinstance(cr.get("error"), dict) else None)
                stats["failed"] += 1
                continue
            cid = str(cr.get("id", "") or "")
            if not cid:
                log.error("section=new_ads row=%s no creative id in response", sr)
                stats["failed"] += 1
                continue

            if messaging_no_wa_dst:
                ad_body = {
                    "name": ad_name,
                    "adset_id": target_adset,
                    "creative": json.dumps({"creative_id": cid}),
                    "status": "PAUSED",
                }
                ad_res = client.graph_post(f"{acct}/ads", ad_body)
                cid_used = cid
            elif messaging_assets is not None:
                ad_res, cid_used = _post_ad_with_optional_creative_retry(
                    client,
                    acct,
                    ad_name=ad_name,
                    target_adset=target_adset,
                    cid=cid,
                    assets=messaging_assets,
                    sheet_row=str(sr),
                    logger=log,
                    manual_oss_delivered=manual_oss_for_ad,
                )
            else:
                ad_body = {
                    "name": ad_name,
                    "adset_id": target_adset,
                    "creative": json.dumps({"creative_id": cid}),
                    "status": "PAUSED",
                }
                ad_res = client.graph_post(f"{acct}/ads", ad_body)
                cid_used = cid

            ad_err = ad_res.get("error")
            if isinstance(ad_err, dict):
                if int(ad_err.get("error_subcode") or 0) == 2446125:
                    log.error(
                        "section=new_ads row=%s ad create failed (2446125): link WhatsApp Business to the "
                        "campaign in Ads Manager. Meta: %s",
                        sr,
                        ad_err.get("error_user_msg") or ad_err.get("message"),
                    )
                else:
                    log_graph_error_payload(log, ad_err)
            elif ad_err:
                log_graph_error_payload(log, None)
            if ad_res.get("error"):
                stats["failed"] += 1
                continue
            log.info(
                "section=new_ads row=%s op=post_ad ad_id=%s creative_id=%s status=ok",
                sr,
                ad_res.get("id"),
                cid_used,
            )
            stats["ok"] += 1
        except GraphAuthError:
            log.error("auth_failure")
            raise SystemExit(1) from None
        except Exception as e:
            log.exception("row=%s err=%s", sr, e)
            stats["failed"] += 1

    elapsed = time.monotonic() - t0
    log.info(
        "summary rows_total=%s dry_run=%s ok=%s would=%s skipped=%s failed=%s dup_skipped=%s elapsed_sec=%.2f",
        stats["rows_total"],
        dry,
        stats["ok"],
        stats["would_execute"],
        stats["skipped"],
        stats["failed"],
        stats["dup_skipped"],
        elapsed,
    )
    client.close()
    if stats["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
