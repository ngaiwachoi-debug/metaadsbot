#!/usr/bin/env python3
"""Execute 📋 新建廣告清單: Native Cloning — NEW_FROM_POST → ``object_story_id`` + ad (dry-run default).

**Creative:** Always ``object_story_id`` (original page post / video). No ``object_story_spec`` or CTWA assembly.

**P00 create path:** If **目標** is empty and **複製自AdSet ID** is set, **GET** template ad set, verify page,
**POST** exclusive campaign (DNA from parent campaign), then **POST** ad set with champion template DNA
(``destination_type``, ``optimization_goal``, bid, etc.) plus Sheet budget and merged targeting. Targeting is
**normalized to Advantage+ / auto placements** (full publisher + device platforms; position locks stripped).

**Ad set create:** Native clone: (1) primary template DNA; (2) **smart_downgrade**; (3) reactive **safe_mode**
(``UNDEFINED`` + ``ENGAGED_USERS`` + ``IMPRESSIONS`` + ``LOWEST_COST_WITHOUT_CAP``). **Proactive Safe Mode**
(messaging template + post without messaging CTA): ordered **compatibility profiles** under ``OUTCOME_ENGAGEMENT``
(first winner ``ON_POST`` + ``POST_ENGAGEMENT``), strict bid-field sanitization, post format filter (video-only
profiles skipped for image/unknown posts), then terminal smart_downgrade + proactive safe if all fail.

**Smart routing (P00):** ``GET`` post ``call_to_action`` plus ``attachments{media_type}`` / ``format`` for
``post_format=video|image|unknown``. If the template is messaging but the post has **no** messaging CTA, run the
proactive profile ladder. If the post **has** a messaging CTA, keep full native template clone.

**Template source:** **複製自 AdSet ID** must match **專頁 ID**. For P00 rows, ``ai_optimizer`` fills this from
``best_p00_template_adset_id`` (same page + pool, lowest 7d CPC among **non-messaging** ad sets; tier 2 relaxes
campaign objective if needed). Empty cell means skip (no ``SHOP_CONFIGS`` fallback for P00 ``NEW_FROM_POST``).

**Audience:** see ``merge_interests_into_targeting``, EXCLUDE/geo, ``ensure_hong_kong_geo``.

**Legacy** path (non-empty **目標**) unchanged where applicable.
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

from action_plan_parse import parse_action_plan, section_by_short_name
from ai_optimizer import (
    compute_champion_tags_by_pool,
    load_refined_rows_from_sheet,
    validate_and_get_ids,
    _p00_champion_tags_for_pool,
)
from meta_audience_hints import (
    ParsedExclusions,
    collect_exclude_raw_segments_from_cells,
    parse_exclude_segments,
)
from engine import SHOP_CONFIGS
from meta_actions_common import add_common_args, init_cli
from meta_actions_logging import log_graph_error_payload
from meta_graph_write import (
    GraphAuthError,
    GraphClient,
    get_account_min_budget_minor,
    hkd_display_string_to_minor,
)
from meta_preflight import (
    get_adset_cached,
    get_campaign_cached,
    preflight_new_ad_row,
)
from meta_targeting_merge import (
    apply_auto_placements_to_targeting,
    drop_root_id_key,
    ensure_hong_kong_geo,
    merge_excluded_geo_into_targeting,
    merge_interests_into_targeting,
    merge_locale_exclusions_into_targeting,
    split_tags,
)
from meta_utils import norm_meta_graph_id, normalize_object_story_id

log = logging.getLogger("new_ads")

_ATTRIBUTION_SPEC_1D_CLICK: list[dict[str, Any]] = [
    {"event_type": "CLICK_THROUGH", "window_days": 1},
]

_EXCLUSIVE_CAMPAIGN_ID_PLACEHOLDER = "<exclusive_campaign_id>"


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


def _strip_meta_adset_copy_suffix(name: str) -> str:
    """Remove trailing Meta copy suffix (locale variants: 副本, Copy, copy)."""
    return re.sub(r"\s*-\s*(副本|Copy|copy)$", "", name).strip()


def _serialize_special_ad_categories(raw: Any) -> str:
    """Graph form field for campaign create: JSON array string."""
    if raw is None:
        return json.dumps([], separators=(",", ":"))
    if isinstance(raw, list):
        return json.dumps(raw, separators=(",", ":"))
    if isinstance(raw, str) and raw.strip().startswith("["):
        return raw.strip()
    return json.dumps([], separators=(",", ":"))


def _boolish_from_graph(raw: Any) -> bool | None:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return None
    t = str(raw).strip().lower()
    if t in ("1", "true", "yes"):
        return True
    if t in ("0", "false", "no"):
        return False
    return None


def _extract_valid_bid_amount_minor(template_data: dict[str, Any]) -> str:
    raw = template_data.get("bid_amount")
    if raw is None:
        return ""
    if isinstance(raw, dict):
        inner = raw.get("amount")
        if inner is not None:
            raw = inner
        else:
            return ""
    s = str(raw).strip()
    if not s:
        return ""
    try:
        if float(s) > 0:
            return s
    except (TypeError, ValueError):
        return ""
    return ""


def _apply_template_bid_strategy_dna(
    payload: dict[str, Any],
    template_data: dict[str, Any],
    *,
    logger: logging.Logger,
    sheet_row: str,
    fallback_bid_minor: str = "",
) -> None:
    """
    Clone bidding DNA from same-shop template ad set.
    - Bid-cap style strategies require bid_amount (Graph 1815857 if missing — Meta may default to bid cap).
    - If template omits bid_amount, use ``fallback_bid_minor`` (e.g. account minimum) when provided.
    """
    bs = str(template_data.get("bid_strategy") or "").strip()
    if not bs:
        og = str(template_data.get("optimization_goal") or "").strip().upper()
        fbm = fallback_bid_minor.strip()
        if og == "CONVERSATIONS" and fbm:
            payload["bid_strategy"] = "LOWEST_COST_WITH_BID_CAP"
            payload["bid_amount"] = fbm
            logger.info(
                "section=new_ads row=%s create=default_bid_strategy=LOWEST_COST_WITH_BID_CAP "
                "(template had no bid_strategy; CONVERSATIONS needs bid_amount)",
                sheet_row,
            )
        return
    bs_u = bs.upper()
    bid_minor = _extract_valid_bid_amount_minor(template_data)
    needs_bid_amount = bs_u in {"LOWEST_COST_WITH_BID_CAP", "TARGET_COST"}
    if needs_bid_amount and not bid_minor and fallback_bid_minor.strip():
        bid_minor = fallback_bid_minor.strip()
        logger.warning(
            "section=new_ads row=%s create=fallback_bid_amount=%s template_strategy=%s",
            sheet_row,
            bid_minor,
            bs,
        )
    if needs_bid_amount and not bid_minor:
        logger.warning(
            "section=new_ads row=%s create=fallback_bid_strategy=%s template=%s reason=missing_bid_amount",
            sheet_row,
            "LOWEST_COST_WITHOUT_CAP",
            bs,
        )
        payload["bid_strategy"] = "LOWEST_COST_WITHOUT_CAP"
        payload.pop("bid_amount", None)
        return
    payload["bid_strategy"] = bs
    if needs_bid_amount:
        payload["bid_amount"] = bid_minor
    else:
        payload.pop("bid_amount", None)


def _campaign_body_has_positive_daily_budget(body: dict[str, Any]) -> bool:
    """True if outgoing campaign create body has campaign-level daily_budget > 0 (minor units, string or numeric)."""
    raw = body.get("daily_budget")
    if raw is None:
        return False
    try:
        return float(str(raw).strip()) > 0.0
    except (TypeError, ValueError):
        return False


def _log_budget_owner_line(
    logger: logging.Logger,
    sheet_row: str,
    *,
    campaign_body: dict[str, Any] | None,
    adset_payload: dict[str, Any],
) -> None:
    """Single INFO: whether budget sits on the new campaign (CBO) vs the new ad set (ABO), with minor units."""
    cb = campaign_body or {}
    ap = adset_payload or {}
    cbo = _campaign_body_has_positive_daily_budget(cb)
    raw_c = cb.get("daily_budget")
    raw_a = ap.get("daily_budget")
    try:
        cm = int(float(str(raw_c).strip())) if raw_c not in (None, "") else 0
    except (TypeError, ValueError):
        cm = 0
    try:
        am = int(float(str(raw_a).strip())) if raw_a not in (None, "") else 0
    except (TypeError, ValueError):
        am = 0
    if cbo:
        logger.info(
            "section=new_ads row=%s budget_owner=campaign CBO campaign_daily_budget_minor=%s adset_daily_budget_minor=0",
            sheet_row,
            cm,
        )
    else:
        logger.info(
            "section=new_ads row=%s budget_owner=adset ABO campaign_daily_budget_minor=0 adset_daily_budget_minor=%s",
            sheet_row,
            am,
        )


def _build_exclusive_campaign_payload(
    parent_camp: dict[str, Any],
    *,
    display_name: str,
    graph_status: str,
    budget_suggest_cell: str,
    currency: str,
    min_minor: int,
    logger: logging.Logger,
    sheet_row: str,
    objective_override: str | None = None,
    force_abo: bool = False,
) -> dict[str, Any]:
    """
    New campaign: inherit objective, special_ad_categories, optional buying_type from parent.
    CBO parent: set campaign daily_budget from sheet (minor). Else ABO at ad set (no campaign budget).
    CBO (campaign ``daily_budget`` > 0): omit ``is_adset_budget_sharing_enabled`` — CBO is implied; sending
    ``true`` with ``daily_budget`` can trigger Meta 4834002. ABO: set ``is_adset_budget_sharing_enabled`` false.

    When ``objective_override`` is set (e.g. proactive P00 safe mode), it replaces the parent's objective.

    ``force_abo``: If True, never set campaign ``daily_budget`` even when the parent uses CBO — budget goes on the
    ad set instead. Required for proactive safe ad sets: Meta rejects ad-set ``bid_strategy`` combinations under CBO
    (1815857 / 2490408 in production).
    """
    ov = str(objective_override or "").strip()
    if ov:
        obj = ov
    else:
        obj = str(parent_camp.get("objective") or "").strip()
        if not obj:
            logger.error("section=new_ads row=%s exclusive_campaign=abort parent missing objective", sheet_row)
            return {}

    name = _strip_meta_adset_copy_suffix(display_name).strip()[:255]
    parent_cdb = float((parent_camp.get("daily_budget") or 0) or 0)
    body: dict[str, Any] = {
        "name": name,
        "objective": obj,
        "status": graph_status,
        "special_ad_categories": _serialize_special_ad_categories(parent_camp.get("special_ad_categories")),
    }
    bt = str(parent_camp.get("buying_type") or "").strip()
    if bt:
        body["buying_type"] = bt

    if parent_cdb > 0 and not force_abo:
        minor = hkd_display_string_to_minor(budget_suggest_cell, currency=currency)
        if minor < int(min_minor):
            logger.warning(
                "section=new_ads row=%s exclusive_campaign bump_budget minor %s -> min %s",
                sheet_row,
                minor,
                int(min_minor),
            )
            minor = int(min_minor)
        if minor <= 0:
            logger.error(
                "section=new_ads row=%s exclusive_campaign=abort CBO parent needs 預算建議 > 0 got %r",
                sheet_row,
                budget_suggest_cell,
            )
            return {}
        body["daily_budget"] = str(minor)
    elif parent_cdb > 0 and force_abo:
        logger.info(
            "section=new_ads row=%s exclusive_campaign force_abo=1 skip campaign daily_budget (parent was CBO); "
            "ad set will own budget",
            sheet_row,
        )

    # Meta 4834002: sending is_adset_budget_sharing_enabled=true together with campaign daily_budget
    # can be rejected (flag + budget read as conflicting). CBO is implied by campaign daily_budget;
    # omit the flag when using campaign-level budget.
    parent_share = _boolish_from_graph(parent_camp.get("is_adset_budget_sharing_enabled"))
    if _campaign_body_has_positive_daily_budget(body):
        body.pop("is_adset_budget_sharing_enabled", None)
        logger.info(
            "section=new_ads row=%s exclusive_campaign omit=is_adset_budget_sharing_enabled "
            "(CBO implied by campaign daily_budget; parent had %s)",
            sheet_row,
            parent_share,
        )
    else:
        body["is_adset_budget_sharing_enabled"] = "false"
    return body


# Post-level CTA types that justify keeping a messaging template clone (Marketing API).
_MESSAGING_CTA_TYPES: frozenset[str] = frozenset(
    {
        "WHATSAPP_MESSAGE",
        "MESSAGE_PAGE",
        "INSTAGRAM_MESSAGE",
        "SEND_MESSAGE",
        "MESSAGE",
    }
)

# Keep in sync with engine._MESSAGING_DESTINATION_TYPES / _is_messaging_template_meta.
_MESSAGING_TEMPLATE_DESTINATION_TYPES: frozenset[str] = frozenset(
    {
        "WHATSAPP_MESSAGE",
        "MESSENGER",
        "WHATSAPP",
        "INSTAGRAM_MESSAGE",
        "MESSAGING_MESSENGER",
        "INSTAGRAM_DIRECT",
    }
)

PostFormatKind = Literal["video", "image", "unknown"]


@dataclass(frozen=True)
class PostRoutingHints:
    """Single Graph GET: messaging CTA + coarse post format for proactive profile filtering."""

    has_messaging_cta: bool
    format_kind: PostFormatKind


@dataclass(frozen=True)
class ProactiveProfile:
    """One proactive compatibility row (OUTCOME_ENGAGEMENT campaign + object_story_id creative)."""

    profile_id: str
    destination_type: str
    optimization_goal: str
    billing_event: str
    bid_mode: Literal["without_cap", "with_bid_cap"]
    video_only: bool = False


# Ordered ladder: without_cap blocks first, then with_bid_cap mirrors; video_only gated by PostRoutingHints.
_PROACTIVE_COMPAT_PROFILES_ORDERED: tuple[ProactiveProfile, ...] = (
    ProactiveProfile("p1_on_post_pe", "ON_POST", "POST_ENGAGEMENT", "IMPRESSIONS", "without_cap"),
    ProactiveProfile("p2_on_post_eu", "ON_POST", "ENGAGED_USERS", "IMPRESSIONS", "without_cap"),
    ProactiveProfile("p3_on_page_eu", "ON_PAGE", "ENGAGED_USERS", "IMPRESSIONS", "without_cap"),
    ProactiveProfile("p4_undef_eu", "UNDEFINED", "ENGAGED_USERS", "IMPRESSIONS", "without_cap"),
    ProactiveProfile("p5_on_post_pe_cap", "ON_POST", "POST_ENGAGEMENT", "IMPRESSIONS", "with_bid_cap"),
    ProactiveProfile("p6_on_post_eu_cap", "ON_POST", "ENGAGED_USERS", "IMPRESSIONS", "with_bid_cap"),
    ProactiveProfile("p7_on_page_eu_cap", "ON_PAGE", "ENGAGED_USERS", "IMPRESSIONS", "with_bid_cap"),
    ProactiveProfile("p8_undef_eu_cap", "UNDEFINED", "ENGAGED_USERS", "IMPRESSIONS", "with_bid_cap"),
    ProactiveProfile(
        "p9_on_video_thruplay", "ON_VIDEO", "THRUPLAY", "THRUPLAY", "without_cap", video_only=True
    ),
    ProactiveProfile(
        "p10_on_video_vv", "ON_VIDEO", "VIDEO_VIEWS", "IMPRESSIONS", "without_cap", video_only=True
    ),
)


def _infer_post_format_kind(graph_post: dict[str, Any]) -> PostFormatKind:
    """Coarse video vs image from attachments / format; unknown when ambiguous or missing."""
    att = graph_post.get("attachments")
    rows: list[Any]
    if isinstance(att, dict):
        rows = list(att.get("data") or []) if isinstance(att.get("data"), list) else []
    elif isinstance(att, list):
        rows = att
    else:
        rows = []
    saw_video = False
    saw_photo = False
    for item in rows:
        if not isinstance(item, dict):
            continue
        mt = str(item.get("media_type") or item.get("type") or "").strip().lower()
        if "video" in mt:
            saw_video = True
        if any(x in mt for x in ("photo", "image", "album")) or mt in ("photo", "image"):
            saw_photo = True
    if saw_video:
        return "video"
    if saw_photo or rows:
        return "image"
    fmt = str(graph_post.get("format") or "").strip().lower()
    if "video" in fmt:
        return "video"
    if fmt in ("photo", "image", "link", "share", "status"):
        return "image"
    return "unknown"


def _fetch_post_routing_hints(
    client: GraphClient, object_story_id: str, logger: logging.Logger, sheet_row: str
) -> PostRoutingHints:
    """GET call_to_action + attachments/format in one round-trip."""
    oid = norm_meta_graph_id(str(object_story_id or "").strip())
    if not oid:
        return PostRoutingHints(has_messaging_cta=False, format_kind="unknown")
    r = client.graph_get(
        oid,
        {"fields": "call_to_action,attachments{media_type,type},format"},
    )
    if not isinstance(r, dict):
        logger.warning(
            "section=new_ads row=%s post_routing_hints unexpected_response id=%s",
            sheet_row,
            oid,
        )
        return PostRoutingHints(has_messaging_cta=False, format_kind="unknown")
    err = r.get("error")
    if isinstance(err, dict):
        logger.warning(
            "section=new_ads row=%s post_routing_hints graph_error id=%s err=%s",
            sheet_row,
            oid,
            err,
        )
        return PostRoutingHints(has_messaging_cta=False, format_kind="unknown")
    cta = r.get("call_to_action")
    has_msg = False
    if isinstance(cta, dict):
        t = str(cta.get("type") or "").strip().upper()
        has_msg = t in _MESSAGING_CTA_TYPES
    fmt_kind = _infer_post_format_kind(r)
    logger.info(
        "section=new_ads row=%s post_format=%s post_routing_hints id=%s",
        sheet_row,
        fmt_kind,
        oid,
    )
    return PostRoutingHints(has_messaging_cta=has_msg, format_kind=fmt_kind)


def _check_post_has_messaging_cta(
    client: GraphClient, object_story_id: str, logger: logging.Logger, sheet_row: str
) -> bool:
    """True if Graph returns call_to_action.type in _MESSAGING_CTA_TYPES (one GET; use _fetch_post_routing_hints for format)."""
    return _fetch_post_routing_hints(client, object_story_id, logger, sheet_row).has_messaging_cta


def _filter_proactive_profiles_for_post_format(
    profiles: tuple[ProactiveProfile, ...], format_kind: PostFormatKind
) -> list[ProactiveProfile]:
    """Drop video_only profiles for image/unknown (unknown: conservative — no doomed ON_VIDEO attempts)."""
    if format_kind == "video":
        return list(profiles)
    return [p for p in profiles if not p.video_only]


def _parse_positive_min_bid_minor(min_bid_minor: str) -> int | None:
    try:
        v = int(str(min_bid_minor or "").strip())
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _apply_proactive_compat_profile(
    base_payload: dict[str, Any],
    enforced_name: str,
    profile: ProactiveProfile,
    logger: logging.Logger,
    sheet_row: str,
    min_bid_minor: str,
) -> dict[str, Any] | None:
    """
    Destructive merge: destination/goal/billing/bid + page-only promoted_object.
    Returns None if with_bid_cap and min_bid_minor is invalid (caller skips POST).
    """
    mb_cap: int | None = None
    if profile.bid_mode == "with_bid_cap":
        mb_cap = _parse_positive_min_bid_minor(min_bid_minor)
        if mb_cap is None:
            logger.info(
                "section=new_ads row=%s proactive_compat skip=%s reason=invalid_min_bid minor=%r",
                sheet_row,
                profile.profile_id,
                min_bid_minor,
            )
            return None

    p = copy.deepcopy(base_payload)
    p["name"] = _strip_meta_adset_copy_suffix(enforced_name)[:255]
    p["destination_type"] = profile.destination_type
    p["optimization_goal"] = profile.optimization_goal
    p["billing_event"] = profile.billing_event
    p.pop("attribution_spec", None)

    if profile.bid_mode == "without_cap":
        _strip_bid_root_keys(p)
        p["bid_strategy"] = "LOWEST_COST_WITHOUT_CAP"
        _strip_bid_root_keys_except_strategy(p)
    else:
        _apply_bid_cap_with_amount(p, str(mb_cap))

    po = _parse_payload_promoted_object_json(p)
    pid = norm_meta_graph_id(str(po.get("page_id") or ""))
    if pid:
        p["promoted_object"] = json.dumps({"page_id": pid}, separators=(",", ":"))
    else:
        p.pop("promoted_object", None)

    logger.info(
        "section=new_ads row=%s proactive_compat apply=%s dst=%s og=%s be=%s bid_mode=%s video_only=%s",
        sheet_row,
        profile.profile_id,
        profile.destination_type,
        profile.optimization_goal,
        profile.billing_event,
        profile.bid_mode,
        profile.video_only,
    )
    return p


def _try_proactive_adset_ladder(
    client: GraphClient,
    base_payload: dict[str, Any],
    *,
    enforced_name: str,
    logger: logging.Logger,
    sheet_row: str,
    min_bid_minor: str,
    post_format_kind: PostFormatKind,
) -> tuple[str | None, dict[str, Any] | None]:
    """Try filtered proactive profiles until create_adset succeeds."""
    profiles = _filter_proactive_profiles_for_post_format(
        _PROACTIVE_COMPAT_PROFILES_ORDERED, post_format_kind
    )
    logger.info(
        "section=new_ads row=%s proactive_compat ladder_start format=%s profiles=%s",
        sheet_row,
        post_format_kind,
        [x.profile_id for x in profiles],
    )
    last_err: dict[str, Any] | None = None
    for n, prof in enumerate(profiles, start=1):
        built = _apply_proactive_compat_profile(
            base_payload, enforced_name, prof, logger, sheet_row, min_bid_minor
        )
        if not built:
            continue
        body = client.create_adset(built)
        err = body.get("error") if isinstance(body.get("error"), dict) else None
        if not err:
            nid = _new_adset_id_from_create_response(body)
            if nid:
                logger.info(
                    "section=new_ads row=%s create_adset attempt=proactive_compat_%s profile=%s ok adset=%s",
                    sheet_row,
                    n,
                    prof.profile_id,
                    nid,
                )
                return nid, None
            logger.error(
                "section=new_ads row=%s create_adset proactive_compat_%s empty id body=%s",
                sheet_row,
                n,
                body,
            )
            return None, None
        last_err = err
        log_graph_error_payload(logger, err, prefix=f"create_adset proactive_compat_{n} ")
    return None, last_err


def _template_row_is_messaging(tpl_row: dict[str, Any]) -> bool:
    og = str(tpl_row.get("optimization_goal") or "").strip().upper()
    if og == "CONVERSATIONS":
        return True
    dt = str(tpl_row.get("destination_type") or "").strip().upper()
    return bool(dt and dt in _MESSAGING_TEMPLATE_DESTINATION_TYPES)


def _new_campaign_id_from_create_response(body: dict[str, Any]) -> str:
    if not isinstance(body, dict) or body.get("error"):
        return ""
    return norm_meta_graph_id(str(body.get("id") or ""))


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
    graph_status: str = "PAUSED",
    sheet_campaign_id: str = "",
    forced_campaign_id: str | None = None,
    planned_cbo_omit_adset_budget: bool | None = None,
    parsed_exclusions: ParsedExclusions | None = None,
    object_story_id: str | None = None,
) -> dict[str, Any]:
    if forced_campaign_id is not None:
        fc_raw = (forced_campaign_id or "").strip()
        if not fc_raw:
            logger.error("section=new_ads row=%s create=abort empty forced_campaign_id", sheet_row)
            return {}
        campaign_id = fc_raw
        if planned_cbo_omit_adset_budget is True:
            cdb = 1.0
        elif planned_cbo_omit_adset_budget is False:
            cdb = 0.0
        else:
            cid_norm = norm_meta_graph_id(fc_raw)
            if not _looks_like_meta_id(cid_norm):
                logger.error(
                    "section=new_ads row=%s create=abort forced_campaign_id not a Meta id "
                    "(use planned_cbo_omit_adset_budget for dry-run placeholder)",
                    sheet_row,
                )
                return {}
            camp = get_campaign_cached(client, cid_norm, campaign_cache)
            cdb = float((camp.get("daily_budget") or 0) or 0)
    else:
        tpl_cid = norm_meta_graph_id(str(template_data.get("campaign_id") or ""))
        sheet_cid = norm_meta_graph_id(str(sheet_campaign_id or "").strip())
        if _looks_like_meta_id(sheet_cid):
            campaign_id = sheet_cid
            if tpl_cid and sheet_cid != tpl_cid:
                logger.warning(
                    "section=new_ads row=%s create=campaign_id from sheet 宣傳活動 ID=%s "
                    "(template ad set had campaign_id=%s)",
                    sheet_row,
                    sheet_cid,
                    tpl_cid,
                )
        elif tpl_cid:
            campaign_id = tpl_cid
        else:
            logger.error(
                "section=new_ads row=%s create=abort missing campaign_id (sheet 宣傳活動 ID empty and template has none)",
                sheet_row,
            )
            return {}
        cid_for_get = norm_meta_graph_id(campaign_id)
        camp = get_campaign_cached(client, cid_for_get, campaign_cache)
        cdb = float((camp.get("daily_budget") or 0) or 0)

    og = str(template_data.get("optimization_goal") or "").strip()
    be = str(template_data.get("billing_event") or "").strip()
    if not og or not be:
        logger.error(
            "section=new_ads row=%s create=abort template missing optimization_goal or billing_event",
            sheet_row,
        )
        return {}

    pe = parsed_exclusions or ParsedExclusions()
    tgt = _parse_targeting_dict(template_data.get("targeting"))
    tgt = ensure_hong_kong_geo(tgt)
    merged = merge_interests_into_targeting(tgt, interest_ids)
    merged = merge_excluded_geo_into_targeting(merged, pe.country_codes)
    merged = merge_locale_exclusions_into_targeting(merged, pe.locale_keys)
    merged = apply_auto_placements_to_targeting(merged)
    if pe.locale_keys:
        logger.warning(
            "section=new_ads row=%s targeting=locale_exclusion_not_sent_to_graph keys=%s "
            "(Marketing API targeting has no stable excluded_locales; geo exclusions applied)",
            sheet_row,
            pe.locale_keys,
        )
    for w in pe.warnings:
        logger.warning("section=new_ads row=%s exclusion_parse=%s", sheet_row, w)
    gl = merged.get("geo_locations") if isinstance(merged.get("geo_locations"), dict) else {}
    exgl = merged.get("excluded_geo_locations") if isinstance(merged.get("excluded_geo_locations"), dict) else {}
    gco = [str(x).upper() for x in (gl.get("countries") or []) if x]
    exco = [str(x).upper() for x in (exgl.get("countries") or []) if x]
    if "HK" in gco and "HK" in exco:
        logger.warning(
            "section=new_ads row=%s targeting=warn geo_locations includes HK and excluded_geo_locations excludes HK "
            "(audience may be empty; check pool EXCLUDE copy)",
            sheet_row,
        )
    merged = drop_root_id_key(merged)

    po_raw = copy.deepcopy(_promoted_object_dict(template_data.get("promoted_object")))
    po_clean = drop_root_id_key(po_raw)
    story_norm = norm_meta_graph_id(str(object_story_id or "").strip())
    if story_norm:
        page_from = norm_meta_graph_id(str(po_clean.get("page_id") or ""))
        if not page_from:
            page_from = _promoted_page_id_from_adset(template_data)
        if page_from:
            # Ad set promoted_object is page-scoped only; post/video is object_story_id on the creative.
            po_clean = {"page_id": page_from}
            logger.info(
                "section=new_ads row=%s create=promoted_object native_clone page_id=%s (post %s on creative only)",
                sheet_row,
                page_from,
                story_norm,
            )
        else:
            logger.warning(
                "section=new_ads row=%s create=promoted_object skip native_post merge reason=no page_id",
                sheet_row,
            )

    clean_name = _strip_meta_adset_copy_suffix(display_name)[:255]
    payload: dict[str, Any] = {
        "name": clean_name,
        "campaign_id": campaign_id,
        "optimization_goal": og,
        "billing_event": be,
        "targeting": json.dumps(merged, separators=(",", ":")),
        "attribution_spec": json.dumps(_ATTRIBUTION_SPEC_1D_CLICK, separators=(",", ":")),
        "status": graph_status,
    }
    if po_clean:
        payload["promoted_object"] = json.dumps(po_clean, separators=(",", ":"))

    # Account min can be 0 on some reads; still need a positive bid_amount for bid-cap messaging ad sets.
    m = int(min_minor)
    fb_bid = str(m) if m > 0 else "100"
    _apply_template_bid_strategy_dna(
        payload,
        template_data,
        logger=logger,
        sheet_row=sheet_row,
        fallback_bid_minor=fb_bid,
    )
    dt = str(template_data.get("destination_type") or "").strip()
    if dt:
        payload["destination_type"] = dt

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


def _redact_campaign_create_body(body: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in body.items():
        if k == "special_ad_categories" and isinstance(v, str) and len(v) > 80:
            out[k] = f"<redacted len={len(v)}>"
        else:
            out[k] = v
    return out


def _new_adset_id_from_create_response(body: dict[str, Any]) -> str:
    if not isinstance(body, dict) or body.get("error"):
        return ""
    return norm_meta_graph_id(str(body.get("id") or ""))


def _row_interest_tag_labels_for_resolution(row: dict) -> list[str]:
    """Interest labels from 建議新受眾標籤 + 受眾隔離標籤 (EXCLUDE/HK: copy lines excluded)."""
    from meta_audience_hints import is_sheet_copy_not_interest_label

    _skip = frozenset({"—", "-", "n/a", "na", "none", ""})

    def _norm_cell(s: str) -> list[str]:
        parts = split_tags(s)
        return [
            p
            for p in parts
            if p.strip().lower() not in _skip and not is_sheet_copy_not_interest_label(p)
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


def _row_parsed_exclusions(row: dict) -> list[str]:
    """Raw EXCLUDE segments from both audience columns (same delimiters as split_tags)."""
    return collect_exclude_raw_segments_from_cells(
        str(row.get("new_audience_tags") or ""),
        str(row.get("isolation_tags") or ""),
    )


def _graph_error_subcode(err: Any) -> int:
    if not isinstance(err, dict):
        return 0
    try:
        return int(err.get("error_subcode") or 0)
    except (TypeError, ValueError):
        return 0


def _log_known_graph_subcode_hints(logger: logging.Logger, err: Any, context: str) -> None:
    """Actionable one-liners for frequent Marketing API subcodes (creative/ad phase)."""
    if not isinstance(err, dict):
        return
    sc = _graph_error_subcode(err)
    if sc == 1815520:
        logger.error(
            "%s subcode=1815520 objective/creative mismatch (e.g. wrong campaign objective for "
            "object_story_id). Fix in Ads Manager or change template/objective.",
            context,
        )
    elif sc == 1815857:
        logger.error(
            "%s subcode=1815857 bid_strategy vs bid_amount inconsistency — check ad set bid fields.",
            context,
        )


# Known Graph subcodes for destination/format issues; retry once with destination_type=UNDEFINED.
_DESTINATION_FORMAT_RETRY_SUBCODES: frozenset[int] = frozenset((2446885, 2446886, 2446722))

def _is_destination_format_graph_error(err: Any) -> bool:
    if not isinstance(err, dict):
        return False
    try:
        code = int(err.get("code") or 0)
    except (TypeError, ValueError):
        code = 0
    if code != 100:
        return False
    sc = _graph_error_subcode(err)
    if sc in _DESTINATION_FORMAT_RETRY_SUBCODES:
        return True
    blob = " ".join(
        [
            str(err.get("message") or ""),
            str(err.get("error_user_msg") or ""),
            str(err.get("error_user_title") or ""),
        ]
    ).lower()
    for kw in ("destination", "whatsapp", "messaging", "incompatible", "format", "object_story"):
        if kw in blob:
            return True
    return False


# Subcodes (and keyword fallback) that trigger layer 2 smart downgrade after primary create_adset fails.
_SMART_DOWNGRADE_SUBCODES: frozenset[int] = frozenset((2446885, 2446886, 2446722, 2490408))
_MESSAGING_PROMOTED_KEYS_TO_STRIP: frozenset[str] = frozenset(
    ("whatsapp_number", "messenger_ads_referral_type", "whatsapp_business_account_id")
)


def _should_trigger_smart_downgrade(err: Any) -> bool:
    """True when primary failure should try UNDEFINED + promoted_object messaging strip (layer 2)."""
    if not isinstance(err, dict):
        return False
    try:
        code = int(err.get("code") or 0)
    except (TypeError, ValueError):
        code = 0
    if code != 100:
        return False
    sc = _graph_error_subcode(err)
    if sc in _SMART_DOWNGRADE_SUBCODES:
        return True
    return _is_destination_format_graph_error(err)


def _strip_bid_root_keys(payload: dict[str, Any]) -> None:
    """Remove every root key whose name starts with ``bid`` (case-insensitive)."""
    for k in list(payload.keys()):
        if k.lower().startswith("bid"):
            payload.pop(k, None)


def _strip_bid_root_keys_except_strategy(payload: dict[str, Any]) -> None:
    """Remove bid_amount / bid_constraints / etc. but keep ``bid_strategy`` (Graph field name starts with bid)."""
    for k in list(payload.keys()):
        if k.lower() == "bid_strategy":
            continue
        if k.lower().startswith("bid"):
            payload.pop(k, None)


def _apply_bid_cap_with_amount(payload: dict[str, Any], bid_amount_minor: str) -> None:
    """Set bid-cap strategy with a concrete ``bid_amount`` (Graph minor units as string)."""
    _strip_bid_root_keys(payload)
    payload["bid_strategy"] = "LOWEST_COST_WITH_BID_CAP"
    payload["bid_amount"] = bid_amount_minor.strip()


def _parse_payload_promoted_object_json(payload: dict[str, Any]) -> dict[str, Any]:
    raw = payload.get("promoted_object")
    if isinstance(raw, dict):
        return copy.deepcopy(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            d = json.loads(raw)
            return d if isinstance(d, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _apply_smart_downgrade_payload(
    base_payload: dict[str, Any],
    enforced_name: str,
    logger: logging.Logger,
    sheet_row: str,
) -> dict[str, Any]:
    """
    Layer 2: destination_type=UNDEFINED; strip messaging keys from promoted_object (post stays on creative).
    Does not change targeting, budget, or billing_event/optimization_goal.
    """
    p = copy.deepcopy(base_payload)
    p["name"] = _strip_meta_adset_copy_suffix(enforced_name)[:255]
    p["destination_type"] = "UNDEFINED"
    d = _parse_payload_promoted_object_json(p)
    for k in _MESSAGING_PROMOTED_KEYS_TO_STRIP:
        d.pop(k, None)
    if d:
        p["promoted_object"] = json.dumps(d, separators=(",", ":"))
    else:
        p.pop("promoted_object", None)
    logger.warning(
        "section=new_ads row=%s create_adset attempt=smart_downgrade destination_type=UNDEFINED "
        "promoted_object=messaging_keys_stripped",
        sheet_row,
    )
    return p


def _apply_safe_mode_payload(
    base_payload: dict[str, Any],
    enforced_name: str,
    logger: logging.Logger,
    sheet_row: str,
    *,
    proactive_routing: bool = False,
    min_bid_minor: str = "100",
) -> dict[str, Any]:
    """
    Layer 3 (last resort): LOWEST_COST_WITHOUT_CAP + page-only promoted_object.

    - **proactive_routing** (P00 messaging template + post without messaging CTA): ``ON_POST`` +
      ``POST_ENGAGEMENT`` + ``IMPRESSIONS`` — pair with exclusive campaign ``OUTCOME_ENGAGEMENT``.
      (``UNDEFINED`` + ``POST_ENGAGEMENT`` under engagement often returns Graph 2490408.)
    - **Else** (fallback after failed primary): ``UNDEFINED`` + ``ENGAGED_USERS`` + ``IMPRESSIONS``.

    Preserves targeting and budget fields from base_payload. Post/video remains object_story_id on creative only.
    ``min_bid_minor`` is accepted for logging compatibility; safe mode does not set bid cap.

    When proactive_routing is True (smart routing before first POST), log at INFO instead of WARNING.
    """
    p = copy.deepcopy(base_payload)
    p["name"] = _strip_meta_adset_copy_suffix(enforced_name)[:255]
    if proactive_routing:
        p["destination_type"] = "ON_POST"
        p["optimization_goal"] = "POST_ENGAGEMENT"
        p["billing_event"] = "IMPRESSIONS"
    else:
        p["destination_type"] = "UNDEFINED"
        p["optimization_goal"] = "ENGAGED_USERS"
        p["billing_event"] = "IMPRESSIONS"
    _strip_bid_root_keys(p)
    p.pop("attribution_spec", None)
    try:
        mb = max(int(str(min_bid_minor or "").strip() or "100"), 1)
    except ValueError:
        mb = 100
    p["bid_strategy"] = "LOWEST_COST_WITHOUT_CAP"
    _strip_bid_root_keys_except_strategy(p)
    po = _parse_payload_promoted_object_json(p)
    pid = norm_meta_graph_id(str(po.get("page_id") or ""))
    if pid:
        p["promoted_object"] = json.dumps({"page_id": pid}, separators=(",", ":"))
    else:
        p.pop("promoted_object", None)
    _dst_log = "ON_POST" if proactive_routing else "UNDEFINED"
    _og_log = "POST_ENGAGEMENT" if proactive_routing else "ENGAGED_USERS"
    if proactive_routing:
        logger.info(
            "section=new_ads row=%s create_adset proactive_safe_mode optimization_goal=%s "
            "billing_event=IMPRESSIONS bid_strategy=LOWEST_COST_WITHOUT_CAP sheet_min_bid_minor_ref=%s "
            "destination_type=%s promoted_object=page_id_only",
            sheet_row,
            _og_log,
            mb,
            _dst_log,
        )
    else:
        logger.warning(
            "section=new_ads row=%s create_adset attempt=safe_mode optimization_goal=%s "
            "billing_event=IMPRESSIONS bid_strategy=LOWEST_COST_WITHOUT_CAP sheet_min_bid_minor_ref=%s "
            "destination_type=%s promoted_object=page_id_only",
            sheet_row,
            _og_log,
            mb,
            _dst_log,
        )
    return p


def _create_adset_native_cloning_fallback(
    client: GraphClient,
    base_payload: dict[str, Any],
    logger: logging.Logger,
    *,
    enforced_name: str,
    sheet_row: str | None = None,
    min_bid_minor: str = "100",
    proactive_safe: bool = False,
    post_format_kind: PostFormatKind = "unknown",
) -> tuple[str | None, dict[str, Any] | None]:
    """
    Native cloning create_adset ladder.

    **proactive_safe=False (default):** primary (template DNA) → smart_downgrade → safe_mode
    (``proactive_routing=False``).

    **proactive_safe=True:** ordered proactive compatibility profiles (no smart_downgrade between them);
    if all fail → smart_downgrade → ``_apply_safe_mode_payload(..., proactive_routing=True)``.
    """
    sr = sheet_row if sheet_row is not None else "?"
    canonical = _strip_meta_adset_copy_suffix(enforced_name)[:255]

    def _try_create(p: dict[str, Any], attempt_label: str) -> tuple[str | None, dict[str, Any] | None]:
        body = client.create_adset(p)
        err = body.get("error") if isinstance(body.get("error"), dict) else None
        if not err:
            nid = _new_adset_id_from_create_response(body)
            if nid:
                logger.info(
                    "section=new_ads row=%s create_adset attempt=%s ok adset=%s",
                    sr,
                    attempt_label,
                    nid,
                )
                return nid, None
            logger.error(
                "section=new_ads row=%s create_adset empty id attempt=%s body=%s",
                sr,
                attempt_label,
                body,
            )
            return None, None
        return None, err

    if proactive_safe:
        nid_l, err_l = _try_proactive_adset_ladder(
            client,
            base_payload,
            enforced_name=enforced_name,
            logger=logger,
            sheet_row=sr,
            min_bid_minor=min_bid_minor,
            post_format_kind=post_format_kind,
        )
        if nid_l:
            return nid_l, None
        if err_l:
            log_graph_error_payload(logger, err_l, prefix="create_adset proactive_compat_exhausted ")

        p_sd = _apply_smart_downgrade_payload(base_payload, enforced_name, logger, sr)
        nid_sd, err_sd = _try_create(p_sd, "proactive_terminal_smart_downgrade")
        if nid_sd:
            return nid_sd, None
        if not err_sd:
            return None, None
        log_graph_error_payload(logger, err_sd, prefix="create_adset proactive_terminal_smart_downgrade ")

        p_safe = _apply_safe_mode_payload(
            base_payload,
            enforced_name,
            logger,
            sr,
            proactive_routing=True,
            min_bid_minor=min_bid_minor,
        )
        nid_safe, err_safe = _try_create(p_safe, "proactive_terminal_safe_mode")
        if nid_safe:
            return nid_safe, None
        if not err_safe:
            return None, None
        log_graph_error_payload(logger, err_safe, prefix="create_adset proactive_terminal_safe_mode ")
        return None, err_safe

    p0 = copy.deepcopy(base_payload)
    p0["name"] = canonical
    nid0, err0 = _try_create(p0, "primary")
    if nid0:
        return nid0, None
    if not err0:
        return None, None

    if not _should_trigger_smart_downgrade(err0):
        log_graph_error_payload(logger, err0, prefix="create_adset ")
        return None, err0

    log_graph_error_payload(logger, err0, prefix="create_adset primary ")
    p_sd = _apply_smart_downgrade_payload(base_payload, enforced_name, logger, sr)
    nid_sd, err_sd = _try_create(p_sd, "smart_downgrade")
    if nid_sd:
        return nid_sd, None
    if not err_sd:
        return None, None
    log_graph_error_payload(logger, err_sd, prefix="create_adset smart_downgrade ")

    p_safe = _apply_safe_mode_payload(
        base_payload, enforced_name, logger, sr, min_bid_minor=min_bid_minor, proactive_routing=False
    )
    nid_safe, err_safe = _try_create(p_safe, "safe_mode")
    if nid_safe:
        return nid_safe, None
    if not err_safe:
        return None, None
    log_graph_error_payload(logger, err_safe, prefix="create_adset safe_mode ")
    return None, err_safe


def main() -> None:
    p = argparse.ArgumentParser(
        description="Create ads from AI_Action_Plan (Native Cloning: object_story_id; NEW_FROM_POST in v1)."
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

    champion_tags_by_pool: dict[tuple[str, str], str] = {}
    try:
        _rref = load_refined_rows_from_sheet()
        if _rref:
            champion_tags_by_pool = compute_champion_tags_by_pool(_rref)
    except Exception as e:
        log.warning("champion_tags_by_pool unavailable (refined sheet): %s", e)

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

        page_sheet = (row.get("page_id") or "").strip()
        story = normalize_object_story_id(
            str(row.get("post_or_object_story_id") or "").strip(),
            page_sheet,
        )
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
            shop = (row.get("shop") or "").strip()
            pool_key = "bun" if _p00_is_bun_pool(row) else "hk"
            interest_tag_labels = _row_interest_tag_labels_for_resolution(row)
            parsed_excl = parse_exclude_segments(_row_parsed_exclusions(row))
            interest_ids, _kept, tag_status = validate_and_get_ids(interest_tag_labels)
            if interest_tag_labels and not interest_ids:
                champ_line = _p00_champion_tags_for_pool(champion_tags_by_pool, shop, pool_key)
                champ_labels = split_tags(champ_line or "")
                interest_ids, _kept, tag_status = validate_and_get_ids(champ_labels)
                if not interest_ids:
                    log.warning(
                        "section=new_ads row=%s create=skip reason=no_interest_ids_after_champion_fallback "
                        "sheet_tags=%s status=%s",
                        sr,
                        interest_tag_labels,
                        tag_status,
                    )
                    stats["skipped"] += 1
                    continue
                log.info(
                    "section=new_ads row=%s audience_tags resolved=%s status=%s source=champion_fallback",
                    sr,
                    len(interest_ids),
                    tag_status,
                )
            elif interest_tag_labels:
                log.info(
                    "section=new_ads row=%s audience_tags resolved=%s status=%s",
                    sr,
                    len(interest_ids),
                    tag_status,
                )

            if dry:
                try:
                    if not _validate_template_page_for_create(
                        client, tpl_id, page_sheet, adset_cache, log, str(sr)
                    ):
                        stats["failed"] += 1
                        continue
                    tpl_row = get_adset_cached(client, tpl_id, adset_cache)
                    tpl_cid = norm_meta_graph_id(str(tpl_row.get("campaign_id") or ""))
                    if not _looks_like_meta_id(tpl_cid):
                        log.error(
                            "section=new_ads row=%s exclusive_campaign=abort template missing campaign_id",
                            sr,
                        )
                        stats["failed"] += 1
                        continue
                    parent_camp = get_campaign_cached(client, tpl_cid, campaign_cache)
                    story_norm = norm_meta_graph_id(str(story or "").strip()) or None
                    routing_hints = (
                        _fetch_post_routing_hints(client, story_norm, log, str(sr))
                        if story_norm
                        else PostRoutingHints(has_messaging_cta=False, format_kind="unknown")
                    )
                    has_cta = routing_hints.has_messaging_cta
                    tpl_messaging = _template_row_is_messaging(tpl_row)
                    proactive_safe = (not has_cta) and tpl_messaging
                    log.info(
                        "section=new_ads row=%s Post %s CTA check: %s post_format=%s. Routing mode: %s",
                        sr,
                        story_norm or "none",
                        has_cta,
                        routing_hints.format_kind,
                        "Proactive Safe Mode" if proactive_safe else "Native Clone",
                    )
                    camp_body = _build_exclusive_campaign_payload(
                        parent_camp,
                        display_name=new_display_name,
                        graph_status="PAUSED",
                        budget_suggest_cell=budget_cell,
                        currency=acct_currency,
                        min_minor=min_budget_minor_i,
                        logger=log,
                        sheet_row=str(sr),
                        objective_override="OUTCOME_ENGAGEMENT" if proactive_safe else None,
                        force_abo=proactive_safe,
                    )
                    if not camp_body:
                        stats["failed"] += 1
                        continue
                    planned_cbo = "daily_budget" in camp_body
                    camp_red = _redact_campaign_create_body(camp_body)
                    log.info(
                        "section=new_ads row=%s dry_run would_get_template=%s",
                        sr,
                        tpl_id,
                    )
                    log.info(
                        "section=new_ads row=%s dry_run would_post_campaign redacted=%s",
                        sr,
                        json.dumps(camp_red, ensure_ascii=False, default=str),
                    )
                    log.info(
                        "section=new_ads row=%s dry_run campaign_dna_meta=%s",
                        sr,
                        json.dumps(
                            {
                                "source_adset_id": tpl_id,
                                "source_campaign_id": tpl_cid,
                                "shop_pool": "BUN" if _p00_is_bun_pool(row) else "HK",
                                "field_provenance": {
                                    "name": "overridden",
                                    "objective": (
                                        "OUTCOME_ENGAGEMENT (proactive_safe)"
                                        if proactive_safe
                                        else "cloned"
                                    ),
                                    "special_ad_categories": "cloned",
                                    "is_adset_budget_sharing_enabled": camp_body.get(
                                        "is_adset_budget_sharing_enabled", "omitted_when_CBO_daily_budget"
                                    ),
                                    "daily_budget": "overridden_from_sheet_if_cbo",
                                    "budget_structure_note": "omit sharing flag when campaign has daily_budget (4834002)",
                                },
                            },
                            ensure_ascii=False,
                            default=str,
                        ),
                    )
                    # Sheet 宣傳活動 ID ignored on create path; leave blank or align after run for preflight.
                    payload_preview = _build_new_adset_payload(
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
                        graph_status="PAUSED",
                        sheet_campaign_id="",
                        forced_campaign_id=_EXCLUSIVE_CAMPAIGN_ID_PLACEHOLDER,
                        planned_cbo_omit_adset_budget=planned_cbo,
                        parsed_exclusions=parsed_excl,
                        object_story_id=story_norm,
                    )
                    if payload_preview and proactive_safe:
                        _mb = str(max(int(min_budget_minor_i or 0), 100))
                        _filtered = _filter_proactive_profiles_for_post_format(
                            _PROACTIVE_COMPAT_PROFILES_ORDERED, routing_hints.format_kind
                        )
                        _applied_preview: dict[str, Any] | None = None
                        for _pf in _filtered:
                            _cand = _apply_proactive_compat_profile(
                                payload_preview,
                                new_display_name,
                                _pf,
                                log,
                                str(sr),
                                _mb,
                            )
                            if _cand:
                                _applied_preview = _cand
                                break
                        if _applied_preview:
                            payload_preview = _applied_preview
                        log.info(
                            "section=new_ads row=%s dry_run proactive_compat preview_profile_order=%s",
                            sr,
                            [p.profile_id for p in _filtered],
                        )
                    if not payload_preview:
                        stats["failed"] += 1
                        continue
                    _log_budget_owner_line(
                        log,
                        str(sr),
                        campaign_body=camp_body,
                        adset_payload=payload_preview,
                    )
                    red = _redact_adset_create_body(payload_preview)
                    log.info(
                        "section=new_ads row=%s dry_run would_post_adsets redacted=%s",
                        sr,
                        json.dumps(red, ensure_ascii=False, default=str),
                    )
                    log.info(
                        "section=new_ads row=%s dry_run adset_dna_meta=%s",
                        sr,
                        json.dumps(
                            {
                                "source_adset_id": tpl_id,
                                "source_campaign_id": tpl_cid,
                                "shop_pool": "BUN" if _p00_is_bun_pool(row) else "HK",
                                "field_provenance": {
                                    "name": "overridden",
                                    "targeting": "overridden_from_tags",
                                    "campaign_id": "overridden_after_campaign_create",
                                    "optimization_goal": (
                                        "proactive_compat ladder (preview=first_applicable_profile)"
                                        if proactive_safe
                                        else "cloned"
                                    ),
                                    "billing_event": (
                                        "per_profile (preview=first_applicable_profile)"
                                        if proactive_safe
                                        else "cloned"
                                    ),
                                    "bid_strategy": (
                                        "per_profile strict_sanitization (preview=first_applicable_profile)"
                                        if proactive_safe
                                        else "cloned_with_pair_guard"
                                    ),
                                    "bid_amount": (
                                        "omitted_or_cap_per_profile (preview=first_applicable_profile)"
                                        if proactive_safe
                                        else "cloned_with_pair_guard"
                                    ),
                                },
                            },
                            ensure_ascii=False,
                            default=str,
                        ),
                    )
                    log.info(
                        "section=new_ads row=%s dry_run note=on_execute create_adset chain: "
                        "native: primary then smart_downgrade then reactive safe_mode; "
                        "proactive: ordered compatibility profiles then terminal smart_downgrade + proactive safe_mode",
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
                tpl_cid = norm_meta_graph_id(str(tpl_row.get("campaign_id") or ""))
                if not _looks_like_meta_id(tpl_cid):
                    log.error(
                        "section=new_ads row=%s exclusive_campaign=abort template missing campaign_id",
                        sr,
                    )
                    stats["failed"] += 1
                    continue
                parent_camp = get_campaign_cached(client, tpl_cid, campaign_cache)
                story_norm = norm_meta_graph_id(str(story or "").strip()) or None
                routing_hints = (
                    _fetch_post_routing_hints(client, story_norm, log, str(sr))
                    if story_norm
                    else PostRoutingHints(has_messaging_cta=False, format_kind="unknown")
                )
                has_cta = routing_hints.has_messaging_cta
                tpl_messaging = _template_row_is_messaging(tpl_row)
                proactive_safe = (not has_cta) and tpl_messaging
                log.info(
                    "section=new_ads row=%s Post %s CTA check: %s post_format=%s. Routing mode: %s",
                    sr,
                    story_norm or "none",
                    has_cta,
                    routing_hints.format_kind,
                    "Proactive Safe Mode" if proactive_safe else "Native Clone",
                )
                camp_body = _build_exclusive_campaign_payload(
                    parent_camp,
                    display_name=new_display_name,
                    graph_status="ACTIVE",
                    budget_suggest_cell=budget_cell,
                    currency=acct_currency,
                    min_minor=min_budget_minor_i,
                    logger=log,
                    sheet_row=str(sr),
                    objective_override="OUTCOME_ENGAGEMENT" if proactive_safe else None,
                    force_abo=proactive_safe,
                )
                if not camp_body:
                    stats["failed"] += 1
                    continue
                camp_resp = client.create_campaign(camp_body)
                new_campaign_id = _new_campaign_id_from_create_response(camp_resp)
                if not new_campaign_id:
                    log_graph_error_payload(
                        log,
                        camp_resp.get("error") if isinstance(camp_resp, dict) else None,
                        prefix="exclusive_campaign ",
                    )
                    log.error(
                        "section=new_ads row=%s exclusive_campaign=FAIL body=%s",
                        sr,
                        json.dumps(camp_resp, ensure_ascii=False, default=str)[:2000],
                    )
                    stats["failed"] += 1
                    continue
                log.info(
                    "section=new_ads row=%s create=ok 🚀 Created EXCLUSIVE Campaign: %s (ID: %s)",
                    sr,
                    camp_body.get("name", ""),
                    new_campaign_id,
                )
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
                    graph_status="ACTIVE",
                    sheet_campaign_id="",
                    forced_campaign_id=new_campaign_id,
                    planned_cbo_omit_adset_budget=None,
                    parsed_exclusions=parsed_excl,
                    object_story_id=story_norm,
                )
                if not payload:
                    stats["failed"] += 1
                    continue
                _log_budget_owner_line(
                    log,
                    str(sr),
                    campaign_body=camp_body,
                    adset_payload=payload,
                )
                new_id, _last_create_err = _create_adset_native_cloning_fallback(
                    client,
                    payload,
                    log,
                    enforced_name=new_display_name,
                    sheet_row=str(sr),
                    min_bid_minor=str(max(int(min_budget_minor_i or 0), 100)),
                    proactive_safe=proactive_safe,
                    post_format_kind=routing_hints.format_kind,
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
            status, pmsgs = preflight_new_ad_row(
                client,
                target_adset_id=target_adset,
                sheet_campaign_id=camp_sheet,
                sheet_page_id=page_sheet,
                adset_cache=adset_cache,
                campaign_cache=campaign_cache,
                page_cache=page_cache,
                logger=log,
            )

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

            creative_payload: dict[str, Any] = {
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

            cr = client.create_adcreative(creative_payload)
            if cr.get("error"):
                _cerr = cr.get("error") if isinstance(cr.get("error"), dict) else None
                log_graph_error_payload(log, _cerr)
                _log_known_graph_subcode_hints(
                    log, _cerr, f"section=new_ads row={sr} op=create_adcreative"
                )
                stats["failed"] += 1
                continue
            cid = str(cr.get("id", "") or "")
            if not cid:
                log.error("section=new_ads row=%s no creative id in response", sr)
                stats["failed"] += 1
                continue

            ad_body = {
                "name": ad_name,
                "adset_id": target_adset,
                "creative": json.dumps({"creative_id": cid}),
                "status": "ACTIVE",
            }
            ad_res = client.create_ad(ad_body)
            cid_used = cid

            ad_err = ad_res.get("error")
            if isinstance(ad_err, dict):
                log_graph_error_payload(log, ad_err)
                _log_known_graph_subcode_hints(
                    log, ad_err, f"section=new_ads row={sr} op=create_ad"
                )
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
            log.error("auth_failure during preflight/post")
            raise SystemExit(1) from None
        except Exception as e:
            log.exception("section=new_ads row=%s err=%s", sr, e)
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
