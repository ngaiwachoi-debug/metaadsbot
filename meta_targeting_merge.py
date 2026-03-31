"""Shared helpers to merge resolved interest IDs into ad set targeting (flexible_spec)."""

from __future__ import annotations

import copy
import re
from typing import Any


def split_tags(s: str) -> list[str]:
    return [x.strip() for x in re.split(r"[,，、\n]+", s or "") if x.strip()]


def merge_interests_into_targeting(targeting: dict, interest_ids: list[str]) -> dict:
    """Add interests to flexible_spec without removing geo/gender/etc."""
    t = copy.deepcopy(targeting) if isinstance(targeting, dict) else {}
    if not interest_ids:
        return t
    fs = t.get("flexible_spec")
    if not fs:
        t["flexible_spec"] = [{"interests": [{"id": i} for i in interest_ids]}]
        return t
    if isinstance(fs, list) and fs and isinstance(fs[0], dict):
        interests = fs[0].get("interests")
        if not isinstance(interests, list):
            interests = []
        existing = {str(x.get("id")) for x in interests if isinstance(x, dict) and x.get("id")}
        for iid in interest_ids:
            if iid not in existing:
                interests.append({"id": iid})
                existing.add(iid)
        fs[0]["interests"] = interests
        t["flexible_spec"] = fs
    else:
        t["flexible_spec"] = [{"interests": [{"id": i} for i in interest_ids]}]
    return t


def drop_root_id_key(obj: Any) -> dict[str, Any]:
    """Remove only the root-level ``id`` key from a Graph object dict (keeps interests[].id intact)."""
    if not isinstance(obj, dict):
        return {}
    return {k: v for k, v in obj.items() if k != "id"}


def ensure_hong_kong_geo(targeting: dict) -> dict:
    """
    Force geo_locations to Hong Kong only (salon walk-in audience in HK).
    Replaces countries/regions/cities with ``countries: [HK]``; preserves ``location_types`` when present.
    """
    t = copy.deepcopy(targeting) if isinstance(targeting, dict) else {}
    prev = t.get("geo_locations") if isinstance(t.get("geo_locations"), dict) else {}
    lt = prev.get("location_types") if isinstance(prev.get("location_types"), list) else None
    gl: dict[str, Any] = {"countries": ["HK"]}
    if lt:
        gl["location_types"] = lt
    else:
        gl["location_types"] = ["home", "recent"]
    t["geo_locations"] = gl
    return t


def merge_excluded_geo_into_targeting(targeting: dict, country_codes: list[str]) -> dict:
    """Union ISO country codes into ``excluded_geo_locations.countries`` (deep copy)."""
    t = copy.deepcopy(targeting) if isinstance(targeting, dict) else {}
    if not country_codes:
        return t
    ex = t.get("excluded_geo_locations")
    if not isinstance(ex, dict):
        ex = {}
    countries = list(ex.get("countries") or []) if isinstance(ex.get("countries"), list) else []
    seen = {str(c).strip().upper() for c in countries if c}
    for c in country_codes:
        cu = (c or "").strip().upper()
        if cu and cu not in seen:
            countries.append(cu)
            seen.add(cu)
    ex["countries"] = countries
    t["excluded_geo_locations"] = ex
    return t


def merge_locale_exclusions_into_targeting(targeting: dict, locale_keys: list[int]) -> dict:
    """
    If Graph accepts locale exclusions on targeting, merge here.
    As of common Marketing API targeting, locale *exclusion* is not a first-class field;
    Caller should log WARNING when ``locale_keys`` is non-empty and this is a no-op.
    """
    _ = locale_keys
    return copy.deepcopy(targeting) if isinstance(targeting, dict) else {}


def apply_auto_placements_to_targeting(targeting: dict) -> dict:
    """
    Advantage+ / auto placements: full platform reach; strip per-platform position locks
    so Meta can optimize placements (not limited to e.g. Reels only).
    """
    t = copy.deepcopy(targeting) if isinstance(targeting, dict) else {}
    for k in list(t.keys()):
        if k.endswith("_positions"):
            t.pop(k, None)
    t["publisher_platforms"] = ["facebook", "instagram", "messenger", "audience_network"]
    t["device_platforms"] = ["mobile", "desktop"]
    return t
