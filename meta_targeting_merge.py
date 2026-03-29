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
