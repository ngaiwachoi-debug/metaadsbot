"""
Pool-level audience exclusion copy and token maps (keep in sync with ai_optimizer hints).

Short-term: executors PATCH targeting with ensure_hong_kong_geo + EXCLUDE merges + interests.
Mid-term: if Meta UI cannot apply tag/exclude changes reliably, prefer clone ad set → amend
budget/tags → activate → pause/delete old (extend executors; same parsing as here).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from meta_targeting_merge import split_tags

# Sync with ai_optimizer._AUDIENCE_EXCLUSION (imported from here in ai_optimizer).
AUDIENCE_EXCLUSION_BY_POOL: dict[str, str] = {
    "BUN": "EXCLUDE Hong Kong/Traditional Chinese",
    "HK": "EXCLUDE Philippines/Indonesia/Tagalog/Expats",
}

# Normalized token (lower) -> ISO 3166-1 alpha-2 for excluded_geo_locations.countries
EXCLUDE_TOKEN_TO_COUNTRY_ISO: dict[str, str] = {
    "philippines": "PH",
    "indonesia": "ID",
    "hong kong": "HK",
    "hongkong": "HK",
}

# Normalized token -> Meta locale key (see meta_targeting.LANG_MAP). Applied only if API supports exclusion.
EXCLUDE_TOKEN_TO_LOCALE_KEY: dict[str, int] = {
    "traditional chinese": 24,
    "tagalog": 64,
    "simplified chinese": 31,
    "english": 6,
}

# Tokens with no stable Graph mapping (log WARNING, skip)
UNSTABLE_EXCLUDE_TOKENS: frozenset[str] = frozenset({"expats"})


def _norm_token(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def is_exclude_segment(text: str) -> bool:
    u = (text or "").strip()
    return bool(u) and u.upper().startswith("EXCLUDE")


def is_sheet_copy_not_interest_label(text: str) -> bool:
    """Human hints (HK: …) are not interest labels; EXCLUDE lines are handled separately."""
    u = (text or "").strip()
    if not u:
        return True
    if is_exclude_segment(u):
        return True
    return bool(re.match(r"^[A-Z]{2,3}:\s*", u))


@dataclass
class ParsedExclusions:
    """From EXCLUDE lines in sheet cells."""

    country_codes: list[str] = field(default_factory=list)
    locale_keys: list[int] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def parse_exclude_segments(raw_segments: list[str]) -> ParsedExclusions:
    """
    Parse segments like 'EXCLUDE Philippines/Tagalog/Expats' or 'EXCLUDE Hong Kong'.
    Splits on / and whitespace after stripping EXCLUDE prefix.
    """
    out = ParsedExclusions()
    seen_c: set[str] = set()
    seen_l: set[int] = set()

    for seg in raw_segments:
        s = (seg or "").strip()
        if not s:
            continue
        if not is_exclude_segment(s):
            continue
        rest = s[7:].strip() if len(s) >= 7 and s.upper().startswith("EXCLUDE") else s
        rest = rest.lstrip(":").strip()
        if not rest:
            continue
        parts = re.split(r"[/／]+", rest)
        if len(parts) == 1:
            parts = re.split(r"[,，、\s]+", rest)
        for part in parts:
            tok = _norm_token(part)
            if not tok:
                continue
            if tok in UNSTABLE_EXCLUDE_TOKENS:
                out.warnings.append(f"EXCLUDE token not mapped (unstable): {part!r}")
                continue
            if tok in EXCLUDE_TOKEN_TO_COUNTRY_ISO:
                cc = EXCLUDE_TOKEN_TO_COUNTRY_ISO[tok]
                if cc not in seen_c:
                    seen_c.add(cc)
                    out.country_codes.append(cc)
                continue
            if tok in EXCLUDE_TOKEN_TO_LOCALE_KEY:
                lk = EXCLUDE_TOKEN_TO_LOCALE_KEY[tok]
                if lk not in seen_l:
                    seen_l.add(lk)
                    out.locale_keys.append(lk)
                continue
            out.warnings.append(f"EXCLUDE token not mapped: {part!r}")

    return out


def collect_exclude_raw_segments_from_cells(*cell_strs: str) -> list[str]:
    """Collect lines/segments that start with EXCLUDE from one or more sheet cells."""
    raw: list[str] = []
    for cell in cell_strs:
        for p in split_tags(cell or ""):
            if is_exclude_segment(p):
                raw.append(p)
    return raw
