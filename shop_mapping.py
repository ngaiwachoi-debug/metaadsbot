"""Shop name mapping from Facebook/IG display names to internal 店名 (from .env SHOP_NAME_MAP)."""

from __future__ import annotations

import json
import os

from dotenv import load_dotenv

load_dotenv()

try:
    SHOP_NAME_MAP: dict = json.loads(os.getenv("SHOP_NAME_MAP", "{}"))
    if not isinstance(SHOP_NAME_MAP, dict):
        SHOP_NAME_MAP = {}
except json.JSONDecodeError:
    SHOP_NAME_MAP = {}


def squish_name(s: str) -> str:
    """Normalize for matching: Meta often returns 'Lounge & Skin' vs map key 'Lounge&Skin'."""
    return "".join(str(s or "").lower().replace("&", "").split())


def map_shop_name(raw_name: str) -> str:
    normalized_raw = str(raw_name or "")
    raw_squish = squish_name(normalized_raw)
    for k, v in SHOP_NAME_MAP.items():
        key = str(k).strip()
        val = str(v).strip()
        if not key:
            continue
        if key in normalized_raw:
            print(f"🏷️ 門店映射：'{normalized_raw}' -> '{val}' (match: '{key}')")
            return val
        ks = squish_name(key)
        if len(ks) >= 4 and ks in raw_squish:
            print(f"🏷️ 門店映射：'{normalized_raw}' -> '{val}' (flex match: '{key}')")
            return val
    return "其他"
