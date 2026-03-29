"""Map Meta ad/page display strings and numeric Graph ids to internal 店名.

Loads from ``SHOP_NAME_MAP_PATH`` (JSON file, multiline-friendly) first, then
env ``SHOP_NAME_MAP``, then ``config.json`` key ``SHOP_NAME_MAP``. Internal 店名 is for
reporting/SHOP_CONFIGS; it need not match the Page's public title on Facebook.
"""

from __future__ import annotations

import json
import os

from dotenv import load_dotenv

_ROOT = os.path.dirname(os.path.abspath(__file__))

load_dotenv()


def _coerce_map(data: object) -> dict[str, str]:
    if not isinstance(data, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in data.items():
        key = str(k).strip()
        if not key:
            continue
        out[key] = str(v).strip() if v is not None else ""
    return out


def _shop_name_map_from_config_json() -> dict[str, str]:
    path = os.path.join(_ROOT, "config.json")
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    raw = data.get("SHOP_NAME_MAP") if isinstance(data, dict) else None
    return _coerce_map(raw) if isinstance(raw, dict) else {}


def load_shop_name_map() -> dict[str, str]:
    """Precedence: SHOP_NAME_MAP_PATH file > env SHOP_NAME_MAP > config.json SHOP_NAME_MAP > empty."""
    path = (os.getenv("SHOP_NAME_MAP_PATH") or "").strip()
    if path and not os.path.isabs(path):
        path = os.path.join(_ROOT, path)
    if path and os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as f:
                return _coerce_map(json.load(f))
        except (json.JSONDecodeError, OSError) as e:
            print(f"⚠️ SHOP_NAME_MAP_PATH 讀取失敗 ({path}): {e}")
    raw = os.getenv("SHOP_NAME_MAP", "") or ""
    if raw.strip():
        try:
            return _coerce_map(json.loads(raw))
        except json.JSONDecodeError:
            print("⚠️ 環境變數 SHOP_NAME_MAP 的 JSON 無法解析。")
    cfg_map = _shop_name_map_from_config_json()
    if cfg_map:
        return cfg_map
    return {}


SHOP_NAME_MAP: dict[str, str] = load_shop_name_map()


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
