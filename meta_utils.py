"""Shared Meta currency / numeric helpers (no I/O)."""

from decimal import Decimal, InvalidOperation


def norm_meta_graph_id(v) -> str:
    """
    Normalize Meta Graph node ids for map keys (canonical digit string).

    Google Sheets / Excel often turn large ids into floats or scientific notation
    (e.g. ``9.38493152688096E+14``); plain ``str`` then fails to match
    ``shop_name_map.json`` keys like ``938493152688096``.
    """
    if v is None or v == "":
        return ""
    if isinstance(v, int) and v > 0:
        return str(v)
    s = str(v).strip().replace(",", "")
    if not s:
        return ""
    if s.isdigit():
        return s
    try:
        d = Decimal(s)
        i = int(d)
        if i <= 0:
            return s
        return str(i)
    except (InvalidOperation, ValueError, OverflowError):
        pass
    return s


def to_hkd_from_meta_minor(value) -> float:
    """
    Meta budget fields are usually in minor currency unit.
    Example: 1000 -> HKD 10.0
    """
    try:
        v = float(value or 0)
    except Exception:
        return 0.0
    if v <= 0:
        return 0.0
    return v / 100.0


def to_float_minor(value) -> float:
    """Raw minor units as float (0 if missing)."""
    try:
        return float(value or 0)
    except Exception:
        return 0.0
