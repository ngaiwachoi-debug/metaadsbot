"""Shared Meta currency / numeric helpers (no I/O)."""


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
