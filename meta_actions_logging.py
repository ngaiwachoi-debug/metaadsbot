"""Shared logging for Meta Action Plan executor CLIs (stderr, full Graph errors)."""

from __future__ import annotations

import json
import logging
import sys
from typing import Any


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    # Never log full Graph URLs (they include access_token).
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    for h in list(root.handlers):
        root.removeHandler(h)
    h = logging.StreamHandler(sys.stderr)
    h.setLevel(level)
    h.setFormatter(
        logging.Formatter(
            fmt="%(levelname)s %(message)s",
        )
    )
    root.addHandler(h)


def log_graph_error_payload(logger: logging.Logger, err: dict[str, Any] | None, prefix: str = "") -> None:
    """Log full Meta `error` object (code, subcode, user_msg, fbtrace_id, …)."""
    if not err or not isinstance(err, dict):
        logger.error("%sGraph error: %s", prefix, err)
        return
    safe = {
        k: err.get(k)
        for k in (
            "message",
            "type",
            "code",
            "error_subcode",
            "error_user_title",
            "error_user_msg",
            "fbtrace_id",
            "is_transient",
        )
        if err.get(k) is not None
    }
    logger.error("%s%s", prefix, json.dumps(safe, ensure_ascii=False, indent=2))


def redact_token(s: str | None) -> str:
    if not s:
        return ""
    t = str(s).strip()
    if len(t) < 12:
        return "***"
    return t[:6] + "…" + t[-4:]
