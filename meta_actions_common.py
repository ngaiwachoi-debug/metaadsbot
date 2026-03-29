"""Shared argparse + execute guard for Meta Action Plan executor CLIs."""

from __future__ import annotations

import argparse
import os

from meta_actions_logging import setup_logging


def add_common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--execute", action="store_true", help="Perform Meta writes (default is dry-run without this flag)")
    p.add_argument("--limit", type=int, default=0, help="Max rows to process (0 = all)")
    p.add_argument("--skip", type=int, default=0, help="Skip first N data rows after parse")
    p.add_argument("--delay-ms", type=int, default=None, help="Override META_ACTION_DELAY_MS between POSTs")
    p.add_argument("--verbose", "-v", action="store_true", help="DEBUG logging")
    p.add_argument("--tab", type=str, default=None, help="Override ACTION_PLAN_TAB worksheet name")


def resolve_dry_run(args: argparse.Namespace) -> bool:
    # --execute wins over default dry-run
    if getattr(args, "execute", False):
        return False
    return True


def require_execute_env() -> None:
    """Plan: require META_ACTION_EXECUTE_CONFIRM=YES when using --execute."""
    if (os.getenv("META_ACTION_EXECUTE_CONFIRM") or "").strip().upper() != "YES":
        raise SystemExit(
            "Refusing --execute: set environment variable META_ACTION_EXECUTE_CONFIRM=YES "
            "(then re-run with --execute)."
        )


def init_cli(args: argparse.Namespace) -> bool:
    setup_logging(verbose=args.verbose)
    dry = resolve_dry_run(args)
    if not dry:
        require_execute_env()
    return dry
