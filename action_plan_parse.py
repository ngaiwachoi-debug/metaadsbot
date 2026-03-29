"""Parse `AI_Action_Plan` Google Sheet into section → list of row dicts (with 1-based sheet row index)."""

from __future__ import annotations

from typing import Any

import gspread

from ai_optimizer import (
    ACTION_PLAN_HEADER_AUDIENCE,
    ACTION_PLAN_HEADER_BUDGET,
    ACTION_PLAN_HEADER_NEW_ADS,
    ACTION_PLAN_HEADER_PAUSE,
    ACTION_PLAN_TAB,
    ACTION_PLAN_TITLE_AUDIENCE,
    ACTION_PLAN_TITLE_BUDGET,
    ACTION_PLAN_TITLE_NEW,
    ACTION_PLAN_TITLE_PAUSE,
    get_google_sheet,
)

def _header_col_index(header_row: list[str], label: str) -> int | None:
    """0-based column for exact header text (stripped), or None."""
    want = (label or "").strip()
    if not want:
        return None
    for i, cell in enumerate(header_row):
        if (cell or "").strip() == want:
            return i
    return None


# title → (canonical Chinese headers from ai_optimizer, English keys for executors)
_SECTION_CONFIG: dict[str, tuple[list[str], list[str]]] = {
    ACTION_PLAN_TITLE_NEW: (
        ACTION_PLAN_HEADER_NEW_ADS,
        [
            "shop",
            "strategy",
            "source_ad_name",
            "template_adset_id",
            "target_adset_id",
            "post_or_object_story_id",
            "page_id",
            "whatsapp_number",
            "campaign_id",
            "last_error",
            "suggested_ad_name",
            "new_audience_tags",
            "isolation_tags",
            "audience_hint_json",
            "budget_suggest",
            "create_mode",
        ],
    ),
    ACTION_PLAN_TITLE_PAUSE: (
        ACTION_PLAN_HEADER_PAUSE,
        [
            "shop",
            "strategy",
            "ad_name",
            "adset_id",
            "ad_id",
            "pause_reason",
            "budget_reclaim",
            "delete_creative",
        ],
    ),
    ACTION_PLAN_TITLE_BUDGET: (
        ACTION_PLAN_HEADER_BUDGET,
        [
            "shop",
            "strategy",
            "ad_name",
            "adset_id",
            "current_budget",
            "suggested_budget",
            "delta",
            "reason",
            "priority",
        ],
    ),
    ACTION_PLAN_TITLE_AUDIENCE: (
        ACTION_PLAN_HEADER_AUDIENCE,
        [
            "shop",
            "strategy",
            "ad_name",
            "adset_id",
            "old_tags",
            "new_tags",
            "isolation",
            "mau",
            "note",
        ],
    ),
}


def load_action_plan_rows(tab_name: str | None = None) -> list[list[str]]:
    """Raw A1:Z grid from worksheet."""
    name = (tab_name or ACTION_PLAN_TAB or "AI_Action_Plan").strip()
    ss = get_google_sheet()
    try:
        ws = ss.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        return []
    return ws.get_all_values()


def parse_action_plan_grid(values: list[list[str]]) -> dict[str, list[dict[str, Any]]]:
    """
    Scan grid for known section titles. Next row = header; following rows = data until blank row.
    Each dict includes `_sheet_row` (1-based) and keys from _SECTION_CONFIG.
    """
    if not values:
        return {k: [] for k in _SECTION_CONFIG}

    result: dict[str, list[dict[str, Any]]] = {k: [] for k in _SECTION_CONFIG}
    idx = 0
    while idx < len(values):
        row = values[idx]
        title_cell = (row[0] if row else "").strip()
        if title_cell not in _SECTION_CONFIG:
            idx += 1
            continue
        canonical_headers, keys = _SECTION_CONFIG[title_cell]
        idx += 1
        if idx >= len(values):
            break
        header_row = values[idx]
        idx += 1
        col_by_label: dict[str, int] = {}
        for lab in canonical_headers:
            ci = _header_col_index(header_row, lab)
            if ci is not None:
                col_by_label[lab] = ci
        while idx < len(values):
            data_row = values[idx]
            sheet_row_1based = idx + 1
            idx += 1
            if not any((c or "").strip() for c in data_row):
                break
            d: dict[str, Any] = {"_sheet_row": sheet_row_1based}
            for lab, ekey in zip(canonical_headers, keys):
                ci = col_by_label.get(lab)
                if ci is not None and ci < len(data_row):
                    d[ekey] = (data_row[ci] or "").strip()
                else:
                    d[ekey] = ""
            result[title_cell].append(d)
    return result


def parse_action_plan(tab_name: str | None = None) -> dict[str, list[dict[str, Any]]]:
    return parse_action_plan_grid(load_action_plan_rows(tab_name))


def section_by_short_name(parsed: dict[str, list[dict[str, Any]]], short: str) -> list[dict[str, Any]]:
    """short in new_ads, pause, budget, audience."""
    m = {
        "new_ads": ACTION_PLAN_TITLE_NEW,
        "pause": ACTION_PLAN_TITLE_PAUSE,
        "budget": ACTION_PLAN_TITLE_BUDGET,
        "audience": ACTION_PLAN_TITLE_AUDIENCE,
    }
    key = m.get(short, short)
    return parsed.get(key, [])
