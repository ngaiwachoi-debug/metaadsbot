# Auto Ad Placing Monitoring Bot

Automation toolkit for monitoring Meta ads, optimizing budget allocation, and generating structured ad operation decisions.

## What it does

- Sync ad performance data from Meta to Google Sheets.
- Generate refined action rows with guardrails (small-sample protection, max running ads cap).
- Detect recent unpromoted posts per shop and create pending test signals.
- Produce machine-readable operation commands for downstream automation.

## Project notes

- Secrets are loaded from `.env` and are not committed.
- Shop/page mapping and shop configs are stored in `config.json`.
- Main pipeline entrypoint: `python callfrommeta.py`

## Privacy

See `PRIVACY_POLICY.md`.
