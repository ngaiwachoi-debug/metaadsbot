# Privacy Policy

Last updated: 2026-03-26

## Overview

This project ("Auto Ad Placing Monitoring Bot") processes advertising performance data to produce monitoring and optimization outputs.

## Data We Process

- Ad performance metrics (for example CPC, spend, clicks, budget).
- Ad metadata (for example ad IDs, ad set IDs, campaign IDs, created time).
- Audience targeting metadata provided by the Meta Ads API.
- Page post metadata used for pending-test detection (for example post ID and timestamp).

## Data Sources

- Meta Graph API / Meta Ads API.
- Google Sheets used by the operator.
- Local configuration files (`config.json`) and runtime artifacts (for example `pending_tests.json`).

## Purpose of Processing

- Monitor ad performance.
- Generate optimization suggestions and machine-readable action commands.
- Detect newly published posts not yet covered by promotion workflows.

## Data Storage

- Processing is primarily local to the operator environment.
- Output artifacts may be written to local files and Google Sheets configured by the operator.

## Credentials and Secrets

- API keys and tokens are expected in environment variables (for example `.env`).
- Secrets should never be committed to source control.

## Data Sharing

- This project does not intentionally sell or share personal data.
- Data may be transferred to third-party services explicitly configured by the operator (for example Meta, Google).

## Security

- Operators are responsible for safeguarding host environments, access tokens, and sheet permissions.
- Recommended practices include least-privilege API scopes and periodic credential rotation.

## Retention

- Data retention depends on operator configuration and external platform policies.
- Local temporary/generated files can be removed at any time by the operator.

## Your Responsibilities

- Ensure lawful use and compliance with applicable advertising, privacy, and platform terms.
- Obtain required permissions before processing account or page data.

## Contact

For repository-specific privacy questions, open an issue in the repository.
