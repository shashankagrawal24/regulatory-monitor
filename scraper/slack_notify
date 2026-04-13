"""
Slack Briefing Poster for Regulatory Monitor v3.1
==================================================
Reads data/latest.json and posts a formatted briefing to Slack.
Designed to run after monitor.py in GitHub Actions.

Usage: python scraper/slack_notify.py
Requires: SLACK_WEBHOOK_URL environment variable
"""

import json
import os
import sys
import requests
from pathlib import Path


LATEST_FILE = Path("data/latest.json")
WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


def load_briefing() -> dict:
    if not LATEST_FILE.exists():
        return {}
    with open(LATEST_FILE) as f:
        return json.load(f)


def format_slack_message(data: dict) -> list[dict]:
    """Build Slack Block Kit message from briefing data."""
    blocks = []
    date_str = data.get("date", "Unknown")
    total = data.get("total",
