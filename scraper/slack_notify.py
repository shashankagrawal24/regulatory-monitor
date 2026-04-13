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
    total = data.get("total", 0)
    high = data.get("high_priority", 0)
    medium = data.get("medium_priority", 0)
    updates = data.get("updates", [])

    # --- Header ---
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": f"Regulatory Brief - {date_str}", "emoji": True}
    })

    # --- Pulse ---
    if total == 0:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "> No material regulatory updates in the last 24 hours.\n> Markets are quiet today."}
        })
        return blocks

    pulse = f":red_circle: *{high} high-priority*  |  :large_yellow_circle: *{medium} medium*  |  *{total} total*"
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": pulse}
    })

    blocks.append({"type": "divider"})

    # --- Top Content Opportunities ---
    views = data.get("views", {})
    content_opps = views.get("best_content_opportunities", [])[:3]
    if content_opps:
        opp_lines = [":mega: *Top Content Opportunities*"]
        for i, opp in enumerate(content_opps, 1):
            title = opp.get("title", "")[:70]
            engage = opp.get("engagement_potential", 0)
            angle = opp.get("content_angle", "")
            formats = ", ".join(opp.get("possible_formats", []))
            opp_lines.append(f"*{i}. {title}*")
            opp_lines.append(f"    Engagement: {engage}/10 | Angle: _{angle}_ | Formats: {formats}")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(opp_lines)}
        })
        blocks.append({"type": "divider"})

    # --- HIGH priority items ---
    high_items = [u for u in updates if u.get("relevance") == "HIGH"]
    if high_items:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":red_circle: *High Priority - Action Required*"}
        })

        for u in high_items[:5]:
            title = u.get("title", "")[:100]
            regulator = u.get("regulator", "")
            category = u.get("category", "")
            urgency = u.get("urgency", "")
            url = u.get("url", "")

            reg_imp = u.get("regulatory_importance", 0)
            retail = u.get("retail_user_impact", 0)
            action = u.get("actionability", 0)
            engage = u.get("engagement_potential", 0)

            user_impact = u.get("user_impact", "")
            content_angle = u.get("content_angle", "")
            nw_angle = u.get("nw_angle", "")
            action_type = u.get("action_type", "none")
            action_deadline = u.get("action_deadline", "")
            formats = ", ".join(u.get("possible_content_formats", []))
            segments = ", ".join(u.get("user_segment_tags", []))
            topics = ", ".join(u.get("topic_tags", []))
            covered = u.get("also_covered_by", [])

            lines = [f"*<{url}|{title}>*"]
            lines.append(f"*{regulator}* | {category} | Urgency: `{urgency}`")
            lines.append(f"Scores: Reg `{reg_imp}` | Impact `{retail}` | Action `{action}` | Engage `{engage}`")
            lines.append(f":bust_in_silhouette: {segments} | :label: {topics}")
            lines.append(f":bulb: {user_impact}")
            lines.append(f":pencil: {content_angle} ({nw_angle})")

            if action_type != "none":
                deadline_str = f" (deadline: {action_deadline})" if action_deadline else ""
                lines.append(f":zap: Action: `{action_type}`{deadline_str}")

            if formats:
                lines.append(f":package: Formats: {formats}")

            if covered:
                lines.append(f"Also covered by: {', '.join(covered)}")

            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines)}
            })
            blocks.append({"type": "divider"})

    # --- MEDIUM priority items (compact list) ---
    med_items = [u for u in updates if u.get("relevance") == "MEDIUM"]
    if med_items:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":large_yellow_circle: *Medium Priority - Monitor*"}
        })

        med_lines = []
        for i, u in enumerate(med_items[:8], 1):
            title = u.get("title", "")[:65]
            reg = u.get("regulator", "")
            cat = u.get("category", "")
            engage = u.get("engagement_potential", 0)
            url = u.get("url", "")
            med_lines.append(f"{i}. *{reg}* - <{url}|{title}> ({cat}, engage: {engage}/10)")

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(med_lines)}
        })

    # --- Action Required summary ---
    action_items = views.get("action_required_items", [])
    if action_items:
        blocks.append({"type": "divider"})
        action_lines = [":warning: *Action Required Items*"]
        for a in action_items[:3]:
            title = a.get("title", "")[:60]
            atype = a.get("action_type", "")
            deadline = a.get("action_deadline", "")
            deadline_str = f" | Deadline: `{deadline}`" if deadline else ""
            action_lines.append(f"- {title} | Action: `{atype}`{deadline_str}")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(action_lines)}
        })

    # --- Footer ---
    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": "SEBI | RBI | IRDAI | PFRDA | CBDT | AMFI | PIB + 20 news feeds | Novelty Wealth (SEBI RIA: INA000019415)"
        }]
    })

    return blocks


def post_to_slack(blocks: list[dict]):
    """Send message to Slack via webhook."""
    payload = {"blocks": blocks}

    if len(blocks) > 48:
        payload["blocks"] = blocks[:47] + [blocks[-1]]

    resp = requests.post(WEBHOOK_URL, json=payload, timeout=15)
    if resp.status_code != 200:
        print(f"Slack post failed ({resp.status_code}): {resp.text}", file=sys.stderr)
        sys.exit(1)
    else:
        print(f"Posted to Slack ({len(blocks)} blocks)")


def main():
    if not WEBHOOK_URL:
        print("SLACK_WEBHOOK_URL not set, skipping Slack notification")
        return

    data = load_briefing()
    if not data:
        print("No briefing data found")
        return

    blocks = format_slack_message(data)
    post_to_slack(blocks)


if __name__ == "__main__":
    main()
