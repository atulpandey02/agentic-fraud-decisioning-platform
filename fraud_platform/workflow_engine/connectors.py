# =============================================================
# CONNECTORS — Slack / email as auditable OUTBOX writes
# =============================================================
# Honest scope: these are DEMO connectors. The load-bearing action
# is writing the message to an `outbox` table — that row IS the
# demo artifact (the exact payload that WOULD be delivered). Real
# delivery is a documented future feature:
#   - Slack: if SLACK_WEBHOOK_URL is set, ALSO POST to the incoming
#     webhook after writing the outbox row — but never required, and
#     a failed POST does not fail the step (the outbox row stands).
#   - Email: outbox-only (SMTP/Gmail-OAuth is the named production gap).
#
# The outbox WRITER is injected (dependency injection), so connectors
# don't import the persistence layer — they're unit-testable with a
# fake writer and swappable for Postgres later without touching this
# file. Same one-owner-of-writes discipline as the rest of the repo.
# =============================================================

import json
import logging
import urllib.request
from typing import Callable, Optional

from . import config

logger = logging.getLogger(__name__)

# (connector_name, payload_dict) -> None. The state layer supplies a
# writer that appends a row to `outbox`; tests supply a list.append.
OutboxWriter = Callable[[str, dict], None]


def make_slack_send(write_outbox: OutboxWriter,
                    webhook_url: Optional[str] = None) -> Callable[[str, str], dict]:
    """Build a `slack_send_message(channel, text)` bound to an outbox
    writer. Delivery to a real incoming webhook happens ONLY if a URL
    is configured, and never gates the step."""
    url = webhook_url if webhook_url is not None else config.SLACK_WEBHOOK_URL

    def slack_send_message(channel: str, text: str) -> dict:
        payload = {"channel": channel, "text": text}
        write_outbox("slack", payload)          # always — the demo artifact
        delivered = False
        if url:
            try:
                req = urllib.request.Request(
                    url, data=json.dumps(payload).encode(),
                    headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(req, timeout=5)
                delivered = True
            except Exception as e:  # noqa: BLE001 — delivery is best-effort
                logger.warning("Slack webhook POST failed (outbox row stands): %s", e)
        return {"connector": "slack", "channel": channel,
                "outbox_written": True, "delivered": delivered}

    return slack_send_message


def make_email_send(write_outbox: OutboxWriter) -> Callable[[str, str, str], dict]:
    """Build an `email_send(to, subject, body)` — outbox-only (mock).
    Real SMTP/Gmail delivery is the named production gap."""

    def email_send(to: str, subject: str, body: str) -> dict:
        payload = {"to": to, "subject": subject, "body": body}
        write_outbox("email", payload)
        return {"connector": "email", "to": to,
                "outbox_written": True, "delivered": False}

    return email_send
