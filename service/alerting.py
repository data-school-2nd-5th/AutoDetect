from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Mapping

import requests

from shared import to_bool

DEFAULT_ALERT_TIMEOUT_SECONDS = 5.0


def build_failure_alert_payload(
    *,
    source: str,
    component: str,
    trigger: str,
    error: BaseException,
    run_id: str | None = None,
    context: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    message = str(error).strip() or error.__class__.__name__
    payload: Dict[str, Any] = {
        "source": source,
        "component": component,
        "trigger": trigger,
        "error_type": error.__class__.__name__,
        "error_message": message,
        "occurred_at_utc": datetime.now(timezone.utc).isoformat(),
        "context": context or {},
    }
    if run_id:
        payload["run_id"] = run_id
    return payload


def send_alert_payload(payload: Mapping[str, Any]) -> bool:
    if not _is_alert_enabled():
        logging.info("Failure alerting is disabled by ALERT_ENABLED.")
        return False

    logicapp_url = _logicapp_url()
    if not logicapp_url:
        logging.error("ALERT_LOGICAPP_URL is required when ALERT_ENABLED=true.")
        return False

    timeout_seconds = _alert_timeout_seconds()
    try:
        response = requests.post(
            logicapp_url,
            json=dict(payload),
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        return True
    except requests.RequestException:
        logging.exception("Failed to send failure alert to Logic App.")
        return False


def notify_failure_alert(
    *,
    source: str,
    component: str,
    trigger: str,
    error: BaseException,
    run_id: str | None = None,
    context: Dict[str, Any] | None = None,
) -> bool:
    payload = build_failure_alert_payload(
        source=source,
        component=component,
        trigger=trigger,
        error=error,
        run_id=run_id,
        context=context,
    )
    return send_alert_payload(payload)


def _is_alert_enabled() -> bool:
    return to_bool(os.getenv("ALERT_ENABLED", "true"))


def _logicapp_url() -> str:
    return os.getenv("ALERT_LOGICAPP_URL", "").strip()


def _alert_timeout_seconds() -> float:
    raw = os.getenv("ALERT_HTTP_TIMEOUT_SECONDS", str(DEFAULT_ALERT_TIMEOUT_SECONDS))
    try:
        timeout_seconds = float(raw)
        if timeout_seconds <= 0:
            raise ValueError
        return timeout_seconds
    except ValueError:
        logging.warning(
            "Invalid ALERT_HTTP_TIMEOUT_SECONDS=%r; using default %.1f",
            raw,
            DEFAULT_ALERT_TIMEOUT_SECONDS,
        )
        return DEFAULT_ALERT_TIMEOUT_SECONDS
