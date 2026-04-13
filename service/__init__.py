from __future__ import annotations
"""Service package.

Keep exports lazy so Databricks-side imports (e.g. ``service.cwe_parser``)
do not eagerly import Azure SDK dependent modules.
"""

from typing import Any
from .analyze import analyze

__all__ = [
    "CweSyncOrchestrator",
    "build_orchestrator",
    "build_failure_alert_payload",
    "send_alert_payload",
    "notify_failure_alert",
    "upload_by_targz_body",
    "ls",
    "analyze",
]


def __getattr__(name: str) -> Any:
    if name == "CweSyncOrchestrator":
        from .cwe_orchestrator import CweSyncOrchestrator

        return CweSyncOrchestrator
    if name == "ls":
        from .save_files import ls

        return ls
    if name in {
        "build_failure_alert_payload",
        "send_alert_payload",
        "notify_failure_alert",
    }:
        from .alerting import (
            build_failure_alert_payload,
            notify_failure_alert,
            send_alert_payload,
        )

        return {
            "build_failure_alert_payload": build_failure_alert_payload,
            "send_alert_payload": send_alert_payload,
            "notify_failure_alert": notify_failure_alert,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def build_orchestrator():
    from .cwe_runtime import build_orchestrator as _build_orchestrator

    return _build_orchestrator()


def upload_by_targz_body(machine_id: str, workspace_id: str, body: bytes):
    from .save_files import upload_by_targz_body as _upload_by_targz_body

    return _upload_by_targz_body(machine_id, workspace_id, body)
