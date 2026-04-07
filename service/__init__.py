"""Service package.

Keep exports lazy so Databricks-side imports (e.g. ``service.cwe_parser``)
do not eagerly import Azure SDK dependent modules.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "CweSyncOrchestrator",
    "build_orchestrator",
    "upload_by_targz_body",
    "ls",
]


def __getattr__(name: str) -> Any:
    if name == "CweSyncOrchestrator":
        from .cwe_orchestrator import CweSyncOrchestrator

        return CweSyncOrchestrator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def build_orchestrator():
    from .cwe_runtime import build_orchestrator as _build_orchestrator

    return _build_orchestrator()


def upload_by_targz_body(machine_id: str, workspace_id: str, path: str, body: bytes):
    from .save_files import upload_by_targz_body as _upload_by_targz_body

    return _upload_by_targz_body(machine_id, workspace_id, path, body)


def ls(path: str):
    from .save_files import ls as _ls

    return _ls(path)
