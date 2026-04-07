"""Service package."""

from .cwe_orchestrator import CweSyncOrchestrator
from .cwe_runtime import build_orchestrator


def upload_by_targz_body(machine_id: str, workspace_id: str, path: str, body: bytes):
    from .save_files import upload_by_targz_body as _upload_by_targz_body

    return _upload_by_targz_body(machine_id, workspace_id, path, body)


def ls(path: str):
    from .save_files import ls as _ls

    return _ls(path)
