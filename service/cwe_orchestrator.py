from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Any, Dict
import zipfile


class CweSyncOrchestrator:
    def __init__(self, source_client, blob_store, state_store, databricks_client) -> None:
        self._source_client = source_client
        self._blob_store = blob_store
        self._state_store = state_store
        self._databricks_client = databricks_client

    def run(self, *, force: bool, trigger: str) -> Dict[str, Any]:
        metadata = self._source_client.get_latest_metadata()
        version_id = metadata["version_id"]
        previous_state = self._state_store.load() or {}
        current_version = previous_state.get("version_id")

        if not force and current_version == version_id:
            return {
                "status": "skipped",
                "reason": "source_unchanged",
                "version_id": version_id,
                "trigger": trigger,
            }

        zip_payload = self._source_client.download_latest_zip()
        xml_bytes = _extract_xml_payload(zip_payload)

        source_xml_path = self._blob_store.save_xml(version_id, xml_bytes)
        run_result = self._databricks_client.run_job(
            source_xml_path=source_xml_path,
            source_version_id=version_id,
        )

        new_state = {
            "version_id": version_id,
            "last_modified": metadata.get("last_modified", ""),
            "processed_at_utc": _utc_now(),
            "trigger": trigger,
            "run_id": run_result.get("run_id"),
        }
        self._state_store.save(new_state)

        return {
            "status": "completed",
            "version_id": version_id,
            "source_xml_path": source_xml_path,
            "run_id": run_result.get("run_id"),
            "trigger": trigger,
        }


def _extract_xml_payload(zip_payload: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(zip_payload), mode="r") as archive:
        xml_names = [name for name in archive.namelist() if name.lower().endswith(".xml")]
        if not xml_names:
            raise ValueError("Zip payload does not contain an XML file")

        if "cwec_latest.xml" in xml_names:
            return archive.read("cwec_latest.xml")

        return archive.read(xml_names[0])


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
