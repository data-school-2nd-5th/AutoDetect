import io
import unittest
import zipfile

from service.cwe_orchestrator import CweSyncOrchestrator


class _FakeSourceClient:
    def __init__(self, metadata, zip_payload):
        self._metadata = metadata
        self._zip_payload = zip_payload
        self.downloaded = False

    def get_latest_metadata(self):
        return self._metadata

    def download_latest_zip(self):
        self.downloaded = True
        return self._zip_payload


class _FakeBlobStore:
    def __init__(self):
        self.saved = []

    def save_xml(self, version_id, xml_bytes):
        path = f"cwe/raw/{version_id}/cwec_latest.xml"
        self.saved.append((version_id, xml_bytes, path))
        return path


class _FakeStateStore:
    def __init__(self, state):
        self._state = state
        self.saved_state = None

    def load(self):
        return self._state

    def save(self, state):
        self.saved_state = state


class _FakeDatabricksClient:
    def __init__(self):
        self.calls = []

    def run_job(self, *, source_xml_path, source_version_id):
        self.calls.append(
            {
                "source_xml_path": source_xml_path,
                "source_version_id": source_version_id,
            }
        )
        return {"run_id": 123}


class CweSyncOrchestratorTests(unittest.TestCase):
    def _zip_xml(self, xml_text: str) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("cwec_latest.xml", xml_text)
        return buffer.getvalue()

    def test_skips_when_source_unchanged(self) -> None:
        metadata = {"version_id": "etag-1", "last_modified": "Fri, 03 Apr 2026 00:00:00 GMT"}
        source = _FakeSourceClient(metadata, self._zip_xml("<root/>"))
        blob = _FakeBlobStore()
        state = _FakeStateStore({"version_id": "etag-1"})
        databricks = _FakeDatabricksClient()

        orchestrator = CweSyncOrchestrator(source, blob, state, databricks)
        result = orchestrator.run(force=False, trigger="timer")

        self.assertEqual("skipped", result["status"])
        self.assertFalse(source.downloaded)
        self.assertEqual([], databricks.calls)

    def test_runs_pipeline_when_source_changed(self) -> None:
        metadata = {"version_id": "etag-2", "last_modified": "Fri, 03 Apr 2026 00:00:00 GMT"}
        source = _FakeSourceClient(metadata, self._zip_xml("<Weakness_Catalog/>"))
        blob = _FakeBlobStore()
        state = _FakeStateStore({"version_id": "etag-1"})
        databricks = _FakeDatabricksClient()

        orchestrator = CweSyncOrchestrator(source, blob, state, databricks)
        result = orchestrator.run(force=False, trigger="timer")

        self.assertEqual("completed", result["status"])
        self.assertEqual(1, len(blob.saved))
        self.assertEqual(1, len(databricks.calls))
        self.assertEqual("etag-2", state.saved_state["version_id"])
        self.assertEqual("etag-2", databricks.calls[0]["source_version_id"])


if __name__ == "__main__":
    unittest.main()
