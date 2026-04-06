from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Dict

import requests
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

from service import CweSyncOrchestrator


@dataclass(frozen=True)
class PipelineSettings:
    source_url: str
    request_timeout_seconds: int
    storage_connection_string: str
    blob_container: str
    blob_output_prefix: str
    state_blob_path: str
    blob_uri_prefix: str
    databricks_host: str
    databricks_token: str
    databricks_job_id: int
    databricks_target_table: str


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_settings() -> PipelineSettings:
    source_url = os.getenv(
        "CWE_SOURCE_URL",
        "https://cwe.mitre.org/data/xml/cwec_latest.xml.zip",
    )
    request_timeout_seconds = int(os.getenv("CWE_REQUEST_TIMEOUT_SECONDS", "30"))

    return PipelineSettings(
        source_url=source_url,
        request_timeout_seconds=request_timeout_seconds,
        storage_connection_string=_get_required_env("AZURE_STORAGE_CONNECTION_STRING"),
        blob_container=os.getenv("CWE_BLOB_CONTAINER", "cwe-data"),
        blob_output_prefix=os.getenv("CWE_BLOB_OUTPUT_PREFIX", "cwe/raw"),
        state_blob_path=os.getenv("CWE_STATE_BLOB_PATH", "cwe/state/version_state.json"),
        blob_uri_prefix=os.getenv("CWE_BLOB_URI_PREFIX", ""),
        databricks_host=_get_required_env("DATABRICKS_HOST"),
        databricks_token=_get_required_env("DATABRICKS_TOKEN"),
        databricks_job_id=int(_get_required_env("DATABRICKS_JOB_ID")),
        databricks_target_table=os.getenv("DELTA_TARGET_TABLE", "main.security.cwe_weaknesses"),
    )


class CweSourceClient:
    def __init__(self, source_url: str, timeout_seconds: int = 30) -> None:
        self._source_url = source_url
        self._timeout_seconds = timeout_seconds

    def get_latest_metadata(self) -> Dict[str, str]:
        response = requests.head(self._source_url, timeout=self._timeout_seconds)
        response.raise_for_status()

        etag = (response.headers.get("ETag") or "").strip('"')
        last_modified = response.headers.get("Last-Modified", "")
        version_seed = f"{etag}|{last_modified}".encode("utf-8")
        version_id = hashlib.sha256(version_seed).hexdigest()[:16]

        return {
            "version_id": version_id,
            "last_modified": last_modified,
            "etag": etag,
        }

    def download_latest_zip(self) -> bytes:
        response = requests.get(self._source_url, timeout=self._timeout_seconds)
        response.raise_for_status()
        return response.content


class XmlBlobStore:
    def __init__(
        self,
        connection_string: str,
        container_name: str,
        output_prefix: str,
        blob_uri_prefix: str = "",
    ) -> None:
        self._blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self._container_name = container_name
        self._output_prefix = output_prefix.rstrip("/")
        self._blob_uri_prefix = blob_uri_prefix.rstrip("/")

    def save_xml(self, version_id: str, xml_bytes: bytes) -> str:
        blob_name = f"{self._output_prefix}/{version_id}/cwec_latest.xml"
        blob_client = self._blob_service_client.get_blob_client(
            container=self._container_name,
            blob=blob_name,
        )
        blob_client.upload_blob(xml_bytes, overwrite=True)

        if self._blob_uri_prefix:
            return f"{self._blob_uri_prefix}/{blob_name}"

        return blob_name


class StateStore:
    def __init__(
        self,
        connection_string: str,
        container_name: str,
        state_blob_path: str,
    ) -> None:
        self._blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self._container_name = container_name
        self._state_blob_path = state_blob_path

    def load(self) -> Dict[str, Any]:
        blob_client = self._blob_service_client.get_blob_client(
            container=self._container_name,
            blob=self._state_blob_path,
        )

        try:
            payload = blob_client.download_blob().readall()
        except ResourceNotFoundError:
            return {}

        return json.loads(payload.decode("utf-8"))

    def save(self, state: Dict[str, Any]) -> None:
        blob_client = self._blob_service_client.get_blob_client(
            container=self._container_name,
            blob=self._state_blob_path,
        )
        blob_client.upload_blob(
            json.dumps(state, ensure_ascii=True, indent=2).encode("utf-8"),
            overwrite=True,
        )


class DatabricksClient:
    def __init__(
        self,
        host: str,
        token: str,
        job_id: int,
        target_table: str,
        timeout_seconds: int = 30,
    ) -> None:
        self._host = host.rstrip("/")
        self._token = token
        self._job_id = job_id
        self._target_table = target_table
        self._timeout_seconds = timeout_seconds

    def run_job(self, *, source_xml_path: str, source_version_id: str) -> Dict[str, Any]:
        url = f"{self._host}/api/2.1/jobs/run-now"
        payload = {
            "job_id": self._job_id,
            "job_parameters": {
                "source_xml_path": source_xml_path,
                "source_version_id": source_version_id,
                "target_table": self._target_table,
            },
        }

        response = requests.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=self._timeout_seconds,
        )
        response.raise_for_status()
        return response.json()


def build_orchestrator() -> CweSyncOrchestrator:
    settings = load_settings()

    source_client = CweSourceClient(
        source_url=settings.source_url,
        timeout_seconds=settings.request_timeout_seconds,
    )
    blob_store = XmlBlobStore(
        connection_string=settings.storage_connection_string,
        container_name=settings.blob_container,
        output_prefix=settings.blob_output_prefix,
        blob_uri_prefix=settings.blob_uri_prefix,
    )
    state_store = StateStore(
        connection_string=settings.storage_connection_string,
        container_name=settings.blob_container,
        state_blob_path=settings.state_blob_path,
    )
    databricks_client = DatabricksClient(
        host=settings.databricks_host,
        token=settings.databricks_token,
        job_id=settings.databricks_job_id,
        target_table=settings.databricks_target_table,
        timeout_seconds=settings.request_timeout_seconds,
    )

    return CweSyncOrchestrator(
        source_client=source_client,
        blob_store=blob_store,
        state_store=state_store,
        databricks_client=databricks_client,
    )
