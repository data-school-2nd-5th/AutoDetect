from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Dict

import requests
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobSasPermissions, BlobServiceClient, generate_blob_sas

from service import CweSyncOrchestrator
from shared import get_required_env


@dataclass(frozen=True)
class PipelineSettings:
    source_url: str
    request_timeout_seconds: int
    storage_connection_string: str
    blob_container: str
    blob_output_prefix: str
    state_blob_path: str
    blob_uri_prefix: str
    databricks_source_uri_mode: str
    blob_sas_expiry_seconds: int
    databricks_host: str
    databricks_token: str
    databricks_job_id: int
    databricks_target_table: str


def _normalize_env(value: str | None, default: str) -> str:
    if value is None:
        return default
    normalized = value.strip()
    if not normalized or normalized.lower() in {"none", "null"}:
        return default
    return normalized


def _parse_connection_string(connection_string: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for item in connection_string.split(";"):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def load_settings() -> PipelineSettings:
    source_url = os.getenv(
        "CWE_SOURCE_URL",
        "https://cwe.mitre.org/data/xml/cwec_latest.xml.zip",
    )
    request_timeout_seconds = int(os.getenv("CWE_REQUEST_TIMEOUT_SECONDS", "30"))

    return PipelineSettings(
        source_url=source_url,
        request_timeout_seconds=request_timeout_seconds,
        storage_connection_string=get_required_env("AZURE_STORAGE_CONNECTION_STRING"),
        blob_container=_normalize_env(os.getenv("CWE_BLOB_CONTAINER"), "cwe-data"),
        blob_output_prefix=_normalize_env(os.getenv("CWE_BLOB_OUTPUT_PREFIX"), "cwe/raw"),
        state_blob_path=_normalize_env(
            os.getenv("CWE_STATE_BLOB_PATH"),
            "cwe/state/version_state.json",
        ),
        blob_uri_prefix=_normalize_env(os.getenv("CWE_BLOB_URI_PREFIX"), ""),
        databricks_source_uri_mode=_normalize_env(
            os.getenv("DATABRICKS_SOURCE_URI_MODE"),
            "sas_url",
        ).lower(),
        blob_sas_expiry_seconds=int(_normalize_env(os.getenv("CWE_BLOB_SAS_EXPIRY_SECONDS"), "3600")),
        databricks_host=get_required_env("DATABRICKS_HOST"),
        databricks_token=get_required_env("DATABRICKS_TOKEN"),
        databricks_job_id=int(get_required_env("DATABRICKS_JOB_ID")),
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
        databricks_source_uri_mode: str = "sas_url",
        blob_sas_expiry_seconds: int = 3600,
    ) -> None:
        self._blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self._container_name = container_name
        self._output_prefix = output_prefix.rstrip("/")
        self._blob_uri_prefix = blob_uri_prefix.rstrip("/")
        self._databricks_source_uri_mode = databricks_source_uri_mode
        self._blob_sas_expiry_seconds = blob_sas_expiry_seconds

        conn_map = _parse_connection_string(connection_string)
        self._account_name = conn_map.get("AccountName", "")
        self._account_key = conn_map.get("AccountKey", "")

    def save_xml(self, version_id: str, xml_bytes: bytes) -> str:
        blob_name = f"{self._output_prefix}/{version_id}/cwec_latest.xml"
        blob_client = self._blob_service_client.get_blob_client(
            container=self._container_name,
            blob=blob_name,
        )
        blob_client.upload_blob(xml_bytes, overwrite=True)

        if self._databricks_source_uri_mode == "sas_url":
            if not self._account_name or not self._account_key:
                raise ValueError(
                    "AccountName/AccountKey are required in AZURE_STORAGE_CONNECTION_STRING "
                    "when DATABRICKS_SOURCE_URI_MODE=sas_url",
                )
            token = generate_blob_sas(
                account_name=self._account_name,
                account_key=self._account_key,
                container_name=self._container_name,
                blob_name=blob_name,
                permission=BlobSasPermissions(read=True),
                start=datetime.now(timezone.utc) - timedelta(minutes=5),
                expiry=datetime.now(timezone.utc) + timedelta(seconds=self._blob_sas_expiry_seconds),
            )
            return f"{blob_client.url}?{token}"

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
            "notebook_params": {
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
        databricks_source_uri_mode=settings.databricks_source_uri_mode,
        blob_sas_expiry_seconds=settings.blob_sas_expiry_seconds,
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
