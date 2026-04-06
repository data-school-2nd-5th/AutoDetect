from __future__ import annotations

from pathlib import Path

from azure.core.exceptions import AzureError
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient


class UploadBlob:
    def __init__(self, connection_string: str, container_name: str) -> None:
        self._blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self._container_name = container_name

    def is_connected(self) -> bool:
        try:
            self._blob_service_client.get_account_information()
            return True
        except AzureError:
            return False

    def ensure_container(self) -> None:
        container_client = self._blob_service_client.get_container_client(self._container_name)

        try:
            container_client.create_container()
        except ResourceExistsError:
            pass

    def save_file(self, local_file: str | Path, blob_path: str) -> str:
        file_path = Path(local_file).expanduser()
        normalized_blob_path = blob_path.strip().lstrip("/")

        if not file_path.is_file():
            raise ValueError(f"Local file not found: {file_path}")

        if not normalized_blob_path:
            raise ValueError("Blob path must not be empty.")

        blob_client = self._blob_service_client.get_blob_client(
            container=self._container_name,
            blob=normalized_blob_path,
        )

        with file_path.open("rb") as file_handle:
            blob_client.upload_blob(file_handle, overwrite=True)

        return normalized_blob_path

    def save_bytes(self, content: bytes, blob_path: str) -> str:
        normalized_blob_path = blob_path.strip().lstrip("/")

        if not normalized_blob_path:
            raise ValueError("Blob path must not be empty.")

        blob_client = self._blob_service_client.get_blob_client(
            container=self._container_name,
            blob=normalized_blob_path,
        )
        blob_client.upload_blob(content, overwrite=True)
        return normalized_blob_path
