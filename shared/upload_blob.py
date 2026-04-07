
from __future__ import annotations
import logging
from pathlib import Path
from typing import Dict, Any
from azure.core.exceptions import AzureError
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient


class UploadBlob:
    def __init__(self, connection_string: str, container_name: str) -> None:
        self._blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        self._container_name = container_name

    def is_connected(self) -> bool:
        try:
            self._blob_service_client.get_account_information()
            return True
        except AzureError:
            return False

    def ensure_container(self) -> None:
        container_client = self._blob_service_client.get_container_client(
            self._container_name
        )

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

    def ls(self, path: str) -> Dict[str, Any]:
        """
        특정 경로의 Blob 목록을 조회합니다.
        가상 디렉토리 구조를 지원하기 위해 name_starts_with와 delimiter를 사용합니다.
        """
        normalized_path = path.strip().lstrip("/")
        # 디렉토리 조회 시 접두어가 반드시 /로 끝나야 하위 항목만 정확히 필터링됨
        if normalized_path and not normalized_path.endswith("/"):
            normalized_path += "/"

        container_client = self._blob_service_client.get_container_client(self._container_name)
        
        try:
            # name_starts_with: 특정 경로로 시작하는 blob 검색
            # delimiter: '/'를 기준으로 가상 디렉토리 구분
            blob_iter = container_client.list_blobs(
                name_starts_with=normalized_path, 
                delimiter="/"
            )

            files = []
            directories = []

            # 1. 하위 가상 디렉토리 (Common Prefixes) 추출
            for prefix in blob_iter.location_mode_get_prefixes():
                # 전체 경로에서 현재 조회한 path 부분을 제외한 이름만 추출
                dir_name = prefix.rstrip("/").split("/")[-1]
                directories.append({
                    "name": dir_name,
                    "full_path": prefix,
                    "type": "directory"
                })

            # 2. 현재 경로의 파일(Blobs) 추출
            for blob in blob_iter:
                # 검색 경로와 완전히 일치하는 디렉토리 자체 blob은 제외
                if blob.name == normalized_path:
                    continue
                
                files.append({
                    "name": blob.name.split("/")[-1],
                    "full_path": blob.name,
                    "type": "file",
                    "size": blob.size,
                    "last_modified": blob.last_modified.isoformat() if blob.last_modified else None,
                    "content_type": blob.content_settings.content_type
                })

            return {
                "path": normalized_path,
                "directories": directories,
                "files": files,
                "count": len(directories) + len(files)
            }

        except AzureError as e:
            logging.error(f"Failed to list blobs in {normalized_path}: {str(e)}")
            raise
