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
    def save_text(self, content: str, blob_path: str) -> str:
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
            # list_blobs 대신 walk_blobs를 사용하면 디렉토리(Prefix) 구분이 훨씬 쉽습니다.
            blob_iter = container_client.walk_blobs(
                name_starts_with=normalized_path, 
                delimiter="/"
            )

            files = []
            directories = []

            for item in blob_iter:
                # 1. 디렉토리(BlobPrefix)인 경우
                if hasattr(item, 'prefix'):
                    # item은 BlobPrefix 객체이며, 'prefix' 속성에 경로가 담겨 있습니다.
                    dir_full_path = item.prefix
                    dir_name = dir_full_path.rstrip("/").split("/")[-1]
                    directories.append({
                        "name": dir_name,
                        "full_path": dir_full_path,
                        "type": "directory"
                    })
                
                # 2. 일반 파일(BlobProperties)인 경우
                else:
                    if item.name == normalized_path:
                        continue
                    
                    files.append({
                        "name": item.name.split("/")[-1],
                        "full_path": item.name,
                        "type": "file",
                        "size": item.size,
                        "last_modified": item.last_modified.isoformat() if item.last_modified else None
                    })

            return {
                "path": normalized_path,
                "directories": directories,
                "files": files,
                "count": len(directories) + len(files)
            }

        except AzureError as e:
            logging.error(f"Failed to list blobs in {normalized_path}: {str(e)}")
            raise e