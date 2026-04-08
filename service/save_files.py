import logging
import tarfile
from io import BytesIO
from pathlib import Path
from posixpath import join as posix_join

from shared import UploadBlob, get_env, parse_args


CONNECTION_STRING = get_env("UPLOAD_CONNECTION_STRING")
CONTAINER_NAME = get_env("UPLOAD_CONTAINER_NAME")

uploader = UploadBlob(CONNECTION_STRING, CONTAINER_NAME)

if not uploader.is_connected():
    logging.error("Uploader is not connected")
    raise ConnectionError()


def ls(path: str):
    return uploader.ls(path)


def upload_by_targz_body(machine_id: str, workspace_id: str,body: bytes):
    base_blob_path = posix_join(
        machine_id.strip().strip("/"),
        workspace_id.strip().strip("/"),
    )
    uploaded_blob_paths: list[str] = []

    with tarfile.open(fileobj=BytesIO(body), mode="r:gz") as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue

            extracted_file = archive.extractfile(member)
            if extracted_file is None:
                continue

            member_path = member.name.strip().lstrip("./").strip("/")
            if not member_path:
                continue

            blob_path = posix_join(base_blob_path, member_path)
            uploaded_blob_path = uploader.save_bytes(extracted_file.read(), blob_path)
            uploaded_blob_paths.append(uploaded_blob_path)

    return uploaded_blob_paths
