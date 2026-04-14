import tarfile
from io import BytesIO
from posixpath import join as posix_join

from shared import azure_storage_manager


def ls(path: str):
    return azure_storage_manager.ls(path)


def upload_by_targz_body(machine_id: str, workspace_id: str, body: bytes):
    base_blob_path = posix_join(
        "workspaces",
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
            uploaded_blob_path = azure_storage_manager.save_bytes(
                extracted_file.read(), blob_path
            )
            uploaded_blob_paths.append(uploaded_blob_path)

    return uploaded_blob_paths

def upload_by_text(machine_id: str, workspace_id: str, text: str):
    base_blob_path = posix_join(
        "workspaces",
        workspace_id.strip().strip("/"),
    )
    blob_path = posix_join(base_blob_path, "work.js")
    uploaded_blob_path = azure_storage_manager.save_text(text, blob_path)
    return uploaded_blob_path