import argparse
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


def parse_upload_mapping(raw_mapping: str) -> tuple[Path, str]:
    if "=" not in raw_mapping:
        raise ValueError(
            f"Invalid upload mapping '{raw_mapping}'. Use the format LOCAL_FILE=BLOB_PATH.",
        )

    local_file_raw, blob_path_raw = raw_mapping.split("=", 1)
    local_file = Path(local_file_raw).expanduser()
    blob_path = blob_path_raw.strip()
    return local_file, blob_path


def upload_by_targz_body(machine_id: str, workspace_id: str, path: str, body: bytes):
    base_blob_path = posix_join(
        machine_id.strip().strip("/"),
        workspace_id.strip().strip("/"),
        path.strip().strip("/"),
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


def main() -> int:
    args = parse_args()
    upload_blob = UploadBlob(
        connection_string=args.connection_string,
        container_name=args.container,
    )

    if args.create_container:
        upload_blob.ensure_container()

    for raw_mapping in args.uploads:
        local_file, blob_path = parse_upload_mapping(raw_mapping)
        uploaded_blob_path = upload_blob.save_file(local_file, blob_path)
        print(f"Uploaded {local_file} -> {args.container}/{uploaded_blob_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
