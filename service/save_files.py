import argparse
import os
import tarfile
import logging
from io import BytesIO
from pathlib import Path
from posixpath import join as posix_join
from shared.upload_blob import UploadBlob


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Save one or more local files to Azure Blob Storage.",
    )
    parser.add_argument(
        "--connection-string",
        default=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        help="Azure Storage connection string. Defaults to AZURE_STORAGE_CONNECTION_STRING.",
    )
    parser.add_argument(
        "--container",
        required=True,
        help="Target blob container name.",
    )
    parser.add_argument(
        "--create-container",
        action="store_true",
        help="Create the container if it does not already exist.",
    )
    parser.add_argument(
        "uploads",
        nargs="+",
        metavar="LOCAL_FILE=BLOB_PATH",
        help="Upload mapping. Example: data/report.csv=archive/2026-04-06/report.csv",
    )
    args = parser.parse_args()

    if not args.connection_string:
        parser.error("A connection string is required via --connection-string or AZURE_STORAGE_CONNECTION_STRING.")

    return args

CONNECTION_STRING = os.getenv('UPLOAD_CONNECTION_STRING')
CONTAINER_NAME=os.getenv('UPLOAD_CONTAINER_NAME')

uploader = UploadBlob(CONNECTION_STRING,CONTAINER_NAME)
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


def upload_by_targz_body(body: bytes, upload_blob: UploadBlob, base_blob_path: str = "") -> list[str]:
    normalized_base_path = base_blob_path.strip().strip("/")
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

            if normalized_base_path:
                blob_path = posix_join(normalized_base_path, member_path)
            else:
                blob_path = member_path

            uploaded_blob_path = upload_blob.save_bytes(extracted_file.read(), blob_path)
            uploaded_blob_paths.append(uploaded_blob_path)

    return uploaded_blob_paths


def main() -> int:
    args = _parse_args()
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
