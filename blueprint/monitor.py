import azure.functions as func
import logging
from io import BytesIO
import tarfile

bp = func.Blueprint()


def _is_targz_payload(body: bytes) -> bool:
    if not body or len(body) < 2:
        return False

    # Gzip files start with the 0x1f8b magic number.
    if body[:2] != b"\x1f\x8b":
        return False

    try:
        with tarfile.open(fileobj=BytesIO(body), mode="r:gz"):
            return True
    except (tarfile.TarError, OSError, EOFError):
        return False

@bp.function_name("upload")
@bp.route(route="monitor/upload", methods=["POST"])
def upload(req: func.HttpRequest):
    logging.info("Python HTTP trigger function processed an upload request.")

    machine_id = req.headers.get("Machine-Id")
    workspace_id = req.headers.get("Workspace-Id")
    path = req.headers.get("Path")

    logging.info(f"Processing upload: machine_id={machine_id}, workspace_id={workspace_id}")

    if not machine_id or not isinstance(machine_id, str):
        logging.warning("Upload failed: Missing or invalid Machine-Id")
        return func.HttpResponse("Machine id must be provided in headers", status_code=400)

    if not workspace_id or not isinstance(workspace_id, str):
        logging.warning(f"Upload failed: Missing or invalid Workspace-Id (Machine: {machine_id})")
        return func.HttpResponse("Workspace id must be provided in headers", status_code=400)

    if not path or not isinstance(path, str):
        logging.warning(f"Upload failed: Missing or invalid Path (Machine: {machine_id})")
        return func.HttpResponse("Path must be provided in headers", status_code=400)

    try:
        body = req.get_body()
        logging.info(f"Received payload size: {len(body)} bytes")

        if not _is_targz_payload(body):
            logging.error(f"Invalid payload format: Not a .tar.gz (Machine: {machine_id}, Path: {path})")
            return func.HttpResponse("Request body must be a valid .tar.gz binary payload", status_code=400)
            
    except Exception as e:
        logging.exception("Error reading request body") # exception은 트레이스백을 함께 기록합니다.
        return func.HttpResponse("Error processing request body", status_code=500)

    logging.info(f"Upload accepted for processing. Path: {path}")
    
    return func.HttpResponse(None, status_code=202)
