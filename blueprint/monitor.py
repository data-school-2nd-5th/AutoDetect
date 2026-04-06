import azure.functions as func
import json
import logging

from service import upload_by_targz_body
from shared import is_targz_payload, sanitize

bp = func.Blueprint()


@bp.function_name("upload")
@bp.route(route="monitor/upload", methods=["POST"])
def upload(req: func.HttpRequest):
    logging.info("Python HTTP trigger function processed an upload request.")

    try:
        machine_id = sanitize(req.headers.get("Machine-Id"), str)
        workspace_id = sanitize(req.headers.get("Workspace-Id"))
        path = sanitize(req.headers.get("Path"))
    except TypeError:
        logging.error("Headers failed")
        return func.HttpResponse("Header Error", status_code=400)

    logging.info(
        f"Processing upload: machine_id={machine_id}, workspace_id={workspace_id}"
    )

    try:
        body = req.get_body()

        if not is_targz_payload(body):
            logging.error(
                f"Invalid payload format: Not a .tar.gz (Machine: {machine_id}, Path: {path})"
            )
            return func.HttpResponse(
                "Request body must be a valid .tar.gz binary payload", status_code=400
            )
        logging.info(f"Received payload size: {len(body)} bytes")
        uploaded_list = upload_by_targz_body(machine_id, workspace_id, path, body)
        return func.HttpResponse(
            json.dumps(uploaded_list),
            status_code=201,
            mimetype="application/json",
        )

    except Exception:
        logging.exception("Error reading request body")
        return func.HttpResponse("Error processing request body", status_code=500)
