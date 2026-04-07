import azure.functions as func
import json
import logging

from service import upload_by_targz_body, ls
from shared import is_targz_payload, sanitize, get_env

bp = func.Blueprint()

if get_env("SKIP_MONITOR", "False").upper() == "FALSE":

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
                    "Request body must be a valid .tar.gz binary payload",
                    status_code=400,
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

    @bp.function_name("debug_ls")
    @bp.route(route="monitor/debug/ls", methods=["get"])
    def debug_ls(req: func.HttpRequest):
        try:
            path = req.params.get("path")
            if not path:
                return func.HttpResponse("No path provided", status_code=400)
            res = ls(path)
            return func.HttpResponse(json.dumps(res), mimetype="application/json")
        except:
            return func.HttpResponse("Internal Server Error", status_code=500)
