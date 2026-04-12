import azure.functions as func
import json
import logging

from service import analyze, upload_by_targz_body, ls
from shared import is_targz_payload, sanitize, get_env

bp = func.Blueprint()

if get_env("SKIP_MONITOR", "False").upper() == "FALSE":

    @bp.function_name("upload")
    @bp.route(route="monitor/upload", methods=["POST"])
    def upload(req: func.HttpRequest):
        logging.info("Python HTTP trigger function processed an upload request.")

        # 1. 헤더에서 값을 먼저 가져오기
        raw_machine_id = req.headers.get("Machine-Id")
        raw_workspace_id = req.headers.get("Workspace-Id")

        try:
            # 2. 값의 존재 여부 확인 및 sanitize
            if raw_machine_id is None or raw_workspace_id is None:
                raise TypeError("Missing required headers")

            machine_id = sanitize(raw_machine_id, str)
            workspace_id = sanitize(raw_workspace_id, str)

        except TypeError as e:
            # 안전한 로깅: 할당되지 않았을 수 있는 변수 대신 raw 값을 사용
            logging.error(
                f"Headers validation failed: {str(e)} | Raw IDs: machine={raw_machine_id}, workspace={raw_workspace_id}"
            )
            return func.HttpResponse("Header Error", status_code=400)

        logging.info(
            f"Processing upload: machine_id={machine_id}, workspace_id={workspace_id}"
        )

        try:
            body = req.get_body()

            if not is_targz_payload(body):
                logging.error(
                    f"Invalid payload format: Not a .tar.gz (Machine: {machine_id})"
                )
                return func.HttpResponse(
                    "Request body must be a valid .tar.gz binary payload",
                    status_code=400,
                )
            logging.info(f"Received payload size: {len(body)} bytes")
            uploaded_list = upload_by_targz_body(machine_id, workspace_id, body)
            return func.HttpResponse(
                json.dumps(uploaded_list),
                status_code=201,
                mimetype="application/json",
            )

        except Exception:
            logging.exception("Error reading request body")
            return func.HttpResponse("Error processing request body", status_code=500)

    @bp.function_name("scripts")
    @bp.route(route="monitor/scripts", methods=["post"])
    def scripts(req: func.HttpRequest):
        logging.info("Python HTTP trigger function processed a scripts request.")

        # 1. 헤더에서 값을 먼저 가져오기
        raw_machine_id = req.headers.get("Machine-Id")
        raw_workspace_id = req.headers.get("Workspace-Id")
        file_name = req.headers.get("File-Name")
        print_file = req.headers.get("Print-File", "false").upper() == "TRUE"

        try:
            # 2. 값의 존재 여부 확인 및 sanitize
            if raw_machine_id is None or raw_workspace_id is None or file_name is None:
                raise TypeError("Missing required headers")

            machine_id = sanitize(raw_machine_id, str)
            workspace_id = sanitize(raw_workspace_id, str)
            file_name = sanitize(file_name, str)

        except TypeError as e:
            # 안전한 로깅: 할당되지 않았을 수 있는 변수 대신 raw 값을 사용
            logging.error(
                f"Headers validation failed: {str(e)} | Raw IDs: machine={raw_machine_id}, workspace={raw_workspace_id}, file_name={file_name}"
            )
            return func.HttpResponse("Header Error", status_code=400)
        logging.info(
            f"Processing scripts upload: machine_id={machine_id}, workspace_id={workspace_id}, file_name={file_name}"
        )
        try:
            body = req.get_body()
            # body는 text/plain임.
            text = body.decode("utf-8")
            logging.info(f"Received script content size: {len(text)} characters")
            if print_file:
                logging.info(f"Script content:\n{text}")
            results = analyze(text)
            return func.HttpResponse(
                json.dumps(results),
                status_code=200,
                mimetype="application/json",
            )

        except Exception:
            logging.exception("Error reading request body")
            return func.HttpResponse("Error processing request body", status_code=500)

    @bp.function_name("debug_ls")
    @bp.route(
        route="monitor/debug/ls", methods=["get"], auth_level=func.AuthLevel.ADMIN
    )
    def debug_ls(req: func.HttpRequest):
        try:
            path = req.params.get("path")
            if not path:
                return func.HttpResponse("No path provided", status_code=400)
            res = ls(path)
            return func.HttpResponse(json.dumps(res), mimetype="application/json")
        except:
            return func.HttpResponse("Internal Server Error", status_code=500)
