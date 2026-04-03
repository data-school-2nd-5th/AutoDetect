import azure.functions as func
import logging

import service.greeting as greeting_service


bp = func.Blueprint()


@bp.function_name("greeting")
@bp.route(route="v1/greeting", methods=["GET", "POST"])
def greeting(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")
    name = req.params.get("name")

    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = {}
        name = req_body.get("name") if isinstance(req_body, dict) else None

    greeting_text = greeting_service.get_greeting(name)
    return func.HttpResponse(greeting_text, status_code=200)
