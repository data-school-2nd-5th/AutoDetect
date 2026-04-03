import azure.functions as func
import logging
import service.greeting as greeting_service


bp = func.Blueprint()

@bp.function_name("greeting")
@bp.route(route='greeting',methods=['GET, POST'])
def f1(req: func.HttpRequest):
    logging.info("Python HTTP trigger function processed a request.")
    name = req.params.get("name")
    greeting = greeting_service.get_greeting(name)
    return func.HttpResponse(greeting, status_code=200)
