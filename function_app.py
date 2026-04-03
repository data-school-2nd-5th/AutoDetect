import azure.functions as func
from blueprint.v1 import bp as v1_bp
from blueprint.cwe import bp as cwe_bp

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


app.register_blueprint(v1_bp)
app.register_blueprint(cwe_bp)
