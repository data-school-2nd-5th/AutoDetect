import azure.functions as func
from blueprint.cwe import bp as cwe_bp
from blueprint.monitor import bp as monitor_bp

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

app.register_blueprint(cwe_bp)
app.register_blueprint(monitor_bp)
