import json
import logging
import os

import azure.functions as func

from service import build_orchestrator, notify_failure_alert
from shared import to_bool,get_env


bp = func.Blueprint()


if get_env('SKIP_CWE','False').upper() =='FALSE':
    @bp.function_name("cwe_sync_timer")
    @bp.schedule(
        schedule="0 0 0 * * *",
        arg_name="mytimer",
        run_on_startup=False,
        use_monitor=True,
    )
    def cwe_sync_timer(mytimer: func.TimerRequest) -> None:
        del mytimer

        force = to_bool(os.getenv("CWE_FORCE_SYNC"))
        try:
            orchestrator = build_orchestrator()
            result = orchestrator.run(force=force, trigger="timer")
            logging.info("CWE timer sync finished: %s", result)
        except Exception as exc:  # pragma: no cover - runtime boundary
            logging.exception("CWE timer sync failed")
            notify_failure_alert(
                source="azure-function",
                component="cwe_sync_timer",
                trigger="timer",
                error=exc,
                context={"force": force},
            )
            raise


    @bp.function_name("cwe_sync_manual")
    @bp.route(route="cwe-sync", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
    def cwe_sync_manual(req: func.HttpRequest) -> func.HttpResponse:
        force = to_bool(req.params.get("force"))

        if not force:
            try:
                body = req.get_json()
            except ValueError:
                body = {}
            force = to_bool(body.get("force")) if isinstance(body, dict) else False

        try:
            orchestrator = build_orchestrator()
            result = orchestrator.run(force=force, trigger="http")
        except Exception as exc:  # pragma: no cover - runtime boundary
            logging.exception("CWE manual sync failed")
            notify_failure_alert(
                source="azure-function",
                component="cwe_sync_manual",
                trigger="http",
                error=exc,
                context={"force": force},
            )
            return func.HttpResponse(
                json.dumps({"status": "failed", "error": str(exc)}),
                mimetype="application/json",
                status_code=500,
            )

        return func.HttpResponse(
            json.dumps(result),
            mimetype="application/json",
            status_code=200,
        )
