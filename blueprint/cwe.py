import json
import logging
import os

import azure.functions as func

from service.cwe_runtime import build_orchestrator


bp = func.Blueprint()


def _to_bool(raw_value: str | None) -> bool:
    return (raw_value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


@bp.function_name("cwe_sync_timer")
@bp.schedule(
    schedule="0 0 0 * * *",
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)
def cwe_sync_timer(mytimer: func.TimerRequest) -> None:
    del mytimer

    force = _to_bool(os.getenv("CWE_FORCE_SYNC"))
    orchestrator = build_orchestrator()
    result = orchestrator.run(force=force, trigger="timer")
    logging.info("CWE timer sync finished: %s", result)


@bp.function_name("cwe_sync_manual")
@bp.route(route="cwe-sync", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def cwe_sync_manual(req: func.HttpRequest) -> func.HttpResponse:
    force = _to_bool(req.params.get("force"))

    if not force:
        try:
            body = req.get_json()
        except ValueError:
            body = {}
        force = _to_bool(body.get("force")) if isinstance(body, dict) else False

    try:
        orchestrator = build_orchestrator()
        result = orchestrator.run(force=force, trigger="http")
    except Exception as exc:  # pragma: no cover - runtime boundary
        logging.exception("CWE manual sync failed")
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
