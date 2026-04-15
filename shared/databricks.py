from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from databricks.sdk import WorkspaceClient
from shared import get_env
import uuid
from enum import Enum


host = get_env("DATABRICKS_WORKSPACE_HOST")
token = get_env("DATABRICKS_WORKSPACE_TOKEN")
job_id = get_env("DATABRICKS_JOB_ID", "507598107786619")
wc = WorkspaceClient(host=host, token=token)

job_dict = dict()


def _json_safe(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    as_dict = getattr(value, "as_dict", None)
    if callable(as_dict):
        return _json_safe(as_dict())
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _json_safe(to_dict())
    return str(value)


def run_databricks_notebook(params=None):
    """Run job now and return ticket_id + run_id immediately."""
    try:
        run_handle = wc.jobs.run_now(job_id=job_id, notebook_params=params)
        run_id = run_handle.run_id

        ticket_id = str(uuid.uuid4())
        job_dict[ticket_id] = run_id

        return ticket_id, run_id
    except Exception as e:
        print(f"Error starting job: {e}")
        raise e


def _task_summary(task):
    task_state = getattr(task, "state", None)
    return {
        "task_key": getattr(task, "task_key", None),
        "run_id": getattr(task, "run_id", None),
        "life_cycle_state": str(getattr(task_state, "life_cycle_state", "UNKNOWN"))
        if task_state
        else "UNKNOWN",
        "result_state": str(getattr(task_state, "result_state", "UNKNOWN"))
        if task_state
        else "UNKNOWN",
        "state_message": getattr(task_state, "state_message", None)
        if task_state
        else None,
    }


def check_job_status(ticket_id: str):
    """Check Databricks run status by ticket id."""
    if ticket_id not in job_dict:
        return {"status": "NOT_FOUND"}

    run_id = job_dict[ticket_id]
    run_info = wc.jobs.get_run(run_id=run_id)

    lifecycle = run_info.state.life_cycle_state
    task_runs = getattr(run_info, "tasks", None) or []

    if lifecycle not in [
        RunLifeCycleState.TERMINATED,
        RunLifeCycleState.SKIPPED,
        RunLifeCycleState.INTERNAL_ERROR,
    ]:
        return {
            "status": "RUNNING",
            "run_id": run_id,
            "tasks": [_task_summary(task) for task in task_runs],
        }

    if run_info.state.result_state == RunResultState.SUCCESS:
        if task_runs:
            task_outputs = []
            for task in task_runs:
                info = _task_summary(task)
                task_run_id = info["run_id"]
                if not task_run_id:
                    info["output"] = None
                    info["error"] = "Missing task run_id"
                    task_outputs.append(info)
                    continue

                try:
                    task_output = wc.jobs.get_run_output(run_id=task_run_id)
                    info["output"] = _json_safe(task_output.notebook_output)
                except Exception as e:
                    info["output"] = None
                    info["error"] = str(e)

                task_outputs.append(info)

            return {"status": "SUCCESS", "run_id": run_id, "tasks": task_outputs}

        output = wc.jobs.get_run_output(run_id=run_id)
        return {
            "status": "SUCCESS",
            "result": _json_safe(output.notebook_output),
            "run_id": run_id,
        }

    if run_info.state.result_state == RunResultState.FAILED:
        return {
            "status": "FAILED",
            "reason": str(run_info.state.state_message),
            "run_id": run_id,
            "tasks": [_task_summary(task) for task in task_runs],
        }

    return {
        "status": "UNKNOWN",
        "run_id": run_id,
        "result_state": str(run_info.state.result_state),
        "tasks": [_task_summary(task) for task in task_runs],
    }


def run_test_notebook(num1: str, num2: str):
    params = {"num1": num1, "num2": num2}
    id, result = run_databricks_notebook(params=params)
    print(f"Notebook run successful with ID: {id}")
    print(f"Notebook result: {result}")
    return id, result
