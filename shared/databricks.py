from databricks.sdk.service.jobs import Wait, Run, RunLifeCycleState, RunResultState
from databricks.sdk import WorkspaceClient
from shared import get_env
import uuid


host = get_env("DATABRICKS_WORKSPACE_HOST")
token = get_env("DATABRICKS_WORKSPACE_TOKEN")
job_id = get_env("DATABRICKS_JOB_ID", "507598107786619")
wc = WorkspaceClient(host=host, token=token)

job_dict = dict()


def run_databricks_notebook(job_id: str, params=None):
    """실행을 트리거하고 즉시 run_id를 반환합니다."""
    try:
        # run_now는 대기하지 않고 즉시 실행 정보를 담은 객체를 반환함
        run_handle = wc.jobs.run_now(job_id=job_id, notebook_params=params)
        run_id = run_handle.run_id

        # 클라이언트(VSCode)에 돌려줄 고유 티켓 생성
        ticket_id = str(uuid.uuid4())
        job_dict[ticket_id] = run_id

        return ticket_id, run_id
    except Exception as e:
        print(f"Error starting job: {e}")
        raise e


def check_job_status(ticket_id: str):
    """티켓 ID를 기반으로 데이터브릭스의 현재 상태를 확인합니다."""
    if ticket_id not in job_dict:
        return {"status": "NOT_FOUND"}

    run_id = job_dict[ticket_id]
    run_info = wc.jobs.get_run(run_id=run_id)

    lifecycle = run_info.state.life_cycle_state

    # 1. 아직 실행 중인 경우
    if lifecycle not in [
        RunLifeCycleState.TERMINATED,
        RunLifeCycleState.SKIPPED,
        RunLifeCycleState.INTERNAL_ERROR,
    ]:
        return {"status": "RUNNING", "run_id": run_id}

    # 2. 종료된 경우 결과 확인
    if run_info.state.result_state == RunResultState.SUCCESS:
        output = wc.jobs.get_run_output(run_id=run_id)
        return {"status": "SUCCESS", "result": output.notebook_output, "run_id": run_id}
    else:
        return {
            "status": "FAILED",
            "reason": str(run_info.state.state_message),
            "run_id": run_id,
        }


def run_test_notebook(num1: str, num2: str):
    job_id = "588467920967662"
    params = {"num1": num1, "num2": num2}
    id, result = run_databricks_notebook(job_id=job_id, params=params)
    print(f"Notebook run successful with ID: {id}")
    print(f"Notebook result: {result}")
    return id, result

# --- VSCode에서 호출할 진입점 함수들 ---

def run_code_notebook(code_snippet: str):
    """[POST] VSCode가 처음 실행 버튼을 눌렀을 때 호출"""
    job_id = "507598107786619"
    params = {"code_snippet": code_snippet}
    
    ticket_id, run_id = run_databricks_notebook(job_id=job_id, params=params)
    print(f"Notebook run successful with ID: {ticket_id}")
    print(f"Notebook result: {run_id}")
    return {"ticket_id": ticket_id, "run_id": run_id}

def poll_result(ticket_id: str):
    """[GET] VSCode가 5초마다 호출"""
    return check_job_status(ticket_id)
