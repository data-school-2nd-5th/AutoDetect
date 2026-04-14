from shared import run_databricks_notebook, check_job_status

def run_notebook_with_code(code_snippet: str):
    """[POST] VSCode가 처음 실행 버튼을 눌렀을 때 호출"""
    params = {"code_snippet": code_snippet}
    ticket_id, run_id = run_databricks_notebook(params=params)
    print(f"Notebook run successful with ID: {ticket_id}")
    print(f"Notebook result: {run_id}")
    return {"ticket_id": ticket_id, "run_id": run_id}


def check_notebook_result(ticket_id: str):
    """[GET] VSCode가 5초마다 호출"""
    return check_job_status(ticket_id)
