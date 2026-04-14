from databricks.sdk import WorkspaceClient
from shared import get_env


host: str = get_env("DATABRICKS_WORKSPACE_HOST")
token: str = get_env("DATABRICKS_WORKSPACE_TOKEN")
wc = WorkspaceClient(host=host, token=token)


def run_databricks_notebook(job_id: str, params=None):
    try:
        result = wc.jobs.run_now(job_id=job_id, notebook_params=params)
        return id, result
    except Exception as e:
        print(f"Error running Databricks notebook: {e}")
        return None


def run_test_notebook(num1: str, num2: str):
    job_id = "588467920967662"
    params = {"num1": num1, "num2": num2}
    results = run_databricks_notebook(job_id=job_id, params=params)
    if not results:
        print("Failed to run notebook")
        raise Exception("Failed to run notebook")
    result = results["result"]
    id = results["id"]
    print(f"Notebook run completed with ID: {id}")
    print(f"Notebook result: {result}")
    return id, result


def run_code_notebook(code_snippet: str):
    job_id = "507598107786619"
    params = {"code_snippet": code_snippet}
    results = run_databricks_notebook(job_id=job_id, params=params)
    if not results:
        print("Failed to run notebook")
        raise Exception("Failed to run notebook")
    result = results["result"]
    id = results["id"]
    print(f"Notebook run completed with ID: {id}")
    print(f"Notebook result: {result}")
    return id, result
