import requests


def test(num1, num2):
    response = requests.get(
        f"http://localhost:7071/api/monitor/debug/run_databricks?num1={num1}&num2={num2}"
    )
    if response.status_code == 200:
        data = response.text
        print(f"Notebook run successful: data={data}")
    else:
        print(f"Notebook run failed with status code: {response.status_code}")


test(10, 20)
