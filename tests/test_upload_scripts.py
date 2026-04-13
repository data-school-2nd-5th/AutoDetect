import requests
import json


def test_scripts_endpoint():
    url = "http://localhost:7071/api/monitor/scripts"
    headers = {
        "Machine-Id": "test-machine",
        "Workspace-Id": "test-workspace",
        "Content-Type": "text/plain",
        "File-Name": "test_script.js",
        "Print-File": "true",
    }
    text = """
import sql from "sql-template-strings";

function getUserData(id, search='name') {
    return sql.run(`SELECT ${search} FROM user WHERE id = ${id};`);
}
"""
    response = requests.post(url, headers=headers, data=text)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
    assert response.status_code == 200
    data = response.json()
    print(f"Parsed JSON: {data}")
    assert isinstance(data, list)


test_scripts_endpoint()
