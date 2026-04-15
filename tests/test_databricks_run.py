from time import sleep

import requests

code_snippet = "Object.keys(process.env).forEach(function (key) {cgiData[key] = process.env[key];});"
base_uri = "http://localhost:7071/api"

def check(location):
    request_uri = f"{base_uri}{location}"
    print(f"Checking notebook result at: {request_uri}")
    while True:
        print(f"Sending GET request to check notebook result...")
        response = requests.get(request_uri)
        data = response.json() if response.ok else None
        if response.ok:
            print(f"Notebook result: {data}")
        else:
            print(f"Failed to check notebook result with status code: {response.status_code}")
        if response.ok and data and data.get("status") in {"SUCCESS", "FAILED"}:
            print(f"Notebook run completed with status: {data['status']}")
            break
        print(f"Retrying in 5 seconds...")
        sleep(5)

def test():
    request_uri = f"{base_uri}/monitor/scripts"
    print(f"Sending POST request to run notebook at: {request_uri}")
    response = requests.post(
        request_uri,
        headers={
            "Machine-Id": "test-machine-id",
            "Workspace-Id": "test-workspace-id",
            "File-Name": "test-notebook.js",
        },
        data=code_snippet,
    )
    if response.status_code == 202:
        location = response.headers.get("Location")
        print(f"Notebook run successful: location={location}")
        check(location)
    else:
        print(f"Notebook run failed with status code: {response.status_code}")


test()
