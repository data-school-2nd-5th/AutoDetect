def get_greeting(name: str | None):
    if name:
        return f"Hello, {name}. This HTTP triggered function executed successfully."
    else:
        return "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
