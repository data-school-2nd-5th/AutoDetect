from base64 import b64encode, b64decode
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

text = "Hello! 안녕 World! こんにちは"
encoded = b64encode(text.encode("utf-8")).decode("utf-8")
print(f"{encoded=}")

decoded = b64decode(encoded).decode("utf-8")
print(f"{decoded=}")

code_snippet = dbutils.widgets.get("code_snippet")

code_snippet = dbutils.widgets.get("code_snippet")
code_snippet = b64decode(code_snippet).decode("utf-8")
# print(f"{code_snippet=}")