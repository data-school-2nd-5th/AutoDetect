from .save_files import upload_by_text

def analyze(machine_id:str, workspace_id:str, text: str) -> list[dict]:
    ## Add your analysis code here
    path = upload_by_text(machine_id, workspace_id, text)
    ## Example response format
    return [
        {
            "codes": "sql.run(`SELECT ${search} FROM user WHERE id = ${id};`)",
            "CVE": {
                "name": "CVE-2024-12345",
                "tags": ["sql-injection", "critical"],
            },
            "suggest": 'sql.run(`SELECT "${search}" FROM user WHERE id = "${id}";`);',
        }
    ]


text = """
import sql from "sql-template-strings";

function getUserData(id, search='name') {
    return sql.run(`SELECT ${search} FROM user WHERE id = ${id};`);
}
"""

## Example of usage
#  text = """
# import sql from "sql-template-strings";
# function getUserData(id, search='name') {
#     return sql.run(`SELECT ${search} FROM user WHERE id = ${id};`);
# }
# """
# results = analyze(text)
# print(results)
# Expected output:
# [
#     {
#         "codes": "sql.run(`SELECT ${search} FROM user WHERE id = ${id};`)",
#         "CVE": {
#             "name": "CVE-2024-12345",
#             "tags": ["sql-injection", "critical"],
#         },
#         "suggest": 'sql.run(`SELECT "${search}" FROM user WHERE id = "${id}";`);',
#     }
# ]
