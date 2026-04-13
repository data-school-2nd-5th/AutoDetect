from shared.upload_blob import blob_controller

def analyze(machine_id:str, workspace_id:str, text: str) -> list[dict]:
    ## Add your analysis code here
    blob_controller.save_text(text, f"/{machine_id}/{workspace_id}/work.js")
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
