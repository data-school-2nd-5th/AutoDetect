from __future__ import annotations

from typing import Dict, List
import xml.etree.ElementTree as ET

from shared import (
    clean_text,
    json_string,
    latest_modification_date,
    node_text,
    parse_alternative_terms,
    parse_demonstrative_examples,
    parse_mapping_notes,
    parse_observed_examples,
    parse_potential_mitigations,
)


def parse_cwe_weaknesses(xml_content: str | bytes) -> List[Dict[str, str]]:
    root = ET.fromstring(xml_content)
    rows: List[Dict[str, str]] = []

    for weakness in root.findall(".//Weakness"):
        modification_dates = [
            clean_text(modified.text)
            for modified in weakness.findall(
                "./Content_History/Modification/Modification_Date"
            )
            if clean_text(modified.text)
        ]
        latest_modification = latest_modification_date(modification_dates)
        if not latest_modification:
            weakness_id = weakness.get("ID", "unknown")
            raise ValueError(
                f"Missing Content_History/Modification/Modification_Date for weakness {weakness_id}"
            )

        row = {
            "weakness_id": weakness.get("ID", ""),
            "title": weakness.get("Name", ""),
            "description": node_text(weakness.find("Description")),
            "extended_description": node_text(weakness.find("Extended_Description")),
            "alternative_terms": json_string(parse_alternative_terms(weakness)),
            "potential_mitigations": json_string(parse_potential_mitigations(weakness)),
            "likelihood_of_exploit": node_text(weakness.find("Likelihood_Of_Exploit")),
            "demonstrative_examples": json_string(
                parse_demonstrative_examples(weakness)
            ),
            "selected_observed_examples": json_string(
                parse_observed_examples(weakness)
            ),
            "vulnerability_mapping_notes": json_string(parse_mapping_notes(weakness)),
            "content_history_last_modified": latest_modification,
        }
        rows.append(row)

    return rows
