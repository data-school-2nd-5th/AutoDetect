from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Iterable, List
import xml.etree.ElementTree as ET


def _strip_namespaces(node: ET.Element) -> None:
    for element in node.iter():
        if isinstance(element.tag, str) and element.tag.startswith("{"):
            element.tag = element.tag.split("}", 1)[1]
        if element.attrib:
            cleaned = {}
            for key, value in element.attrib.items():
                if key.startswith("{"):
                    key = key.split("}", 1)[1]
                cleaned[key] = value
            element.attrib.clear()
            element.attrib.update(cleaned)


def parse_cwe_weaknesses(xml_content: str | bytes) -> List[Dict[str, str]]:
    root = ET.fromstring(xml_content)
    _strip_namespaces(root)
    rows: List[Dict[str, str]] = []

    for weakness in root.findall(".//Weakness"):
        modification_dates = [
            _clean_text(modified.text)
            for modified in weakness.findall(
                "./Content_History/Modification/Modification_Date"
            )
            if _clean_text(modified.text)
        ]
        latest_modification = _latest_modification_date(modification_dates)

        row = {
            "weakness_id": weakness.get("ID", ""),
            "title": weakness.get("Name", ""),
            "description": _node_text(weakness.find("Description")),
            "extended_description": _node_text(weakness.find("Extended_Description")),
            "alternative_terms": _json_string(_parse_alternative_terms(weakness)),
            "potential_mitigations": _json_string(_parse_potential_mitigations(weakness)),
            "likelihood_of_exploit": _node_text(weakness.find("Likelihood_Of_Exploit")),
            "demonstrative_examples": _json_string(
                _parse_demonstrative_examples(weakness)
            ),
            "selected_observed_examples": _json_string(
                _parse_observed_examples(weakness)
            ),
            "vulnerability_mapping_notes": _json_string(_parse_mapping_notes(weakness)),
            "content_history_last_modified": latest_modification,
        }
        rows.append(row)

    if not rows:
        raise ValueError("No Weakness entries parsed from source XML")

    return rows


def _parse_alternative_terms(weakness: ET.Element) -> List[Dict[str, str]]:
    terms: List[Dict[str, str]] = []
    for item in weakness.findall("./Alternate_Terms/Alternate_Term"):
        terms.append(
            {
                "term": _node_text(item.find("Term")),
                "description": _node_text(item.find("Description")),
            }
        )
    return terms


def _parse_potential_mitigations(weakness: ET.Element) -> List[Dict[str, str]]:
    mitigations: List[Dict[str, str]] = []
    for mitigation in weakness.findall("./Potential_Mitigations/Mitigation"):
        mitigations.append(
            {
                "mitigation_id": mitigation.get("Mitigation_ID", ""),
                "phase": _node_text(mitigation.find("Phase")),
                "strategy": _node_text(mitigation.find("Strategy")),
                "description": _node_text(mitigation.find("Description")),
            }
        )
    return mitigations


def _parse_demonstrative_examples(weakness: ET.Element) -> List[Dict[str, str]]:
    examples: List[Dict[str, str]] = []
    for example in weakness.findall("./Demonstrative_Examples/Demonstrative_Example"):
        example_codes = [
            _node_text(node)
            for node in example.findall("Example_Code")
            if _node_text(node)
        ]
        body_texts = [
            _node_text(node) for node in example.findall("Body_Text") if _node_text(node)
        ]
        examples.append(
            {
                "example_id": example.get("Demonstrative_Example_ID", ""),
                "intro_text": _node_text(example.find("Intro_Text")),
                "body_text": "\n".join(body_texts),
                "example_code": "\n".join(example_codes),
            }
        )
    return examples


def _parse_observed_examples(weakness: ET.Element) -> List[Dict[str, str]]:
    observed: List[Dict[str, str]] = []
    for sample in weakness.findall("./Observed_Examples/Observed_Example"):
        observed.append(
            {
                "reference": _node_text(sample.find("Reference")),
                "description": _node_text(sample.find("Description")),
                "link": _node_text(sample.find("Link")),
            }
        )
    return observed


def _parse_mapping_notes(weakness: ET.Element) -> Dict[str, Any]:
    notes = weakness.find("Mapping_Notes")
    if notes is None:
        return {}

    reasons = [
        {
            "type": reason.get("Type", ""),
            "value": _node_text(reason),
        }
        for reason in notes.findall("./Reasons/Reason")
    ]

    return {
        "usage": _node_text(notes.find("Usage")),
        "rationale": _node_text(notes.find("Rationale")),
        "comments": _node_text(notes.find("Comments")),
        "reasons": reasons,
    }


def _latest_modification_date(modification_dates: Iterable[str]) -> str:
    parsed_dates: List[datetime] = []
    fallback_dates: List[str] = []

    for date_value in modification_dates:
        if not date_value:
            continue
        fallback_dates.append(date_value)
        try:
            parsed_dates.append(datetime.strptime(date_value, "%Y-%m-%d"))
        except ValueError:
            continue

    if parsed_dates:
        return max(parsed_dates).strftime("%Y-%m-%d")

    if fallback_dates:
        return max(fallback_dates)

    return ""


def _node_text(node: ET.Element | None) -> str:
    if node is None:
        return ""
    return _clean_text(" ".join(piece for piece in node.itertext()))


def _clean_text(text: str | None) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def _json_string(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False)
