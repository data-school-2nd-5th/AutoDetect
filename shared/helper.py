import argparse
import io
import json
import zipfile
import os
import tarfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, Iterable, List


def to_bool(raw_value: str | None) -> bool:
    return (raw_value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def is_targz_payload(body: bytes) -> bool:
    if not body or len(body) < 2:
        return False

    if body[:2] != b"\x1f\x8b":
        return False

    try:
        with tarfile.open(fileobj=BytesIO(body), mode="r:gz"):
            return True
    except (tarfile.TarError, OSError, EOFError):
        return False


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(text: str | None) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def extract_xml_payload(zip_payload: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(zip_payload), mode="r") as archive:
        xml_names = [
            name for name in archive.namelist() if name.lower().endswith(".xml")
        ]
        if not xml_names:
            raise ValueError("Zip payload does not contain an XML file")

        if "cwec_latest.xml" in xml_names:
            return archive.read("cwec_latest.xml")

        return archive.read(xml_names[0])


def node_text(node: ET.Element | None) -> str:
    if node is None:
        return ""
    return clean_text(" ".join(piece for piece in node.itertext()))


def parse_alternative_terms(weakness: ET.Element) -> List[Dict[str, str]]:
    terms: List[Dict[str, str]] = []
    for item in weakness.findall("./Alternate_Terms/Alternate_Term"):
        terms.append(
            {
                "term": node_text(item.find("Term")),
                "description": node_text(item.find("Description")),
            }
        )
    return terms


def parse_potential_mitigations(weakness: ET.Element) -> List[Dict[str, str]]:
    mitigations: List[Dict[str, str]] = []
    for mitigation in weakness.findall("./Potential_Mitigations/Mitigation"):
        mitigations.append(
            {
                "mitigation_id": mitigation.get("Mitigation_ID", ""),
                "phase": node_text(mitigation.find("Phase")),
                "strategy": node_text(mitigation.find("Strategy")),
                "description": node_text(mitigation.find("Description")),
            }
        )
    return mitigations


def parse_demonstrative_examples(weakness: ET.Element) -> List[Dict[str, str]]:
    examples: List[Dict[str, str]] = []
    for example in weakness.findall("./Demonstrative_Examples/Demonstrative_Example"):
        example_codes = [
            node_text(node)
            for node in example.findall("Example_Code")
            if node_text(node)
        ]
        body_texts = [
            node_text(node) for node in example.findall("Body_Text") if node_text(node)
        ]
        examples.append(
            {
                "example_id": example.get("Demonstrative_Example_ID", ""),
                "intro_text": node_text(example.find("Intro_Text")),
                "body_text": "\n".join(body_texts),
                "example_code": "\n".join(example_codes),
            }
        )
    return examples


def parse_observed_examples(weakness: ET.Element) -> List[Dict[str, str]]:
    observed: List[Dict[str, str]] = []
    for sample in weakness.findall("./Observed_Examples/Observed_Example"):
        observed.append(
            {
                "reference": node_text(sample.find("Reference")),
                "description": node_text(sample.find("Description")),
                "link": node_text(sample.find("Link")),
            }
        )
    return observed


def parse_mapping_notes(weakness: ET.Element) -> Dict[str, Any]:
    notes = weakness.find("Mapping_Notes")
    if notes is None:
        return {}

    reasons = [
        {
            "type": reason.get("Type", ""),
            "value": node_text(reason),
        }
        for reason in notes.findall("./Reasons/Reason")
    ]

    return {
        "usage": node_text(notes.find("Usage")),
        "rationale": node_text(notes.find("Rationale")),
        "comments": node_text(notes.find("Comments")),
        "reasons": reasons,
    }


def latest_modification_date(modification_dates: Iterable[str]) -> str:
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


def json_string(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Save one or more local files to Azure Blob Storage.",
    )
    parser.add_argument(
        "--connection-string",
        help="Azure Storage connection string. Defaults to AZURE_STORAGE_CONNECTION_STRING.",
    )
    parser.add_argument(
        "--container",
        required=True,
        help="Target blob container name.",
    )
    parser.add_argument(
        "--create-container",
        action="store_true",
        help="Create the container if it does not already exist.",
    )
    parser.add_argument(
        "uploads",
        nargs="+",
        metavar="LOCAL_FILE=BLOB_PATH",
        help="Upload mapping. Example: data/report.csv=archive/2026-04-06/report.csv",
    )
    args = parser.parse_args()

    if not args.connection_string:
        parser.error(
            "A connection string is required via --connection-string or AZURE_STORAGE_CONNECTION_STRING."
        )

    return args
