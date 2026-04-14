from __future__ import annotations

from typing import Any, Type, TypeVar
from .helper import (
    clean_text,
    extract_xml_payload,
    get_required_env,
    is_targz_payload,
    json_string,
    latest_modification_date,
    node_text,
    parse_alternative_terms,
    parse_args,
    parse_demonstrative_examples,
    parse_mapping_notes,
    parse_observed_examples,
    parse_potential_mitigations,
    to_bool,
    utc_now,
)
from .get_env import get_env
from .azure_storage import ResourceExistsError, azure_storage_manager

__all__ = [
    "UploadBlob",
    "ResourceExistsError",
    "azure_storage_manager",
    "clean_text",
    "extract_xml_payload",
    "get_env",
    "get_required_env",
    "is_targz_payload",
    "json_string",
    "latest_modification_date",
    "node_text",
    "parse_alternative_terms",
    "parse_args",
    "parse_demonstrative_examples",
    "parse_mapping_notes",
    "parse_observed_examples",
    "parse_potential_mitigations",
    "sanitize",
    "to_bool",
    "utc_now",
]
T = TypeVar("T")


def sanitize(d: Any, t: Type[T]) -> T:
    if not isinstance(d, t):
        raise TypeError(f"Expected {t}, but got {type(d)}")
    return d
