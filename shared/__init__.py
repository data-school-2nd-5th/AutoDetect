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
    to_bool,
    utc_now,
)
from .get_env import get_env
from .upload_blob import ResourceExistsError, UploadBlob

T = TypeVar('T')
def sanitize(d: Any, t: Type[T]) -> T:
    if not isinstance(d, t):
        raise TypeError(f"Expected {t}, but got {type(d)}")
    return d
