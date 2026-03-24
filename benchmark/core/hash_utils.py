from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping


def spec_hash(payload: Mapping[str, Any], *, length: int = 8) -> str:
    encoded = json.dumps(dict(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()[: max(1, int(length))]


__all__ = ["spec_hash"]
