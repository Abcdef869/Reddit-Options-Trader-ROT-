from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, is_dataclass
from typing import Any, Dict


def _to_jsonable(obj: Any) -> Any:
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    return obj


class JsonlLogger:
    def __init__(self, root: str = "storage") -> None:
        self.root = root
        os.makedirs(root, exist_ok=True)

    def write(self, stream: str, record: Dict[str, Any]) -> None:
        path = os.path.join(self.root, f"{stream}.jsonl")
        record = dict(record)
        record.setdefault("ts", int(time.time()))
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(_to_jsonable(record), ensure_ascii=False) + "\n")
