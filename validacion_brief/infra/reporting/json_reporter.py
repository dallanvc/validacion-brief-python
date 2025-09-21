"""
JSON reporting utilities.

Functions in this module mirror the ``jsonReporter.ts`` file from the
TypeScript project.  They provide simple wrappers for ensuring a
directory exists and writing JSON files atomically.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any


def ensure_dir(directory: str) -> None:
    """Ensure a directory exists, creating it recursively if necessary."""
    logging.info("[jsonReporter] ensure_dir", extra={"dir": directory})
    Path(directory).mkdir(parents=True, exist_ok=True)


def write_json(file_path: str, data: Any) -> None:
    """Write an object to a JSON file, ensuring the directory exists."""
    ensure_dir(os.path.dirname(file_path))
    logging.info("[jsonReporter] write_json", extra={"file": file_path})
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
