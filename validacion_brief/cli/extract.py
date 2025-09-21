"""
Extract all promotion validations.

This script corresponds to ``src/cli/extract.ts``.  It simply invokes
``validate_all`` without sending any emails.
"""

from __future__ import annotations

import logging
import sys

from ..services.brief_exec import validate_all


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    validate_all(None)


if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        logging.error('Error executing cli/extract', exc_info=err)
        sys.exit(2)
