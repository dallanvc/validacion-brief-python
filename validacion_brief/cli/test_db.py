"""
Test database connectivity for promotions and mesas databases.

This script corresponds to ``src/cli/testDb.ts``.  It attempts to
connect to both configured databases and execute a simple query.
"""

from __future__ import annotations

import logging
import sys

from ..infra.db import get_connection


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    try:
        db1 = get_connection('promos')
        res1 = db1.query('SELECT 1 as ok')
        db1.close()
        logging.info('PROMOS DB OK: %s', res1.get('rows'))
    except Exception as e:
        logging.error('PROMOS DB FAIL:', exc_info=e)
    try:
        db2 = get_connection('mesas')
        res2 = db2.query('SELECT 1 as ok')
        db2.close()
        logging.info('MESAS DB OK: %s', res2.get('rows'))
    except Exception as e:
        logging.error('MESAS DB FAIL:', exc_info=e)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(2)
