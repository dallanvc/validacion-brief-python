"""
Compatibility layer for mesas database queries (206 series).

Provides a single function to query upcoming tournament date ranges for
mesas.  Mirrors the behaviour of ``DatabaseConnection206.ts``.
"""

from __future__ import annotations

from typing import List

from ..config import config
from ..config.queries import queries
from ..infra.db import get_connection


def query_fechas_mesas() -> List[dict]:
    """Retrieve the start and end dates of upcoming mesas tournaments."""
    db = get_connection('mesas')
    sql = queries['fechasMesas'](config.DB_SCHEMA_MESAS)
    try:
        result = db.query(sql)
        return result.get('rows', [])
    finally:
        db.close()
