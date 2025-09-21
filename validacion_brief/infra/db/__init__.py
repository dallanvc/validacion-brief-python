"""
Database abstractions for SQL Server connections.

This subpackage defines a small wrapper around either the ``pymssql`` or
``pyodbc`` libraries.  It exposes a ``connect`` function that returns an
object with ``query`` and ``close`` methods, mirroring the TypeScript
implementation in ``mssql.ts``.
"""

from .mssql import connect, Db  # noqa: F401
from .connection_factory import get_connection  # noqa: F401
