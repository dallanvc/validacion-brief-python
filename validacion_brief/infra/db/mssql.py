"""
SQL Server connection utilities.

The original TypeScript project relies on the ``mssql`` library to
connect to SQL Server via connection strings.  In Python we cannot
assume any particular driver is installed, so this module attempts to
connect using either ``pymssql`` or ``pyodbc``.  If neither driver is
available, the ``connect`` function will raise an ``ImportError``.

The ``connect`` function returns an instance of ``Db``, which exposes
``query(sql: str, params: dict | None)`` and ``close()`` methods.  SQL
strings may include named parameters prefixed with ``@`` (e.g.
``@idPromocion``).  When using ``pymssql``, these parameter tokens will
be replaced with Python ``%(id)s`` placeholders.  ``pyodbc`` uses
``?`` placeholders; this module will convert named parameters to
positional arguments when using ``pyodbc``.

Example usage::

    from validacion_brief.infra.db import connect
    db = connect(os.environ['MSSQL_PROMOS_URL'])
    rows = db.query("SELECT * FROM table WHERE id = @id", {'id': 123})
    db.close()

Note that if neither driver is installed this code will raise an
exception.  For a production system you should install either
``pymssql`` or ``pyodbc`` and the appropriate SQL Server ODBC driver.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Tuple, Optional, Iterable


class Db:
    """Lightweight wrapper around a DB connection.

    Instances of this class provide ``query`` and ``close`` methods.  They
    are returned by the ``connect`` function defined below.
    """

    def __init__(self, conn: Any, driver: str) -> None:
        self._conn = conn
        self._driver = driver

    def query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a query and return a dict with a ``rows`` list.

        The ``sql`` may contain named parameters beginning with ``@``.
        ``params`` should be a mapping of parameter names (without the
        ``@``) to values.  When using ``pymssql`` the named parameters
        will be rewritten to ``%(name)s`` placeholders.  When using
        ``pyodbc`` the named parameters will be replaced with ``?`` and
        the values passed positionally in the order they appear.

        Args:
            sql: The SQL statement with named parameters.
            params: A mapping of parameter names to values.

        Returns:
            A dictionary containing a ``rows`` key whose value is a list
            of rows returned by the query.  Each row is a mapping from
            column name to value.
        """
        params = params or {}
        if self._driver == "pymssql":
            return self._execute_pymssql(sql, params)
        elif self._driver == "pyodbc":
            return self._execute_pyodbc(sql, params)
        else:
            raise RuntimeError(f"Unsupported driver: {self._driver}")

    def _execute_pymssql(self, sql: str, params: Dict[str, Any]) -> Dict[str, Any]:
        # Replace @param tokens with %(param)s placeholders
        def replacer(match: re.Match[str]) -> str:
            name = match.group(1)
            return f"%({name})s"

        query = re.sub(r"@([A-Za-z0-9_]+)", replacer, sql)
        with self._conn.cursor(as_dict=True) as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall() or []
        return {"rows": rows}

    def _execute_pyodbc(self, sql: str, params: Dict[str, Any]) -> Dict[str, Any]:
        # Extract parameter names in order of appearance
        names = re.findall(r"@([A-Za-z0-9_]+)", sql)
        # Replace named parameters with positional placeholders
        query = re.sub(r"@([A-Za-z0-9_]+)", "?", sql)
        values: Iterable[Any] = [params.get(name) for name in names]
        cursor = self._conn.cursor()
        cursor.execute(query, list(values))
        columns = [col[0] for col in cursor.description] if cursor.description else []
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()] if columns else []
        return {"rows": rows}

    def close(self) -> None:
        self._conn.close()


def parse_connection_string(input_str: str) -> Dict[str, Any]:
    """Parse a SQL Server connection string into its components.

    The behaviour mirrors the TypeScript implementation.  It supports
    ``mssql://`` URLs or semicolon‐separated key/value pairs.  The
    resulting dictionary contains keys such as ``server``, ``port``,
    ``user``, ``password`` and ``database`` as well as ``encrypt`` and
    ``trustServerCertificate`` flags.

    Args:
        input_str: The connection string to parse.

    Returns:
        A dictionary of connection parameters.
    """
    s = (input_str or "").strip()
    if not s:
        raise ValueError("Empty connection string")
    # Normalise mssql:// prefix
    norm = re.sub(r"^sqlserver://", "mssql://", s, flags=re.IGNORECASE)
    if norm.lower().startswith("mssql://"):
        # Return URL as-is; downstream clients can handle it
        return {"url": norm}
    parts = [p.strip() for p in norm.split(";") if p.strip()]
    kv: Dict[str, str] = {}
    for p in parts:
        if '=' not in p:
            continue
        k, v = p.split('=', 1)
        kv[k.strip().lower()] = v.strip()
    server_raw = kv.get('server') or kv.get('data source') or kv.get('address') or kv.get('addr') or kv.get('network address')
    if not server_raw:
        raise ValueError('No Server= found in connection string')
    server = server_raw
    port: Optional[int] = None
    m = re.match(r"^(.*?),(\d+)$", server_raw)
    if m:
        server = m.group(1)
        port = int(m.group(2))
    user = kv.get('uid') or kv.get('user id') or kv.get('user')
    password = kv.get('pwd') or kv.get('password')
    database = kv.get('database') or kv.get('initial catalog')
    encrypt_str = (kv.get('encrypt') or 'true').lower()
    tsc_str = (kv.get('trustservercertificate') or kv.get('trust server certificate') or 'true').lower()
    encrypt = encrypt_str in ('true', 'yes', '1')
    trust_server_certificate = tsc_str in ('true', 'yes', '1')
    return {
        'server': server,
        'user': user,
        'password': password,
        'port': port,
        'database': database,
        'encrypt': encrypt,
        'trustServerCertificate': trust_server_certificate,
    }


def connect(raw: str) -> Db:
    """Connect to a SQL Server database.

    This function attempts to create a connection using either
    ``pymssql`` or ``pyodbc``.  If both are unavailable a runtime
    error will be raised.  Connection pooling is delegated to the
    underlying library.

    Args:
        raw: The raw connection string from the environment.

    Returns:
        A ``Db`` instance exposing ``query`` and ``close`` methods.
    """
    cfg = parse_connection_string(raw)
    url = cfg.get('url')
    # Try pymssql first
    try:
        import pymssql  # type: ignore[import]
        server = cfg.get('server')
        user = cfg.get('user')
        password = cfg.get('password')
        database = cfg.get('database')
        port = cfg.get('port')
        # Note: ``autocommit`` is enabled by default in pymssql
        conn = pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database,
            port=port or 1433,
        )
        return Db(conn, "pymssql")
    except ImportError:
        pass
    # Fallback to pyodbc
    try:
        import pyodbc  # type: ignore[import]
        # If the URL form is provided, let pyodbc parse it
        if url:
            conn = pyodbc.connect(url, autocommit=True)
        else:
            driver = 'ODBC Driver 17 for SQL Server'
            server = cfg.get('server')
            port = cfg.get('port')
            user = cfg.get('user')
            password = cfg.get('password')
            database = cfg.get('database')
            encrypt = cfg.get('encrypt', True)
            trust = cfg.get('trustServerCertificate', True)
            server_expr = f"{server},{port}" if port else server
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server_expr};"
                f"DATABASE={database};"
                f"UID={user};PWD={password};"
                f"Encrypt={'yes' if encrypt else 'no'};"
                f"TrustServerCertificate={'yes' if trust else 'no'};"
            )
            conn = pyodbc.connect(conn_str, autocommit=True)
        return Db(conn, "pyodbc")
    except ImportError:
        raise ImportError(
            "Neither pymssql nor pyodbc is installed. Install one of them to connect to SQL Server."
        )
