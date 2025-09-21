"""
Database connection factory.

This module mirrors the TypeScript ``ConnectionFactory.ts``.  It uses
the configured connection strings to lazily create new database
connections.  See ``validacion_brief.config.env.Config`` for
configuration variables.
"""

from __future__ import annotations

from typing import Dict, Callable

from ...config import config
from .mssql import connect, Db


# Registry mapping aliases to callables that return a ``Db`` instance.
_registry: Dict[str, Callable[[], Db]] = {
    'promos': lambda: connect(config.MSSQL_PROMOS_URL),
    'mesas': lambda: connect(config.MSSQL_MESAS_URL),
}


def get_connection(alias: str) -> Db:
    """Obtain a new database connection by alias.

    Args:
        alias: Either ``'promos'`` or ``'mesas'``.

    Returns:
        A new ``Db`` instance connected to the appropriate database.

    Raises:
        KeyError: If the alias is not registered.
    """
    try:
        factory = _registry[alias]
    except KeyError:
        raise KeyError(f"No connection defined for alias: {alias}") from None
    return factory()
