"""
Environment configuration loader.

This module reads the same environment variables used by the
TypeScript version of the project and exposes them via a simple
``Config`` class. Required variables will raise a ``ValueError`` if
missing. Optional variables default to ``None`` or sensible
defaults where appropriate.

Supported variables:

* ``MSSQL_PROMOS_URL`` – connection string for the promotions database.
* ``MSSQL_MESAS_URL`` – connection string for the mesas database.
* ``EMAIL_ENDPOINT`` – endpoint URL for sending HTML emails.
* ``EMAIL_KEY`` – API key for the email service.
* ``EMAIL_TO`` – default recipient email address.
* ``DB_SCHEMA_PROMOS`` – schema name for the promotions DB (default ``'dbo'``).
* ``DB_SCHEMA_MESAS`` – schema name for the mesas DB (default ``'dbo'``).

The resulting ``config`` instance can be imported from
``validacion_brief.config``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
load_dotenv()

@dataclass
class Config:
    """Holds environment configuration for the application."""

    MSSQL_PROMOS_URL: str
    MSSQL_MESAS_URL: str
    EMAIL_ENDPOINT: Optional[str] = None
    EMAIL_KEY: Optional[str] = None
    EMAIL_TO: Optional[str] = None
    DB_SCHEMA_PROMOS: str = "dbo"
    DB_SCHEMA_MESAS: str = "dbo"


def _load_env() -> Config:
    """Load configuration from environment variables.

    Raises:
        ValueError: If a required environment variable is missing or empty.

    Returns:
        Config: A populated configuration dataclass.
    """

    def _require(name: str) -> str:
        value = os.environ.get(name)
        if not value:
            raise ValueError(f"Environment variable {name} is required")
        return value

    mssql_promos_url = _require("MSSQL_PROMOS_URL")
    mssql_mesas_url = _require("MSSQL_MESAS_URL")
    email_endpoint = os.environ.get("EMAIL_ENDPOINT")
    email_key = os.environ.get("EMAIL_KEY")
    email_to = os.environ.get("EMAIL_TO")
    db_schema_promos = os.environ.get("DB_SCHEMA_PROMOS", "dbo")
    db_schema_mesas = os.environ.get("DB_SCHEMA_MESAS", "dbo")

    return Config(
        MSSQL_PROMOS_URL=mssql_promos_url,
        MSSQL_MESAS_URL=mssql_mesas_url,
        EMAIL_ENDPOINT=email_endpoint,
        EMAIL_KEY=email_key,
        EMAIL_TO=email_to,
        DB_SCHEMA_PROMOS=db_schema_promos,
        DB_SCHEMA_MESAS=db_schema_mesas,
    )


# Create a single configuration instance when this module is imported.
config: Config = _load_env()
