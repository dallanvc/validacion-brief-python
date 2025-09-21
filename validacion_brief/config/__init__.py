"""
Expose the loaded configuration as ``config``.

Importing from this module will load environment variables and
populate a singleton ``Config`` instance. Example:

    from validacion_brief.config import config
    print(config.MSSQL_PROMOS_URL)
"""

from .env import config, Config  # noqa: F401
