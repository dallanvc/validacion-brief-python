"""
Run the full BRIEF validation workflow from the command line.

This script mirrors ``src/cli/brief.ts``.  It accepts optional
``--promos`` and ``--correo`` arguments.  ``--promos`` should be a
comma‑separated list of promotion IDs or ``all`` to validate every
promotion in the configuration file.  ``--correo`` specifies an
additional recipient for the summary email.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import List, Optional

from ..services.brief_exec import validate_all
from ..services.email_report import send_summary_email


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run BRIEF validation')
    parser.add_argument('--promos', type=str, default='all', help='Comma separated list of promotion IDs to validate or "all"')
    parser.add_argument('--correo', type=str, help='Email address to send the summary report to')
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    promos = None
    if args.promos:
        promos = args.promos.split(',')
    logging.basicConfig(level=logging.INFO)
    logging.info('[cli/brief] Parsed arguments', extra={'promos': promos, 'correo': args.correo})
    # Run validations
    validate_all(promos)
    try:
        if args.correo:
            send_summary_email([args.correo])
        else:
            send_summary_email(None)
    except Exception as e:
        logging.error('(correo omitido)', exc_info=e)


if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        logging.error('Error executing cli/brief', exc_info=err)
        sys.exit(2)
