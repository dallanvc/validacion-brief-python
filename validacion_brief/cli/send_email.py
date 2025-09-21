"""
Send a summary email of previous validation results.

This script mirrors ``src/cli/sendEmail.ts``.  It accepts an
optional ``--correo`` argument to specify additional recipients.
"""

from __future__ import annotations

import argparse
import logging
import sys

from ..services.email_report import send_summary_email


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Send BRIEF validation email')
    parser.add_argument('--correo', type=str, help='Email address to send the summary report to')
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    logging.info('[cli/send_email] correo arg', extra={'correo': args.correo})
    if args.correo:
        send_summary_email([args.correo])
    else:
        send_summary_email(None)


if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        logging.error('Error executing cli/send_email', exc_info=err)
        sys.exit(2)
