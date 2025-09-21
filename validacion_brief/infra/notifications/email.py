"""
Email sending utility.

Replicates the TypeScript module ``src/infra/notifications/email.ts``.
This function posts an HTML email to a configured endpoint.  It
supports adding extra recipients on the fly, concatenated with the
default ``EMAIL_TO`` from the environment.  The API key is sent in
the ``ApiKeyApp`` header.
"""

from __future__ import annotations

import logging
from typing import List, Optional

import requests

from ...config import config


def send_html_email(subject: str, html: str, extra_to: Optional[List[str]] = None) -> dict:
    """Send an HTML email using the configured notification endpoint.

    Args:
        subject: The email subject line.
        html: The HTML body of the email.
        extra_to: Optional list of additional recipient email addresses.

    Returns:
        The JSON response from the email service.

    Raises:
        ValueError: If ``EMAIL_ENDPOINT`` or ``EMAIL_KEY`` are missing.
        requests.RequestException: If the HTTP request fails.
    """
    if not config.EMAIL_ENDPOINT or not config.EMAIL_KEY:
        raise ValueError("EMAIL_ENDPOINT/EMAIL_KEY not configured (set in environment)")
    # Build recipient list
    to_list: List[str] = []
    if config.EMAIL_TO:
        to_list.append(config.EMAIL_TO)
    if extra_to:
        to_list.extend([addr for addr in extra_to if addr])
    para = ",".join(to_list)
    json_body = {
        "key": "NOTIFICACIONESQA",
        "para": para,
        "conCopia": [],
        "adjuntos": [],
        "copiaOculta": "",
        "asunto": subject,
        "body": html,
        "isHtml": True,
    }
    # Log email details without sensitive information
    logging.info("[email] Sending HTML email", extra={"para": para, "subject": subject})
    response = requests.post(
        config.EMAIL_ENDPOINT,
        json=json_body,
        headers={
            "accept": "*/*",
            "ApiKeyApp": config.EMAIL_KEY,
            "Content-Type": "application/json",
        },
        timeout=20,
    )
    response.raise_for_status()
    return response.json()
