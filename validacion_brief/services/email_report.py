"""
Generate and send summary emails about BRIEF validation results.

This module translates ``src/services/emailReport.ts``.  It reads JSON
validation files from ``pages/Brief/Validaciones``, summarises the
status of each promotion and constructs an HTML table.  It then
sends an email via the configured email service.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..infra.notifications.email import send_html_email


def _read_json(path: Path) -> Optional[Dict[str, any]]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def build_summary() -> Tuple[str, List[Dict[str, any]]]:
    """Build an HTML summary of all promotion validations.

    Returns a tuple of (html, rows).  Each row is a dict with keys
    ``promo``, ``status``, ``errors`` and ``details``.
    """
    base = Path('pages/Brief/Validaciones')
    logging.info('[buildSummary] Reading summaries from', extra={'base': str(base)})
    try:
        dirs = [d for d in base.iterdir() if d.is_dir()]
    except Exception:
        dirs = []
    rows: List[Dict[str, any]] = []
    for d in dirs:
        files = [
            'validacion_multiplicador.json',
            'validacion_equivalencias.json',
            'validacion_configuraciones.json',
            'validacion_premios.json',
            'validacion_etapas.json',
        ]
        has_error = False
        error_count = 0
        all_skipped = True
        category_statuses: Dict[str, str] = {}
        for f in files:
            j = _read_json(d / f)
            # Derive a human friendly category name from the file name
            category = f.replace('validacion_', '').replace('.json', '').replace('_', ' ').title()
            if not j:
                category_statuses[category] = 'SKIPPED'
                continue
            status = j.get('status', 'SKIPPED')
            category_statuses[category] = status
            if status != 'SKIPPED':
                all_skipped = False
            if status == 'ERROR':
                has_error = True
                error_count += 1
        overall_status = 'SKIPPED' if all_skipped else ('ERROR' if has_error else 'OK')
        details = '; '.join([f"{cat}: {st}" for cat, st in category_statuses.items()])
        rows.append({'promo': d.name, 'status': overall_status, 'errors': error_count, 'details': details})
    logging.info('[buildSummary] Promotions summarised', extra={'rows': rows})
    # Build HTML table
    html_rows = []
    for r in rows:
        status_display = '✅ OK' if r['status'] == 'OK' else ('❌ ERROR' if r['status'] == 'ERROR' else '⏭️ SKIPPED')
        html_rows.append(f"<tr><td>{r['promo']}</td><td>{status_display}</td><td style=\"text-align:center\">{r['errors']}</td><td>{r['details']}</td></tr>")
    html = (
        '<div style="font-family:Arial,Helvetica,sans-serif">'
        '<h3>Resumen de validaciones BRIEF</h3>'
        '<table border="1" cellspacing="0" cellpadding="6">'
        '<tr><th>Promoción</th><th>Estado</th><th>Errores</th><th>Detalles</th></tr>'
        f"{''.join(html_rows)}"'</table></div>'
    )
    return html, rows


def send_summary_email(extra_to: Optional[List[str]] = None) -> None:
    """Send the summary email to default and extra recipients."""
    html, rows = build_summary()
    incorrectas = [r['promo'] for r in rows if r['status'] == 'ERROR']
    if incorrectas:
        subject = f"BRIEF: {len(incorrectas)} promociones con errores ({', '.join(incorrectas)})"
    else:
        subject = "BRIEF: todas las promociones OK"
    logging.info('[sendSummaryEmail] Subject and recipients', extra={'subject': subject, 'extra': extra_to})
    send_html_email(subject, html, extra_to)
