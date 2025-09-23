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
from typing import Dict, List, Optional, Tuple, Any

from ..infra.notifications.email import send_html_email


def _read_json(path: Path) -> Optional[Any]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None


def build_summary() -> Tuple[str, List[Dict[str, Any]]]:
    """Build an HTML summary of all promotion validations.

    This implementation supports both legacy per-category summary files
    (``validacion_multiplicador.json`` etc.) and the newer per-segment
    validation lists (``validacion_segmentos.json`` and ``validacion_etapas.json``).
    It produces an overall status per promotion and collates error messages
    from individual segments into the details column.

    Returns a tuple of (html, rows).  Each row is a dict with keys
    ``promo``, ``status``, ``errors`` and ``details``.
    """
    base = Path('pages/Brief/Validaciones')
    logging.info('[buildSummary] Reading summaries from', extra={'base': str(base)})
    try:
        dirs = [d for d in base.iterdir() if d.is_dir()]
    except Exception:
        dirs = []
    rows: List[Dict[str, Any]] = []
    # Map human-friendly category names to the keys used in per-segment results
    cat_to_key = {
        'Multiplicador': 'multiplicador',
        'Equivalencias': 'equivalencias',
        'Configuraciones': 'configuraciones',
        'Premios': 'premios',
        'Etapas': 'etapas',
    }
    for d in dirs:
        category_statuses: Dict[str, Optional[str]] = {}
        category_msgs: Dict[str, List[str]] = {cat: [] for cat in cat_to_key}
        has_error = False
        error_count = 0
        all_skipped = True
        # First, attempt to read per-category summary files (legacy format)
        for cat, key in cat_to_key.items():
            fname = f"validacion_{key}.json"
            j = _read_json(d / fname)
            if isinstance(j, dict) and 'status' in j:
                status = j.get('status', 'SKIPPED')
                details = j.get('details') or ''
                category_statuses[cat] = status
                # If details present, split by semicolon or newline to messages
                if isinstance(details, str) and details:
                    category_msgs[cat].append(details.strip())
                if status != 'SKIPPED':
                    all_skipped = False
                if status == 'ERROR':
                    has_error = True
                    error_count += 1
                continue
            # Otherwise we'll compute status from per-segment results later
            category_statuses[cat] = None  # mark to compute later
        # Load per-segment results if needed
        seg_list: Optional[List[Dict[str, Any]]] = None
        seg_path = d / 'validacion_segmentos.json'
        if seg_path.exists():
            try:
                with open(seg_path, 'r', encoding='utf-8') as fh:
                    seg_data = json.load(fh)
                if isinstance(seg_data, list):
                    seg_list = seg_data
            except Exception:
                seg_list = None
        # validacion_etapas.json: list of per-segment stage validations
        etapas_list: Optional[List[Dict[str, Any]]] = None
        etapas_path = d / 'validacion_etapas.json'
        if etapas_path.exists():
            try:
                with open(etapas_path, 'r', encoding='utf-8') as fh:
                    etapas_data = json.load(fh)
                if isinstance(etapas_data, list):
                    etapas_list = etapas_data
            except Exception:
                etapas_list = None
        # Derive statuses and messages from per-segment results for categories
        for cat, key in cat_to_key.items():
            # Skip categories already determined from legacy summary
            if category_statuses[cat] is not None:
                continue
            # If there are no segments data, mark as SKIPPED
            if key != 'etapas' and not seg_list:
                category_statuses[cat] = 'SKIPPED'
                continue
            if key == 'etapas' and not (seg_list or etapas_list):
                category_statuses[cat] = 'SKIPPED'
                continue
            any_ok = False
            any_error = False
            # Temporary list to gather messages for this category
            tmp_msgs: List[str] = []
            if key != 'etapas' and seg_list:
                for item in seg_list:
                    seg_id = item.get('segmento') or item.get('segmentId') or item.get('id')
                    cat_data = item.get(key)
                    if not isinstance(cat_data, dict):
                        continue
                    status = cat_data.get('status')
                    if status == 'SKIPPED':
                        continue
                    if status == 'OK':
                        any_ok = True
                    elif status == 'ERROR':
                        any_error = True
                        # Build a human friendly message describing the error
                        if key == 'multiplicador':
                            exp = cat_data.get('expected')
                            found = cat_data.get('found')
                            tmp_msgs.append(f"Seg {seg_id}: esperado {exp}, encontrado {found}")
                        elif key == 'equivalencias':
                            tmp_msgs.append(f"Seg {seg_id}: diferencias en equivalencias")
                        elif key == 'configuraciones':
                            diffs = cat_data.get('diffs') or {}
                            if diffs:
                                diff_details = ', '.join([
                                    f"{k}: esp {v['expected']}, obt {v['found']}"
                                    for k, v in diffs.items()
                                ])
                                tmp_msgs.append(f"Seg {seg_id}: {diff_details}")
                            else:
                                tmp_msgs.append(f"Seg {seg_id}: diferencias en configuraciones")
                        elif key == 'premios':
                            tmp_msgs.append(f"Seg {seg_id}: diferencias en premios")
            elif key == 'etapas':
                # Derive from per-stage validations and segments etapas status
                if etapas_list:
                    for item in etapas_list:
                        seg_id = item.get('segmento')
                        validaciones = item.get('validaciones')
                        if not isinstance(validaciones, list):
                            continue
                        seg_any_error = False
                        for v in validaciones:
                            estado = v.get('estado')
                            if estado == 'ERROR':
                                seg_any_error = True
                                etapa = v.get('etapa') or v.get('nombre_etapa')
                                regla = v.get('regla')
                                exp_val = v.get('valor_esperado')
                                found_val = v.get('valor_encontrado')
                                # Include stage name in the message
                                if etapa:
                                    tmp_msgs.append(
                                        f"Seg {seg_id} - {etapa}: {regla} (esp {exp_val}, obt {found_val})"
                                    )
                                else:
                                    tmp_msgs.append(
                                        f"Seg {seg_id}: {regla} (esp {exp_val}, obt {found_val})"
                                    )
                        if seg_any_error:
                            any_error = True
                        elif validaciones:
                            any_ok = True
                # If no detailed etapas validations or none had errors, fall back to segment etapas status
                if not any_error and seg_list:
                    for item in seg_list:
                        seg_id = item.get('segmento')
                        cat_data = item.get('etapas')
                        if not isinstance(cat_data, dict):
                            continue
                        status = cat_data.get('status')
                        if status == 'SKIPPED':
                            continue
                        if status == 'OK':
                            any_ok = True
                        elif status == 'ERROR':
                            any_error = True
                            missing = cat_data.get('missing') or []
                            extra = cat_data.get('extra') or []
                            parts = []
                            if missing:
                                parts.append(f"faltan {', '.join(missing)}")
                            if extra:
                                parts.append(f"sobran {', '.join(extra)}")
                            if parts:
                                tmp_msgs.append(f"Seg {seg_id}: {' y '.join(parts)}")
                            else:
                                tmp_msgs.append(f"Seg {seg_id}: diferencias en etapas")
            # Determine overall status for the category
            if any_error:
                status = 'ERROR'
            elif any_ok:
                status = 'OK'
            else:
                status = 'SKIPPED'
            category_statuses[cat] = status
            if tmp_msgs:
                category_msgs[cat].extend(tmp_msgs)
            if status != 'SKIPPED':
                all_skipped = False
            if status == 'ERROR':
                has_error = True
                error_count += 1
        # Build the detail string combining statuses and messages
        # Format as an unordered list for better readability
        list_items: List[str] = []
        for cat, status in category_statuses.items():
            msgs = category_msgs.get(cat) or []
            if msgs:
                # Join multiple messages with semicolon for consistency inside the list item
                msg_str = '; '.join(msgs)
                list_items.append(
                    f"<li><strong>{cat}:</strong> {status} - {msg_str}</li>"
                )
            else:
                list_items.append(
                    f"<li><strong>{cat}:</strong> {status}</li>"
                )
        details_html = (
            '<ul style="margin:0;padding-left:18px">' + ''.join(list_items) + '</ul>'
        )
        overall_status = 'SKIPPED' if all_skipped else ('ERROR' if has_error else 'OK')
        rows.append(
            {
                'promo': d.name,
                'status': overall_status,
                'errors': error_count,
                'details': details_html,
            }
        )
    logging.info('[buildSummary] Promotions summarised', extra={'rows': rows})
    # Build HTML table
    html_rows: List[str] = []
    for r in rows:
        status_display = (
            '✅ OK' if r['status'] == 'OK' else ('❌ ERROR' if r['status'] == 'ERROR' else '⏭️ SKIPPED')
        )
        html_rows.append(
            f"<tr><td>{r['promo']}</td><td>{status_display}</td>"
            f"<td style=\"text-align:center\">{r['errors']}</td>"
            f"<td>{r['details']}</td></tr>"
        )
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
