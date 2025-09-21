"""
Validation of mesas start and end dates.

This module corresponds to ``services/ConfiguracionMesasPage.ts`` in the
TypeScript project.  It loads an expected configuration from
``pages/Brief/JsonGenerales/mesas_config.json`` and compares it
against rows returned from the mesas database.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from ..compat.database_connection206 import query_fechas_mesas
from ..infra.reporting.json_reporter import ensure_dir, write_json


def load_mesas_config() -> Dict[str, Any]:
    """Load mesas configuration from the JSON file.

    The file may either be an object with a ``mesas`` array or a raw array.
    The function normalises the structure into a dictionary with a
    ``mesas`` key.
    """
    path = Path('pages/Brief/JsonGenerales/mesas_config.json')
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, dict) and 'mesas' in data:
        return data
    # If it's a list, wrap it into {mesas: [...]}
    if isinstance(data, list):
        return {'mesas': data}
    raise ValueError('Unexpected mesas_config.json structure')


def validate_mesas_fechas() -> None:
    """Validate that mesas start/end dates match expected values."""
    expected = load_mesas_config()
    rows = query_fechas_mesas()
    # Build a map from segmento/codigo to expected start and end times
    exp_map: Dict[str, Dict[str, Any]] = {}
    for mesa in expected.get('mesas', []):
        # Accept both 'segmento' and 'codigo'
        segment_id = str(mesa.get('segmento') or mesa.get('codigo'))
        start = mesa.get('inicio') or mesa.get('dia_inicio')
        end = mesa.get('fin') or mesa.get('dia_fin')
        exp_map[segment_id] = {'inicio': start, 'fin': end}
    diffs: List[Dict[str, Any]] = []
    for r in rows:
        seg = str(r.get('segmento') or r.get('codigo') or r.get('id') or '')
        expected_entry = exp_map.get(seg)
        if not expected_entry:
            diffs.append({'segmento': seg, 'status': 'ERROR', 'reason': 'Segmento no esperado'})
            continue
        ok_inicio = str(expected_entry['inicio']) == str(r.get('inicio'))
        ok_fin = str(expected_entry['fin']) == str(r.get('fin'))
        if not ok_inicio or not ok_fin:
            diffs.append({
                'segmento': seg,
                'expected': expected_entry,
                'found': {'inicio': r.get('inicio'), 'fin': r.get('fin')},
                'status': 'ERROR',
            })
    status = 'ERROR' if diffs else 'OK'
    out_dir = Path('pages/Brief/Validaciones/mesas')
    ensure_dir(str(out_dir))
    write_json(str(out_dir / 'validacion_fechas.json'), {
        'status': status,
        'diffs': diffs,
        'expected': expected,
        'found': rows,
    })
