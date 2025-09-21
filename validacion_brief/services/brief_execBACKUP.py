"""
Main validation logic for BRIEF promotions.

This module is a direct translation of ``services/briefExec.ts`` from
the TypeScript project.  It loads execution configurations from a
JSON file, queries the database for actual data and writes out
validation reports under ``pages/Brief/Validaciones/<promoId>``.  It
also supports validating individual execution segments.

Functions defined here are synchronous for simplicity; the
TypeScript originals were ``async`` but Python's synchronous file I/O
and database calls suffice.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..compat.database_connection27 import (
    queryMultiplicador,
    queryEquivalencias,
    queryConfiguraciones,
    queryPremios,
    queryEtapas,
    querySegmentos,
    queryMultiplicadorSeg,
    queryEquivalenciasSeg,
    queryConfiguracionesSeg,
    queryPremiosSeg,
    queryEtapasSeg,
)
from ..infra.reporting.json_reporter import ensure_dir, write_json


# Type aliases for readability
PromoConfig = Dict[str, Any]


def load_ejecucion_config() -> Dict[str, PromoConfig]:
    """Load the execution configuration JSON for all promotions."""
    path = Path('pages/Brief/JsonGenerales/Ejecucion_config.json')
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _out_dir_for_promo(promo_id: str) -> Path:
    return Path(f'pages/Brief/Validaciones/{promo_id}')


def validate_segments(promo_id: str, cfg: PromoConfig) -> None:
    """Run validations per execution segment of a promotion."""
    out_dir = _out_dir_for_promo(promo_id)
    try:
        segments = querySegmentos(int(promo_id))
        if not segments:
            return
        results: List[Dict[str, Any]] = []
        for seg in segments:
            seg_id = int(seg['id_ejecucion_segmento'])
            seg_name = seg.get('nombre_segmento')
            seg_result: Dict[str, Any] = {
                'segmento': seg_id,
                'nombreSegmento': seg_name,
            }
            # Multiplicador per segment
            if isinstance(cfg.get('multiplicador'), (int, float)):
                rows = queryMultiplicadorSeg(seg_id)
                values = [float(r.get('valor_multiplicador')) for r in rows]
                unique = sorted(set(values))
                seg_result['multiplicador'] = {
                    'expected': cfg['multiplicador'],
                    'found': unique,
                    'status': 'OK' if len(unique) == 1 and unique[0] == float(cfg['multiplicador']) else 'ERROR',
                }
            else:
                seg_result['multiplicador'] = {
                    'status': 'SKIPPED',
                    'reason': 'No "multiplicador" in JSON',
                }
            # Equivalencias per segment
            if cfg.get('equivalencias'):
                db_rows = queryEquivalenciasSeg(seg_id)
                # Reuse the same normalization functions
                def normalize_config(input_val: Any) -> List[Dict[str, Optional[float]]]:
                    if not input_val:
                        return []
                    arr = input_val if isinstance(input_val, list) else [input_val]
                    result: List[Dict[str, Optional[float]]] = []
                    for e in arr:
                        min_val = float(e.get('minimo') or e.get('min') or e.get('condicion_minima') or 0)
                        max_key = e.get('maximo') if 'maximo' in e else e.get('max')
                        max_val = float(max_key) if max_key is not None else None
                        puntaje = float(e.get('puntaje') or e.get('valor_puntaje') or 0)
                        result.append({'min': min_val, 'max': max_val, 'puntaje': puntaje})
                    return result
                def normalize_rows_seg(rows: List[Dict[str, Any]]) -> List[Dict[str, Optional[float]]]:
                    result: List[Dict[str, Optional[float]]] = []
                    for r in rows:
                        min_val = float(r['condicion_minima'])
                        max_raw = r.get('condicion_maxima')
                        max_val = float(max_raw) if max_raw is not None else None
                        puntaje = float(r['valor_puntaje'])
                        result.append({'min': min_val, 'max': max_val, 'puntaje': puntaje})
                    return result
                expected_eq = normalize_config(cfg['equivalencias'])
                found_eq = normalize_rows_seg(db_rows)
                # Sort lists for comparison
                expected_eq.sort(key=lambda x: (x['min'], x['max'] if x['max'] is not None else float('inf'), x['puntaje']))
                found_eq.sort(key=lambda x: (x['min'], x['max'] if x['max'] is not None else float('inf'), x['puntaje']))
                seg_result['equivalencias'] = {
                    'expected': expected_eq,
                    'found': found_eq,
                    'status': 'OK' if json.dumps(expected_eq, sort_keys=True) == json.dumps(found_eq, sort_keys=True) else 'ERROR',
                }
            else:
                seg_result['equivalencias'] = {
                    'status': 'SKIPPED',
                    'reason': 'No "equivalencias" in JSON',
                }
            # Configuraciones per segment
            if cfg.get('configuraciones') and isinstance(cfg['configuraciones'], dict) and cfg['configuraciones']:
                rows_cfg = queryConfiguracionesSeg(seg_id)
                found_map: Dict[str, Any] = {}
                for r in rows_cfg:
                    key = str(r['codigo_compuesto']).upper()
                    found_map[key] = r['valor_entero']
                diffs: Dict[str, Dict[str, Any]] = {}
                all_ok = True
                for k, v in cfg['configuraciones'].items():
                    key = str(k).upper()
                    if str(found_map.get(key)) != str(v):
                        diffs[key] = {'expected': v, 'found': found_map.get(key)}
                        all_ok = False
                seg_result['configuraciones'] = {
                    'expected': cfg['configuraciones'],
                    'found': found_map,
                    'diffs': diffs,
                    'status': 'OK' if all_ok else 'ERROR',
                }
            else:
                seg_result['configuraciones'] = {
                    'status': 'SKIPPED',
                    'reason': 'No "configuraciones" in JSON',
                }
            # Premios per segment
            if cfg.get('premios') and isinstance(cfg['premios'], list):
                db_rows = queryPremiosSeg(seg_id)
                def normalize_premios_seg(arr: List[Any]) -> List[Dict[str, float]]:
                    result: List[Dict[str, float]] = []
                    for r in arr:
                        condicion_minima = float(r.get('condicion_minima') or r.get('minimo') or r.get('min') or 0)
                        condicion_maxima = float(r.get('condicion_maxima') or r.get('maximo') or r.get('max') or 0)
                        valor_premio = float(r.get('valor_premio') or r.get('valor') or 0)
                        cantidad_ganadores = float(r.get('cantidad_ganadores') or r.get('cantidad') or 0)
                        result.append({
                            'condicion_minima': condicion_minima,
                            'condicion_maxima': condicion_maxima,
                            'valor_premio': valor_premio,
                            'cantidad_ganadores': cantidad_ganadores,
                        })
                    result.sort(key=lambda x: (x['valor_premio'], x['condicion_minima'], x['cantidad_ganadores']))
                    return result
                expected_premios = normalize_premios_seg(cfg['premios'])
                found_premios = normalize_premios_seg(db_rows)
                seg_result['premios'] = {
                    'expected': expected_premios,
                    'found': found_premios,
                    'status': 'OK' if json.dumps(expected_premios, sort_keys=True) == json.dumps(found_premios, sort_keys=True) else 'ERROR',
                }
            else:
                seg_result['premios'] = {
                    'status': 'SKIPPED',
                    'reason': 'No "premios" in JSON',
                }
            # Etapas per segment
            if cfg.get('etapas'):
                etapas_cfg = cfg['etapas']
                expected_names: List[str] = []
                if isinstance(etapas_cfg, list):
                    expected_names = [str(e.get('nombre') or e.get('nombre_etapa') or '').strip() for e in etapas_cfg if (e.get('nombre') or e.get('nombre_etapa'))]
                elif isinstance(etapas_cfg, dict):
                    expected_names = list(etapas_cfg.keys())
                expected_names = [n.upper() for n in expected_names]
                expected_names.sort()
                db_rows = queryEtapasSeg(seg_id)
                found_names = list({str(r['nombre_etapa']).upper() for r in db_rows})
                found_names.sort()
                missing = [n for n in expected_names if n not in found_names]
                extra = [n for n in found_names if n not in expected_names]
                seg_result['etapas'] = {
                    'expected': expected_names,
                    'found': found_names,
                    'missing': missing,
                    'extra': extra,
                    'status': 'OK' if not missing and not extra else 'ERROR',
                }
            else:
                seg_result['etapas'] = {
                    'status': 'SKIPPED',
                    'reason': 'No "etapas" in JSON',
                }
            results.append(seg_result)
        write_json(str(out_dir / 'validacion_segmentos.json'), results)
    except Exception as err:
        logging.error('[validateSegments] Error validating segments for promo', extra={'promo': promo_id, 'error': err})


def validate_all(promos: Optional[List[str]] = None) -> None:
    """Validate all promotions or a subset specified by the caller."""
    cfg = load_ejecucion_config()
    list_to_validate = promos if promos and promos[0] != 'all' else list(cfg.keys())
    logging.info('[validateAll] Promos to validate', extra={'list': list_to_validate})
    for promo_id in list_to_validate:
        # Skip unknown IDs gracefully
        promo_cfg = cfg.get(promo_id)
        if not promo_cfg:
            logging.warning(f"Promo ID {promo_id} not found in config; skipping")
            continue
        validate_segments(promo_id, promo_cfg)
