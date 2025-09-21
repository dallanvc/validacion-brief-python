from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

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

    def parse_range_position(pos: str) -> (int, int, int):

        try:
            if '-' in pos:
                start_str, end_str = pos.split('-', 1)
                start = int(start_str)
                end = int(end_str)
                return start, end, (end - start + 1)
            # Single position
            val = int(pos)
            return val, 0, 1
        except Exception:
            # Fallback: treat as single position
            try:
                val = int(pos)
                return val, 0, 1
            except Exception:
                return 0, 0, 1

    def parse_ranking_top(filepath: Path) -> Optional[PromoConfig]:

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                rules = json.load(f)
            defaults = rules.get('defaults', {})
            config: PromoConfig = {}
            # Basic fields
            if 'multiplicador' in defaults:
                config['multiplicador'] = defaults['multiplicador']
            if 'equivalencias' in defaults:
                config['equivalencias'] = defaults['equivalencias']
            if 'configuraciones' in defaults:
                config['configuraciones'] = defaults['configuraciones']
            # Premios: convert position ranges to condition ranges and winner counts
            premios = defaults.get('premios', [])
            premios_cfg: List[Dict[str, Any]] = []
            for p in premios:
                pos = p.get('posicion')
                premio_val = p.get('premio')
                if pos is None or premio_val is None:
                    continue
                cond_min, cond_max, ganadores = parse_range_position(str(pos))
                premios_cfg.append({
                    'valor_premio': float(premio_val),
                    'cantidad_ganadores': float(ganadores),
                    'condicion_minima': float(cond_min),
                    'condicion_maxima': float(cond_max),
                })
            if premios_cfg:
                config['premios'] = premios_cfg
            # Copy durations and hours for stage validation
            durations = defaults.get('durations')
            if durations:
                config['durations'] = durations
            hours = defaults.get('hours')
            if hours:
                config['hours'] = hours
            # Stage names: map logic keys to legacy stage names
            stage_map = {
                'planificacion': 'PLANIFICADO',
                'preEjecucion': 'PRE_EJECUCION',
                'validacion': 'VALIDACION',
                'recalculo': 'RECALCULO',
                'resultado': 'RESULTADO',
                'resultadoIview': 'RESULTADO_IVIEW',
                'pago': 'PAGOS_FISICO',
                'vencido': 'PAGOS_FISICO_VENCIDOS',
                'finalizado': 'FINALIZADO',
                'acumulacion': 'ACUMULACION',
            }
            etapas = {}
            logic = rules.get('logic', {})
            if isinstance(logic, dict):
                for stage_key in logic.keys():
                    name = stage_map.get(stage_key, stage_key.upper())
                    etapas[name] = {}
            if etapas:
                config['etapas'] = etapas
            return config
        except Exception:
            return None

    def parse_sorteos_rules(filepath: Path) -> Dict[str, PromoConfig]:
        """Parse the sorteo rules file and return a mapping of mode names to configs."""
        configs: Dict[str, PromoConfig] = {}
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                rules = json.load(f)

            etapas: Dict[str, Dict[str, Any]] = {}
            schema_et = rules.get('schema_et_sorteos', {})
            state_def = schema_et.get('state') if isinstance(schema_et, dict) else None
            stage_keys: List[str] = []
            if isinstance(state_def, dict):
                for key in state_def.keys():
                    # Remove the 'etapa' prefix (case‑insensitive) and
                    # upper‑case the remaining string.  For example,
                    # 'etapaPlanificacion' becomes 'PLANIFICACION'.
                    cleaned = key
                    if cleaned.lower().startswith('etapa'):
                        cleaned = cleaned[5:]
                    stage_keys.append(cleaned.upper())
            else:
                logic_sorteos = rules.get('logic_sorteos', {})
                if isinstance(logic_sorteos, dict):
                    stage_keys = [k.upper() for k in logic_sorteos.keys()]
            for st_name in stage_keys:
                etapas[st_name] = {}

            # Process each mode defined in the rules.  Each mode has its own
            # defaults (multiplicador, equivalencias, configuraciones, premios).
            modes = rules.get('modes', {})
            for mode_name, mode_def in modes.items():
                defaults = mode_def.get('defaults', {})
                cfg: PromoConfig = {}
                # Copy the simple numeric or list fields
                if 'multiplicador' in defaults:
                    cfg['multiplicador'] = defaults['multiplicador']
                if 'equivalencias' in defaults:
                    cfg['equivalencias'] = defaults['equivalencias']
                if 'configuraciones' in defaults:
                    cfg['configuraciones'] = defaults['configuraciones']
                # Convert premios to the expected legacy format
                premios_list = defaults.get('premios', [])
                premios_cfg: List[Dict[str, Any]] = []
                for p in premios_list:
                    premio_val = p.get('premio')
                    ganadores_val = p.get('ganadores')
                    if premio_val is None or ganadores_val is None:
                        continue
                    premios_cfg.append({
                        'valor_premio': float(premio_val),
                        'cantidad_ganadores': float(ganadores_val),
                        'condicion_minima': 0,
                        'condicion_maxima': 0,
                    })
                if premios_cfg:
                    cfg['premios'] = premios_cfg
                # Share the same stage names across modes
                if etapas:
                    cfg['etapas'] = dict(etapas)

                # Copy common durations and hours from top-level definitions if present
                durations_common = rules.get('durations_et_sorteos')
                if durations_common:
                    cfg['durations_common'] = durations_common
                hours_common = rules.get('hours_et_sorteos')
                if hours_common:
                    cfg['hours_common'] = hours_common
                # Copy mode‑specific durations for accumulation cycles if present
                durations_mode = defaults.get('durations')
                if durations_mode:
                    cfg['durations_mode'] = durations_mode
                configs[mode_name] = cfg
        except Exception:
            # In case of any parsing error, return what has been built so far
            pass
        return configs

    def parse_salta_y_gana(filepath: Path) -> Optional[PromoConfig]:
        """Parse the Salta y Gana rules file into a configuration dict."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                rules = json.load(f)
            defaults = rules.get('defaults', {})
            cfg: PromoConfig = {}
            if 'multiplicador' in defaults:
                cfg['multiplicador'] = defaults['multiplicador']
            if 'equivalencias' in defaults:
                cfg['equivalencias'] = defaults['equivalencias']
            if 'configuraciones' in defaults:
                cfg['configuraciones'] = defaults['configuraciones']
            premios_list = defaults.get('premios', [])
            premios_cfg: List[Dict[str, Any]] = []
            for p in premios_list:
                premio_val = p.get('premio')
                ganadores_val = p.get('ganadores')
                if premio_val is None or ganadores_val is None:
                    continue
                premios_cfg.append({
                    'valor_premio': float(premio_val),
                    'cantidad_ganadores': float(ganadores_val),
                    'condicion_minima': 0,
                    'condicion_maxima': 0,
                })
            if premios_cfg:
                cfg['premios'] = premios_cfg
            # Copy durations and hours for stage validation
            durations = defaults.get('durations')
            if durations:
                cfg['durations'] = durations
            hours = defaults.get('hours')
            if hours:
                cfg['hours'] = hours

            # Stage names: prefer the keys under ``schema.state`` (removing
            # 'etapa' prefix and upper‑casing) to derive the legacy stage
            # names.  Fall back to the keys from ``logic`` when necessary.
            etapas: Dict[str, Any] = {}
            schema = rules.get('schema') if isinstance(rules.get('schema'), dict) else None
            state_def = schema.get('state') if schema else None
            stage_keys: List[str] = []
            if isinstance(state_def, dict):
                for key in state_def.keys():
                    cleaned = key
                    if cleaned.lower().startswith('etapa'):
                        cleaned = cleaned[5:]
                    stage_keys.append(cleaned.upper())
            else:
                logic = rules.get('logic', {})
                if isinstance(logic, dict):
                    stage_keys = [k.upper() for k in logic.keys()]
            for st_name in stage_keys:
                etapas[st_name] = {}
            if etapas:
                cfg['etapas'] = etapas
            return cfg
        except Exception:
            return None


    config: Dict[str, PromoConfig] = {}

    base_dir = Path(__file__).parent

    # Promotion 17 – TOP / Ranking
    ranking_file = base_dir / 'pages/Brief/JsonGenerales/ranking-top.rules.full.json'
    ranking_cfg = parse_ranking_top(ranking_file)
    if ranking_cfg:
        config['17'] = ranking_cfg

    # Promotion 18 / 19 – Sorteos (Estelar, Sueños)
    sorteos_file = base_dir / 'pages/Brief/JsonGenerales/sorteos.rules.full.json'
    sorteos_cfgs = parse_sorteos_rules(sorteos_file)
    # Map mode names to their corresponding promo IDs
    for mode_name, cfg in sorteos_cfgs.items():
        name_lower = mode_name.lower()
        if name_lower == 'estelar':
            config['18'] = cfg
        elif name_lower in ('suenos', 'sueños'):
            config['19'] = cfg

    # Promotion 22 – Salta y Gana
    salta_file = base_dir / 'pages/Brief/JsonGenerales/sorteos.saltaYGana.rules.json'
    salta_cfg = parse_salta_y_gana(salta_file)
    if salta_cfg:
        config['22'] = salta_cfg
    return config

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

def _normalize_stage_name(name: str) -> str:
    """Normalize stage names by removing accents, underscores and trimming."""
    if not isinstance(name, str):
        return ''
    # Replace accented characters with their non‑accented counterparts
    translation = str.maketrans('ÁÉÍÓÚÑÜ', 'AEIOUNU')
    normalized = name.upper().translate(translation)
    # Replace underscores with spaces and collapse multiple spaces
    normalized = normalized.replace('_', ' ').strip()
    # Replace multiple spaces with single space
    while '  ' in normalized:
        normalized = normalized.replace('  ', ' ')
    return normalized

def _parse_datetime(value: Any) -> Optional[datetime]:  # type: ignore[name-defined]
    """Attempt to parse a date/time string into a datetime object."""
    from datetime import datetime
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        # Try ISO format or 'YYYY-MM-DD HH:MM:SS'
        return datetime.fromisoformat(str(value).replace('T', ' '))
    except Exception:
        # Try splitting date and time manually
        try:
            return datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
        except Exception:
            return None

def validate_etapas(promo_id: str, cfg: PromoConfig) -> None:

    out_dir = _out_dir_for_promo(promo_id)
    try:
        segments = querySegmentos(int(promo_id))
        if not segments:
            return
        all_results: List[Dict[str, Any]] = []
        for seg in segments:
            seg_id = int(seg['id_ejecucion_segmento'])
            seg_name = seg.get('nombre_segmento')
            stage_rows = queryEtapasSeg(seg_id)
            if not stage_rows:
                continue
            # Build a mapping of stage name (normalized) to its data
            stage_data: Dict[str, Dict[str, Any]] = {}
            for row in stage_rows:
                raw_name = row.get('nombre_etapa') or row.get('nombre') or ''
                name_norm = _normalize_stage_name(str(raw_name))
                start_dt = _parse_datetime(row.get('fecha_inicio'))
                end_dt = _parse_datetime(row.get('fecha_fin'))
                if start_dt is None or end_dt is None:
                    continue
                stage_data[name_norm] = {
                    'start': start_dt,
                    'end': end_dt,
                }
            validations: List[Dict[str, Any]] = []
            # Helper to format datetime as string
            def fmt(dt: datetime) -> str:
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            # Helper to parse a time string into a datetime.time object.  Accepts
            # 'HH:MM' or 'HH:MM:SS'.  Returns None on failure.
            def parse_time_str(value: Any) -> Optional[Any]:
                from datetime import time
                if not value:
                    return None
                try:
                    parts = str(value).split(':')
                    if len(parts) == 2:
                        h, m = int(parts[0]), int(parts[1])
                        s = 0
                    elif len(parts) == 3:
                        h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
                    else:
                        return None
                    return time(h, m, s)
                except Exception:
                    return None
            # Helper to build a datetime from a reference date and a time string with
            # a day offset.  The time is taken from the ``hours`` definition and
            # the date is adjusted by ``days_offset``.  Returns None if time
            # parsing fails.
            def build_dt(reference: datetime, time_str: str, days_offset: int = 0) -> Optional[datetime]:
                t = parse_time_str(time_str)
                if t is None:
                    return None
                dt_date = reference.date() + timedelta(days=days_offset)
                return datetime.combine(dt_date, t)
            # Determine the mapping of normalized stage names to keys in the
            # configuration's durations/hours for the current promotion.  This
            # allows us to translate database stage names into configuration keys.
            if promo_id == '17':
                stage_key_map = {
                    'PLANIFICADO': 'planificacion',
                    'PLANIFICACION': 'planificacion',
                    'PRE EJECUCION': 'preEjecucion',
                    'PRE_EJECUCION': 'preEjecucion',
                    'ACUMULACION': 'acumulacion',
                    'ACUMULACIÓN': 'acumulacion',
                    'VALIDACION': 'validacion',
                    'RECALCULO': 'recalculo',
                    'RESULTADO': 'resultado',
                    'RESULTADO IVIEW': 'resultadoIview',
                    'RESULTADO_IVIEW': 'resultadoIview',
                    'PAGOS FISICO': 'pago',
                    'PAGOS FISICO VENCIDOS': 'vencido',
                    'FINALIZADO': 'finalizado',
                }
                durations = cfg.get('durations', {})
                hours_cfg = cfg.get('hours', {})
            elif promo_id in ('18', '19'):
                stage_key_map = {
                    'PLANIFICADO': 'planificacion',
                    'PLANIFICACION': 'planificacion',
                    'PRE EJECUCION': 'preEjecucion',
                    'PRE_EJECUCION': 'preEjecucion',
                    'ACUMULACION': 'acumulacion',
                    'ACUMULACIÓN': 'acumulacion',
                    'RECALCULO': 'recalculo',
                    'CANJES': 'canjes',
                    'CANJE': 'canjes',
                    'CANJE1': 'canje1',
                    'CANJE 1': 'canje1',
                    'CANJE2': 'canje2',
                    'CANJE 2': 'canje2',
                    'VALIDACION': 'validacion',
                    'RESULTADO': 'resultado',
                    'FINALIZADO': 'finalizado',
                }
                durations = {}
                # For sorteos the durations are split into common and mode-specific.
                if 'durations_common' in cfg:
                    durations.update(cfg['durations_common'])
                if 'durations_mode' in cfg:
                    durations.update(cfg['durations_mode'])
                hours_cfg = {}
                if 'hours_common' in cfg:
                    hours_cfg.update(cfg['hours_common'])
            elif promo_id == '22':
                stage_key_map = {
                    'PLANIFICADO': 'planificacion',
                    'PLANIFICACION': 'planificacion',
                    'PRE EJECUCION': 'preEjecucion',
                    'PRE_EJECUCION': 'preEjecucion',
                    'ACUMULACION': 'acumulacion',
                    'ACUMULACIÓN': 'acumulacion',
                    'SORTEO1': 'sorteo1',
                    'SORTEO 1': 'sorteo1',
                    'SORTEO2': 'sorteo2',
                    'SORTEO 2': 'sorteo2',
                    'CANJE1': 'canje1',
                    'CANJE 1': 'canje1',
                    'CANJE2': 'canje2',
                    'CANJE 2': 'canje2',
                    'CANJES': 'canje',
                    'RECALCULO': 'recalculo',
                    'VALIDACION': 'validacion',
                    'RESULTADO': 'resultado',
                    'FINALIZADO': 'finalizado',
                }
                durations = cfg.get('durations', {})
                hours_cfg = cfg.get('hours', {})
            else:
                stage_key_map = {}
                durations = {}
                hours_cfg = {}
            # Determine reference datetimes from actual data.  We use the
            # accumulation stage as the anchor for most calculations.
            acum = stage_data.get('ACUMULACION') or stage_data.get('ACUMULACIÓN')
            if not acum:
                # Without an accumulation stage the validations cannot proceed.
                all_results.append({
                    'segmento': seg_id,
                    'nombreSegmento': seg_name,
                    'validaciones': validations
                })
                continue
            acum_start_dt = acum['start']
            acum_end_dt = acum['end']
            # Compute expected start/end datetimes for each relevant stage based on
            # configuration.  Use actual end times of preceding stages as
            # references when appropriate.  The expected datetimes are stored in
            # a dictionary keyed by the normalized stage name.
            expected_starts: Dict[str, datetime] = {}
            expected_ends: Dict[str, datetime] = {}
            # Helper to add a validation entry
            def add_validation(stage_name: str, rule: str, expected_dt: Optional[datetime], actual_dt: Optional[datetime]) -> None:
                if expected_dt is None or actual_dt is None:
                    validations.append({
                        'etapa': stage_name,
                        'regla': rule,
                        'valor_esperado': 'N/D',
                        'valor_encontrado': 'N/D',
                        'estado': 'SKIPPED'
                    })
                    return
                diff = abs((actual_dt - expected_dt).total_seconds())
                validations.append({
                    'etapa': stage_name,
                    'regla': rule,
                    'valor_esperado': fmt(expected_dt),
                    'valor_encontrado': fmt(actual_dt),
                    'estado': 'OK' if diff <= 1 else 'ERROR'
                })
            # Build expected schedule for each stage present in stage_data
            for norm_name, times in stage_data.items():
                # Map the normalized name to the configuration key
                config_key = stage_key_map.get(norm_name)
                if not config_key:
                    continue
                # Retrieve the hour definition for this stage if present
                hour_def = hours_cfg.get(config_key) if isinstance(hours_cfg, dict) else None
                start_time_str = hour_def.get('start') if isinstance(hour_def, dict) else None
                end_time_str = hour_def.get('end') if isinstance(hour_def, dict) else None
                # Determine the reference datetime for this stage.  By default
                # stages refer to the accumulation start (planificacion, preEjecucion)
                # or accumulation end (validacion, recalculo) or canje end for
                # subsequent stages.  For ranking, validacion and recalculo use
                # accumulation end; resultado uses validacion start; resultadoIview
                # uses validacion start; pago uses accumulation end; vencido uses
                # accumulation end; finalizado uses resultado start.  For sorteos
                # (18/19), validacion uses canjes end; recalc uses accumulation end;
                # resultado uses validacion start; finalizado uses resultado start.
                # For Salta y Gana, validacion uses canje2 end; recalculo uses
                # accumulation end; resultado uses validacion start; finalizado uses
                # resultado start.
                ref_dt: Optional[datetime] = None
                day_offset = 0
                # Determine base for ranking
                if promo_id == '17':
                    if config_key in ('planificacion', 'preEjecucion'):
                        ref_dt = acum_start_dt
                        day_offset = -durations.get('planificacion', 0) if config_key == 'planificacion' else 0
                    elif config_key == 'acumulacion':
                        ref_dt = acum_start_dt
                        day_offset = 0
                    elif config_key in ('validacion', 'recalculo'):
                        ref_dt = acum_end_dt
                        day_offset = durations.get(config_key, 0)
                    elif config_key == 'resultado':
                        # resultado anchors on validacion start date if present, else accumulation end date
                        val_start = expected_starts.get('VALIDACION')
                        if val_start:
                            ref_dt = val_start
                        else:
                            ref_dt = acum_end_dt
                        day_offset = durations.get(config_key, 0)
                    elif config_key == 'resultadoIview':
                        # resultIview anchors on validacion start
                        val_start = expected_starts.get('VALIDACION')
                        if val_start:
                            ref_dt = val_start
                        else:
                            ref_dt = acum_end_dt
                        day_offset = durations.get(config_key, 0)
                    elif config_key == 'pago':
                        ref_dt = acum_end_dt
                        day_offset = durations.get(config_key, 0)
                    elif config_key == 'vencido':
                        ref_dt = acum_end_dt
                        day_offset = durations.get(config_key, 0)
                    elif config_key == 'finalizado':
                        # finalizado anchors on resultado start if present
                        res_start = expected_starts.get('RESULTADO')
                        if res_start:
                            ref_dt = res_start
                        else:
                            ref_dt = acum_end_dt
                        day_offset = durations.get(config_key, 0)
                elif promo_id in ('18', '19'):
                    # Sorteos: use accumulation end for recalc and canjes; canjes for validacion;
                    # validacion for resultado; resultado for finalizado
                    if config_key in ('planificacion', 'preEjecucion'):
                        ref_dt = acum_start_dt
                        day_offset = -durations.get('planificacion', 0) if config_key == 'planificacion' else 0
                    elif config_key == 'acumulacion':
                        ref_dt = acum_start_dt
                        day_offset = 0
                    elif config_key == 'recalculo':
                        ref_dt = acum_end_dt
                        day_offset = durations.get('recalculo', 0)
                    elif config_key == 'canjes':
                        ref_dt = acum_end_dt
                        day_offset = 0
                    elif config_key == 'validacion':
                        # validacion starts after canjes end
                        canj_end = expected_ends.get('CANJES') or expected_ends.get('CANJE')
                        if canj_end:
                            ref_dt = canj_end
                        else:
                            ref_dt = acum_end_dt
                        day_offset = durations.get('validacion', 0)
                    elif config_key == 'resultado':
                        val_start = expected_starts.get('VALIDACION')
                        if val_start:
                            ref_dt = val_start
                        else:
                            ref_dt = acum_end_dt
                        day_offset = durations.get('resultado', 0)
                    elif config_key == 'finalizado':
                        res_start = expected_starts.get('RESULTADO')
                        if res_start:
                            ref_dt = res_start
                        else:
                            ref_dt = acum_end_dt
                        day_offset = durations.get('finalizado', 0)
                elif promo_id == '22':
                    # Salta y Gana: specific anchors for stages
                    if config_key in ('planificacion', 'preEjecucion'):
                        ref_dt = acum_start_dt
                        day_offset = -durations.get('planificacion', 0) if config_key == 'planificacion' else 0
                    elif config_key == 'acumulacion':
                        ref_dt = acum_start_dt
                        day_offset = 0
                    elif config_key in ('sorteo1', 'sorteo2'):
                        # Sorteos start the day after accumulation ends
                        ref_dt = acum_end_dt
                        # sorteo1 begins 1 day after; sorteo2 shares same day as sorteo1
                        day_offset = 1
                    elif config_key in ('canje1', 'canje2', 'canje'):
                        # canje1 begins at accumulation end; canje2 begins right after canje1
                        if config_key == 'canje1' or config_key == 'canje':
                            ref_dt = acum_end_dt
                            day_offset = durations.get('canje1', 0)
                        else:  # canje2
                            # start after canje1 ends
                            canje1_end = expected_ends.get('CANJE1') or expected_ends.get('CANJE 1') or expected_ends.get('CANJE')
                            if canje1_end:
                                ref_dt = canje1_end
                            else:
                                ref_dt = acum_end_dt
                            day_offset = durations.get('canje2', 0)
                    elif config_key == 'recalculo':
                        ref_dt = acum_end_dt
                        day_offset = durations.get('recalculo', 0)
                    elif config_key == 'validacion':
                        # begins after canje2 ends
                        canje2_end = expected_ends.get('CANJE2') or expected_ends.get('CANJE 2') or expected_ends.get('CANJE')
                        if canje2_end:
                            ref_dt = canje2_end
                        else:
                            ref_dt = acum_end_dt
                        day_offset = durations.get('validacion', 0)
                    elif config_key == 'resultado':
                        val_start = expected_starts.get('VALIDACION')
                        if val_start:
                            ref_dt = val_start
                        else:
                            ref_dt = acum_end_dt
                        day_offset = durations.get('resultado', 0)
                    elif config_key == 'finalizado':
                        res_start = expected_starts.get('RESULTADO')
                        if res_start:
                            ref_dt = res_start
                        else:
                            ref_dt = acum_end_dt
                        day_offset = durations.get('finalizado', 0)
                # Only build expectations if both hours and reference are available
                if ref_dt is not None and start_time_str:
                    expected_start_dt = build_dt(ref_dt, start_time_str, day_offset)
                else:
                    expected_start_dt = None
                if ref_dt is not None and end_time_str is not None:
                    # End may have the same day_offset as start plus the duration for this stage
                    end_offset = durations.get(config_key, 0)
                    expected_end_dt = build_dt(ref_dt, end_time_str, day_offset + end_offset)
                else:
                    expected_end_dt = None
                # Record expectations so subsequent stages can reference them
                expected_starts[norm_name] = expected_start_dt if expected_start_dt else times['start']
                expected_ends[norm_name] = expected_end_dt if expected_end_dt else times['end']
                # Compare actual start/end with expected ones
                if expected_start_dt:
                    add_validation(norm_name, 'Inicio según reglas de configuración', expected_start_dt, times['start'])
                if expected_end_dt:
                    add_validation(norm_name, 'Fin según reglas de configuración', expected_end_dt, times['end'])
            # Additional validation: for Salta y Gana ensure that la validación
            # dura lo mismo que la suma de los sorteos
            if promo_id == '22':
                # Compute total sorteo duration from actual data
                total_sorteo_seconds = 0.0
                for name, key in [('SORTEO1', 'sorteo1'), ('SORTEO 1', 'sorteo1'), ('SORTEO2', 'sorteo2'), ('SORTEO 2', 'sorteo2')]:
                    sd = stage_data.get(_normalize_stage_name(name))
                    if sd:
                        total_sorteo_seconds += (sd['end'] - sd['start']).total_seconds()
                val_stage = stage_data.get('VALIDACION')
                if val_stage and total_sorteo_seconds > 0:
                    val_seconds = (val_stage['end'] - val_stage['start']).total_seconds()
                    validations.append({
                        'etapa': 'VALIDACION',
                        'regla': 'Duración igual a la suma de los SORTEOS',
                        'valor_esperado': f"{total_sorteo_seconds} segundos",
                        'valor_encontrado': f"{val_seconds} segundos",
                        'estado': 'OK' if abs(val_seconds - total_sorteo_seconds) <= 1 else 'ERROR'
                    })
            all_results.append({
                'segmento': seg_id,
                'nombreSegmento': seg_name,
                'validaciones': validations
            })
        write_json(str(out_dir / 'validacion_etapas.json'), all_results)
    except Exception as err:
        logging.error('[validateEtapas] Error validating etapas for promo', extra={'promo': promo_id, 'error': err})

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
        # After validating segments, validate the stage durations and times
        validate_etapas(promo_id, promo_cfg)