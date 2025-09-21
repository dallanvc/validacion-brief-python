"""
Compatibility layer for database queries (27 series).

This module provides a collection of helper functions that execute SQL
queries defined in ``validacion_brief.config.queries`` against either
the promotions or mesas database.  Each function logs the SQL and
parameters prior to execution and returns the list of rows.

The naming of functions (e.g. ``queryMultiplicador``) mirrors the
TypeScript code.  Functions suffixed with ``Seg`` operate on
individual execution segments rather than promotions as a whole.
"""

from __future__ import annotations

import logging
from typing import Any, List

from ..config import config
from ..config.queries import queries
from ..infra.db import get_connection


def _execute(alias: str, sql_key: str, params: dict) -> List[dict]:
    """Internal helper to obtain a connection, run a query and close it."""
    db = get_connection(alias)
    sql_func = queries[sql_key]
    sql = sql_func(config.DB_SCHEMA_PROMOS if alias == 'promos' else config.DB_SCHEMA_MESAS)
    logging.info(f"[DB] {sql_key} executing", extra={"sql": sql, "params": params})
    try:
        result = db.query(sql, params)
        rows = result.get('rows', [])
        logging.info(f"[DB] {sql_key} returned rows", extra={"count": len(rows)})
        return rows
    finally:
        db.close()


def queryMultiplicador(promo_id: int) -> List[dict]:
    return _execute('promos', 'multiplicador', {'idPromocion': promo_id})


def queryEquivalencias(promo_id: int) -> List[dict]:
    return _execute('promos', 'equivalencias', {'idPromocion': promo_id})


def queryConfiguraciones(promo_id: int) -> List[dict]:
    return _execute('promos', 'configuraciones', {'idPromocion': promo_id})


def queryPremios(promo_id: int) -> List[dict]:
    return _execute('promos', 'premios', {'idPromocion': promo_id})


def queryEtapas(promo_id: int) -> List[dict]:
    return _execute('promos', 'etapas', {'idPromocion': promo_id})


def querySegmentos(promo_id: int) -> List[dict]:
    return _execute('promos', 'segmentos', {'idPromocion': promo_id})


def queryMultiplicadorSeg(segment_id: int) -> List[dict]:
    return _execute('promos', 'multiplicadorSeg', {'idSegmento': segment_id})


def queryEquivalenciasSeg(segment_id: int) -> List[dict]:
    return _execute('promos', 'equivalenciasSeg', {'idSegmento': segment_id})


def queryConfiguracionesSeg(segment_id: int) -> List[dict]:
    return _execute('promos', 'configuracionesSeg', {'idSegmento': segment_id})


def queryPremiosSeg(segment_id: int) -> List[dict]:
    return _execute('promos', 'premiosSeg', {'idSegmento': segment_id})


def queryEtapasSeg(segment_id: int) -> List[dict]:
    return _execute('promos', 'etapasSeg', {'idSegmento': segment_id})
