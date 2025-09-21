"""
SQL query templates replicated from the TypeScript project.

The queries are defined as functions taking a single ``schema``
argument.  Although the original TypeScript code accepted a
``schema`` parameter, none of the templates actually substitute the
argument into the SQL.  The functions are therefore present for
API‑compatibility and to mirror the original structure.

Each function returns a raw SQL string containing named parameters
(``@idPromocion`` or ``@idSegmento``).  When using ``pymssql`` these
should be converted to ``%(idPromocion)s`` style by the query
implementation; see ``validacion_brief.infra.db.mssql``.
"""

from __future__ import annotations

from typing import Callable, Dict


def multiplicador(schema: str) -> str:
    return """
        SELECT e.id_ejecucion_segmento, e.id_promocion, e.id_premio,
               e.id_calendario_multiplicador, e.id_equivalencia_Puntaje,
               e.nombre_segmento, m.valor_multiplicador,
               m.fecha_hora_inicio, m.fecha_hora_fin
        FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
        JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_cronograma_multiplicador] m
          ON e.id_calendario_multiplicador = m.id_calendario
        WHERE e.id_promocion = @idPromocion;
    """.strip()


def equivalencias(schema: str) -> str:
    return """
        SELECT e.id_ejecucion_segmento, e.id_promocion, e.id_equivalencia_Puntaje,
               e.nombre_segmento, eq.condicion_minima, eq.condicion_maxima, eq.valor_puntaje
        FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
        JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_Equivalencia_Puntaje_Detalle] eq
          ON e.id_equivalencia_Puntaje = eq.id_equivalencia_puntaje
        WHERE e.id_promocion = @idPromocion;
    """.strip()


def configuraciones(schema: str) -> str:
    return """
        SELECT e.id_ejecucion_segmento, e.id_promocion, e.nombre_segmento,
               cf.id_configuracion_detalle, cf.codigo_compuesto,
               cf.nombre, cf.valor_entero
        FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
        JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_configuracion_detalle] cf
          ON e.id_configuracion = cf.id_configuracion
        WHERE e.id_promocion = @idPromocion;
    """.strip()


def premios(schema: str) -> str:
    return """
        SELECT e.id_ejecucion_segmento, e.id_promocion, e.id_premio,
               p.condicion_minima, p.condicion_maxima, p.valor_premio, p.cantidad_ganadores
        FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
        JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_premio_detalle] p
          ON e.id_premio = p.id_premio
        WHERE e.id_promocion = @idPromocion
        ORDER BY p.valor_premio;
    """.strip()


def etapas(schema: str) -> str:
    return """
        SELECT e.id_ejecucion_segmento, e.id_promocion,
               et.fecha_inicio, et.fecha_fin, et.nombre_etapa
        FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
        JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_etapa] et
          ON e.id_ejecucion_segmento = et.id_ejecucion_segmento
        WHERE e.id_promocion = @idPromocion
        ORDER BY et.fecha_inicio ASC;
    """.strip()


def segmentos(schema: str) -> str:
    return """
        SELECT id_ejecucion_segmento, nombre_segmento
        FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento]
        WHERE id_promocion = @idPromocion
          and fecha_inicio <= GETDATE()
          AND fecha_fin >= GETDATE();
    """.strip()


def multiplicadorSeg(schema: str) -> str:
    return """
        SELECT e.id_ejecucion_segmento, e.id_promocion, e.id_premio,
               e.id_calendario_multiplicador, e.id_equivalencia_Puntaje,
               e.nombre_segmento, m.valor_multiplicador,
               m.fecha_hora_inicio, m.fecha_hora_fin
        FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
        JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_cronograma_multiplicador] m
          ON e.id_calendario_multiplicador = m.id_calendario
        WHERE e.id_ejecucion_segmento = @idSegmento;
    """.strip()


def equivalenciasSeg(schema: str) -> str:
    return """
        SELECT e.id_ejecucion_segmento, e.id_promocion, e.id_equivalencia_Puntaje,
               e.nombre_segmento, eq.condicion_minima, eq.condicion_maxima, eq.valor_puntaje
        FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
        JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_Equivalencia_Puntaje_Detalle] eq
          ON e.id_equivalencia_Puntaje = eq.id_equivalencia_puntaje
        WHERE e.id_ejecucion_segmento = @idSegmento;
    """.strip()


def configuracionesSeg(schema: str) -> str:
    return """
        SELECT e.id_ejecucion_segmento, e.id_promocion, e.nombre_segmento,
               cf.id_configuracion_detalle, cf.codigo_compuesto,
               cf.nombre, cf.valor_entero
        FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
        JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_configuracion_detalle] cf
          ON e.id_configuracion = cf.id_configuracion
        WHERE e.id_ejecucion_segmento = @idSegmento;
    """.strip()


def premiosSeg(schema: str) -> str:
    return """
        SELECT e.id_ejecucion_segmento, e.id_promocion, e.id_premio,
               p.condicion_minima, p.condicion_maxima, p.valor_premio, p.cantidad_ganadores
        FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
        JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_premio_detalle] p
          ON e.id_premio = p.id_premio
        WHERE e.id_ejecucion_segmento = @idSegmento
        ORDER BY p.valor_premio;
    """.strip()


def etapasSeg(schema: str) -> str:
    return """
        SELECT e.id_ejecucion_segmento, e.id_promocion,
               et.fecha_inicio, et.fecha_fin, et.nombre_etapa
        FROM [bd_promocion_ejecucion].[sch_promocion].[trx_ejecucion_segmento] e
        JOIN [bd_promocion_ejecucion].[sch_configuracion].[cfg_etapa] et
          ON e.id_ejecucion_segmento = et.id_ejecucion_segmento
        WHERE e.id_ejecucion_segmento = @idSegmento
        ORDER BY et.fecha_inicio ASC;
    """.strip()


def fechasMesas(schema: str) -> str:
    return """
        SELECT TOP (1000) [id],
               [id_torneo],
               [nombre_promocion],
               [fecha_inicio_torneo] AS inicio,
               [fecha_fin_torneo]   AS fin
        FROM [Mesas].[dbo].[s_torneo_mesas.configuracion]
        WHERE fecha_inicio_torneo >= GETDATE()
          AND id_torneo = 1;
    """.strip()


# Aggregate the functions in a dictionary to mirror the original TypeScript
# ``queries`` export.  This allows code to reference
# ``queries.multiplicador(...)`` etc.
queries: Dict[str, Callable[[str], str]] = {
    "multiplicador": multiplicador,
    "equivalencias": equivalencias,
    "configuraciones": configuraciones,
    "premios": premios,
    "etapas": etapas,
    "segmentos": segmentos,
    "multiplicadorSeg": multiplicadorSeg,
    "equivalenciasSeg": equivalenciasSeg,
    "configuracionesSeg": configuracionesSeg,
    "premiosSeg": premiosSeg,
    "etapasSeg": etapasSeg,
    "fechasMesas": fechasMesas,
}
