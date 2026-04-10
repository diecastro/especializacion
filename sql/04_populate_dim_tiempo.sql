-- =============================================================================
-- Inventarios 360 — Poblar dw.dim_tiempo
-- =============================================================================
-- Genera una fila por fecha en el rango definido.
-- Ejecutar una sola vez después de crear el esquema (idempotente por ON CONFLICT).
--
-- Rango:
--   Inicio : 2015-01-01  (cubre fechas de ingreso históricas)
--   Fin    : 2030-12-31  (cubre fechas de vencimiento futuras)
-- =============================================================================

insert into dw.dim_tiempo (fecha, anio, mes, dia, trimestre)
select
    d::date                             as fecha,
    extract(year    from d)::integer    as anio,
    extract(month   from d)::integer    as mes,
    extract(day     from d)::integer    as dia,
    extract(quarter from d)::integer    as trimestre
from generate_series(
    '2015-01-01'::date,
    '2030-12-31'::date,
    '1 day'::interval
) as t(d)
on conflict (fecha) do nothing;
