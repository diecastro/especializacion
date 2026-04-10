-- =============================================================================
-- Inventarios 360 — Reglas de negocio
-- Esquema destino: dw (funciones reutilizables por ETL y consultas SQL)
-- =============================================================================
--
-- UMBRALES VIGENTES
-- Modificar estas constantes si cambian los criterios de negocio.
-- Los mismos valores deben mantenerse sincronizados en src/etl/transform.py.
--
--   CRITICO          : 0 <= dias_para_vencimiento <= 15
--   PROXIMO A VENCER : 16 <= dias_para_vencimiento <= 30
--   VENCIDO          : dias_para_vencimiento < 0
--   VIGENTE          : dias_para_vencimiento > 30
--   SIN FECHA        : fecha_vencimiento nula
--
-- SCORE DE RIESGO (entero 0–4):
--   +3 si vencido
--   +2 si critico
--   +1 si proximo_a_vencer
--   +1 adicional si baja_rotacion (dias_en_inventario > 90)
--
-- ÁREAS RESPONSABLES:
--   Mercadeo          → proximo_a_vencer, critico
--   Canales/Comercial → vigente + baja_rotacion
--   Sostenibilidad    → vencido
-- =============================================================================

-- ----------------------------------------------------------------------------
-- dw.clasificar_estado_inventario
-- Clasifica un registro según dias_para_vencimiento.
-- Equivalente Python: clasificar_estado() en src/etl/transform.py
-- ----------------------------------------------------------------------------
create or replace function dw.clasificar_estado_inventario(
    p_dias_para_vencimiento int
)
returns text
language plpgsql
immutable
as $$
begin
    if p_dias_para_vencimiento is null then
        return 'sin_fecha';
    end if;

    -- Vencido: ya superó la fecha de vencimiento
    if p_dias_para_vencimiento < 0 then
        return 'vencido';
    end if;

    -- Crítico: quedan 0–15 días
    if p_dias_para_vencimiento <= 15 then
        return 'critico';
    end if;

    -- Próximo a vencer: quedan 16–30 días
    if p_dias_para_vencimiento <= 30 then
        return 'proximo_a_vencer';
    end if;

    -- Vigente: más de 30 días
    return 'vigente';
end;
$$;

comment on function dw.clasificar_estado_inventario is
    'Clasifica estado de inventario por dias_para_vencimiento. '
    'Umbrales: vencido < 0, critico 0-15, proximo_a_vencer 16-30, vigente > 30. '
    'Sincronizado con UMBRAL_CRITICO=15 y UMBRAL_PROXIMO=30 en src/etl/transform.py.';

-- ----------------------------------------------------------------------------
-- dw.calcular_segmento_rotacion
-- Proxy de rotación basado en antigüedad en inventario.
-- A mayor tiempo en bodega → menor rotación.
-- ----------------------------------------------------------------------------
create or replace function dw.calcular_segmento_rotacion(
    p_dias_en_inventario int
)
returns text
language plpgsql
immutable
as $$
begin
    if p_dias_en_inventario is null or p_dias_en_inventario < 0 then
        return 'sin_dato';
    end if;

    if p_dias_en_inventario <= 30 then
        return 'alta_rotacion';
    end if;

    if p_dias_en_inventario <= 90 then
        return 'media_rotacion';
    end if;

    return 'baja_rotacion';
end;
$$;

comment on function dw.calcular_segmento_rotacion is
    'Clasifica rotación como proxy de dias_en_inventario: '
    'alta <= 30d, media <= 90d, baja > 90d. '
    'No usa la columna Rotacion del Excel (está vacía).';

-- ----------------------------------------------------------------------------
-- dw.calcular_score_riesgo
-- Score entero 0–4. Base para priorización y modelo ML.
--   +3 si vencido
--   +2 si critico
--   +1 si proximo_a_vencer
--   +1 adicional si baja_rotacion
-- ----------------------------------------------------------------------------
create or replace function dw.calcular_score_riesgo(
    p_estado_inventario  text,
    p_segmento_rotacion  text
)
returns integer
language plpgsql
immutable
as $$
declare
    v_score integer := 0;
begin
    -- Componente estado
    case p_estado_inventario
        when 'vencido'          then v_score := v_score + 3;
        when 'critico'          then v_score := v_score + 2;
        when 'proximo_a_vencer' then v_score := v_score + 1;
        else null;
    end case;

    -- Componente rotación
    if p_segmento_rotacion = 'baja_rotacion' then
        v_score := v_score + 1;
    end if;

    return v_score;
end;
$$;

comment on function dw.calcular_score_riesgo is
    'Score de riesgo entero 0-4. '
    'Composición: +3 vencido, +2 critico, +1 proximo_a_vencer, +1 baja_rotacion. '
    'Sincronizado con score_riesgo() en src/etl/transform.py y mart.ml_inventory_features.riesgo_alto.';
