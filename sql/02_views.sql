create or replace view mart.v_inventory_base as
select
    ic.inventory_sk,
    ic.product_container_id,
    ic.source_file,
    ic.fecha_corte,
    ic.marca,
    ic.item_id,
    ic.descripcion,
    ic.categoria,
    ic.contenedor,
    ic.unds,
    ic.fecha_ingreso,
    ic.fecha_vencimiento,
    ic.dias_en_inventario,
    ic.dias_para_vencimiento,
    ic.estado_inventario,
    ic.segmento_rotacion,
    ic.score_riesgo,
    ic.mes_vencimiento,
    ic.anio_vencimiento,
    ic.row_null_pct,
    ic.calidad_flag,
    ic.created_at,
    case
        when ic.estado_inventario in ('vencido', 'critico') then 1
        else 0
    end as riesgo_alto
from dw.inventory_clean ic;


create or replace view mart.v_inventory_estado_resumen as
select
    fecha_corte,
    estado_inventario,
    count(*) as registros,
    count(distinct product_container_id) as contenedores_unicos,
    sum(unds) as unds_totales,
    avg(dias_en_inventario) as dias_en_inventario_promedio,
    avg(dias_para_vencimiento) as dias_para_vencimiento_promedio,
    avg(score_riesgo) as score_riesgo_promedio
from mart.v_inventory_base
group by fecha_corte, estado_inventario;


create or replace view mart.v_inventory_categoria_resumen as
select
    fecha_corte,
    categoria,
    count(*) as registros,
    count(distinct item_id) as items_unicos,
    count(distinct product_container_id) as contenedores_unicos,
    sum(unds) as unds_totales,
    sum(case when estado_inventario = 'vigente' then unds else 0 end) as unds_vigente,
    sum(case when estado_inventario = 'proximo_a_vencer' then unds else 0 end) as unds_proximo,
    sum(case when estado_inventario = 'critico' then unds else 0 end) as unds_critico,
    sum(case when estado_inventario = 'vencido' then unds else 0 end) as unds_vencido,
    avg(dias_en_inventario) as dias_en_inventario_promedio,
    avg(dias_para_vencimiento) as dias_para_vencimiento_promedio,
    avg(score_riesgo) as score_riesgo_promedio,
    100.0 * sum(case when estado_inventario = 'vencido' then unds else 0 end)
        / nullif(sum(unds), 0) as pct_unds_vencido,
    100.0 * sum(case when estado_inventario in ('proximo_a_vencer', 'critico') then unds else 0 end)
        / nullif(sum(unds), 0) as pct_unds_en_riesgo
from mart.v_inventory_base
group by fecha_corte, categoria;


create or replace view mart.v_inventory_marca_resumen as
select
    fecha_corte,
    marca,
    count(*) as registros,
    count(distinct item_id) as items_unicos,
    count(distinct product_container_id) as contenedores_unicos,
    sum(unds) as unds_totales,
    sum(case when estado_inventario = 'vigente' then unds else 0 end) as unds_vigente,
    sum(case when estado_inventario = 'proximo_a_vencer' then unds else 0 end) as unds_proximo,
    sum(case when estado_inventario = 'critico' then unds else 0 end) as unds_critico,
    sum(case when estado_inventario = 'vencido' then unds else 0 end) as unds_vencido,
    avg(dias_en_inventario) as dias_en_inventario_promedio,
    avg(dias_para_vencimiento) as dias_para_vencimiento_promedio,
    avg(score_riesgo) as score_riesgo_promedio,
    100.0 * sum(case when estado_inventario = 'vencido' then unds else 0 end)
        / nullif(sum(unds), 0) as pct_unds_vencido,
    100.0 * sum(case when estado_inventario in ('proximo_a_vencer', 'critico') then unds else 0 end)
        / nullif(sum(unds), 0) as pct_unds_en_riesgo
from mart.v_inventory_base
group by fecha_corte, marca;


create or replace view mart.v_inventory_risk_priority as
select
    fecha_corte,
    product_container_id,
    marca,
    item_id,
    descripcion,
    categoria,
    contenedor,
    unds,
    fecha_ingreso,
    fecha_vencimiento,
    dias_en_inventario,
    dias_para_vencimiento,
    estado_inventario,
    segmento_rotacion,
    score_riesgo,
    case
        when estado_inventario = 'vencido' then 'sostenibilidad'
        when estado_inventario in ('critico', 'proximo_a_vencer') then 'mercadeo'
        when segmento_rotacion = 'baja_rotacion' then 'canales'
        else 'monitoreo'
    end as area_responsable,
    row_number() over (
        partition by fecha_corte
        order by score_riesgo desc nulls last,
                 unds desc nulls last,
                 dias_para_vencimiento asc nulls last
    ) as prioridad_global
from mart.v_inventory_base;


create or replace view mart.v_inventory_expiration_calendar as
select
    fecha_corte,
    anio_vencimiento,
    mes_vencimiento,
    estado_inventario,
    count(*) as registros,
    count(distinct product_container_id) as contenedores_unicos,
    sum(unds) as unds_totales,
    avg(score_riesgo) as score_riesgo_promedio
from mart.v_inventory_base
group by
    fecha_corte,
    anio_vencimiento,
    mes_vencimiento,
    estado_inventario;


create or replace view mart.v_inventory_ml_dataset as
select
    fecha_corte,
    product_container_id,
    item_id,
    marca,
    categoria,
    unds,
    dias_en_inventario,
    dias_para_vencimiento,
    mes_vencimiento,
    anio_vencimiento,
    estado_inventario,
    segmento_rotacion,
    score_riesgo,
    riesgo_alto
from mart.v_inventory_base
where unds is not null
  and dias_en_inventario is not null
  and dias_para_vencimiento is not null;
