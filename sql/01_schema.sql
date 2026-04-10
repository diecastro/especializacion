create schema if not exists stg;
create schema if not exists dw;
create schema if not exists mart;


create table if not exists stg.inventory_raw (
    load_id bigserial primary key,
    source_file text not null,
    source_sheet text not null default 'Page1_1',
    loaded_at timestamptz not null default now(),

    marca text,
    item_id text,
    id_inventario text,
    descripcion text,
    categoria text,
    unds_raw text,
    fecha_ingreso_raw text,
    fecha_vencimiento_raw text,
    dias_antes_vencimiento_raw text,
    contenedor text,
    rotacion_raw text,
    estado_raw text
);


create table if not exists dw.inventory_clean (
    inventory_sk bigserial primary key,
    product_container_id text not null,
    source_file text not null,
    fecha_corte date not null,

    marca text,
    item_id text,
    descripcion text,
    categoria text,
    contenedor text,

    unds numeric(18,2),
    fecha_ingreso date,
    fecha_vencimiento date,

    dias_en_inventario integer,
    dias_para_vencimiento integer,

    estado_inventario text,
    segmento_rotacion text,
    score_riesgo integer,

    mes_vencimiento integer,
    anio_vencimiento integer,

    row_null_pct numeric(5,2),
    calidad_flag text,
    created_at timestamptz not null default now(),

    constraint uq_inventory_clean_product_container_fecha
        unique (product_container_id, fecha_corte)
);


create table if not exists dw.dim_producto (
    producto_sk bigserial primary key,
    item_id text not null,
    descripcion text,
    categoria text,
    marca text,
    constraint uq_dim_producto unique (item_id, descripcion, categoria, marca)
);


create table if not exists dw.dim_tiempo (
    tiempo_sk bigserial primary key,
    fecha date not null unique,
    anio integer not null,
    mes integer not null,
    dia integer not null,
    trimestre integer not null
);


create table if not exists mart.fact_inventory_snapshot (
    snapshot_sk bigserial primary key,
    fecha_corte date not null,
    product_container_id text not null,

    producto_sk bigint references dw.dim_producto(producto_sk),
    tiempo_ingreso_sk bigint references dw.dim_tiempo(tiempo_sk),
    tiempo_vencimiento_sk bigint references dw.dim_tiempo(tiempo_sk),

    unds numeric(18,2),
    dias_en_inventario integer,
    dias_para_vencimiento integer,
    score_riesgo integer,

    estado_inventario text,
    segmento_rotacion text,

    created_at timestamptz not null default now(),
    constraint uq_fact_inventory_snapshot unique (product_container_id, fecha_corte)
);


create table if not exists mart.inventory_kpi_by_categoria (
    fecha_corte date not null,
    categoria text not null,
    unds_totales numeric(18,2),
    unds_vigente numeric(18,2),
    unds_proximo numeric(18,2),
    unds_critico numeric(18,2),
    unds_vencido numeric(18,2),
    dias_en_inventario_promedio numeric(18,2),
    dias_para_vencimiento_promedio numeric(18,2),
    score_riesgo_promedio numeric(18,2),
    created_at timestamptz not null default now(),
    primary key (fecha_corte, categoria)
);


create table if not exists mart.ml_inventory_features (
    feature_sk bigserial primary key,
    fecha_corte date not null,
    product_container_id text not null,

    item_id text,
    marca text,
    categoria text,
    unds numeric(18,2),
    dias_en_inventario integer,
    dias_para_vencimiento integer,
    mes_vencimiento integer,
    anio_vencimiento integer,
    estado_inventario text,
    segmento_rotacion text,
    score_riesgo integer,
    riesgo_alto integer,

    created_at timestamptz not null default now(),
    constraint uq_ml_inventory_features unique (product_container_id, fecha_corte)
);


create index if not exists idx_inventory_clean_categoria
    on dw.inventory_clean (categoria);

create index if not exists idx_inventory_clean_marca
    on dw.inventory_clean (marca);

create index if not exists idx_inventory_clean_estado
    on dw.inventory_clean (estado_inventario);

create index if not exists idx_inventory_clean_score
    on dw.inventory_clean (score_riesgo);

create index if not exists idx_inventory_clean_vencimiento
    on dw.inventory_clean (fecha_vencimiento);

create index if not exists idx_inventory_clean_product_container
    on dw.inventory_clean (product_container_id);
