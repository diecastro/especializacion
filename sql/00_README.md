# SQL — Inventarios 360

Esquema PostgreSQL del proyecto de BI para optimización de inventarios cosméticos.
Motor objetivo: **Neon PostgreSQL**.

## Arquitectura de esquemas

```
stg   → staging (datos crudos del Excel, sin transformar)
dw    → data warehouse (datos limpios, dimensiones, funciones)
mart  → data mart (hechos, KPIs, vistas analíticas, features ML)
```

## Archivos y orden de ejecución

| # | Archivo | Propósito |
|---|---------|-----------|
| 01 | `01_schema.sql` | Crea los esquemas `stg`, `dw`, `mart` y todas sus tablas e índices |
| 02 | `02_views.sql` | Vistas analíticas en `mart.*` para Power BI y consultas ad hoc |
| 03 | `03_reglas_negocio.sql` | Funciones PL/pgSQL en `dw.*`: clasificación de estado, segmento rotación y score de riesgo |
| 04 | `04_populate_dim_tiempo.sql` | Pobla `dw.dim_tiempo` con rango 2015–2030 (idempotente) |

> La carga de datos desde Python se realiza en `src/etl/` y `src/db/loader.py`.

## Tablas principales

### stg (staging)
| Tabla | Descripción |
|-------|-------------|
| `stg.inventory_raw` | Datos crudos tal como llegan del Excel |

### dw (warehouse)
| Tabla | Descripción |
|-------|-------------|
| `dw.inventory_clean` | Datos limpios y enriquecidos con variables derivadas |
| `dw.dim_producto` | Dimensión de productos (item_id + descripción + categoría + marca) |
| `dw.dim_tiempo` | Dimensión de fechas 2015–2030 |

### mart (data mart)
| Tabla / Vista | Descripción |
|---------------|-------------|
| `mart.fact_inventory_snapshot` | Tabla de hechos principal |
| `mart.inventory_kpi_by_categoria` | KPIs agregados por categoría |
| `mart.ml_inventory_features` | Features preparadas para el modelo ML |
| `mart.v_inventory_base` | Vista base desnormalizada (incluye `riesgo_alto`) |
| `mart.v_inventory_estado_resumen` | Resumen por estado del inventario |
| `mart.v_inventory_categoria_resumen` | KPIs por categoría |
| `mart.v_inventory_marca_resumen` | KPIs por marca |
| `mart.v_inventory_risk_priority` | Ranking de prioridad global por riesgo |
| `mart.v_inventory_expiration_calendar` | Calendario de vencimientos |
| `mart.v_inventory_ml_dataset` | Dataset listo para entrenar el modelo ML |

## Reglas de negocio (umbrales)

| Estado | Condición | Área responsable |
|--------|-----------|-----------------|
| `vencido` | `dias_para_vencimiento < 0` | Sostenibilidad |
| `critico` | `0 – 15 días` | Mercadeo |
| `proximo_a_vencer` | `16 – 30 días` | Mercadeo |
| `vigente` | `> 30 días` | Operaciones |
| `sin_fecha` | fecha nula | Revisar fuente |

**Score de riesgo (entero 0–4):** +3 vencido · +2 crítico · +1 próximo · +1 baja rotación

Para cambiar umbrales: editar `03_reglas_negocio.sql` **y** `src/etl/transform.py` (mantener sincronizados).

## Cómo ejecutar

```bash
psql $DATABASE_URL -f sql/01_schema.sql
psql $DATABASE_URL -f sql/02_views.sql
psql $DATABASE_URL -f sql/03_reglas_negocio.sql
psql $DATABASE_URL -f sql/04_populate_dim_tiempo.sql
```

O desde Python (ver `src/db/loader.py`):

```python
from src.db.loader import SchemaLoader
SchemaLoader().run_all()
```
