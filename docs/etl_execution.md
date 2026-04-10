# ETL Execution Steps and Dependencies

## Objetivo

Documentar cómo debe ejecutarse el ETL del proyecto Inventarios 360, qué dependencias requiere y cuál debe ser el orden de ejecución.

## Referencias obligatorias

La implementación del ETL debe seguir:

- [AGENTE_ANALITICA.md](/Users/diegocastro/Desktop/especializacion/AGENTE_ANALITICA.md)
- [INSTRUCCIONES_AGENTE.md](/Users/diegocastro/Desktop/especializacion/INSTRUCCIONES_AGENTE.md)
- [sql/01_schema.sql](/Users/diegocastro/Desktop/especializacion/sql/01_schema.sql)
- [sql/02_views.sql](/Users/diegocastro/Desktop/especializacion/sql/02_views.sql)

## Fuente de datos

Archivo principal:

- `datos_proyecto/Fechas de Vencimiento Beauty Care 1.xlsx`

Hoja principal:

- `Page1_1`

Hoja secundaria:

- `Hoja2`

`Hoja2` solo debe usarse para validación o reconciliación.

## Dependencias funcionales

### Dependencias de datos

- archivo Excel disponible localmente;
- hoja `Page1_1` accesible;
- reglas de negocio definidas para estado y riesgo.

### Dependencias técnicas

Dependencias mínimas esperadas en Python:

- `pandas`
- `numpy`
- `openpyxl`
- `sqlalchemy`
- driver PostgreSQL:
  - `psycopg` o `psycopg2`

Dependencias opcionales si el ETL incorpora validaciones o logging más estructurado:

- `python-dotenv`
- `pydantic`
- `rich`

### Dependencias de base de datos

La base Neon debe estar disponible mediante variables de entorno:

- `PGHOST`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`
- `PGSSLMODE`
- `PGCHANNELBINDING`

## Artefactos esperados

### SQL

- [sql/01_schema.sql](/Users/diegocastro/Desktop/especializacion/sql/01_schema.sql)
- [sql/02_views.sql](/Users/diegocastro/Desktop/especializacion/sql/02_views.sql)

### Código

La implementación puede vivir en una estructura similar a:

- `src/etl/`
- `src/db/`
- `proyecto_graduacion.ipynb`

## Orden de ejecución recomendado

### Paso 0 — Preparación del entorno

- instalar dependencias;
- configurar variables de entorno;
- validar acceso a Neon;
- verificar existencia del archivo fuente.

### Paso 1 — Inicializar la base

Ejecutar:

1. `sql/01_schema.sql`
2. `sql/02_views.sql`

Objetivo:

- crear esquemas;
- crear tablas base;
- crear vistas BI.

## Paso 2 — Extracción

- leer Excel desde `Page1_1`;
- normalizar nombres de columnas;
- capturar metadata de origen:
  - archivo
  - hoja
  - timestamp de carga

Salida esperada:

- DataFrame crudo listo para staging

## Paso 3 — Carga a staging

Tabla destino:

- `stg.inventory_raw`

Objetivo:

- persistir una copia casi cruda para trazabilidad;
- permitir auditoría y reproceso.

## Paso 4 — Transformación

### Tipificación

- `unds` a numérico
- `fecha_ingreso` a fecha
- `fecha_vencimiento` a fecha

### Limpieza

- ignorar `id_inventario`
- tratar nulos según política de la guía
- revisar outliers
- revisar fechas futuras
- revisar fechas inválidas

### Variables derivadas obligatorias

- `product_container_id`
- `dias_en_inventario`
- `dias_para_vencimiento`
- `estado_inventario`
- `segmento_rotacion`
- `score_riesgo`
- `mes_vencimiento`
- `anio_vencimiento`
- `row_null_pct`
- `calidad_flag` si se implementa

## Paso 5 — Validación de calidad

Validaciones mínimas:

- conteo de registros origen vs staging vs clean;
- nulos por columna antes y después;
- duplicados de `product_container_id`;
- coherencia entre `dias_para_vencimiento` y `estado_inventario`;
- revisión de rangos de `unds`;
- revisión de `fecha_ingreso` y `fecha_vencimiento`;
- reconciliación por categoría contra `Hoja2` si aporta valor.

## Paso 6 — Carga limpia

Tabla destino:

- `dw.inventory_clean`

Objetivo:

- dejar una capa limpia y analítica, lista para BI.

## Paso 7 — Carga dimensional y marts

Tablas posibles:

- `dw.dim_producto`
- `dw.dim_tiempo`
- `mart.fact_inventory_snapshot`
- `mart.ml_inventory_features`

Este paso puede ser incremental.
La prioridad inicial es asegurar `dw.inventory_clean`.

## Paso 8 — Validación post-load

- ejecutar consultas básicas sobre `dw.inventory_clean`;
- validar que las vistas de `sql/02_views.sql` respondan correctamente;
- revisar que los agregados coincidan con lo esperado;
- confirmar que el dataset ML se pueda derivar sin errores.

## Paso 9 — Consumo analítico

Consumidores previstos:

- `proyecto_graduacion.ipynb`
- dashboard BI
- proceso de entrenamiento ML

## Ejecución recomendada por capas

### Primera iteración mínima viable

1. crear esquema SQL
2. cargar `stg.inventory_raw`
3. poblar `dw.inventory_clean`
4. validar vistas BI

### Segunda iteración

1. poblar dimensiones
2. poblar fact table
3. poblar dataset ML

### Tercera iteración

1. automatizar ejecución
2. mejorar logging
3. agregar pruebas de validación

## Dependencias lógicas entre pasos

- no se puede cargar limpio sin staging;
- no se puede construir BI sin `dw.inventory_clean`;
- no se debe entrenar ML sin validar la calidad del ETL;
- no se deben consumir vistas BI sin haber cargado datos limpios.

## Criterios de éxito del ETL

- el proceso corre de forma reproducible;
- los datos quedan tipificados y trazables;
- las reglas de negocio quedan aplicadas;
- Neon queda poblado correctamente;
- las vistas BI funcionan;
- el notebook y el ML pueden consumir la salida sin transformaciones manuales ad hoc.

## Riesgos técnicos a vigilar

- falta de `openpyxl`;
- errores de tipificación de fechas;
- datos anómalos en `dias_antes_de_vencimiento`;
- duplicidad inesperada del identificador compuesto;
- problemas de conexión SSL o credenciales en Neon;
- desalineación entre reglas de negocio del ETL y consumo en dashboard o ML.
