# Plan y Prompt — Implementación del ETL

## Objetivo

Este documento define:

- un plan operativo para implementar el ETL del proyecto Inventarios 360;
- un prompt reutilizable para pedir la implementación del ETL a un agente técnico.

La implementación debe seguir obligatoriamente las guías:

- [AGENTE_ANALITICA.md](/Users/diegocastro/Desktop/especializacion/AGENTE_ANALITICA.md)
- [INSTRUCCIONES_AGENTE.md](/Users/diegocastro/Desktop/especializacion/INSTRUCCIONES_AGENTE.md)

## Plan de implementación del ETL

### 1. Objetivo del ETL

Construir un ETL reproducible que:

- lea la hoja `Page1_1` del archivo Excel fuente;
- limpie y tipifique los datos;
- derive variables críticas de inventario y vencimiento;
- cargue la información en Neon PostgreSQL;
- deje una base lista para BI y para el componente ML.

### 2. Fuente de datos

Archivo fuente:

- `datos_proyecto/Fechas de Vencimiento Beauty Care 1.xlsx`

Hoja válida para ETL:

- `Page1_1`

Hoja no principal:

- `Hoja2`

`Hoja2` solo debe usarse para validación o reconciliación, no como fuente transaccional principal.

### 3. Reglas obligatorias de negocio y preparación

Tomadas de las guías:

- usar identificador compuesto:
  - `product_container_id = item_id + "_" + contenedor`
- no usar `id_inventario` como llave;
- no confiar en `dias_antes_de_vencimiento` del archivo original;
- reconstruir `estado_inventario`;
- reconstruir `rotacion` o `segmento_rotacion`;
- calcular obligatoriamente:
  - `dias_en_inventario = fecha_corte - fecha_ingreso`
  - `dias_para_vencimiento = fecha_vencimiento - fecha_corte`

### 4. Fecha de corte

La `fecha_corte` debe ser fija y explícita para reproducibilidad.

Debe declararse al inicio del ETL y del notebook.
Si se cambia, debe quedar documentado.

### 5. Pasos operativos del ETL

#### Extracción

- leer el Excel desde `Page1_1`;
- normalizar nombres de columnas a `snake_case`;
- persistir una copia cruda en `stg.inventory_raw`.

#### Transformación

- convertir:
  - `unds` a numérico;
  - `fecha_ingreso` a fecha;
  - `fecha_vencimiento` a fecha;
- eliminar o ignorar variables irrelevantes;
- tratar nulos según la política de `AGENTE_ANALITICA.md`;
- identificar outliers y distinguir errores de variabilidad natural;
- construir:
  - `product_container_id`
  - `dias_en_inventario`
  - `dias_para_vencimiento`
  - `estado_inventario`
  - `segmento_rotacion`
  - `score_riesgo`
  - `mes_vencimiento`
  - `anio_vencimiento`
- calcular `row_null_pct`;
- generar `calidad_flag` si aplica.

#### Carga

- cargar staging en `stg.inventory_raw`;
- cargar tabla limpia en `dw.inventory_clean`;
- alimentar estructuras derivadas si aplica:
  - `dw.dim_producto`
  - `dw.dim_tiempo`
  - `mart.fact_inventory_snapshot`
  - `mart.ml_inventory_features`

### 6. Validaciones obligatorias

Antes de cerrar el ETL:

- validar tipos finales;
- validar nulos por columna antes y después;
- revisar duplicados de `product_container_id`;
- revisar fechas futuras en `fecha_ingreso`;
- revisar nulos o inconsistencias en `fecha_vencimiento`;
- verificar coherencia entre `dias_para_vencimiento` y `estado_inventario`;
- reconciliar totales por categoría contra `Hoja2` cuando sea útil;
- registrar supuestos de limpieza y reglas de negocio aplicadas.

### 7. Salidas esperadas

- ETL reproducible en código;
- carga funcional en Neon;
- datos limpios en `dw.inventory_clean`;
- base lista para vistas BI;
- base lista para dataset de ML.

### 8. Archivos esperados de implementación

Dependiendo del diseño final, la implementación puede distribuirse en:

- `proyecto_graduacion.ipynb`
- `src/etl/`
- `src/db/`
- `sql/01_schema.sql`
- `sql/02_views.sql`

## Prompt reutilizable para implementar el ETL

```text
Implementa el ETL del proyecto Inventarios 360 siguiendo estrictamente estas guías:

1. /Users/diegocastro/Desktop/especializacion/AGENTE_ANALITICA.md
2. /Users/diegocastro/Desktop/especializacion/INSTRUCCIONES_AGENTE.md

Contexto del proyecto:
- La fuente principal es /Users/diegocastro/Desktop/especializacion/datos_proyecto/Fechas de Vencimiento Beauty Care 1.xlsx
- Solo debe usarse la hoja Page1_1 como fuente principal del ETL
- Hoja2 solo puede usarse para validación o reconciliación
- La base de datos destino es Neon PostgreSQL y la conexión debe hacerse mediante variables de entorno
- El esquema SQL base ya existe en:
  - /Users/diegocastro/Desktop/especializacion/sql/01_schema.sql
  - /Users/diegocastro/Desktop/especializacion/sql/02_views.sql

Objetivo técnico:
Construir un ETL reproducible que extraiga, limpie, transforme y cargue la data del Excel hacia PostgreSQL, dejando una base lista para BI y para el modelo de ML.

Reglas obligatorias:
- Normaliza nombres de columnas a snake_case
- Usa una fecha_corte fija y explícita
- Crea el identificador compuesto:
  product_container_id = item_id + "_" + contenedor
- No uses id_inventario como llave porque está vacío
- No confíes en dias_antes_de_vencimiento del Excel; recalculalo desde cero
- Reconstruye estado_inventario con reglas de negocio configurables
- Reconstruye segmento_rotacion usando dias_en_inventario como proxy
- Calcula obligatoriamente:
  - dias_en_inventario = fecha_corte - fecha_ingreso
  - dias_para_vencimiento = fecha_vencimiento - fecha_corte
- Genera también:
  - score_riesgo
  - mes_vencimiento
  - anio_vencimiento

Reglas de calidad y preparación:
- Sigue la lógica de limpieza de nulos y outliers definida en AGENTE_ANALITICA.md
- Distingue outliers por error de outliers por variabilidad natural
- Elimina o ignora variables irrelevantes y columnas no confiables
- Documenta los supuestos y decisiones de limpieza

Carga a PostgreSQL:
- Carga staging en stg.inventory_raw
- Carga limpio en dw.inventory_clean
- Si corresponde, pobla también:
  - dw.dim_producto
  - dw.dim_tiempo
  - mart.fact_inventory_snapshot
  - mart.ml_inventory_features

Entregables esperados:
- Código del ETL en archivos del proyecto
- Lógica clara y reproducible
- Validaciones de calidad
- Uso de variables de entorno para Neon, sin credenciales hardcodeadas
- Integración con proyecto_graduacion.ipynb cuando sea necesario para demostración o validación

Antes de terminar:
- verifica tipos finales
- verifica duplicados de product_container_id
- verifica coherencia entre dias_para_vencimiento y estado_inventario
- deja claro cómo ejecutar el ETL

No hagas una propuesta abstracta. Implementa los archivos necesarios en el proyecto.
```

## Uso recomendado

Usar este prompt cuando el siguiente paso sea construir el ETL real en código, no solo diseñarlo.
