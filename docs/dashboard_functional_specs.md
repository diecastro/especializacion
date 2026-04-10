# Dashboard Functional Specs

## Objetivo

Definir las especificaciones funcionales del dashboard de Inventarios 360 para consumo de negocio, con foco en:

- estado actual del inventario;
- riesgo de vencimiento;
- priorización de acciones;
- soporte a decisiones de mercadeo, canales y sostenibilidad;
- visibilidad de resultados analíticos y del modelo ML.

## Audiencia

### Stakeholders principales

- `Mercadeo`
- `Canales / Comercial`
- `Sostenibilidad`
- `Liderazgo / dirección`
- `Equipo analítico`

## Fuente de datos

El dashboard debe consumir preferiblemente desde las vistas creadas en Neon PostgreSQL:

- [sql/02_views.sql](/Users/diegocastro/Desktop/especializacion/sql/02_views.sql)

Vistas disponibles:

- `mart.v_inventory_base`
- `mart.v_inventory_estado_resumen`
- `mart.v_inventory_categoria_resumen`
- `mart.v_inventory_marca_resumen`
- `mart.v_inventory_risk_priority`
- `mart.v_inventory_expiration_calendar`
- `mart.v_inventory_ml_dataset`

## Reglas funcionales generales

- El dashboard debe usar `fecha_corte` como referencia explícita.
- Los cálculos de vencimiento y antigüedad deben venir del ETL, no recalcularse en la capa visual si ya existen en la base.
- Las visualizaciones deben responder preguntas de negocio concretas.
- Los filtros deben ser consistentes entre páginas.
- El usuario debe poder pasar de una vista resumen a una vista priorizada de detalle.

## Filtros globales

Filtros recomendados para todas las páginas:

- `fecha_corte`
- `marca`
- `categoria`
- `estado_inventario`
- `segmento_rotacion`

Filtros opcionales según página:

- `area_responsable`
- `anio_vencimiento`
- `mes_vencimiento`
- `riesgo_alto`

## Página 1 — Resumen Ejecutivo

### Objetivo funcional

Dar una foto general del inventario y del nivel de riesgo actual.

### Fuente principal

- `mart.v_inventory_estado_resumen`
- `mart.v_inventory_risk_priority`

### Visuales

#### KPIs

- unidades totales
- unidades vigentes
- unidades próximas a vencer
- unidades críticas
- unidades vencidas

#### Distribución por estado

- gráfico de barras apiladas o donut por `estado_inventario`

#### Alertas prioritarias

- tabla corta con top registros de mayor prioridad

### Preguntas de negocio

- ¿Cuál es el estado general del inventario hoy?
- ¿Qué proporción del inventario está en riesgo?
- ¿Qué casos requieren acción inmediata?

## Página 2 — Riesgo por Categoría

### Objetivo funcional

Identificar categorías con mayor volumen, mayor riesgo y mayor exposición al vencimiento.

### Fuente principal

- `mart.v_inventory_categoria_resumen`

### Visuales

- barras por `unds_totales`
- barras por `pct_unds_vencido`
- barras por `pct_unds_en_riesgo`
- heatmap categoría vs estado
- scatter de criticidad

### Métricas clave

- `unds_totales`
- `unds_vencido`
- `unds_proximo`
- `score_riesgo_promedio`
- `dias_para_vencimiento_promedio`

### Preguntas de negocio

- ¿Qué categorías concentran más inventario?
- ¿Qué categorías tienen mayor riesgo?
- ¿Qué categorías deben priorizarse comercialmente?

## Página 3 — Riesgo por Marca

### Objetivo funcional

Priorizar marcas con mayor exposición operativa o comercial.

### Fuente principal

- `mart.v_inventory_marca_resumen`
- `mart.v_inventory_base`

### Visuales

- top 10 marcas por inventario
- top 10 marcas por riesgo
- barra apilada por marca y estado
- tabla comparativa por marca

### Métricas clave

- `unds_totales`
- `pct_unds_vencido`
- `pct_unds_en_riesgo`
- `dias_en_inventario_promedio`
- `score_riesgo_promedio`

### Preguntas de negocio

- ¿Qué marcas representan mayor riesgo?
- ¿Qué marcas concentran inventario inmovilizado?
- ¿Qué marcas necesitan intervención inmediata?

## Página 4 — Priorización Operativa

### Objetivo funcional

Traducir el análisis a una cola de acción concreta.

### Fuente principal

- `mart.v_inventory_risk_priority`

### Visuales

- tabla priorizada principal
- barras por `area_responsable`
- filtros operativos por estado, categoría, marca y área

### Campos mínimos de la tabla

- `prioridad_global`
- `product_container_id`
- `marca`
- `item_id`
- `descripcion`
- `categoria`
- `contenedor`
- `unds`
- `dias_en_inventario`
- `dias_para_vencimiento`
- `estado_inventario`
- `segmento_rotacion`
- `score_riesgo`
- `area_responsable`

### Preguntas de negocio

- ¿Qué debe atenderse primero?
- ¿Qué área debe actuar sobre cada caso?
- ¿Dónde está concentrada la carga operativa?

## Página 5 — Calendario de Vencimientos

### Objetivo funcional

Visualizar la concentración temporal del riesgo.

### Fuente principal

- `mart.v_inventory_expiration_calendar`

### Visuales

- matriz de `anio_vencimiento` por `mes_vencimiento`
- barras por mes de vencimiento
- barra apilada por mes y estado

### Preguntas de negocio

- ¿En qué meses se concentra el vencimiento?
- ¿Cuál es el próximo pico de riesgo?
- ¿Qué parte de ese calendario ya está comprometida?

## Página 6 — Analítica / ML

### Objetivo funcional

Explicar el riesgo y monitorear la utilidad del modelo.

### Fuente principal

- `mart.v_inventory_ml_dataset`
- salidas del modelo ML

### Visuales

- distribución de `riesgo_alto`
- boxplots por clase
- feature importance
- matriz de confusión
- métricas de desempeño del modelo

### Preguntas de negocio

- ¿Qué variables explican más el riesgo?
- ¿Qué tan bien está funcionando el modelo?
- ¿El score de riesgo está alineado con la realidad operativa?

## Comportamiento esperado del dashboard

- navegación simple entre páginas;
- filtros persistentes entre páginas cuando aplique;
- capacidad de exportar tablas de detalle;
- tiempo de respuesta razonable;
- consistencia visual y semántica entre indicadores.

## Reglas de diseño

- evitar visuales decorativos;
- priorizar legibilidad;
- mostrar claramente semáforos o estados;
- separar resumen ejecutivo de detalle operativo;
- usar colores consistentes para `vigente`, `proximo_a_vencer`, `critico` y `vencido`.

## Criterios de aceptación

- cada página responde una pregunta de negocio clara;
- cada visual tiene una fuente SQL definida;
- los KPIs coinciden con la lógica del ETL;
- el dashboard permite priorización real de inventario;
- el consumo por stakeholders no requiere conocimiento técnico de SQL.
