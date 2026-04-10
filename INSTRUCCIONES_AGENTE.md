# Instrucciones Para El Agente

## Propósito del proyecto

Este proyecto corresponde al trabajo de grado **"Inventarios 360: Inteligencia de Negocios Aplicada para la optimización de inventarios de cosméticos"**.

El objetivo ya no es solo explorar un archivo en un notebook.
El objetivo ahora es construir un **proyecto completo de Business Intelligence** que incluya:

- extracción, transformación y carga de datos desde Excel;
- almacenamiento estructurado en PostgreSQL;
- modelo analítico para reporting;
- visualización y KPIs de negocio;
- un componente de machine learning alineado con el problema principal;
- documentación suficiente para sustentar académica y técnicamente la solución.

## Meta principal de negocio

La solución debe ayudar a la empresa a tomar decisiones sobre inventario cosmético, especialmente en relación con:

- productos con baja rotación;
- inventario próximo a vencerse;
- inventario vencido;
- priorización de acciones comerciales, logísticas y de sostenibilidad;
- reducción de pérdidas por vencimiento;
- mejor visibilidad del estado real del inventario.

## Alcance actualizado

El proyecto debe quedar concebido como una solución BI end-to-end.

### Componentes obligatorios

1. **Fuente de datos**
   Archivo Excel principal:
   - `datos_proyecto/Fechas de Vencimiento Beauty Care 1.xlsx`

2. **ETL**
   Flujo para:
   - leer la fuente;
   - limpiar los datos;
   - tipificar columnas;
   - derivar variables;
   - validar calidad;
   - dejar datasets listos para análisis y carga.

3. **Base de datos PostgreSQL**
   Persistir la información limpia y modelada en PostgreSQL.

4. **Modelo analítico**
   Diseñar una estructura útil para BI, idealmente con enfoque dimensional.

5. **Machine Learning**
   Crear un modelo que apoye el objetivo de negocio.

6. **Notebook principal**
   `proyecto_graduacion.ipynb` debe integrarse como pieza central de análisis, demostración o prototipo.

## Restricción actualizada sobre archivos

La regla anterior de trabajar solo dentro de `proyecto_graduacion.ipynb` ya no aplica como restricción absoluta.

Ahora:

- `proyecto_graduacion.ipynb` sigue siendo un entregable principal;
- pero el agente **sí puede crear archivos adicionales** si son necesarios para un proyecto BI serio y mantenible.

Ejemplos válidos:

- scripts ETL;
- archivos SQL;
- documentación;
- configuración de conexión;
- artefactos del modelo ML;
- utilidades de carga a PostgreSQL.

La prioridad es una arquitectura clara y defendible, no forzar todo en un solo notebook.

## Fuentes revisadas

### Documentos de negocio y proyecto

- `Inventarios360_Anteproyecto.pdf`
- `Inventarios360_Presentacion.pptx`
- `Formato Información Básica Trabajo de Grado Especialización en Inteligencia de Negocios 1.xlsx`

### Fuente de datos activa

- `datos_proyecto/Fechas de Vencimiento Beauty Care 1.xlsx`

### Ejemplos de referencia

La carpeta `ejemplos/` puede usarse como guía de estructura, narrativa y organización, pero no como restricción técnica.

## Fuente de datos actual

### Archivo principal

`datos_proyecto/Fechas de Vencimiento Beauty Care 1.xlsx`

### Hojas detectadas

- `Page1_1`: hoja principal del inventario.
- `Hoja2`: resumen agregado por categoría.

### Perfilado rápido de `Page1_1`

La hoja principal contiene aproximadamente:

- `15950` filas;
- `12` columnas.

Columnas detectadas:

- `Marca`
- `Item ID`
- `ID Inventario`
- `Descripción`
- `Categoría`
- `Unds`
- `Fecha de Ingreso`
- `Fecha Vencimiento`
- `Dias antes de Vencimiento`
- `Contenedor`
- `Rotación`
- `Estado`

### Observaciones importantes del perfilado

- `ID Inventario` está completamente vacío en la versión actual.
- `Rotación` está vacía.
- `Estado` está casi completamente vacío.
- `Fecha Vencimiento` tiene pocos nulos.
- `Dias antes de Vencimiento` presenta al menos valores claramente anómalos y debe validarse.
- `Hoja2` parece ser una tabla pivote de validación, no la base transaccional principal.

Esto implica que parte de la inteligencia de negocio tendrá que construirse en la transformación y no simplemente leerse del archivo origen.

## Dirección correcta del proyecto

El agente debe trabajar bajo la idea de que este proyecto no es solo descriptivo.
Debe producir una solución en capas:

1. **Capa de ingestión**
   Leer Excel de forma controlada.

2. **Capa de limpieza y transformación**
   Estandarizar columnas, tipos y reglas.

3. **Capa de almacenamiento**
   Persistir datos procesados en PostgreSQL.

4. **Capa analítica**
   Construir KPIs, segmentaciones y vistas útiles.

5. **Capa predictiva o de apoyo a decisión**
   Implementar ML alineado con el riesgo o la priorización del inventario.

6. **Capa de consumo**
   Notebook, SQL analítico y futura base para Power BI o dashboard equivalente.

## Objetivo del ETL

El ETL debe convertir la fuente Excel en una base confiable para BI.

### Tareas mínimas del ETL

- leer la hoja `Page1_1`;
- limpiar nombres de columnas;
- convertir `Unds` a numérico;
- convertir `Fecha de Ingreso` y `Fecha Vencimiento` a tipo fecha;
- validar o recalcular `Dias antes de Vencimiento`;
- construir un identificador compuesto de trazabilidad con `Item ID + Contenedor`;
- crear una fecha de corte explícita para cálculos temporales;
- derivar `dias_en_inventario`;
- derivar `dias_para_vencimiento`;
- derivar una versión propia de `estado_inventario`;
- derivar una versión propia de `rotacion` o criticidad si aplica;
- registrar nulos, inconsistencias y supuestos;
- generar una salida limpia para carga a PostgreSQL.

### Reglas recomendadas a derivar

Definir reglas explícitas y documentadas para etiquetas como:

- `vigente`
- `proximo_a_vencer`
- `vencido`
- `critico`

Los umbrales deben quedar visibles y modificables.

### Regla nueva de identificación

Para este proyecto, el identificador operativo del registro no debe apoyarse solo en `Item ID`.

Debe construirse una clave compuesta con:

- `Item ID`
- `Contenedor`

Sugerencia de implementación:

- `product_container_id = Item ID + "_" + Contenedor`

Esta clave debe usarse al menos dentro del notebook y, si aporta valor al modelo de datos, también dentro de la capa transformada o persistida.

### Regla nueva de antigüedad en inventario

Uno de los cálculos base del proyecto debe ser la antigüedad del inventario.

La fórmula requerida es:

- `dias_en_inventario = fecha_corte - Fecha de Ingreso`

Donde:

- `fecha_corte` debe ser explícita;
- y preferiblemente fija para garantizar reproducibilidad en análisis y modelo.

Sugerencia práctica:

- usar una variable como `fecha_corte`;
- calcular `dias_en_inventario` en días;
- evitar depender directamente de la fecha del sistema sin documentarla.

Este cálculo debe ser obligatorio dentro de `proyecto_graduacion.ipynb`, porque será una base para:

- análisis de permanencia del inventario;
- evaluación de riesgo;
- decisiones de priorización;
- y soporte al componente de machine learning.

## PostgreSQL

La solución debe usar PostgreSQL como base persistente del proyecto.
El motor objetivo será **Neon PostgreSQL**.

### Objetivo de la base

PostgreSQL debe servir para:

- almacenar la data limpia;
- soportar consultas analíticas;
- organizar el modelo dimensional;
- dejar una base conectable a herramientas BI.

### Expectativa mínima de diseño

Se espera al menos una de estas dos aproximaciones:

1. una tabla limpia principal con vistas analíticas;
2. un modelo dimensional con hechos y dimensiones.

La segunda opción es preferible si el tiempo y la data lo permiten.

### Diseño sugerido

#### Tabla o hecho principal

Una tabla de inventario con granularidad por registro o contenedor.

#### Dimensiones sugeridas

- dimensión producto;
- dimensión marca;
- dimensión categoría;
- dimensión tiempo;
- dimensión estado o criticidad, si se modela explícitamente.

#### Campos derivados esperados

- `product_container_id`
- `unds`
- `fecha_ingreso`
- `fecha_vencimiento`
- `dias_en_inventario`
- `dias_para_vencimiento`
- `estado_inventario`
- `segmento_rotacion`
- `score_riesgo` si se construye

### Regla de implementación

La carga a PostgreSQL debe ser reproducible.
No dejar procesos manuales ambiguos.
Debe quedar claro:

- cómo se conecta;
- qué tablas crea;
- qué proceso inserta o actualiza la información.

### Configuración de conexión

La conexión a PostgreSQL debe manejarse mediante variables de entorno y no con credenciales hardcodeadas en notebooks, scripts o documentación versionada.

Variables esperadas:

- `PGHOST`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`
- `PGSSLMODE`
- `PGCHANNELBINDING`

Como el entorno objetivo es Neon:

- `PGSSLMODE` debe permanecer en `require`;
- `PGCHANNELBINDING` debe permanecer en `require`.

La implementación debe asumir que estas variables existen en el entorno antes de ejecutar la carga o las consultas.

### Reglas de seguridad

- No escribir credenciales reales dentro de archivos tracked del proyecto.
- No dejar contraseñas visibles en notebooks.
- Si se requiere un archivo local de configuración, usar un `.env` local ignorado por git.
- Si se crea documentación de conexión, usar únicamente nombres de variables de entorno o placeholders.

## Machine Learning

El proyecto ahora **sí incluye ML**.
Ese componente debe apoyar el objetivo del negocio, no ser un agregado decorativo.

### Principio rector

El modelo debe responder una pregunta útil para la toma de decisiones.

### Opciones válidas

Las opciones más coherentes con la fuente actual son:

1. **Clasificación de riesgo de inventario**
   Predecir o clasificar si un registro es `bajo`, `medio` o `alto` riesgo según vencimiento, antigüedad, volumen y otras variables derivadas.

2. **Clasificación de estado del inventario**
   Modelar una versión supervisada o semisupervisada de `estado_inventario` si se construye una etiqueta robusta.

3. **Segmentación no supervisada**
   Agrupar productos o registros para identificar patrones de criticidad o comportamiento del inventario.

4. **Modelo de priorización**
   Construir un score predictivo o analítico para ordenar inventario según urgencia de acción.

### Recomendación práctica

Si la etiqueta de negocio puede construirse con reglas claras, una buena estrategia es:

- crear primero una etiqueta derivada;
- entrenar luego un modelo de clasificación interpretable;
- usar el modelo como apoyo a priorización y explicación.

### Modelos preferidos

Priorizar modelos defendibles e interpretables, por ejemplo:

- Logistic Regression
- Decision Tree
- Random Forest
- XGBoost o LightGBM solo si aporta valor real y se puede justificar
- KMeans si se hace segmentación no supervisada

### No hacer

- No usar deep learning sin justificación fuerte.
- No usar modelos complejos solo por impresionar.
- No entrenar un modelo sin definir antes la pregunta de negocio.

## KPIs esperados

El proyecto debe construir KPIs útiles para BI.

### KPIs operativos mínimos

- unidades totales en inventario;
- unidades por marca;
- unidades por categoría;
- unidades por producto;
- antigüedad del inventario por registro;
- antigüedad promedio por producto;
- antigüedad promedio por marca;
- antigüedad promedio por categoría;
- número de ítems únicos;
- número de contenedores;
- inventario vigente;
- inventario próximo a vencer;
- inventario vencido;
- porcentaje próximo a vencer;
- porcentaje vencido;
- antigüedad promedio del inventario;
- días promedio para vencimiento;
- top categorías de mayor riesgo;
- top marcas de mayor riesgo.

### KPIs analíticos avanzados

- score de riesgo por registro o producto;
- concentración de riesgo por categoría;
- exposición por volumen y vencimiento;
- segmentación ABC o Pareto si resulta útil;
- prioridad de acción por área.

## Reglas de negocio

El agente debe dejar reglas claras, medibles y auditables.

Ejemplos:

- `Mercadeo`: inventario próximo a vencer que requiere campañas.
- `Canales/Comercial`: inventario de baja rotación o alto volumen inmovilizado.
- `Sostenibilidad`: inventario vencido o con alto riesgo de pérdida.

No dejar reglas implícitas.
Toda regla debe quedar documentada.

## Rol de `proyecto_graduacion.ipynb`

El notebook principal debe servir como:

- demostración del flujo analítico;
- validación del ETL;
- exploración y profiling;
- análisis de KPIs;
- experimentación y presentación del modelo ML;
- explicación de hallazgos.

No es obligatorio que toda la lógica viva ahí.
Pero sí debe quedar como pieza central de lectura y sustentación.

## Organización técnica recomendada

El agente puede proponer y crear una estructura como esta, o una equivalente:

- `proyecto_graduacion.ipynb`
- `src/etl/`
- `src/db/`
- `src/features/`
- `src/models/`
- `sql/`
- `docs/`

La estructura final debe ser simple, razonable y mantenible.

## Estilo de implementación

El proyecto debe verse como un trabajo de BI serio:

- código limpio;
- pasos reproducibles;
- convenciones claras;
- documentación suficiente;
- supuestos explícitos;
- resultados interpretables;
- foco en valor de negocio.

## Límites

- No inventar datos faltantes como si fueran reales.
- No asumir que columnas vacías del Excel son confiables.
- No dejar dependencias críticas ocultas.
- No diseñar una arquitectura innecesariamente compleja.
- No construir un modelo ML desconectado del problema de inventario.

## Resultado esperado final

Al avanzar este proyecto, se espera terminar con:

- un proceso ETL reproducible desde el Excel;
- datos limpios y tipificados;
- carga funcional a PostgreSQL;
- estructura analítica útil para BI;
- KPIs de inventario y vencimiento;
- reglas de negocio accionables;
- notebook principal bien documentado;
- un modelo ML útil para priorizar o clasificar riesgo de inventario;
- base sólida para dashboard en Power BI o herramienta equivalente.

## Regla de decisión para futuras modificaciones

Si surge duda sobre si algo debe incluirse o no, aplicar esta regla:

**incluir solo aquello que acerque el proyecto a una solución BI end-to-end, reproducible y defendible, orientada a optimizar inventarios de cosméticos mediante ETL, PostgreSQL, analítica de negocio y ML.**
