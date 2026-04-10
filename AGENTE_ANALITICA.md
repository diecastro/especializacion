# GUÍA DE ANALÍTICA DE DATOS — AGENTE INVENTARIOS 360

> **Propósito:** Esta guía condensa el conocimiento de las lecciones del curso de Minería de Datos y lo aplica al proyecto *Inventarios 360*. Es una referencia viva — orienta sin limitar. El agente debe usarla como punto de partida, adaptarla al contexto real de los datos y tomar decisiones técnicas justificadas.

---

## CONTEXTO DEL PROYECTO

El proyecto construye una solución BI end-to-end para optimizar la gestión de inventarios de productos cosméticos. La fuente de datos es un Excel de SAP con ~15.950 registros y las siguientes columnas clave:

| Columna | Notas |
|---|---|
| `Marca`, `Item ID`, `Descripción`, `Categoría` | Dimensiones del producto |
| `Unds` | Unidades — convertir a numérico |
| `Fecha de Ingreso`, `Fecha Vencimiento` | Fechas — convertir a datetime |
| `Dias antes de Vencimiento` | Recalcular desde cero — no confiar en el valor original |
| `Contenedor` | Parte del identificador compuesto |
| `Rotación`, `Estado` | Columnas vacías — construir desde reglas propias |
| `ID Inventario` | Completamente vacío — ignorar |

**Identificador operativo compuesto:**
```python
df['product_container_id'] = df['Item ID'].astype(str) + "_" + df['Contenedor'].astype(str)
```

**Fecha de corte fija para reproducibilidad:**
```python
fecha_corte = pd.Timestamp("2025-04-01")  # Ajustar según necesidad del proyecto
```

---

## FASE 1 — COMPRENSIÓN DEL NEGOCIO

> *Equivalente a Business Understanding en CRISP-DM.*

Antes de escribir una línea de código, el agente debe tener claro:

- **Pregunta de negocio:** ¿Qué productos están en riesgo de vencer sin ser vendidos?
- **Stakeholders y sus necesidades:**
  - **Mercadeo:** Inventario próximo a vencer → campañas de evacuación
  - **Canales / Comercial:** Productos de baja rotación o alto volumen inmovilizado
  - **Sostenibilidad:** Productos vencidos → disposición final
- **Criterios de éxito:** KPIs de vencimiento, rotación y riesgo definidos, cuantificables y accionables.
- **Alcance:** No incluye implementación en SAP, automatización en producción ni ML de alta complejidad sin justificación.

---

## FASE 2 — PREPARACIÓN DE DATOS (ETL)

> Esta fase sigue los 9 pasos canónicos de preparación. No saltarse ninguno.

### Paso 1 — Integración y carga

```python
import pandas as pd

df = pd.read_excel("datos_proyecto/Fechas de Vencimiento Beauty Care 1.xlsx",
                   sheet_name="Page1_1")
```

- Trabajar **solo con `Page1_1`**. `Hoja2` es un resumen agregado para referencia.
- Limpiar nombres de columnas al cargar:
```python
df.columns = (df.columns
              .str.strip()
              .str.lower()
              .str.replace(' ', '_')
              .str.replace('á','a').str.replace('é','e')
              .str.replace('í','i').str.replace('ó','o')
              .str.replace('ú','u'))
```

### Paso 2 — Eliminar variables irrelevantes y redundantes

Eliminar o no usar como features:
- `id_inventario` → completamente vacío
- Cualquier columna con >90% nulos que no sea reconstruible
- No usar nombre/descripción libre como feature numérica directamente (son texto no estructurado)

### Paso 3 — Estadística descriptiva

Siempre antes de transformar:

```python
print(df.shape)
print(df.dtypes)
print(df.isnull().sum())
print(df.describe())
df['categoria'].value_counts()
df['marca'].value_counts().head(20)
```

Visualizar distribuciones de variables numéricas con histogramas y boxplots. Documentar hallazgos.

### Paso 4 — Limpieza de outliers

Reglas de calidad explícitas (documentar umbral y decisión):

| Variable | Regla | Acción si fuera de rango |
|---|---|---|
| `unds` | > 0, sin valores negativos | Asignar NULL |
| `dias_para_vencimiento` | Recalcular desde fechas | No usar columna original |
| `fecha_ingreso` | Fecha válida, no futura | Marcar para revisión |
| `fecha_vencimiento` | Fecha válida | Nulo si inválida |

**Outliers por error → asignar NULL. Outliers por variabilidad natural → no tocar.**

### Paso 5 — Limpieza de nulos

```
- Si un REGISTRO tiene >15% de sus columnas nulas → eliminar
- Si una VARIABLE tiene >15% nulos y no es crítica → eliminar
- Si una VARIABLE tiene ≤15% nulos → imputar:
    - Numérica simétrica → media
    - Numérica asimétrica → mediana
    - Categórica → moda
    - Si es crítica y tiene >15% → modelar imputación
- Variable objetivo (estado_inventario) → NUNCA imputar; eliminar el registro
```

### Paso 6 — Conversión de tipos

```python
df['unds'] = pd.to_numeric(df['unds'], errors='coerce')
df['fecha_ingreso'] = pd.to_datetime(df['fecha_ingreso'], errors='coerce')
df['fecha_vencimiento'] = pd.to_datetime(df['fecha_vencimiento'], errors='coerce')
```

### Paso 7 — Variables derivadas (obligatorias)

```python
fecha_corte = pd.Timestamp("2025-04-01")  # FIJA Y EXPLÍCITA

# Días en inventario
df['dias_en_inventario'] = (fecha_corte - df['fecha_ingreso']).dt.days

# Días para vencimiento (positivo = aún vigente, negativo = vencido)
df['dias_para_vencimiento'] = (df['fecha_vencimiento'] - fecha_corte).dt.days

# Identificador compuesto
df['product_container_id'] = (df['item_id'].astype(str) + "_" 
                               + df['contenedor'].astype(str))
```

### Paso 8 — Reglas de negocio para `estado_inventario`

> Los umbrales son configurables — documentar y dejar como variables al tope del script.

```python
UMBRAL_PROXIMO = 30    # días para considerar "próximo a vencer"
UMBRAL_CRITICO = 15    # días para considerar "crítico"

def clasificar_estado(dias):
    if pd.isna(dias):
        return 'sin_fecha'
    elif dias < 0:
        return 'vencido'
    elif dias <= UMBRAL_CRITICO:
        return 'critico'
    elif dias <= UMBRAL_PROXIMO:
        return 'proximo_a_vencer'
    else:
        return 'vigente'

df['estado_inventario'] = df['dias_para_vencimiento'].apply(clasificar_estado)
```

### Paso 9 — Ingeniería de características adicionales

```python
# Segmento de rotación (construir desde días en inventario como proxy)
def segmento_rotacion(dias_inv):
    if pd.isna(dias_inv) or dias_inv < 0:
        return 'sin_dato'
    elif dias_inv <= 30:
        return 'alta_rotacion'
    elif dias_inv <= 90:
        return 'media_rotacion'
    else:
        return 'baja_rotacion'

df['segmento_rotacion'] = df['dias_en_inventario'].apply(segmento_rotacion)

# Score de riesgo simple (base para ML)
def score_riesgo(row):
    score = 0
    if row['estado_inventario'] == 'vencido': score += 3
    elif row['estado_inventario'] == 'critico': score += 2
    elif row['estado_inventario'] == 'proximo_a_vencer': score += 1
    if row['segmento_rotacion'] == 'baja_rotacion': score += 1
    return score

df['score_riesgo'] = df.apply(score_riesgo, axis=1)

# Mes y año de vencimiento (útil para análisis temporal)
df['mes_vencimiento'] = df['fecha_vencimiento'].dt.month
df['anio_vencimiento'] = df['fecha_vencimiento'].dt.year
```

### Antes de pasar a la siguiente fase

- [ ] Verificar que no queden tipos object donde se esperan numéricos o fechas
- [ ] Documentar % de nulos por columna antes y después de limpieza
- [ ] Registrar todos los supuestos aplicados
- [ ] Guardar dataset limpio en variable `df_clean` o archivo intermedio

---

## FASE 3 — ANÁLISIS DESCRIPTIVO (MINERÍA DESCRIPTIVA)

> Responde: ¿Cómo está el inventario hoy?

### 3.1 KPIs Operativos

```python
# Unidades totales
total_unds = df_clean['unds'].sum()

# Por estado
resumen_estado = df_clean.groupby('estado_inventario')['unds'].agg(['sum','count'])
resumen_estado['pct_unds'] = resumen_estado['sum'] / total_unds * 100

# Por marca
por_marca = df_clean.groupby('marca').agg(
    unds_total=('unds','sum'),
    items_unicos=('item_id','nunique'),
    dias_inv_promedio=('dias_en_inventario','mean'),
    dias_venc_promedio=('dias_para_vencimiento','mean')
).sort_values('unds_total', ascending=False)

# Por categoría
por_categoria = df_clean.groupby('categoria').agg(
    unds_total=('unds','sum'),
    items_unicos=('item_id','nunique'),
    pct_vencido=('estado_inventario', lambda x: (x=='vencido').mean()*100)
).sort_values('pct_vencido', ascending=False)
```

### 3.2 Clustering — Segmentación de Inventario

> Agrupar registros por comportamiento de inventario. Útil para estrategias diferenciadas por segmento.

**Cuándo usarlo:** Cuando se quiere descubrir grupos naturales sin imponer etiquetas.

**Flujo:**
1. Preparar features numéricas (normalizar)
2. Eliminar dummies innecesarias
3. Aplicar K-Means
4. Evaluar con Silhouette, Dunn, Davies-Bouldin
5. Perfilar cada cluster con estadísticas descriptivas

```python
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import silhouette_score

features_cluster = ['dias_en_inventario', 'dias_para_vencimiento', 'unds']
df_cluster = df_clean[features_cluster].dropna()

scaler = MinMaxScaler()
X_scaled = scaler.fit_transform(df_cluster)

# Método del codo para elegir k
inercias = []
for k in range(2, 9):
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    km.fit(X_scaled)
    inercias.append(km.inertia_)

# Modelo final (ajustar k según el codo)
k_optimo = 4
model_km = KMeans(n_clusters=k_optimo, random_state=42, n_init=10)
df_cluster['cluster'] = model_km.fit_predict(X_scaled)

sil = silhouette_score(X_scaled, df_cluster['cluster'])
print(f"Silhouette Score: {sil:.4f}")  # Mejor cuanto más cercano a 1
```

**Métricas de evaluación de clustering:**

| Métrica | Interpretación | Mejor valor |
|---|---|---|
| Inercia | Cohesión interna | Menor es mejor |
| Silhouette | Separación entre clusters | Cercano a 1 |
| Davies-Bouldin | Similitud entre clusters | Cercano a 0 |
| Dunn Index | Separación / diámetro | Mayor es mejor |

### 3.3 Selección de Variables (Feature Importance)

> Identificar qué variables más explican el estado del inventario.

```python
# Correlaciones (para variables numéricas)
correlaciones = df_clean[['dias_en_inventario','dias_para_vencimiento','unds','score_riesgo']].corr()

# Regla: eliminar si correlación > 0.8 entre predictores (redundancia)
# Regla: eliminar si correlación < 0.05 con variable objetivo (irrelevancia)

# Árbol de decisión como selector de features
from sklearn.tree import DecisionTreeClassifier
import pandas as pd

X_fs = df_clean[['dias_en_inventario','dias_para_vencimiento','unds']].fillna(0)
y_fs = df_clean['estado_inventario']

dt_fs = DecisionTreeClassifier(criterion='gini', max_depth=5, min_samples_leaf=50)
dt_fs.fit(X_fs, y_fs)

importancias = pd.Series(dt_fs.feature_importances_, index=X_fs.columns).sort_values(ascending=False)
print(importancias)
```

### 3.4 Reglas de Asociación (opcional — si se analiza co-ocurrencia de categorías)

> Úsalo si el negocio pregunta: ¿Qué categorías de productos vencen juntas frecuentemente?

```python
# Requiere datos en formato transaccional o binario
# Mínimo soporte recomendado: 0.05–0.1
# Mínima confianza recomendada: 0.5–0.6

from apyori import apriori
# reglas = apriori(transacciones, min_support=0.05, min_confidence=0.5)
# Interpretar: soporte (frecuencia), confianza (P(B|A)), lift (>1 = relación positiva)
```

---

## FASE 4 — MODELO DE MACHINE LEARNING (MINERÍA PREDICTIVA)

> Responde: ¿Qué tan en riesgo estará este producto?

### 4.1 Definición de la pregunta de negocio

Antes de elegir algoritmo, definir explícitamente:

> *"¿Puede el modelo predecir si un registro de inventario terminará siendo de alto riesgo (vencido o crítico) basándose en sus características actuales?"*

**Variable objetivo recomendada:**
```python
# Versión binaria (más simple, más interpretable)
df_clean['riesgo_alto'] = df_clean['estado_inventario'].isin(['vencido','critico']).astype(int)

# Versión multiclase
df_clean['nivel_riesgo'] = df_clean['score_riesgo'].apply(
    lambda x: 'alto' if x >= 3 else ('medio' if x >= 1 else 'bajo')
)
```

### 4.2 Preparación de features para ML

```python
from sklearn.preprocessing import MinMaxScaler, LabelEncoder
import pandas as pd

features = ['dias_en_inventario', 'unds', 'mes_vencimiento']

# Para métodos basados en distancia (KNN, Redes Neuronales, SVM, Regresión):
#   → Normalizar variables numéricas
#   → Crear dummies para variables categóricas
#   → Codificar variable objetivo

# Para métodos basados en reglas (Árbol, Random Forest, Bayes):
#   → No necesitan normalización (discretizan automáticamente)
#   → Pueden manejar categóricas directamente (con LabelEncoder)

# Dummies para categóricas predictoras
df_ml = pd.get_dummies(df_clean[features + ['marca','categoria']], drop_first=True)

X = df_ml
y = df_clean['riesgo_alto']
```

### 4.3 División y balanceo

```python
from sklearn.model_selection import train_test_split
from imblearn.over_sampling import SMOTE

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.30, random_state=42, stratify=y
)

# Balanceo solo si hay desbalance significativo (revisar primero)
print(y_train.value_counts(normalize=True))

# Si hay desbalance:
sm = SMOTE(random_state=42)
X_bal, y_bal = sm.fit_resample(X_train, y_train)

# Si hay variables categóricas en X:
# from imblearn.over_sampling import SMOTENC
# sm = SMOTENC(categorical_features=[idx1, idx2, ...], random_state=42)
```

### 4.4 Algoritmos recomendados y su código

#### Árbol de Decisión (recomendado como baseline — interpretable)

```python
from sklearn.tree import DecisionTreeClassifier

model_dt = DecisionTreeClassifier(
    criterion='gini',
    max_depth=10,
    min_samples_leaf=50,
    random_state=42
)
model_dt.fit(X_bal, y_bal)
```

#### Random Forest (recomendado para producción — robusto)

```python
from sklearn.ensemble import RandomForestClassifier

model_rf = RandomForestClassifier(
    n_estimators=100,
    max_samples=0.8,
    criterion='gini',
    max_depth=15,
    min_samples_leaf=20,
    random_state=42
)
model_rf.fit(X_bal, y_bal)
```

#### Regresión Logística (recomendado si se quiere explicabilidad por coeficientes)

```python
from sklearn.linear_model import LogisticRegression

model_lr = LogisticRegression(
    solver='lbfgs',
    multi_class='auto',
    max_iter=1000,
    random_state=42
)
# Requiere normalización previa de X
model_lr.fit(X_bal, y_bal)
```

#### K-Nearest Neighbor

```python
from sklearn.neighbors import KNeighborsClassifier

model_knn = KNeighborsClassifier(n_neighbors=5, metric='euclidean')
model_knn.fit(X_bal, y_bal)
# Requiere normalización previa de X
```

#### Redes Neuronales (solo si hay suficiente data y se puede justificar)

```python
from sklearn.neural_network import MLPClassifier

model_nn = MLPClassifier(
    activation='relu',
    hidden_layer_sizes=(64, 32),
    learning_rate='adaptive',
    learning_rate_init=0.001,
    max_iter=500,
    random_state=42
)
model_nn.fit(X_bal, y_bal)
# Requiere normalización previa de X
```

#### SVM (útil para fronteras de decisión no lineales)

```python
from sklearn.svm import SVC

model_svm = SVC(kernel='rbf', probability=True, random_state=42)
model_svm.fit(X_bal, y_bal)
# Requiere normalización previa de X
```

### 4.5 Ensambles (cuando un solo modelo no es suficiente)

#### Bagging

```python
from sklearn.ensemble import BaggingClassifier

model_bag = BaggingClassifier(
    estimator=model_dt,
    n_estimators=20,
    max_samples=0.7,
    random_state=42
)
model_bag.fit(X_bal, y_bal)
```

#### Boosting (AdaBoost)

```python
from sklearn.ensemble import AdaBoostClassifier

model_ada = AdaBoostClassifier(
    estimator=model_dt,
    n_estimators=50,
    random_state=42
)
model_ada.fit(X_bal, y_bal)
```

#### Votación Hard

```python
from sklearn.ensemble import VotingClassifier

clasificadores = [('dt', model_dt), ('knn', model_knn), ('rf', model_rf)]
model_vote = VotingClassifier(estimators=clasificadores, voting='hard')
model_vote.fit(X_bal, y_bal)
```

#### Votación Soft (requiere modelos que soporten `predict_proba`)

```python
model_vote_soft = VotingClassifier(
    estimators=clasificadores,
    voting='soft',
    weights=[0.2, 0.3, 0.5]   # ajustar según desempeño individual
)
model_vote_soft.fit(X_bal, y_bal)
```

#### Stacking

```python
from sklearn.ensemble import StackingClassifier

model_stack = StackingClassifier(
    estimators=clasificadores,
    final_estimator=LogisticRegression()
)
model_stack.fit(X_bal, y_bal)
```

### 4.6 Validación cruzada (preferir sobre división simple cuando sea posible)

```python
from sklearn.model_selection import cross_validate

scores = cross_validate(
    model_rf, X, y, cv=10,
    scoring=('f1_weighted', 'accuracy', 'precision_weighted',
             'recall_weighted', 'roc_auc_ovr'),
    return_train_score=False
)

for metrica, valores in scores.items():
    if metrica.startswith('test_'):
        print(f"{metrica}: {valores.mean():.4f} ± {valores.std():.4f}")
```

### 4.7 Evaluación de modelos de clasificación

```python
from sklearn.metrics import (classification_report, confusion_matrix,
                              ConfusionMatrixDisplay, roc_auc_score)

y_pred = model_rf.predict(X_test)
y_proba = model_rf.predict_proba(X_test)

print(classification_report(y_test, y_pred))
print(f"AUC-ROC: {roc_auc_score(y_test, y_proba[:,1]):.4f}")

ConfusionMatrixDisplay(confusion_matrix(y_test, y_pred)).plot()
```

**Métricas de clasificación — qué usar según contexto:**

| Métrica | Fórmula | Cuándo priorizar |
|---|---|---|
| **Exactitud (Accuracy)** | (TP+TN) / Total | Clases balanceadas |
| **Precisión** | TP / (TP+FP) | Minimizar falsos positivos |
| **Recall (Sensibilidad)** | TP / (TP+FN) | Minimizar falsos negativos (riesgo) |
| **F1** | 2 × (P×R)/(P+R) | Desbalance — balance entre P y R |
| **AUC-ROC** | Área bajo curva ROC | Evaluación general del modelo |

> Para inventario en riesgo, **priorizar Recall** sobre Precisión: es peor no detectar un producto que va a vencer (falso negativo) que generar una alerta innecesaria.

### 4.8 Evaluación de modelos de regresión

```python
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import numpy as np

mse  = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)
mae  = mean_absolute_error(y_test, y_pred)
r2   = r2_score(y_test, y_pred)

print(f"MSE: {mse:.2f} | RMSE: {rmse:.2f} | MAE: {mae:.2f} | R²: {r2:.4f}")
```

### 4.9 Guardar y reutilizar el modelo

```python
import pickle

# Guardar
with open('src/models/modelo_riesgo.pkl', 'wb') as f:
    pickle.dump(model_rf, f)

# Cargar
with open('src/models/modelo_riesgo.pkl', 'rb') as f:
    model_cargado = pickle.load(f)

# Predecir sobre datos nuevos
predicciones = model_cargado.predict(X_nuevos)
```

---

## FASE 5 — CARGA A POSTGRESQL (NEON)

> Persistir datos procesados para BI y análisis SQL.

### Configuración (solo variables de entorno — NUNCA credenciales en código)

```python
import os
import psycopg2
from sqlalchemy import create_engine

# Variables de entorno requeridas: PGHOST, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE
conn_string = (
    f"postgresql://{os.environ['PGUSER']}:{os.environ['PGPASSWORD']}"
    f"@{os.environ['PGHOST']}/{os.environ['PGDATABASE']}"
    f"?sslmode={os.environ.get('PGSSLMODE','require')}"
)
engine = create_engine(conn_string)
```

### Modelo dimensional sugerido

```sql
-- Dimensión Producto
CREATE TABLE dim_producto (
    item_id         VARCHAR(50) PRIMARY KEY,
    descripcion     TEXT,
    categoria       VARCHAR(100),
    marca           VARCHAR(100)
);

-- Dimensión Marca
CREATE TABLE dim_marca (
    marca_id  SERIAL PRIMARY KEY,
    marca     VARCHAR(100) UNIQUE
);

-- Dimensión Estado / Criticidad
CREATE TABLE dim_estado (
    estado_id   SERIAL PRIMARY KEY,
    estado      VARCHAR(50),
    descripcion TEXT
);

-- Tabla de Hechos: Inventario
CREATE TABLE fact_inventario (
    product_container_id   VARCHAR(100) PRIMARY KEY,
    item_id                VARCHAR(50),
    contenedor             VARCHAR(100),
    unds                   NUMERIC,
    fecha_ingreso          DATE,
    fecha_vencimiento      DATE,
    fecha_corte            DATE,
    dias_en_inventario     INTEGER,
    dias_para_vencimiento  INTEGER,
    estado_inventario      VARCHAR(50),
    segmento_rotacion      VARCHAR(50),
    score_riesgo           INTEGER,
    mes_vencimiento        INTEGER,
    anio_vencimiento       INTEGER
);
```

### Carga desde pandas

```python
# Carga limpia (reemplaza si ya existe)
df_clean.to_sql('fact_inventario', engine, if_exists='replace', index=False)

# Verificar
with engine.connect() as conn:
    result = conn.execute("SELECT COUNT(*) FROM fact_inventario")
    print(f"Registros cargados: {result.fetchone()[0]}")
```

---

## FASE 6 — KPIs Y VISTAS ANALÍTICAS

### Vistas SQL recomendadas

```sql
-- KPIs por estado del inventario
CREATE VIEW v_kpis_estado AS
SELECT
    estado_inventario,
    COUNT(*)                          AS cantidad_registros,
    SUM(unds)                         AS unidades_totales,
    ROUND(100.0 * SUM(unds) / SUM(SUM(unds)) OVER (), 2) AS pct_unidades,
    ROUND(AVG(dias_en_inventario), 1) AS dias_inv_promedio,
    ROUND(AVG(dias_para_vencimiento), 1) AS dias_venc_promedio
FROM fact_inventario
GROUP BY estado_inventario;

-- Top 10 marcas con mayor riesgo
CREATE VIEW v_marcas_riesgo AS
SELECT
    p.marca,
    SUM(CASE WHEN f.estado_inventario IN ('vencido','critico') THEN f.unds ELSE 0 END) AS unds_en_riesgo,
    ROUND(100.0 * SUM(CASE WHEN f.estado_inventario IN ('vencido','critico') THEN f.unds ELSE 0 END)
          / NULLIF(SUM(f.unds), 0), 2) AS pct_riesgo
FROM fact_inventario f
JOIN dim_producto p ON f.item_id = p.item_id
GROUP BY p.marca
ORDER BY unds_en_riesgo DESC;
```

### Reglas de activación por área

| Estado | Segmento Rotación | Área Responsable | Acción |
|---|---|---|---|
| `proximo_a_vencer` o `critico` | Cualquiera | **Mercadeo** | Campaña de evacuación / promoción |
| `vigente` | `baja_rotacion` | **Canales / Comercial** | Revisión de estrategia comercial |
| `vencido` | Cualquiera | **Sostenibilidad** | Proceso de disposición final |
| `critico` + `baja_rotacion` | `baja_rotacion` | **Mercadeo + Canales** | Acción urgente combinada |

---

## FASE 7 — BUENAS PRÁCTICAS Y REGLAS DE ORO

### Sobre el código

- Todos los umbrales como constantes al inicio del archivo, nunca hardcodeados dentro de funciones.
- Cada transformación documentada con un comentario de por qué, no solo de qué.
- Supuestos registrados explícitamente (ej: `# Supuesto: fecha_corte fija para reproducibilidad`).
- No inventar datos faltantes. Si una columna está vacía, decirlo y construir la variable desde cero.

### Sobre los modelos

- Siempre empezar con el modelo más simple (árbol de decisión). Si no es suficiente, escalar.
- No usar deep learning ni XGBoost sin justificación clara.
- Para inventario: priorizar **Recall** sobre Precisión en la métrica de optimización.
- Documentar las features usadas, los parámetros y el desempeño final en el notebook.

### Sobre los datos

- No confiar en `Dias antes de Vencimiento` del Excel. Recalcular siempre.
- No usar `Rotación` ni `Estado` del Excel como verdad — están vacíos.
- Documentar % de nulos antes y después de cada paso de limpieza.

### Sobre PostgreSQL

- Nunca credenciales en código, notebooks ni commits.
- La carga debe ser reproducible: el script puede ejecutarse varias veces sin generar duplicados ni errores.
- Validar conteos después de cada carga.

### Sobre el proyecto

- El notebook `proyecto_graduacion.ipynb` es la pieza central de sustentación — debe contar la historia completa del análisis.
- Scripts auxiliares en `src/` para lógica reutilizable.
- Archivos SQL en `sql/`.
- No hacer arquitecturas innecesariamente complejas.

---

## METODOLOGÍA DE REFERENCIA: CRISP-DM

Este proyecto sigue el ciclo CRISP-DM. Cada fase puede iterar:

```
Business Understanding
        ↓
Data Understanding ←──────────────┐
        ↓                         │
Data Preparation                  │ (iterar si aparecen
        ↓                         │  nuevos hallazgos)
    Modeling                      │
        ↓                         │
   Evaluation ────────────────────┘
        ↓
  Deployment
```

**No es un proceso lineal.** Si en Modeling se descubre un problema en los datos, volver a Data Preparation.

---

## TAXONOMÍA DE TÉCNICAS — REFERENCIA RÁPIDA

### ¿Qué técnica usar según la pregunta?

| Pregunta de negocio | Tipo de análisis | Técnica recomendada |
|---|---|---|
| ¿Qué grupos de productos tienen comportamientos similares? | Descriptivo | Clustering (K-Means) |
| ¿Qué categorías de productos vencen juntas? | Descriptivo | Reglas de Asociación (Apriori) |
| ¿Qué variables más explican el riesgo? | Descriptivo | Selección de Factores |
| ¿Este producto va a vencer? (sí/no) | Predictivo | Clasificación (RF, Árbol, LR) |
| ¿En cuántos días va a vencer? | Predictivo | Regresión (Árbol, RF) |
| ¿Cuál es el volumen esperado de vencidos el próximo mes? | Predictivo | Regresión / Series de Tiempo |

### Guía de transformaciones según algoritmo

| Algoritmo | Normalización | Dummies | Discretización |
|---|---|---|---|
| Árbol de Decisión | ❌ No necesaria | ❌ No necesaria | ❌ Automática |
| Random Forest | ❌ No necesaria | ❌ No necesaria | ❌ Automática |
| Naive Bayes | ❌ No necesaria | ✅ Sí | ✅ Recomendada |
| KNN | ✅ **Obligatoria** | ✅ Sí | ❌ No aplica |
| SVM | ✅ **Obligatoria** | ✅ Sí | ❌ No aplica |
| Redes Neuronales | ✅ **Obligatoria** | ✅ Sí | ❌ No aplica |
| Regresión Logística | ✅ Recomendada | ✅ Sí | ❌ No aplica |
| K-Means | ✅ **Obligatoria** | ✅ Sí | ❌ No aplica |

---

## RESULTADO ESPERADO AL COMPLETAR EL PROYECTO

- [ ] ETL reproducible desde Excel de SAP hasta PostgreSQL
- [ ] Dataset limpio con variables derivadas documentadas
- [ ] KPIs operativos y analíticos calculados
- [ ] Clustering de inventario con perfilamiento de segmentos
- [ ] Modelo de clasificación de riesgo evaluado y guardado
- [ ] Reglas de negocio por área documentadas y auditables
- [ ] Tablas y vistas en PostgreSQL Neon
- [ ] `proyecto_graduacion.ipynb` como pieza central de sustentación
- [ ] Base sólida para dashboard en Power BI

---

*Guía construida a partir de las lecciones del curso de Minería de Datos (Introducción, Preparación de Datos, Minería Predictiva, Minería Descriptiva y Ensambles) y contextualizada al proyecto Inventarios 360. Implementación exclusivamente en Python.*
