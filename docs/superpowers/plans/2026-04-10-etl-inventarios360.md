# ETL Inventarios 360 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir un ETL reproducible que lea el Excel de inventarios cosméticos, limpie y enriquezca los datos, y los cargue en Neon PostgreSQL (esquemas `stg`, `dw`, `mart`).

**Architecture:** Pipeline en 4 módulos separados (extract → transform → validate → load), orquestados por `pipeline.py`. Cada módulo es importable y testeable de forma independiente. El notebook `proyecto_graduacion.ipynb` llama al pipeline como demostración.

**Tech Stack:** Python 3.10+, pandas, openpyxl, SQLAlchemy, psycopg2-binary, python-dotenv, pytest

---

## File Map

| Acción | Archivo | Responsabilidad |
|--------|---------|----------------|
| Crear | `src/etl/__init__.py` | Expone `FECHA_CORTE`, `UMBRAL_CRITICO`, `UMBRAL_PROXIMO` |
| Crear | `src/etl/extract.py` | Leer Excel, normalizar columnas, calcular `row_null_pct` |
| Crear | `src/etl/transform.py` | Tipos, variables derivadas, reglas de negocio |
| Crear | `src/etl/validate.py` | Validaciones post-transformación, reporte de calidad |
| Crear | `src/etl/pipeline.py` | Orquestador extract → transform → validate → load |
| Crear | `tests/__init__.py` | Paquete de tests |
| Crear | `tests/etl/__init__.py` | Paquete de tests ETL |
| Crear | `tests/etl/test_extract.py` | Tests de extracción y normalización |
| Crear | `tests/etl/test_transform.py` | Tests de transformación y reglas de negocio |
| Crear | `tests/etl/test_validate.py` | Tests de validaciones |
| Crear | `tests/etl/conftest.py` | Fixtures compartidos (DataFrames de muestra) |

---

## Task 1: Constantes globales del ETL

**Files:**
- Create: `src/etl/__init__.py`

- [ ] **Step 1: Crear el módulo con las constantes**

```python
# src/etl/__init__.py
"""
Inventarios 360 — ETL
Constantes de negocio. Modificar aquí si cambian los umbrales.
Mantener sincronizado con sql/03_reglas_negocio.sql.
"""
import pandas as pd

# Fecha de corte fija para reproducibilidad.
# Supuesto: se usa 2025-04-01 como fecha de referencia del análisis.
# Si se cambia, documentar el motivo y re-ejecutar el pipeline completo.
FECHA_CORTE: pd.Timestamp = pd.Timestamp("2025-04-01")

# Fuente de datos
SOURCE_FILE: str = "datos_proyecto/Fechas de Vencimiento Beauty Care 1.xlsx"
SHEET_NAME: str = "Page1_1"

# Umbrales de estado de inventario (días para vencimiento)
# Sincronizados con dw.clasificar_estado_inventario en sql/03_reglas_negocio.sql
UMBRAL_CRITICO: int = 15   # 0–15 días  → 'critico'
UMBRAL_PROXIMO: int = 30   # 16–30 días → 'proximo_a_vencer'
                            # > 30 días  → 'vigente'
                            # < 0        → 'vencido'

# Umbral de nulos por fila (fracción de columnas relevantes)
# Columnas estructuralmente vacías en la fuente se excluyen del cálculo
UMBRAL_NULOS_FILA: float = 0.15

# Columnas a excluir del cálculo de row_null_pct (vacías por diseño en la fuente)
COLS_VACIAS_POR_DISENO: list[str] = ["id_inventario", "rotacion_raw", "estado_raw"]
```

- [ ] **Step 2: Verificar que el módulo importa sin errores**

```bash
cd /Users/diegocastro/Desktop/especializacion
python -c "from src.etl import FECHA_CORTE, UMBRAL_CRITICO, UMBRAL_PROXIMO; print(FECHA_CORTE, UMBRAL_CRITICO, UMBRAL_PROXIMO)"
```

Esperado: `2025-04-01 00:00:00 15 30`

- [ ] **Step 3: Commit**

```bash
git add src/etl/__init__.py
git commit -m "feat(etl): add ETL constants module with business thresholds"
```

---

## Task 2: Fixtures de tests compartidos

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/etl/__init__.py`
- Create: `tests/etl/conftest.py`

- [ ] **Step 1: Crear paquetes vacíos**

```bash
touch /Users/diegocastro/Desktop/especializacion/tests/__init__.py
touch /Users/diegocastro/Desktop/especializacion/tests/etl/__init__.py
```

- [ ] **Step 2: Crear conftest.py con DataFrames de muestra**

```python
# tests/etl/conftest.py
import pandas as pd
import pytest


@pytest.fixture
def df_raw_sample():
    """DataFrame que simula la hoja Page1_1 del Excel con casos representativos."""
    return pd.DataFrame({
        "Marca": ["MarcaA", "MarcaA", "MarcaB", "MarcaC", None],
        "Item ID": ["ITM001", "ITM001", "ITM002", "ITM003", "ITM004"],
        "ID Inventario": [None, None, None, None, None],   # siempre vacío
        "Descripción": ["Prod A1", "Prod A1", "Prod B1", "Prod C1", "Prod D1"],
        "Categoría": ["Cat1", "Cat1", "Cat2", "Cat1", "Cat2"],
        "Unds": ["10", "5", "-3", "0", "20"],              # incluye negativos y ceros
        "Fecha de Ingreso": [
            "2024-01-15", "2024-01-15", "2023-06-01", "2025-06-01", "2024-11-20"
        ],
        "Fecha Vencimiento": [
            "2025-04-10",   # 9 días → critico
            "2025-05-15",   # 44 días → vigente
            "2025-03-20",   # -12 días → vencido
            None,           # fecha nula
            "2025-04-20",   # 19 días → proximo_a_vencer
        ],
        "Dias antes de Vencimiento": ["9", "44", "-12", None, "19"],  # no confiar
        "Contenedor": ["BOX", "CAN", "BOX", "TUBE", "BOX"],
        "Rotación": [None, None, None, None, None],         # siempre vacío
        "Estado": [None, None, None, None, None],           # casi siempre vacío
    })


@pytest.fixture
def df_clean_sample():
    """DataFrame limpio mínimo para tests de carga."""
    fecha_corte = pd.Timestamp("2025-04-01")
    return pd.DataFrame({
        "product_container_id": ["ITM001_BOX", "ITM001_CAN", "ITM002_BOX"],
        "marca": ["MarcaA", "MarcaA", "MarcaB"],
        "item_id": ["ITM001", "ITM001", "ITM002"],
        "descripcion": ["Prod A1", "Prod A1", "Prod B1"],
        "categoria": ["Cat1", "Cat1", "Cat2"],
        "contenedor": ["BOX", "CAN", "BOX"],
        "unds": [10.0, 5.0, None],
        "fecha_ingreso": pd.to_datetime(["2024-01-15", "2024-01-15", "2023-06-01"]),
        "fecha_vencimiento": pd.to_datetime(["2025-04-10", "2025-05-15", "2025-03-20"]),
        "dias_en_inventario": [442, 442, 670],
        "dias_para_vencimiento": [9, 44, -12],
        "estado_inventario": ["critico", "vigente", "vencido"],
        "segmento_rotacion": ["baja_rotacion", "baja_rotacion", "baja_rotacion"],
        "score_riesgo": [3, 1, 4],
        "mes_vencimiento": [4, 5, 3],
        "anio_vencimiento": [2025, 2025, 2025],
        "row_null_pct": [0.0, 0.0, 0.0],
        "calidad_flag": ["ok", "ok", "ok"],
    })
```

- [ ] **Step 3: Verificar que conftest importa sin errores**

```bash
cd /Users/diegocastro/Desktop/especializacion
python -m pytest tests/etl/conftest.py --collect-only
```

Esperado: `no tests ran` (sin errores de import)

- [ ] **Step 4: Commit**

```bash
git add tests/__init__.py tests/etl/__init__.py tests/etl/conftest.py
git commit -m "test(etl): add shared fixtures for ETL tests"
```

---

## Task 3: Módulo de extracción

**Files:**
- Create: `src/etl/extract.py`
- Create: `tests/etl/test_extract.py`

- [ ] **Step 1: Escribir los tests**

```python
# tests/etl/test_extract.py
import pandas as pd
import pytest
from unittest.mock import patch

from src.etl.extract import normalize_columns, compute_row_null_pct, load_excel


def test_normalize_columns_snake_case():
    df = pd.DataFrame(columns=["Fecha de Ingreso", "Item ID", "Descripción", "Categoría"])
    result = normalize_columns(df)
    assert list(result.columns) == ["fecha_de_ingreso", "item_id", "descripcion", "categoria"]


def test_normalize_columns_strips_spaces():
    df = pd.DataFrame(columns=[" Marca ", "Unds "])
    result = normalize_columns(df)
    assert list(result.columns) == ["marca", "unds"]


def test_compute_row_null_pct_all_present(df_raw_sample):
    from src.etl import COLS_VACIAS_POR_DISENO
    df = normalize_columns(df_raw_sample)
    result = compute_row_null_pct(df, exclude_cols=COLS_VACIAS_POR_DISENO)
    # La primera fila tiene solo marca=MarcaA y demás presentes → 0%
    assert result.iloc[0] == pytest.approx(0.0)


def test_compute_row_null_pct_with_nulls():
    df = pd.DataFrame({
        "col_a": [None, "x"],
        "col_b": [None, "y"],
        "col_c": ["z", "z"],
        "col_d": ["w", "w"],
    })
    result = compute_row_null_pct(df, exclude_cols=[])
    assert result.iloc[0] == pytest.approx(0.5)   # 2 de 4 nulos
    assert result.iloc[1] == pytest.approx(0.0)


def test_load_excel_returns_dataframe(tmp_path):
    """Verifica que load_excel devuelve un DataFrame con las columnas normalizadas."""
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Page1_1"
    ws.append(["Marca", "Item ID", "Descripción"])
    ws.append(["MarcaA", "ITM001", "Prod A"])
    path = tmp_path / "test.xlsx"
    wb.save(path)

    df = load_excel(str(path), sheet_name="Page1_1")
    assert isinstance(df, pd.DataFrame)
    assert "marca" in df.columns
    assert "item_id" in df.columns
    assert len(df) == 1
```

- [ ] **Step 2: Ejecutar tests y verificar que fallan**

```bash
cd /Users/diegocastro/Desktop/especializacion
python -m pytest tests/etl/test_extract.py -v
```

Esperado: `ModuleNotFoundError: No module named 'src.etl.extract'`

- [ ] **Step 3: Implementar `src/etl/extract.py`**

```python
# src/etl/extract.py
"""
ETL — Extracción
Lee el archivo Excel, normaliza columnas y calcula row_null_pct.
"""
from pathlib import Path

import pandas as pd

from src.etl import COLS_VACIAS_POR_DISENO


# Mapa de rename explícito para columnas con caracteres especiales o espacios
_RENAME_MAP = {
    "fecha_de_ingreso": "fecha_ingreso",
    "fecha_vencimiento_1": "fecha_vencimiento",   # por si hay variante con número
    "dias_antes_de_vencimiento": "dias_antes_vencimiento_raw",
    "rotacion": "rotacion_raw",
    "estado": "estado_raw",
    "id_inventario": "id_inventario",
    "descripcion": "descripcion",
    "categoria": "categoria",
}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nombres de columnas a snake_case sin acentos.

    Operaciones:
    - strip de espacios
    - lowercase
    - espacios → guiones bajos
    - reemplaza vocales acentuadas
    """
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace("á", "a").str.replace("é", "e")
        .str.replace("í", "i").str.replace("ó", "o")
        .str.replace("ú", "u").str.replace("ñ", "n")
    )
    # Aplicar renames explícitos para columnas conocidas
    df = df.rename(columns={k: v for k, v in _RENAME_MAP.items() if k in df.columns})
    return df


def compute_row_null_pct(df: pd.DataFrame, exclude_cols: list[str]) -> pd.Series:
    """
    Calcula la fracción de columnas nulas por fila, excluyendo columnas
    estructuralmente vacías en la fuente.

    Returns:
        pd.Series con valores 0.0–1.0, mismo índice que df.
    """
    cols_relevantes = [c for c in df.columns if c not in exclude_cols]
    if not cols_relevantes:
        return pd.Series(0.0, index=df.index)
    return df[cols_relevantes].isnull().mean(axis=1)


def load_excel(filepath: str, sheet_name: str = "Page1_1") -> pd.DataFrame:
    """
    Lee el Excel fuente, normaliza columnas y calcula row_null_pct.

    Args:
        filepath:   Ruta al archivo Excel.
        sheet_name: Nombre de la hoja a leer (solo Page1_1 es fuente válida).

    Returns:
        DataFrame con columnas normalizadas y columna 'row_null_pct' agregada.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {path.resolve()}")

    df = pd.read_excel(filepath, sheet_name=sheet_name, dtype=str)
    df = normalize_columns(df)
    df["row_null_pct"] = compute_row_null_pct(df, exclude_cols=COLS_VACIAS_POR_DISENO)
    return df
```

- [ ] **Step 4: Ejecutar tests y verificar que pasan**

```bash
cd /Users/diegocastro/Desktop/especializacion
python -m pytest tests/etl/test_extract.py -v
```

Esperado: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add src/etl/extract.py tests/etl/test_extract.py
git commit -m "feat(etl): add extract module with column normalization and null tracking"
```

---

## Task 4: Módulo de transformación

**Files:**
- Create: `src/etl/transform.py`
- Create: `tests/etl/test_transform.py`

- [ ] **Step 1: Escribir los tests**

```python
# tests/etl/test_transform.py
import pandas as pd
import pytest

from src.etl.transform import (
    cast_types,
    build_derived_columns,
    classify_estado,
    classify_segmento_rotacion,
    compute_score_riesgo,
    apply_calidad_flag,
    clean_outliers,
)
from src.etl import FECHA_CORTE


# --- classify_estado ---

def test_classify_estado_vencido():
    assert classify_estado(-1) == "vencido"

def test_classify_estado_vencido_boundary():
    assert classify_estado(-100) == "vencido"

def test_classify_estado_critico_zero():
    assert classify_estado(0) == "critico"

def test_classify_estado_critico():
    assert classify_estado(10) == "critico"

def test_classify_estado_critico_boundary():
    assert classify_estado(15) == "critico"

def test_classify_estado_proximo():
    assert classify_estado(16) == "proximo_a_vencer"

def test_classify_estado_proximo_boundary():
    assert classify_estado(30) == "proximo_a_vencer"

def test_classify_estado_vigente():
    assert classify_estado(31) == "vigente"

def test_classify_estado_null():
    assert classify_estado(None) == "sin_fecha"


# --- classify_segmento_rotacion ---

def test_segmento_alta():
    assert classify_segmento_rotacion(10) == "alta_rotacion"

def test_segmento_alta_boundary():
    assert classify_segmento_rotacion(30) == "alta_rotacion"

def test_segmento_media():
    assert classify_segmento_rotacion(31) == "media_rotacion"

def test_segmento_media_boundary():
    assert classify_segmento_rotacion(90) == "media_rotacion"

def test_segmento_baja():
    assert classify_segmento_rotacion(91) == "baja_rotacion"

def test_segmento_negativo():
    assert classify_segmento_rotacion(-5) == "sin_dato"

def test_segmento_null():
    assert classify_segmento_rotacion(None) == "sin_dato"


# --- compute_score_riesgo ---

def test_score_vencido_baja():
    assert compute_score_riesgo("vencido", "baja_rotacion") == 4

def test_score_vencido_alta():
    assert compute_score_riesgo("vencido", "alta_rotacion") == 3

def test_score_critico_baja():
    assert compute_score_riesgo("critico", "baja_rotacion") == 3

def test_score_critico_alta():
    assert compute_score_riesgo("critico", "alta_rotacion") == 2

def test_score_proximo_baja():
    assert compute_score_riesgo("proximo_a_vencer", "baja_rotacion") == 2

def test_score_vigente():
    assert compute_score_riesgo("vigente", "alta_rotacion") == 0

def test_score_vigente_baja():
    assert compute_score_riesgo("vigente", "baja_rotacion") == 1


# --- cast_types ---

def test_cast_types_unds_numeric(df_raw_sample):
    from src.etl.extract import normalize_columns
    df = normalize_columns(df_raw_sample)
    result = cast_types(df)
    assert pd.api.types.is_float_dtype(result["unds"])

def test_cast_types_fechas_datetime(df_raw_sample):
    from src.etl.extract import normalize_columns
    df = normalize_columns(df_raw_sample)
    result = cast_types(df)
    assert pd.api.types.is_datetime64_any_dtype(result["fecha_ingreso"])
    assert pd.api.types.is_datetime64_any_dtype(result["fecha_vencimiento"])


# --- clean_outliers ---

def test_clean_outliers_negative_unds():
    df = pd.DataFrame({"unds": [10.0, -3.0, 0.0, 5.0]})
    result = clean_outliers(df)
    assert pd.isna(result.loc[1, "unds"])   # -3 → null
    assert pd.isna(result.loc[2, "unds"])   # 0 → null
    assert result.loc[0, "unds"] == 10.0
    assert result.loc[3, "unds"] == 5.0


# --- build_derived_columns ---

def test_build_derived_columns_product_container_id():
    df = pd.DataFrame({
        "item_id": ["ITM001"],
        "contenedor": ["BOX"],
        "fecha_ingreso": pd.to_datetime(["2024-01-15"]),
        "fecha_vencimiento": pd.to_datetime(["2025-04-10"]),
        "unds": [10.0],
    })
    result = build_derived_columns(df, FECHA_CORTE)
    assert result.loc[0, "product_container_id"] == "ITM001_BOX"

def test_build_derived_columns_dias_en_inventario():
    df = pd.DataFrame({
        "item_id": ["ITM001"],
        "contenedor": ["BOX"],
        "fecha_ingreso": pd.to_datetime(["2024-01-15"]),
        "fecha_vencimiento": pd.to_datetime(["2025-05-15"]),
        "unds": [10.0],
    })
    result = build_derived_columns(df, FECHA_CORTE)
    # 2025-04-01 - 2024-01-15 = 442 días
    assert result.loc[0, "dias_en_inventario"] == 442

def test_build_derived_columns_dias_para_vencimiento():
    df = pd.DataFrame({
        "item_id": ["ITM001"],
        "contenedor": ["BOX"],
        "fecha_ingreso": pd.to_datetime(["2024-01-15"]),
        "fecha_vencimiento": pd.to_datetime(["2025-04-10"]),
        "unds": [10.0],
    })
    result = build_derived_columns(df, FECHA_CORTE)
    # 2025-04-10 - 2025-04-01 = 9 días
    assert result.loc[0, "dias_para_vencimiento"] == 9

def test_build_derived_columns_estado_vencido():
    df = pd.DataFrame({
        "item_id": ["ITM002"],
        "contenedor": ["BOX"],
        "fecha_ingreso": pd.to_datetime(["2023-06-01"]),
        "fecha_vencimiento": pd.to_datetime(["2025-03-20"]),
        "unds": [5.0],
    })
    result = build_derived_columns(df, FECHA_CORTE)
    assert result.loc[0, "estado_inventario"] == "vencido"
    assert result.loc[0, "dias_para_vencimiento"] == -12


# --- apply_calidad_flag ---

def test_calidad_flag_ok():
    row = pd.Series({
        "unds": 10.0, "fecha_ingreso": pd.Timestamp("2024-01-01"),
        "fecha_vencimiento": pd.Timestamp("2025-06-01"),
    })
    assert apply_calidad_flag(row, pd.Timestamp("2025-04-01")) == "ok"

def test_calidad_flag_unds_invalidas():
    row = pd.Series({
        "unds": None, "fecha_ingreso": pd.Timestamp("2024-01-01"),
        "fecha_vencimiento": pd.Timestamp("2025-06-01"),
    })
    assert apply_calidad_flag(row, pd.Timestamp("2025-04-01")) == "unds_invalidas"

def test_calidad_flag_fecha_vencimiento_nula():
    row = pd.Series({
        "unds": 10.0, "fecha_ingreso": pd.Timestamp("2024-01-01"),
        "fecha_vencimiento": None,
    })
    assert apply_calidad_flag(row, pd.Timestamp("2025-04-01")) == "fecha_vencimiento_nula"

def test_calidad_flag_fecha_ingreso_futura():
    row = pd.Series({
        "unds": 10.0, "fecha_ingreso": pd.Timestamp("2026-01-01"),
        "fecha_vencimiento": pd.Timestamp("2027-01-01"),
    })
    assert apply_calidad_flag(row, pd.Timestamp("2025-04-01")) == "fecha_ingreso_futura"

def test_calidad_flag_multiples_alertas():
    row = pd.Series({
        "unds": None, "fecha_ingreso": pd.Timestamp("2026-01-01"),
        "fecha_vencimiento": None,
    })
    assert apply_calidad_flag(row, pd.Timestamp("2025-04-01")) == "multiples_alertas"
```

- [ ] **Step 2: Ejecutar tests y verificar que fallan**

```bash
cd /Users/diegocastro/Desktop/especializacion
python -m pytest tests/etl/test_transform.py -v 2>&1 | head -20
```

Esperado: `ModuleNotFoundError: No module named 'src.etl.transform'`

- [ ] **Step 3: Implementar `src/etl/transform.py`**

```python
# src/etl/transform.py
"""
ETL — Transformación
Limpieza de tipos, outliers, variables derivadas y reglas de negocio.
Umbrales centralizados en src/etl/__init__.py.
"""
import pandas as pd

from src.etl import (
    FECHA_CORTE,
    UMBRAL_CRITICO,
    UMBRAL_PROXIMO,
    COLS_VACIAS_POR_DISENO,
    UMBRAL_NULOS_FILA,
)


# --------------------------------------------------------------------------
# Clasificadores de negocio (funciones puras, testeables individualmente)
# --------------------------------------------------------------------------

def classify_estado(dias_para_vencimiento) -> str:
    """
    Clasifica el estado de inventario según días para vencimiento.
    Sincronizado con dw.clasificar_estado_inventario en sql/03_reglas_negocio.sql.
    """
    if pd.isna(dias_para_vencimiento):
        return "sin_fecha"
    d = int(dias_para_vencimiento)
    if d < 0:
        return "vencido"
    if d <= UMBRAL_CRITICO:
        return "critico"
    if d <= UMBRAL_PROXIMO:
        return "proximo_a_vencer"
    return "vigente"


def classify_segmento_rotacion(dias_en_inventario) -> str:
    """
    Proxy de rotación basado en antigüedad en inventario.
    Sincronizado con dw.calcular_segmento_rotacion en sql/03_reglas_negocio.sql.
    """
    if pd.isna(dias_en_inventario) or int(dias_en_inventario) < 0:
        return "sin_dato"
    d = int(dias_en_inventario)
    if d <= 30:
        return "alta_rotacion"
    if d <= 90:
        return "media_rotacion"
    return "baja_rotacion"


def compute_score_riesgo(estado_inventario: str, segmento_rotacion: str) -> int:
    """
    Score entero 0–4.
    +3 vencido, +2 critico, +1 proximo_a_vencer, +1 baja_rotacion.
    Sincronizado con dw.calcular_score_riesgo en sql/03_reglas_negocio.sql.
    """
    score = 0
    if estado_inventario == "vencido":
        score += 3
    elif estado_inventario == "critico":
        score += 2
    elif estado_inventario == "proximo_a_vencer":
        score += 1
    if segmento_rotacion == "baja_rotacion":
        score += 1
    return score


def apply_calidad_flag(row: pd.Series, fecha_corte: pd.Timestamp) -> str:
    """
    Determina el flag de calidad de un registro.

    Valores posibles:
        'ok'                    — sin alertas
        'unds_invalidas'        — unds nulo tras limpieza
        'fecha_vencimiento_nula'— fecha_vencimiento nula
        'fecha_ingreso_futura'  — fecha_ingreso > fecha_corte
        'multiples_alertas'     — más de una condición anterior
    """
    alertas = []
    if pd.isna(row.get("unds")):
        alertas.append("unds_invalidas")
    if pd.isna(row.get("fecha_vencimiento")):
        alertas.append("fecha_vencimiento_nula")
    fi = row.get("fecha_ingreso")
    if pd.notna(fi) and pd.Timestamp(fi) > fecha_corte:
        alertas.append("fecha_ingreso_futura")

    if len(alertas) == 0:
        return "ok"
    if len(alertas) == 1:
        return alertas[0]
    return "multiples_alertas"


# --------------------------------------------------------------------------
# Pasos de transformación
# --------------------------------------------------------------------------

def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas a sus tipos correctos."""
    df = df.copy()
    df["unds"] = pd.to_numeric(df["unds"], errors="coerce")
    df["fecha_ingreso"] = pd.to_datetime(df["fecha_ingreso"], errors="coerce")
    df["fecha_vencimiento"] = pd.to_datetime(df["fecha_vencimiento"], errors="coerce")
    return df


def clean_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trata outliers por error (los anula):
    - unds <= 0: error de datos, se anula (no puede haber 0 o negativo en inventario activo)

    Outliers por variabilidad natural (dias_en_inventario alto, unds muy grande)
    se preservan — son datos válidos aunque extremos.
    """
    df = df.copy()
    # Supuesto: unds <= 0 es un error de registro, no variabilidad natural
    df.loc[df["unds"] <= 0, "unds"] = None
    return df


def drop_low_quality_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Elimina registros con >UMBRAL_NULOS_FILA de columnas relevantes nulas.
    Las columnas estructuralmente vacías (id_inventario, rotacion_raw, estado_raw)
    se excluyen del cálculo para no sesgar el umbral.

    Returns:
        (df_clean, df_dropped) — registros aceptados y descartados.
    """
    cols_relevantes = [c for c in df.columns if c not in COLS_VACIAS_POR_DISENO]
    null_frac = df[cols_relevantes].isnull().mean(axis=1)
    mask_ok = null_frac <= UMBRAL_NULOS_FILA
    return df[mask_ok].copy(), df[~mask_ok].copy()


def build_derived_columns(df: pd.DataFrame, fecha_corte: pd.Timestamp) -> pd.DataFrame:
    """
    Construye todas las variables derivadas obligatorias.

    Supuesto: fecha_corte es fija (2025-04-01). No se usa la fecha del sistema.
    """
    df = df.copy()

    # Clave operativa compuesta
    df["product_container_id"] = (
        df["item_id"].astype(str) + "_" + df["contenedor"].astype(str)
    )

    # Antigüedad en inventario (base para rotación y ML)
    df["dias_en_inventario"] = (fecha_corte - df["fecha_ingreso"]).dt.days

    # Días para vencimiento (positivo = aún vigente, negativo = vencido)
    df["dias_para_vencimiento"] = (df["fecha_vencimiento"] - fecha_corte).dt.days

    # Estado de inventario (regla de negocio)
    df["estado_inventario"] = df["dias_para_vencimiento"].apply(classify_estado)

    # Segmento de rotación (proxy por antigüedad)
    df["segmento_rotacion"] = df["dias_en_inventario"].apply(classify_segmento_rotacion)

    # Score de riesgo (entero 0–4)
    df["score_riesgo"] = df.apply(
        lambda r: compute_score_riesgo(r["estado_inventario"], r["segmento_rotacion"]),
        axis=1,
    ).astype(int)

    # Dimensiones temporales de vencimiento
    df["mes_vencimiento"] = df["fecha_vencimiento"].dt.month
    df["anio_vencimiento"] = df["fecha_vencimiento"].dt.year

    # Flag de calidad
    df["calidad_flag"] = df.apply(
        lambda r: apply_calidad_flag(r, fecha_corte), axis=1
    )

    return df


def transform(df: pd.DataFrame, fecha_corte: pd.Timestamp = FECHA_CORTE) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aplica el pipeline de transformación completo.

    Args:
        df:           DataFrame crudo normalizado (salida de extract.load_excel).
        fecha_corte:  Fecha fija de referencia.

    Returns:
        (df_clean, df_dropped) — transformado y descartados.
    """
    df = cast_types(df)
    df = clean_outliers(df)
    df, df_dropped = drop_low_quality_rows(df)
    df = build_derived_columns(df, fecha_corte)
    return df, df_dropped
```

- [ ] **Step 4: Ejecutar tests y verificar que pasan**

```bash
cd /Users/diegocastro/Desktop/especializacion
python -m pytest tests/etl/test_transform.py -v
```

Esperado: todos los tests pasan (`passed` en verde)

- [ ] **Step 5: Commit**

```bash
git add src/etl/transform.py tests/etl/test_transform.py
git commit -m "feat(etl): add transform module with business rules and derived columns"
```

---

## Task 5: Módulo de validación

**Files:**
- Create: `src/etl/validate.py`
- Create: `tests/etl/test_validate.py`

- [ ] **Step 1: Escribir los tests**

```python
# tests/etl/test_validate.py
import pandas as pd
import pytest

from src.etl.validate import (
    check_types,
    check_duplicates,
    check_estado_coherence,
    check_fechas,
    run_all_validations,
)
from src.etl import FECHA_CORTE


def test_check_types_passes_on_clean(df_clean_sample):
    issues = check_types(df_clean_sample)
    assert issues == []


def test_check_types_fails_on_string_unds():
    df = pd.DataFrame({"unds": ["10", "5"], "dias_en_inventario": [100, 200],
                       "dias_para_vencimiento": [10, 20], "score_riesgo": [1, 0]})
    issues = check_types(df)
    assert any("unds" in i for i in issues)


def test_check_duplicates_no_dups(df_clean_sample):
    issues = check_duplicates(df_clean_sample)
    assert issues == []


def test_check_duplicates_finds_dups():
    df = pd.DataFrame({
        "product_container_id": ["ITM001_BOX", "ITM001_BOX", "ITM002_CAN"],
    })
    issues = check_duplicates(df)
    assert len(issues) == 1
    assert "1 duplicados" in issues[0]


def test_check_estado_coherence_passes(df_clean_sample):
    issues = check_estado_coherence(df_clean_sample)
    assert issues == []


def test_check_estado_coherence_fails():
    df = pd.DataFrame({
        "dias_para_vencimiento": [-5, 10],
        "estado_inventario": ["vigente", "vencido"],   # invertidos a propósito
    })
    issues = check_estado_coherence(df)
    assert len(issues) > 0


def test_check_fechas_flags_future_ingreso():
    df = pd.DataFrame({
        "fecha_ingreso": pd.to_datetime(["2026-01-01", "2024-01-01"]),
        "fecha_vencimiento": pd.to_datetime(["2027-01-01", "2025-06-01"]),
    })
    issues = check_fechas(df, FECHA_CORTE)
    assert any("futura" in i for i in issues)


def test_run_all_validations_returns_dict(df_clean_sample):
    report = run_all_validations(df_clean_sample, FECHA_CORTE)
    assert isinstance(report, dict)
    assert "tipos" in report
    assert "duplicados" in report
    assert "coherencia_estado" in report
    assert "fechas" in report
```

- [ ] **Step 2: Ejecutar tests y verificar que fallan**

```bash
cd /Users/diegocastro/Desktop/especializacion
python -m pytest tests/etl/test_validate.py -v 2>&1 | head -10
```

Esperado: `ModuleNotFoundError`

- [ ] **Step 3: Implementar `src/etl/validate.py`**

```python
# src/etl/validate.py
"""
ETL — Validaciones post-transformación.
Detecta problemas de calidad y genera un reporte estructurado.
No modifica los datos — solo reporta.
"""
import pandas as pd


def check_types(df: pd.DataFrame) -> list[str]:
    """Verifica que las columnas numéricas y de fecha tengan el tipo correcto."""
    issues = []
    numeric_cols = ["unds", "dias_en_inventario", "dias_para_vencimiento", "score_riesgo"]
    for col in numeric_cols:
        if col in df.columns and not pd.api.types.is_numeric_dtype(df[col]):
            issues.append(f"Columna '{col}' debería ser numérica, es {df[col].dtype}")
    date_cols = ["fecha_ingreso", "fecha_vencimiento"]
    for col in date_cols:
        if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
            issues.append(f"Columna '{col}' debería ser datetime, es {df[col].dtype}")
    return issues


def check_duplicates(df: pd.DataFrame) -> list[str]:
    """Verifica que no haya product_container_id duplicados."""
    issues = []
    if "product_container_id" not in df.columns:
        return issues
    n_dups = df["product_container_id"].duplicated().sum()
    if n_dups > 0:
        issues.append(
            f"product_container_id: {n_dups} duplicados encontrados. "
            f"Revisar si la granularidad del archivo tiene múltiples contenedores por ítem."
        )
    return issues


def check_estado_coherence(df: pd.DataFrame) -> list[str]:
    """
    Verifica que estado_inventario sea coherente con dias_para_vencimiento.
    No lanza error — reporta inconsistencias para auditoría.
    """
    issues = []
    if "dias_para_vencimiento" not in df.columns or "estado_inventario" not in df.columns:
        return issues

    # Vencidos deben tener dias_para_vencimiento < 0
    mask_vencido_inco = (df["estado_inventario"] == "vencido") & (df["dias_para_vencimiento"] >= 0)
    n = mask_vencido_inco.sum()
    if n > 0:
        issues.append(f"{n} registros marcados 'vencido' con dias_para_vencimiento >= 0")

    # Vigentes no deben tener dias_para_vencimiento negativo
    mask_vigente_inco = (df["estado_inventario"] == "vigente") & (df["dias_para_vencimiento"] < 0)
    n = mask_vigente_inco.sum()
    if n > 0:
        issues.append(f"{n} registros marcados 'vigente' con dias_para_vencimiento < 0")

    return issues


def check_fechas(df: pd.DataFrame, fecha_corte: pd.Timestamp) -> list[str]:
    """Verifica anomalías en columnas de fecha."""
    issues = []
    if "fecha_ingreso" in df.columns:
        n_futura = (df["fecha_ingreso"] > fecha_corte).sum()
        if n_futura > 0:
            issues.append(
                f"{n_futura} registros con fecha_ingreso futura (> {fecha_corte.date()}). "
                "Posible error en la fuente. Se preservan con calidad_flag='fecha_ingreso_futura'."
            )
    if "fecha_vencimiento" in df.columns:
        n_nula = df["fecha_vencimiento"].isna().sum()
        if n_nula > 0:
            issues.append(f"{n_nula} registros con fecha_vencimiento nula.")
    return issues


def run_all_validations(df: pd.DataFrame, fecha_corte: pd.Timestamp) -> dict:
    """
    Ejecuta todas las validaciones y devuelve un reporte estructurado.

    Returns:
        Dict con clave por tipo de validación y lista de issues encontrados.
        Lista vacía = sin problemas en esa categoría.
    """
    report = {
        "tipos": check_types(df),
        "duplicados": check_duplicates(df),
        "coherencia_estado": check_estado_coherence(df),
        "fechas": check_fechas(df, fecha_corte),
    }

    total_issues = sum(len(v) for v in report.values())
    if total_issues == 0:
        print("Validacion completada: sin problemas detectados.")
    else:
        print(f"Validacion completada: {total_issues} advertencia(s) encontradas.")
        for categoria, issues in report.items():
            for issue in issues:
                print(f"  [{categoria}] {issue}")

    return report
```

- [ ] **Step 4: Ejecutar tests y verificar que pasan**

```bash
cd /Users/diegocastro/Desktop/especializacion
python -m pytest tests/etl/test_validate.py -v
```

Esperado: todos los tests pasan

- [ ] **Step 5: Commit**

```bash
git add src/etl/validate.py tests/etl/test_validate.py
git commit -m "feat(etl): add validate module with post-transform quality checks"
```

---

## Task 6: Pipeline orquestador

**Files:**
- Create: `src/etl/pipeline.py`

- [ ] **Step 1: Implementar el orquestador**

```python
# src/etl/pipeline.py
"""
ETL — Pipeline orquestador.
Ejecuta el flujo completo: extract → transform → validate → load.

Uso desde línea de comandos:
    python -m src.etl.pipeline

Uso desde notebook:
    from src.etl.pipeline import run_pipeline
    df_clean, report = run_pipeline(load_to_db=True)
"""
from pathlib import Path

import pandas as pd

from src.etl import FECHA_CORTE, SOURCE_FILE, SHEET_NAME
from src.etl.extract import load_excel
from src.etl.transform import transform
from src.etl.validate import run_all_validations


def run_pipeline(
    filepath: str = SOURCE_FILE,
    fecha_corte: pd.Timestamp = FECHA_CORTE,
    load_to_db: bool = False,
    echo_sql: bool = False,
) -> tuple[pd.DataFrame, dict]:
    """
    Ejecuta el pipeline ETL completo.

    Args:
        filepath:    Ruta al archivo Excel fuente.
        fecha_corte: Fecha fija de referencia (default: FECHA_CORTE).
        load_to_db:  Si True, carga los resultados a Neon PostgreSQL.
        echo_sql:    Si True, imprime las queries SQL ejecutadas.

    Returns:
        (df_clean, validation_report)
    """
    source_file = Path(filepath).name
    print(f"=== ETL Inventarios 360 ===")
    print(f"  Fuente      : {filepath}")
    print(f"  Hoja        : {SHEET_NAME}")
    print(f"  fecha_corte : {fecha_corte.date()}")
    print()

    # 1. Extracción
    print("[1/4] Extrayendo datos...")
    df_raw = load_excel(filepath, sheet_name=SHEET_NAME)
    print(f"      {len(df_raw):,} registros leídos, {len(df_raw.columns)} columnas.")

    # 2. Transformación
    print("[2/4] Transformando...")
    df_clean, df_dropped = transform(df_raw, fecha_corte)
    print(f"      {len(df_clean):,} registros limpios | {len(df_dropped):,} descartados.")

    # 3. Validación
    print("[3/4] Validando...")
    report = run_all_validations(df_clean, fecha_corte)

    # Resumen de estados
    if "estado_inventario" in df_clean.columns:
        print("\n  Distribución de estados:")
        for estado, n in df_clean["estado_inventario"].value_counts().items():
            pct = n / len(df_clean) * 100
            print(f"    {estado:<20} {n:>6,}  ({pct:.1f}%)")
    print()

    # 4. Carga (opcional)
    if load_to_db:
        print("[4/4] Cargando a Neon PostgreSQL...")
        from src.db.loader import InventoryLoader
        loader = InventoryLoader(
            df_raw=df_raw,
            df_clean=df_clean,
            fecha_corte=fecha_corte,
            source_file=source_file,
        )
        loader.load_all()
    else:
        print("[4/4] Carga a DB omitida (load_to_db=False).")

    print("\n=== Pipeline completado ===")
    return df_clean, report


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ETL Inventarios 360")
    parser.add_argument("--load", action="store_true", help="Cargar a Neon PostgreSQL")
    parser.add_argument("--echo", action="store_true", help="Mostrar queries SQL")
    args = parser.parse_args()

    run_pipeline(load_to_db=args.load, echo_sql=args.echo)
```

- [ ] **Step 2: Ejecutar el pipeline en modo dry-run (sin carga a DB)**

```bash
cd /Users/diegocastro/Desktop/especializacion
python -m src.etl.pipeline
```

Esperado:
```
=== ETL Inventarios 360 ===
  Fuente      : datos_proyecto/Fechas de Vencimiento Beauty Care 1.xlsx
  Hoja        : Page1_1
  fecha_corte : 2025-04-01

[1/4] Extrayendo datos...
      15950 registros leídos, 12 columnas.
[2/4] Transformando...
      XXXXX registros limpios | X descartados.
[3/4] Validando...
[4/4] Carga a DB omitida (load_to_db=False).

=== Pipeline completado ===
```

- [ ] **Step 3: Ejecutar suite de tests completa**

```bash
cd /Users/diegocastro/Desktop/especializacion
python -m pytest tests/etl/ -v
```

Esperado: todos los tests pasan

- [ ] **Step 4: Commit**

```bash
git add src/etl/pipeline.py
git commit -m "feat(etl): add pipeline orchestrator with dry-run and optional DB load"
```

---

## Task 7: Integración con el notebook

**Files:**
- Modify: `proyecto_graduacion.ipynb`

- [ ] **Step 1: Agregar celda de configuración al inicio del notebook**

Agregar como primera celda de código:

```python
# Inventarios 360 — Configuración del entorno
import sys
from pathlib import Path

# Asegurar que src/ esté en el path
sys.path.insert(0, str(Path.cwd()))

from dotenv import load_dotenv
load_dotenv()

from src.etl import FECHA_CORTE, UMBRAL_CRITICO, UMBRAL_PROXIMO
print(f"fecha_corte  : {FECHA_CORTE.date()}")
print(f"umbral_critico : {UMBRAL_CRITICO} días")
print(f"umbral_proximo : {UMBRAL_PROXIMO} días")
```

- [ ] **Step 2: Agregar celda de ejecución del pipeline**

```python
from src.etl.pipeline import run_pipeline

# load_to_db=True requiere variables de entorno configuradas en .env
df_clean, validation_report = run_pipeline(load_to_db=False)
df_clean.head()
```

- [ ] **Step 3: Agregar celda de verificación de calidad**

```python
import pandas as pd

print(f"Shape: {df_clean.shape}")
print(f"\nTipos:")
print(df_clean.dtypes)
print(f"\nNulos por columna:")
print(df_clean.isnull().sum())
print(f"\nDistribución de calidad_flag:")
print(df_clean["calidad_flag"].value_counts())
print(f"\nDistribución de estado_inventario:")
print(df_clean["estado_inventario"].value_counts())
print(f"\nRango de score_riesgo: {df_clean['score_riesgo'].min()} – {df_clean['score_riesgo'].max()}")
```

- [ ] **Step 4: Commit**

```bash
git add proyecto_graduacion.ipynb
git commit -m "feat(notebook): integrate ETL pipeline into main notebook"
```

---

## Self-Review

### Spec coverage

| Requisito del spec | Task que lo cubre |
|--------------------|-------------------|
| Leer hoja `Page1_1` | Task 3 — `load_excel` |
| Normalizar columnas a snake_case | Task 3 — `normalize_columns` |
| `product_container_id` compuesto | Task 4 — `build_derived_columns` |
| No usar `id_inventario` | Task 1 — excluido en `COLS_VACIAS_POR_DISENO` |
| No confiar en `dias_antes_de_vencimiento` | Task 4 — recalculado desde fechas |
| `dias_en_inventario` y `dias_para_vencimiento` | Task 4 |
| `estado_inventario` con reglas configurables | Task 4 — umbrales en `__init__.py` |
| `segmento_rotacion` por proxy | Task 4 |
| `score_riesgo` entero 0–4 | Task 4 |
| `mes_vencimiento`, `anio_vencimiento` | Task 4 |
| `row_null_pct` | Task 3 — `compute_row_null_pct` |
| `calidad_flag` con 5 valores definidos | Task 4 — `apply_calidad_flag` |
| Limpieza de outliers (unds <= 0 → null) | Task 4 — `clean_outliers` |
| Validar tipos post-transform | Task 5 |
| Validar duplicados de `product_container_id` | Task 5 |
| Coherencia `dias_para_vencimiento` ↔ `estado_inventario` | Task 5 |
| Fechas futuras en `fecha_ingreso` | Task 5 |
| Cargar `stg.inventory_raw` | Task 6 — `load_to_db=True` → `InventoryLoader` |
| Cargar `dw.inventory_clean` | Task 6 |
| Cargar `mart.fact_inventory_snapshot` | Task 6 |
| Cargar `mart.ml_inventory_features` | Task 6 |
| Sin credenciales hardcodeadas | Task 6 — variables de entorno + `load_dotenv` |
| Integración con notebook | Task 7 |

### Placeholder scan
- Sin TBDs ni TODOs
- Todos los pasos tienen código concreto
- Comandos de test con output esperado incluidos

### Type consistency
- `classify_estado(dias_para_vencimiento) -> str` — usado consistentemente en `build_derived_columns`
- `compute_score_riesgo(estado, segmento) -> int` — retorna `int`, se castea en `build_derived_columns`
- `run_pipeline()` devuelve `(pd.DataFrame, dict)` — consistente en Task 6 y Task 7
- `InventoryLoader(df_raw, df_clean, fecha_corte, source_file)` — firma ya definida en `src/db/loader.py`
