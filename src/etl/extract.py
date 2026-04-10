"""
ETL — Extracción
Lee el archivo Excel, normaliza columnas y calcula row_null_pct.
"""
from pathlib import Path

import pandas as pd

from src.etl import COLS_VACIAS_POR_DISENO


# Mapa de rename explícito para columnas con nombres ambiguos tras normalización
_RENAME_MAP = {
    "fecha_de_ingreso": "fecha_ingreso",
    "dias_antes_de_vencimiento": "dias_antes_vencimiento_raw",
    "rotacion": "rotacion_raw",
    "estado": "estado_raw",
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
    return df


def compute_row_null_pct(df: pd.DataFrame, exclude_cols: list) -> pd.Series:
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
    # Calcular row_null_pct antes de renombrar (COLS_VACIAS_POR_DISENO usa nombres pre-rename)
    df["row_null_pct"] = compute_row_null_pct(df, exclude_cols=list(COLS_VACIAS_POR_DISENO))
    # Aplicar renames explícitos para columnas conocidas de la fuente
    df = df.rename(columns={k: v for k, v in _RENAME_MAP.items() if k in df.columns})
    return df
