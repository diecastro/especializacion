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
    result = compute_row_null_pct(df, exclude_cols=list(COLS_VACIAS_POR_DISENO))
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
