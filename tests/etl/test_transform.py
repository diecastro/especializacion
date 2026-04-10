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
    impute_unds,
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


# --- impute_unds ---

def test_impute_unds_fills_with_median():
    # Mediana de [10, 20, 30] = 20
    df = pd.DataFrame({"unds": [10.0, 20.0, None, 30.0, None]})
    result = impute_unds(df)
    assert result["unds"].isna().sum() == 0
    assert result.loc[2, "unds"] == 20.0
    assert result.loc[4, "unds"] == 20.0

def test_impute_unds_no_op_when_no_nulls():
    df = pd.DataFrame({"unds": [10.0, 20.0, 30.0]})
    result = impute_unds(df)
    assert list(result["unds"]) == [10.0, 20.0, 30.0]

def test_impute_unds_no_op_without_column():
    df = pd.DataFrame({"otra_col": [1, 2, 3]})
    result = impute_unds(df)
    assert list(result.columns) == ["otra_col"]

def test_impute_unds_uses_median_not_mean():
    # Distribución asimétrica: mediana=10, media=370/3≈123
    df = pd.DataFrame({"unds": [5.0, 10.0, None, 1000.0]})
    result = impute_unds(df)
    # Mediana de [5, 10, 1000] = 10
    assert result.loc[2, "unds"] == 10.0


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
