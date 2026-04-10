import pandas as pd
import pytest
from src.etl.validate import (
    check_types, check_duplicates, check_estado_coherence,
    check_fechas, run_all_validations,
)
from src.etl import FECHA_CORTE


def test_check_types_passes_on_clean(df_clean_sample):
    issues = check_types(df_clean_sample)
    assert issues == []


def test_check_types_fails_on_string_unds():
    df = pd.DataFrame({
        "unds": ["10", "5"],
        "dias_en_inventario": [100, 200],
        "dias_para_vencimiento": [10, 20],
        "score_riesgo": [1, 0],
    })
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
        "estado_inventario": ["vigente", "vencido"],  # intentionally wrong
    })
    issues = check_estado_coherence(df)
    assert len(issues) > 0


def test_check_fechas_flags_future_ingreso():
    df = pd.DataFrame({
        "fecha_ingreso": pd.to_datetime(["2026-01-01", "2024-01-01"]),
        "fecha_vencimiento": pd.to_datetime(["2027-01-01", "2025-06-01"]),
    })
    issues = check_fechas(df, pd.Timestamp(FECHA_CORTE))
    assert any("futura" in i for i in issues)


def test_run_all_validations_returns_dict(df_clean_sample):
    report = run_all_validations(df_clean_sample, pd.Timestamp(FECHA_CORTE))
    assert isinstance(report, dict)
    assert "tipos" in report
    assert "duplicados" in report
    assert "coherencia_estado" in report
    assert "fechas" in report
