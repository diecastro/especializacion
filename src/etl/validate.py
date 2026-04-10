# src/etl/validate.py
"""
ETL — Validaciones post-transformación.
Detecta problemas de calidad y genera un reporte estructurado.
No modifica los datos — solo reporta.
"""
import pandas as pd


def check_types(df: pd.DataFrame) -> list[str]:
    """Verifica que columnas numéricas y de fecha tengan el tipo correcto."""
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
    """Verifica que estado_inventario sea coherente con dias_para_vencimiento."""
    issues = []
    if "dias_para_vencimiento" not in df.columns or "estado_inventario" not in df.columns:
        return issues
    mask_vencido_inco = (
        (df["estado_inventario"] == "vencido") & (df["dias_para_vencimiento"] >= 0)
    )
    n = mask_vencido_inco.sum()
    if n > 0:
        issues.append(f"{n} registros marcados 'vencido' con dias_para_vencimiento >= 0")
    mask_vigente_inco = (
        (df["estado_inventario"] == "vigente") & (df["dias_para_vencimiento"] < 0)
    )
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
                "Se preservan con calidad_flag='fecha_ingreso_futura'."
            )
    if "fecha_vencimiento" in df.columns:
        n_nula = df["fecha_vencimiento"].isna().sum()
        if n_nula > 0:
            issues.append(f"{n_nula} registros con fecha_vencimiento nula.")
    return issues


def run_all_validations(df: pd.DataFrame, fecha_corte: pd.Timestamp) -> dict:
    """
    Ejecuta todas las validaciones y devuelve un reporte estructurado.
    Returns: dict con clave por tipo y lista de issues. Lista vacía = sin problemas.
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
