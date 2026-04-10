# src/etl/transform.py
"""
ETL — Transformación. Umbrales en src/etl/__init__.py, sincronizados con sql/03_reglas_negocio.sql.
"""
import pandas as pd
from src.etl import FECHA_CORTE, UMBRAL_CRITICO, UMBRAL_PROXIMO, COLS_VACIAS_POST_RENAME, UMBRAL_NULOS_FILA


def classify_estado(dias_para_vencimiento) -> str:
    """Clasifica estado según días para vencimiento."""
    if dias_para_vencimiento is None or pd.isna(dias_para_vencimiento):
        return "sin_fecha"
    d = int(dias_para_vencimiento)
    if d < 0: return "vencido"
    if d <= UMBRAL_CRITICO: return "critico"
    if d <= UMBRAL_PROXIMO: return "proximo_a_vencer"
    return "vigente"


def classify_segmento_rotacion(dias_en_inventario) -> str:
    """Proxy de rotación por antigüedad en inventario."""
    if dias_en_inventario is None or pd.isna(dias_en_inventario):
        return "sin_dato"
    d = int(dias_en_inventario)
    if d < 0: return "sin_dato"
    if d <= 30: return "alta_rotacion"
    if d <= 90: return "media_rotacion"
    return "baja_rotacion"


def compute_score_riesgo(estado_inventario: str, segmento_rotacion: str) -> int:
    """Score entero 0-4: +3 vencido, +2 critico, +1 proximo, +1 baja_rotacion."""
    score = 0
    if estado_inventario == "vencido": score += 3
    elif estado_inventario == "critico": score += 2
    elif estado_inventario == "proximo_a_vencer": score += 1
    if segmento_rotacion == "baja_rotacion": score += 1
    return score


def apply_calidad_flag(row: pd.Series, fecha_corte) -> str:
    """Valores: ok | unds_invalidas | fecha_vencimiento_nula | fecha_ingreso_futura | multiples_alertas"""
    fc = pd.Timestamp(fecha_corte)
    alertas = []
    if pd.isna(row.get("unds")):
        alertas.append("unds_invalidas")
    if pd.isna(row.get("fecha_vencimiento")):
        alertas.append("fecha_vencimiento_nula")
    fi = row.get("fecha_ingreso")
    if fi is not None and pd.notna(fi) and pd.Timestamp(fi) > fc:
        alertas.append("fecha_ingreso_futura")
    if len(alertas) == 0: return "ok"
    if len(alertas) == 1: return alertas[0]
    return "multiples_alertas"


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte unds a numérico y fechas a datetime."""
    df = df.copy()
    df["unds"] = pd.to_numeric(df["unds"], errors="coerce").astype(float)
    # Maneja ambos nombres: fecha_de_ingreso (pre-rename) y fecha_ingreso (post-rename)
    fecha_ingreso_col = "fecha_ingreso" if "fecha_ingreso" in df.columns else "fecha_de_ingreso"
    if fecha_ingreso_col in df.columns:
        df[fecha_ingreso_col] = pd.to_datetime(df[fecha_ingreso_col], errors="coerce")
        # Renombra a fecha_ingreso si es necesario
        if fecha_ingreso_col != "fecha_ingreso":
            df = df.rename(columns={fecha_ingreso_col: "fecha_ingreso"})
    if "fecha_vencimiento" in df.columns:
        df["fecha_vencimiento"] = pd.to_datetime(df["fecha_vencimiento"], errors="coerce")
    return df


def clean_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Anula unds <= 0 (error de registro). Outliers naturales se preservan."""
    df = df.copy()
    if "unds" in df.columns:
        df.loc[df["unds"] <= 0, "unds"] = None
    return df


def drop_low_quality_rows(df: pd.DataFrame) -> tuple:
    """Elimina registros con >UMBRAL_NULOS_FILA nulos en columnas relevantes."""
    cols_relevantes = [c for c in df.columns if c not in COLS_VACIAS_POST_RENAME]
    null_frac = df[cols_relevantes].isnull().mean(axis=1)
    mask_ok = null_frac <= UMBRAL_NULOS_FILA
    return df[mask_ok].copy(), df[~mask_ok].copy()


def build_derived_columns(df: pd.DataFrame, fecha_corte=FECHA_CORTE) -> pd.DataFrame:
    """Construye variables derivadas obligatorias. fecha_corte fija para reproducibilidad."""
    df = df.copy()
    fc = pd.Timestamp(fecha_corte)  # acepta datetime.date o pd.Timestamp
    df["product_container_id"] = df["item_id"].astype(str) + "_" + df["contenedor"].astype(str)
    df["dias_en_inventario"] = (fc - df["fecha_ingreso"]).dt.days
    df["dias_para_vencimiento"] = (df["fecha_vencimiento"] - fc).dt.days
    df["estado_inventario"] = df["dias_para_vencimiento"].apply(classify_estado)
    df["segmento_rotacion"] = df["dias_en_inventario"].apply(classify_segmento_rotacion)
    df["score_riesgo"] = df.apply(
        lambda r: compute_score_riesgo(r["estado_inventario"], r["segmento_rotacion"]), axis=1
    ).astype(int)
    df["mes_vencimiento"] = df["fecha_vencimiento"].dt.month
    df["anio_vencimiento"] = df["fecha_vencimiento"].dt.year
    df["calidad_flag"] = df.apply(lambda r: apply_calidad_flag(r, fc), axis=1)
    return df


def transform(df: pd.DataFrame, fecha_corte=FECHA_CORTE) -> tuple:
    """Pipeline completo. Returns (df_clean, df_dropped)."""
    df = cast_types(df)
    df = clean_outliers(df)
    df, df_dropped = drop_low_quality_rows(df)
    df = build_derived_columns(df, fecha_corte)
    return df, df_dropped
