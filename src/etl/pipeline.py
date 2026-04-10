# src/etl/pipeline.py
"""
ETL — Pipeline orquestador.
Uso: python -m src.etl.pipeline [--load] [--echo]
"""
from pathlib import Path
import pandas as pd
from src.etl import FECHA_CORTE, SOURCE_FILE, SHEET_NAME
from src.etl.extract import load_excel
from src.etl.transform import transform
from src.etl.validate import run_all_validations


def run_pipeline(
    filepath: str = SOURCE_FILE,
    fecha_corte=FECHA_CORTE,
    load_to_db: bool = False,
    echo_sql: bool = False,
) -> tuple:
    """
    Pipeline ETL completo: extract → transform → validate → load (opcional).
    Returns: (df_clean, validation_report)
    """
    source_file = Path(filepath).name
    print("=== ETL Inventarios 360 ===")
    print(f"  Fuente      : {filepath}")
    print(f"  Hoja        : {SHEET_NAME}")
    print(f"  fecha_corte : {pd.Timestamp(fecha_corte).date()}")
    print()

    print("[1/4] Extrayendo datos...")
    df_raw = load_excel(filepath, sheet_name=SHEET_NAME)
    print(f"      {len(df_raw):,} registros leídos, {len(df_raw.columns)} columnas.")

    print("[2/4] Transformando...")
    df_clean, df_dropped = transform(df_raw, fecha_corte)
    print(f"      {len(df_clean):,} registros limpios | {len(df_dropped):,} descartados.")

    print("[3/4] Validando...")
    report = run_all_validations(df_clean, pd.Timestamp(fecha_corte))

    if "estado_inventario" in df_clean.columns:
        print("\n  Distribución de estados:")
        for estado, n in df_clean["estado_inventario"].value_counts().items():
            pct = n / len(df_clean) * 100
            print(f"    {estado:<22} {n:>7,}  ({pct:.1f}%)")
    print()

    if load_to_db:
        print("[4/4] Cargando a Neon PostgreSQL...")
        from src.db.loader import InventoryLoader
        loader = InventoryLoader(
            df_raw=df_raw,
            df_clean=df_clean,
            fecha_corte=pd.Timestamp(fecha_corte),
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
