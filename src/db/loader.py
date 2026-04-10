"""
Módulo de carga a PostgreSQL (Neon).

Responsabilidades:
- Ejecutar los scripts SQL de creación del esquema (SchemaLoader).
- Cargar el DataFrame limpio del ETL a las tablas del modelo (InventoryLoader).
- Garantizar idempotencia: re-ejecutar no duplica datos.

Orden de carga (InventoryLoader.load_all):
    1. stg.inventory_raw      — datos crudos para trazabilidad
    2. dw.dim_producto         — dimensión de productos únicos
    3. dw.inventory_clean      — datos limpios con variables derivadas
    4. mart.fact_inventory_snapshot — tabla de hechos con FKs
    5. mart.ml_inventory_features   — features para el modelo ML

Uso:
    from src.db.loader import SchemaLoader, InventoryLoader

    SchemaLoader().run_all()              # crear tablas, vistas y funciones
    InventoryLoader(df_raw, df_clean, fecha_corte, source_file).load_all()
"""

from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.connection import get_connection, get_engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = PROJECT_ROOT / "sql"


# =============================================================================
# SchemaLoader — ejecuta los scripts DDL en orden
# =============================================================================

class SchemaLoader:
    """Ejecuta los scripts SQL en orden para crear el esquema en Neon."""

    SQL_FILES = [
        "01_schema.sql",
        "02_views.sql",
        "03_reglas_negocio.sql",
        "04_populate_dim_tiempo.sql",
    ]

    def run_file(self, filename: str) -> None:
        """Ejecuta un archivo SQL dividiendo por punto y coma."""
        filepath = SQL_DIR / filename
        if not filepath.exists():
            raise FileNotFoundError(f"No se encontro: {filepath}")

        sql = filepath.read_text(encoding="utf-8")
        # Usar autocommit para DDL (psycopg2 requiere salir de la transacción)
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("COMMIT"))
            for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
                try:
                    conn.execute(text(stmt))
                    conn.execute(text("COMMIT"))
                except Exception as exc:
                    print(f"    Advertencia [{filename}]: {exc}")

        print(f"  OK: {filename}")

    def run_all(self) -> None:
        """Ejecuta todos los scripts SQL en orden."""
        print("Creando esquema en Neon PostgreSQL...")
        for filename in self.SQL_FILES:
            self.run_file(filename)
        print("Esquema listo.")


# =============================================================================
# InventoryLoader — carga el DataFrame del ETL al modelo
# =============================================================================

class InventoryLoader:
    """
    Carga el inventario procesado al modelo stg → dw → mart.

    Args:
        df_raw:       DataFrame crudo (columnas originales del Excel).
        df_clean:     DataFrame limpio (resultado del ETL).
        fecha_corte:  Fecha de corte usada en el ETL (pd.Timestamp).
        source_file:  Nombre del archivo fuente (para trazabilidad).
    """

    # Columnas requeridas en df_clean
    REQUIRED_COLUMNS = {
        "product_container_id", "marca", "item_id", "descripcion",
        "categoria", "contenedor", "unds", "fecha_ingreso", "fecha_vencimiento",
        "dias_en_inventario", "dias_para_vencimiento",
        "estado_inventario", "segmento_rotacion", "score_riesgo",
        "mes_vencimiento", "anio_vencimiento",
        "row_null_pct", "calidad_flag",
    }

    def __init__(
        self,
        df_raw: pd.DataFrame,
        df_clean: pd.DataFrame,
        fecha_corte: pd.Timestamp,
        source_file: str,
    ):
        missing = self.REQUIRED_COLUMNS - set(df_clean.columns)
        if missing:
            raise ValueError(f"df_clean no tiene las columnas requeridas: {missing}")

        self.df_raw = df_raw.copy()
        self.df_clean = df_clean.copy()
        self.fecha_corte = fecha_corte
        self.source_file = source_file
        self.engine = get_engine()

    # ------------------------------------------------------------------
    # 1. stg.inventory_raw
    # ------------------------------------------------------------------

    def load_stg_raw(self) -> int:
        """Carga los datos crudos a stg.inventory_raw para trazabilidad."""
        df = self.df_raw.copy()
        df["source_file"] = self.source_file

        # Renombrar columnas al formato del staging
        rename_map = {
            "Marca": "marca",
            "Item ID": "item_id",
            "ID Inventario": "id_inventario",
            "Descripción": "descripcion",
            "Categoría": "categoria",
            "Unds": "unds_raw",
            "Fecha de Ingreso": "fecha_ingreso_raw",
            "Fecha Vencimiento": "fecha_vencimiento_raw",
            "Dias antes de Vencimiento": "dias_antes_vencimiento_raw",
            "Contenedor": "contenedor",
            "Rotación": "rotacion_raw",
            "Estado": "estado_raw",
        }
        df = df.rename(columns=rename_map)

        stg_cols = [
            "source_file", "marca", "item_id", "id_inventario", "descripcion",
            "categoria", "unds_raw", "fecha_ingreso_raw", "fecha_vencimiento_raw",
            "dias_antes_vencimiento_raw", "contenedor", "rotacion_raw", "estado_raw",
        ]
        # Solo columnas que existen en el DataFrame
        stg_cols = [c for c in stg_cols if c in df.columns]

        df[stg_cols].to_sql(
            "inventory_raw",
            self.engine,
            schema="stg",
            if_exists="append",
            index=False,
            chunksize=500,
        )
        return len(df)

    # ------------------------------------------------------------------
    # 2. dw.dim_producto
    # ------------------------------------------------------------------

    def load_dim_producto(self) -> pd.DataFrame:
        """
        Inserta productos únicos en dw.dim_producto.
        Clave de unicidad: (item_id, descripcion, categoria, marca).

        Returns:
            DataFrame con producto_sk + item_id para resolver FKs.
        """
        productos = (
            self.df_clean[["item_id", "descripcion", "categoria", "marca"]]
            .drop_duplicates()
        )

        with self.engine.connect() as conn:
            conn.execute(text("COMMIT"))
            for _, row in productos.iterrows():
                stmt = text("""
                    INSERT INTO dw.dim_producto (item_id, descripcion, categoria, marca)
                    VALUES (:item_id, :descripcion, :categoria, :marca)
                    ON CONFLICT (item_id, descripcion, categoria, marca) DO NOTHING
                """)
                conn.execute(stmt, row.to_dict())
            conn.execute(text("COMMIT"))

        with get_connection() as conn:
            return pd.read_sql(
                "SELECT producto_sk, item_id FROM dw.dim_producto", conn
            )

    # ------------------------------------------------------------------
    # 3. dw.inventory_clean
    # ------------------------------------------------------------------

    def load_inventory_clean(self) -> int:
        """
        Carga los datos limpios a dw.inventory_clean.
        Idempotente por constraint unique (product_container_id, fecha_corte).
        """
        df = self.df_clean.copy()
        df["fecha_corte"] = self.fecha_corte.date()
        df["source_file"] = self.source_file

        clean_cols = [
            "product_container_id", "source_file", "fecha_corte",
            "marca", "item_id", "descripcion", "categoria", "contenedor",
            "unds", "fecha_ingreso", "fecha_vencimiento",
            "dias_en_inventario", "dias_para_vencimiento",
            "estado_inventario", "segmento_rotacion", "score_riesgo",
            "mes_vencimiento", "anio_vencimiento",
            "row_null_pct", "calidad_flag",
        ]

        with self.engine.connect() as conn:
            conn.execute(text("COMMIT"))
            rows = df[clean_cols].to_dict(orient="records")
            for chunk_start in range(0, len(rows), 500):
                chunk = rows[chunk_start:chunk_start + 500]
                stmt = text("""
                    INSERT INTO dw.inventory_clean
                        (product_container_id, source_file, fecha_corte,
                         marca, item_id, descripcion, categoria, contenedor,
                         unds, fecha_ingreso, fecha_vencimiento,
                         dias_en_inventario, dias_para_vencimiento,
                         estado_inventario, segmento_rotacion, score_riesgo,
                         mes_vencimiento, anio_vencimiento,
                         row_null_pct, calidad_flag)
                    VALUES
                        (:product_container_id, :source_file, :fecha_corte,
                         :marca, :item_id, :descripcion, :categoria, :contenedor,
                         :unds, :fecha_ingreso, :fecha_vencimiento,
                         :dias_en_inventario, :dias_para_vencimiento,
                         :estado_inventario, :segmento_rotacion, :score_riesgo,
                         :mes_vencimiento, :anio_vencimiento,
                         :row_null_pct, :calidad_flag)
                    ON CONFLICT (product_container_id, fecha_corte) DO NOTHING
                """)
                conn.execute(stmt, chunk)
            conn.execute(text("COMMIT"))

        return len(df)

    # ------------------------------------------------------------------
    # 4. mart.fact_inventory_snapshot
    # ------------------------------------------------------------------

    def load_fact_snapshot(self, df_producto: pd.DataFrame) -> int:
        """
        Carga mart.fact_inventory_snapshot resolviendo FKs a dw.dim_producto
        y dw.dim_tiempo.
        """
        df = self.df_clean.copy()
        df["fecha_corte"] = self.fecha_corte.date()

        # Resolver FK producto
        df = df.merge(df_producto, on="item_id", how="left")

        # Resolver FKs tiempo (ingreso y vencimiento)
        with get_connection() as conn:
            df_tiempo = pd.read_sql(
                "SELECT tiempo_sk, fecha FROM dw.dim_tiempo", conn
            )

        df = df.merge(
            df_tiempo.rename(columns={"fecha": "fecha_ingreso", "tiempo_sk": "tiempo_ingreso_sk"}),
            on="fecha_ingreso", how="left",
        )
        df = df.merge(
            df_tiempo.rename(columns={"fecha": "fecha_vencimiento", "tiempo_sk": "tiempo_vencimiento_sk"}),
            on="fecha_vencimiento", how="left",
        )

        fact_cols = [
            "fecha_corte", "product_container_id",
            "producto_sk", "tiempo_ingreso_sk", "tiempo_vencimiento_sk",
            "unds", "dias_en_inventario", "dias_para_vencimiento",
            "score_riesgo", "estado_inventario", "segmento_rotacion",
        ]

        with self.engine.connect() as conn:
            conn.execute(text("COMMIT"))
            rows = df[fact_cols].to_dict(orient="records")
            for chunk_start in range(0, len(rows), 500):
                chunk = rows[chunk_start:chunk_start + 500]
                stmt = text("""
                    INSERT INTO mart.fact_inventory_snapshot
                        (fecha_corte, product_container_id,
                         producto_sk, tiempo_ingreso_sk, tiempo_vencimiento_sk,
                         unds, dias_en_inventario, dias_para_vencimiento,
                         score_riesgo, estado_inventario, segmento_rotacion)
                    VALUES
                        (:fecha_corte, :product_container_id,
                         :producto_sk, :tiempo_ingreso_sk, :tiempo_vencimiento_sk,
                         :unds, :dias_en_inventario, :dias_para_vencimiento,
                         :score_riesgo, :estado_inventario, :segmento_rotacion)
                    ON CONFLICT (product_container_id, fecha_corte) DO NOTHING
                """)
                conn.execute(stmt, chunk)
            conn.execute(text("COMMIT"))

        return len(df)

    # ------------------------------------------------------------------
    # 5. mart.ml_inventory_features
    # ------------------------------------------------------------------

    def load_ml_features(self) -> int:
        """
        Carga mart.ml_inventory_features con la variable objetivo riesgo_alto.
        Solo registros sin nulos en las features críticas.
        """
        df = self.df_clean.copy()
        df["fecha_corte"] = self.fecha_corte.date()
        df["riesgo_alto"] = df["estado_inventario"].isin(["vencido", "critico"]).astype(int)

        # Solo registros con features completas (regla del ETL)
        df = df.dropna(subset=["unds", "dias_en_inventario", "dias_para_vencimiento"])

        ml_cols = [
            "fecha_corte", "product_container_id", "item_id",
            "marca", "categoria", "unds",
            "dias_en_inventario", "dias_para_vencimiento",
            "mes_vencimiento", "anio_vencimiento",
            "estado_inventario", "segmento_rotacion",
            "score_riesgo", "riesgo_alto",
        ]

        with self.engine.connect() as conn:
            conn.execute(text("COMMIT"))
            rows = df[ml_cols].to_dict(orient="records")
            for chunk_start in range(0, len(rows), 500):
                chunk = rows[chunk_start:chunk_start + 500]
                stmt = text("""
                    INSERT INTO mart.ml_inventory_features
                        (fecha_corte, product_container_id, item_id,
                         marca, categoria, unds,
                         dias_en_inventario, dias_para_vencimiento,
                         mes_vencimiento, anio_vencimiento,
                         estado_inventario, segmento_rotacion,
                         score_riesgo, riesgo_alto)
                    VALUES
                        (:fecha_corte, :product_container_id, :item_id,
                         :marca, :categoria, :unds,
                         :dias_en_inventario, :dias_para_vencimiento,
                         :mes_vencimiento, :anio_vencimiento,
                         :estado_inventario, :segmento_rotacion,
                         :score_riesgo, :riesgo_alto)
                    ON CONFLICT (product_container_id, fecha_corte) DO NOTHING
                """)
                conn.execute(stmt, chunk)
            conn.execute(text("COMMIT"))

        return len(df)

    # ------------------------------------------------------------------
    # Orquestador principal
    # ------------------------------------------------------------------

    def load_all(self) -> None:
        """Ejecuta la carga completa en el orden correcto."""
        print(f"Cargando inventario — fecha_corte: {self.fecha_corte.date()} | fuente: {self.source_file}")

        print("  [1/5] stg.inventory_raw...")
        n = self.load_stg_raw()
        print(f"        {n:,} filas cargadas.")

        print("  [2/5] dw.dim_producto...")
        df_producto = self.load_dim_producto()
        print(f"        {len(df_producto):,} productos en dimensión.")

        print("  [3/5] dw.inventory_clean...")
        n = self.load_inventory_clean()
        print(f"        {n:,} filas cargadas.")

        print("  [4/5] mart.fact_inventory_snapshot...")
        n = self.load_fact_snapshot(df_producto)
        print(f"        {n:,} filas cargadas.")

        print("  [5/5] mart.ml_inventory_features...")
        n = self.load_ml_features()
        print(f"        {n:,} filas cargadas.")

        print("Carga completa.")
