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
        """
        Ejecuta un archivo SQL completo usando una conexión psycopg2 directa.
        No divide por ';' para preservar funciones PL/pgSQL con bloques $$ ... $$.
        """
        filepath = SQL_DIR / filename
        if not filepath.exists():
            raise FileNotFoundError(f"No se encontro: {filepath}")

        sql = filepath.read_text(encoding="utf-8")
        raw_conn = get_engine().raw_connection()
        try:
            cur = raw_conn.cursor()
            cur.execute(sql)
            raw_conn.commit()
            cur.close()
        except Exception as exc:
            raw_conn.rollback()
            raise RuntimeError(f"Error ejecutando {filename}: {exc}") from exc
        finally:
            raw_conn.close()

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

    @staticmethod
    def _to_records(df: pd.DataFrame) -> list:
        """
        Convierte un DataFrame a lista de dicts reemplazando NaT y NaN por None.
        Necesario porque psycopg2 serializa pd.NaT como la cadena 'NaT' en lugar
        de NULL, lo que causa InvalidDatetimeFormat en PostgreSQL.
        """
        # astype(object) convierte datetime64 → Python datetime y NaT → NaT (objeto).
        # where(notna, None) reemplaza NaT/NaN por Python None (que psycopg2 envía como NULL).
        return df.astype(object).where(pd.notna(df), other=None).to_dict(orient="records")

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
        """
        Carga los datos crudos a stg.inventory_raw para trazabilidad.
        df_raw ya viene con columnas en snake_case del extractor (load_excel),
        por lo que no se necesita rename. Solo se mapean los tres campos que el
        staging almacena como texto _raw pero el extractor nombra sin sufijo.
        """
        df = self.df_raw.copy()
        df["source_file"] = self.source_file

        # Mapeo exclusivo para columnas cuyo nombre difiere entre df_raw y stg
        # (el extractor usa fecha_ingreso/fecha_vencimiento/unds; el staging los guarda como _raw)
        rename_map = {
            "unds": "unds_raw",
            "fecha_ingreso": "fecha_ingreso_raw",
            "fecha_vencimiento": "fecha_vencimiento_raw",
        }
        df = df.rename(columns=rename_map)

        stg_cols = [
            "source_file", "marca", "item_id", "id_inventario", "descripcion",
            "categoria", "unds_raw", "fecha_ingreso_raw", "fecha_vencimiento_raw",
            "dias_antes_vencimiento_raw", "contenedor", "rotacion_raw", "estado_raw",
        ]
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
                "SELECT producto_sk, item_id, descripcion, categoria, marca FROM dw.dim_producto",
                conn,
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
            rows = self._to_records(df[clean_cols])
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

        # Resolver FK producto usando los 4 atributos de la clave de unicidad de dim_producto
        # para evitar joins ambiguos cuando un item_id tiene múltiples combinaciones de atributos
        df = df.merge(df_producto, on=["item_id", "descripcion", "categoria", "marca"], how="left")

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

        # Validar FKs antes de insertar para detectar relaciones rotas
        n_sin_producto = df["producto_sk"].isna().sum()
        n_sin_t_ingreso = df["tiempo_ingreso_sk"].isna().sum()
        n_sin_t_vencimiento = df["tiempo_vencimiento_sk"].isna().sum()
        if n_sin_producto:
            print(f"    Advertencia: {n_sin_producto} filas sin producto_sk (quedarán con FK nula).")
        if n_sin_t_ingreso:
            print(f"    Advertencia: {n_sin_t_ingreso} filas sin tiempo_ingreso_sk (fecha_ingreso fuera de dim_tiempo).")
        if n_sin_t_vencimiento:
            print(f"    Advertencia: {n_sin_t_vencimiento} filas sin tiempo_vencimiento_sk (fecha_vencimiento fuera de dim_tiempo).")

        with self.engine.connect() as conn:
            conn.execute(text("COMMIT"))
            rows = self._to_records(df[fact_cols])
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
            rows = self._to_records(df[ml_cols])
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
