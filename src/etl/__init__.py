"""
Inventarios 360 — ETL
Constantes de negocio. Modificar aquí si cambian los umbrales.
Mantener sincronizado con sql/03_reglas_negocio.sql.
"""
from datetime import date

# Fecha de corte fija para reproducibilidad.
# Supuesto: se usa 2025-04-01 como fecha de referencia del análisis.
# Si se cambia, documentar el motivo y re-ejecutar el pipeline completo.
FECHA_CORTE: date = date(2025, 4, 1)

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
# Nombres normalizados a snake_case tal como salen de normalize_columns (antes de _RENAME_MAP)
# Usado en extract.py → compute_row_null_pct (se ejecuta ANTES de aplicar _RENAME_MAP)
COLS_VACIAS_POR_DISENO: tuple[str, ...] = ("id_inventario", "rotacion", "estado")

# Mismas columnas con sus nombres POST _RENAME_MAP (después del rename en load_excel)
# Usado en transform.py → drop_low_quality_rows (se ejecuta DESPUÉS de load_excel)
COLS_VACIAS_POST_RENAME: tuple[str, ...] = ("id_inventario", "rotacion_raw", "estado_raw")
