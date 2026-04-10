import pandas as pd
import pytest


@pytest.fixture
def df_raw_sample():
    """DataFrame que simula la hoja Page1_1 del Excel con casos representativos."""
    return pd.DataFrame({
        "Marca": ["MarcaA", "MarcaA", "MarcaB", "MarcaC", None],
        "Item ID": ["ITM001", "ITM001", "ITM002", "ITM003", "ITM004"],
        "ID Inventario": [None, None, None, None, None],   # siempre vacío
        "Descripción": ["Prod A1", "Prod A1", "Prod B1", "Prod C1", "Prod D1"],
        "Categoría": ["Cat1", "Cat1", "Cat2", "Cat1", "Cat2"],
        "Unds": ["10", "5", "-3", "0", "20"],              # incluye negativos y ceros
        "Fecha de Ingreso": [
            "2024-01-15", "2024-01-15", "2023-06-01", "2025-06-01", "2024-11-20"
        ],
        "Fecha Vencimiento": [
            "2025-04-10",   # 9 días → critico
            "2025-05-15",   # 44 días → vigente
            "2025-03-20",   # -12 días → vencido
            None,           # fecha nula
            "2025-04-20",   # 19 días → proximo_a_vencer
        ],
        "Dias antes de Vencimiento": ["9", "44", "-12", None, "19"],  # no confiar
        "Contenedor": ["BOX", "CAN", "BOX", "TUBE", "BOX"],
        "Rotación": [None, None, None, None, None],         # siempre vacío
        "Estado": [None, None, None, None, None],           # casi siempre vacío
    })


@pytest.fixture
def df_clean_sample():
    """DataFrame limpio mínimo para tests de carga."""
    return pd.DataFrame({
        "product_container_id": ["ITM001_BOX", "ITM001_CAN", "ITM002_BOX"],
        "marca": ["MarcaA", "MarcaA", "MarcaB"],
        "item_id": ["ITM001", "ITM001", "ITM002"],
        "descripcion": ["Prod A1", "Prod A1", "Prod B1"],
        "categoria": ["Cat1", "Cat1", "Cat2"],
        "contenedor": ["BOX", "CAN", "BOX"],
        "unds": [10.0, 5.0, None],
        "fecha_ingreso": pd.to_datetime(["2024-01-15", "2024-01-15", "2023-06-01"]),
        "fecha_vencimiento": pd.to_datetime(["2025-04-10", "2025-05-15", "2025-03-20"]),
        "dias_en_inventario": [442, 442, 670],
        "dias_para_vencimiento": [9, 44, -12],
        "estado_inventario": ["critico", "vigente", "vencido"],
        "segmento_rotacion": ["baja_rotacion", "baja_rotacion", "baja_rotacion"],
        "score_riesgo": [3, 1, 4],
        "mes_vencimiento": [4, 5, 3],
        "anio_vencimiento": [2025, 2025, 2025],
        "row_null_pct": [0.0, 0.0, 0.0],
        "calidad_flag": ["ok", "ok", "ok"],
    })
