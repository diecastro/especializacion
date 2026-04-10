"""
Módulo de conexión a Neon PostgreSQL.

Lee la configuración exclusivamente desde variables de entorno.
No hardcodear credenciales en este archivo ni en ningún archivo trackeado.

Variables de entorno requeridas:
    PGHOST           — host de Neon (ej: ep-xxx.us-east-2.aws.neon.tech)
    PGDATABASE       — nombre de la base de datos
    PGUSER           — usuario de la base de datos
    PGPASSWORD       — contraseña
    PGSSLMODE        — debe ser 'require' para Neon
    PGCHANNELBINDING — debe ser 'require' para Neon

Arquitectura de esquemas:
    stg   → staging (datos crudos del Excel)
    dw    → warehouse (datos limpios, dimensiones, funciones)
    mart  → data mart (hechos, KPIs, vistas analíticas, features ML)

Uso rápido:
    from src.db.connection import get_engine, get_connection, test_connection

    test_connection()
    with get_connection() as conn:
        df = pd.read_sql("SELECT * FROM mart.v_inventory_base LIMIT 10", conn)
"""

import os
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

# Cargar .env local si existe (ignorado por git, solo para desarrollo local)
load_dotenv()

# --------------------------------------------------------------------------
# Esquemas del proyecto
# --------------------------------------------------------------------------

SCHEMAS = ["stg", "dw", "mart"]
_SEARCH_PATH = "stg, dw, mart, public"

# Parámetros requeridos — lanzar error explícito si faltan
_REQUIRED_VARS = ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD")


# --------------------------------------------------------------------------
# URL de conexión
# --------------------------------------------------------------------------

def _build_url() -> str:
    """Construye la URL de conexión a partir de variables de entorno."""
    missing = [v for v in _REQUIRED_VARS if not os.getenv(v)]
    if missing:
        raise EnvironmentError(
            f"Faltan variables de entorno requeridas: {', '.join(missing)}\n"
            "Define estas variables en tu entorno o en un archivo .env local."
        )

    host = os.environ["PGHOST"]
    database = os.environ["PGDATABASE"]
    user = os.environ["PGUSER"]
    password = os.environ["PGPASSWORD"]
    sslmode = os.getenv("PGSSLMODE", "require")

    return (
        f"postgresql+psycopg2://{user}:{password}@{host}/{database}"
        f"?sslmode={sslmode}"
    )


# --------------------------------------------------------------------------
# Engine SQLAlchemy (singleton)
# --------------------------------------------------------------------------

_engine = None


def get_engine(echo: bool = False):
    """
    Devuelve el engine SQLAlchemy (singleton).

    Args:
        echo: Si True, imprime las consultas SQL ejecutadas (útil para debug).

    Returns:
        sqlalchemy.engine.Engine
    """
    global _engine
    if _engine is None:
        url = _build_url()
        _engine = create_engine(
            url,
            echo=echo,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            connect_args={
                "connect_timeout": 10,
                # search_path se aplica via el listener 'connect' de abajo.
                # No usar "options": "-c search_path=..." aquí: el pooler de Neon
                # (PgBouncer) rechaza parámetros de startup en la cadena de conexión.
            },
        )

        @event.listens_for(_engine, "connect")
        def set_search_path(dbapi_conn, connection_record):
            cursor = dbapi_conn.cursor()
            cursor.execute(f"SET search_path TO {_SEARCH_PATH}")
            cursor.close()

    return _engine


def reset_engine():
    """Cierra y elimina el engine actual (útil en tests o si cambian las vars)."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None


# --------------------------------------------------------------------------
# Context manager para conexiones raw
# --------------------------------------------------------------------------

@contextmanager
def get_connection(echo: bool = False):
    """
    Context manager que entrega una conexión SQLAlchemy lista para usar.

    Ejemplo:
        with get_connection() as conn:
            df = pd.read_sql("SELECT * FROM mart.v_inventory_base", conn)

    Yields:
        sqlalchemy.engine.Connection
    """
    engine = get_engine(echo=echo)
    with engine.connect() as conn:
        yield conn


def get_session_factory():
    """Devuelve una fábrica de sesiones ORM."""
    return sessionmaker(bind=get_engine())


# --------------------------------------------------------------------------
# Utilidades de diagnóstico
# --------------------------------------------------------------------------

def test_connection() -> bool:
    """
    Verifica que la conexión a Neon funcione correctamente.

    Returns:
        True si la conexión es exitosa, False si falla.
    """
    try:
        with get_connection() as conn:
            row = conn.execute(
                text("SELECT current_database(), current_user, version()")
            ).fetchone()
            print("Conexion exitosa:")
            print(f"  Base de datos : {row[0]}")
            print(f"  Usuario       : {row[1]}")
            print(f"  PostgreSQL    : {row[2].split(',')[0]}")
        return True
    except Exception as exc:
        print(f"Error de conexion: {exc}")
        return False


def get_schema_info() -> dict:
    """
    Devuelve el tamaño de cada tabla en los esquemas stg, dw y mart.

    Returns:
        Dict con 'schema.tabla' → tamaño en formato legible.
    """
    query = text("""
        SELECT
            table_schema || '.' || table_name AS tabla,
            pg_size_pretty(
                pg_total_relation_size(
                    quote_ident(table_schema) || '.' || quote_ident(table_name)
                )
            ) AS size
        FROM information_schema.tables
        WHERE table_schema = ANY(:schemas)
          AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name
    """)
    info = {}
    with get_connection() as conn:
        for row in conn.execute(query, {"schemas": SCHEMAS}):
            info[row[0]] = row[1]
    return info


if __name__ == "__main__":
    test_connection()
