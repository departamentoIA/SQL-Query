from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, unquote_plus

import polars as pl
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from pkg.config import get_connection_string
from pkg.globals import DEFAULT_COMMAND_TIMEOUT, DEFAULT_FETCH_BATCH_SIZE


def _odbc_connect_string_from_sqlalchemy_url(sqlalchemy_url: str) -> str:
    """Extrae el odbc_connect del URL generado por config.py."""
    if "odbc_connect=" not in sqlalchemy_url:
        raise ValueError("La cadena de conexión no contiene el parámetro odbc_connect")

    encoded_part = sqlalchemy_url.split("odbc_connect=", maxsplit=1)[1]
    parsed = parse_qs(encoded_part, keep_blank_values=True)
    if "odbc_connect" in parsed:
        return unquote_plus(parsed["odbc_connect"][0])
    return unquote_plus(encoded_part)


def create_engine_from_config() -> Engine:
    """Crea un engine SQLAlchemy a partir de pkg/config.py."""
    connection_string = get_connection_string()
    return create_engine(connection_string, future=True, pool_pre_ping=True)


def create_pyodbc_connection(timeout: int = DEFAULT_COMMAND_TIMEOUT) -> pyodbc.Connection:
    """Crea una conexión pyodbc reutilizando la lógica base de config.py."""
    sqlalchemy_url = get_connection_string()
    odbc_connect_string = _odbc_connect_string_from_sqlalchemy_url(sqlalchemy_url)
    connection = pyodbc.connect(odbc_connect_string, autocommit=True)
    connection.timeout = timeout
    return connection


def execute_setup_sql(cursor: pyodbc.Cursor, statements: list[str]) -> None:
    """Ejecuta sentencias preparatorias en la misma sesión."""
    for index, statement in enumerate(statements, start=1):
        sql = statement.strip()
        if not sql:
            continue
        print(f"[INFO] Ejecutando setup_sql #{index}")
        cursor.execute(sql)
        while cursor.nextset():
            pass


def fetch_query_in_batches(
    cursor: pyodbc.Cursor,
    query: str,
    batch_size: int = DEFAULT_FETCH_BATCH_SIZE,
) -> pl.DataFrame:
    """Ejecuta el SELECT final y construye un DataFrame de Polars por lotes."""
    cursor.execute(query)

    if cursor.description is None:
        return pl.DataFrame()

    columns = [col[0] for col in cursor.description]
    batches: list[pl.DataFrame] = []

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break

        batch_dict: dict[str, list[Any]] = {
            column: [row[idx] for row in rows]
            for idx, column in enumerate(columns)
        }
        batches.append(pl.DataFrame(batch_dict, strict=False))
        print(f"[INFO] Lote leído: {len(rows):,} filas")

    if not batches:
        return pl.DataFrame(schema=columns)

    return pl.concat(batches, how="vertical_relaxed")


def run_query_job(job: dict[str, Any]) -> pl.DataFrame:
    """Ejecuta un job de extracción completo."""
    setup_sql = job.get("setup_sql", [])
    final_sql = job["final_sql"]
    fetch_batch_size = job.get("fetch_batch_size", DEFAULT_FETCH_BATCH_SIZE)
    timeout = job.get("command_timeout", DEFAULT_COMMAND_TIMEOUT)

    connection = create_pyodbc_connection(timeout=timeout)
    try:
        cursor = connection.cursor()
        execute_setup_sql(cursor, setup_sql)
        df = fetch_query_in_batches(
            cursor=cursor,
            query=final_sql,
            batch_size=fetch_batch_size,
        )
        return df
    finally:
        connection.close()


def test_connection() -> None:
    """Prueba rápida de conectividad."""
    engine = create_engine_from_config()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 AS ok"))
            print(result.scalar_one())
    finally:
        engine.dispose()
