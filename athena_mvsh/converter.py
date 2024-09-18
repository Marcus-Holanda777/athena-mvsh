import binascii
import json
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
import pandas as pd
from duckdb import DuckDBPyConnection
from pathlib import Path
import os


def convert_df_athena(col: pd.Series) -> str:

    col_type = (
        pd.api.types
        .infer_dtype(
            col, 
            skipna=True
        )
    )

    if col_type == "datetime64" or col_type == "datetime":
        return "TIMESTAMP"
    
    elif col_type == "timedelta":
        return "INT"
    
    elif col_type == "timedelta64":
        return "BIGINT"
    
    elif col_type == "floating":
        if col.dtype == "float32":
            return "FLOAT"
        else:
            return "DOUBLE"
    
    elif col_type == "integer":
        if col.dtype == "int32":
            return "INT"
        else:
            return "BIGINT"
    
    elif col_type == "boolean":
        return "BOOLEAN"
    
    elif col_type == "date":
        return "DATE"
    
    elif col_type == "bytes":
        return "BINARY"
    
    elif col_type in ["complex", "time"]:
        raise ValueError(f"Data type `{col_type}` is not supported")
    
    return "STRING"


def map_convert_df_athena(df: pd.DataFrame):
    return [
        (c, convert_df_athena(df[c])) 
        for c in df.columns
    ]


def convert_tp_duckdb(col_type: str) -> str:
    col_type = col_type.upper()

    if col_type in('BIT', 'BLOB'):
        return "BINARY"
    
    if col_type.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    
    if col_type.startswith('TIME'):
        return "TIME"
    
    if col_type in("UBIGINT", "", "UHUGEINT", "HUGEINT"):
        return "BIGINT"
    
    if col_type == 'USMALLINT':
        return 'SMALLINT'
    
    if col_type == "UTINYINT":
        return "TINYINT"
    
    if col_type == "UINTEGER":
        return "INTEGER"
    
    if col_type in("UUID", "VARCHAR", "ENUM", "UNION"):
        return "STRING"
    
    if col_type == "LIST":
        return "ARRAY"
    
    return col_type


def map_convert_duckdb_athena(
    con: DuckDBPyConnection, 
    file: list[str] | str
):
    
    temp_tbl = f"""
        CREATE OR REPLACE TEMP TABLE validade_type 
        AS FROM read_parquet({file!r}) LIMIT 1;
    """
    con.sql(temp_tbl)

    stmt = """SELECT column_name, data_type 
      FROM information_schema.columns
      where table_name = 'validade_type'
	  ORDER BY ordinal_position;
    """
	 
    rst = con.sql(stmt).fetchall()

    return [
        (col, convert_tp_duckdb(tep)) 
        for col, tep in rst
    ]


def strtobool(val):
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return 1
    elif val in ("n", "no", "f", "false", "off", "0"):
        return 0
    else:
        raise ValueError(f"invalid truth value {val!r}")


def _to_date(varchar_value: str | None) -> date | None:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%Y-%m-%d").date()


def _to_datetime(varchar_value: str | None) -> datetime | None:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%Y-%m-%d %H:%M:%S.%f")


def _to_time(varchar_value: str | None) -> time | None:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, "%H:%M:%S.%f").time()


def _to_float(varchar_value: str | None) -> float | None:
    if varchar_value is None:
        return None
    return float(varchar_value)


def _to_int(varchar_value: str | None) -> int | None:
    if varchar_value is None:
        return None
    return int(varchar_value)


def _to_decimal(varchar_value: str | None) -> Decimal | None:
    if not varchar_value:
        return None
    return Decimal(varchar_value)


def _to_boolean(varchar_value: str | None) -> bool | None:
    if not varchar_value:
        return None
    return bool(strtobool(varchar_value))


def _to_binary(varchar_value: str | None) -> bytes | None:
    if varchar_value is None:
        return None
    return binascii.a2b_hex("".join(varchar_value.split(" ")))


def _to_json(varchar_value: str | None) -> Any | None:
    if varchar_value is None:
        return None
    return json.loads(varchar_value)


def _to_default(varchar_value: str | None) -> str | None:
    return varchar_value


MAP_CONVERT = {
    "boolean": _to_boolean,
    "tinyint": _to_int,
    "smallint": _to_int,
    "integer": _to_int,
    "bigint": _to_int,
    "float": _to_float,
    "real": _to_float,
    "double": _to_float,
    "char": _to_default,
    "varchar": _to_default,
    "string": _to_default,
    "timestamp": _to_datetime,
    "date": _to_date,
    "time": _to_time,
    "varbinary": _to_binary,
    "array": _to_default,
    "map": _to_default,
    "row": _to_default,
    "decimal": _to_decimal,
    "json": _to_json,
}