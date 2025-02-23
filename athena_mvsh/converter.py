import binascii
import json
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
import pandas as pd
from duckdb import DuckDBPyConnection
from typing import cast
from pyarrow import Schema, DataType
import pyarrow as pa
import re
from functools import reduce


def partition_func_iceberg(columns: list[str]) -> list[str]:
    patterns = [
        r'(year|month|day|hour)\((.+?)\)',
        r'(bucket|truncate)\((.+?),\s*(.+?)\)',
    ]

    def formatar_coluna(match):
        if match.lastindex == 2:
            return f'{match.group(1)}(`{match.group(2).strip()}`)'
        elif match.lastindex == 3:
            return f'{match.group(1)}({match.group(2).split(",")[0].strip()}, `{match.group(3).strip()}`)'

    rst = []
    for string in columns:
        pattern_sub = re.compile(r'["`](.*?)["`]', re.I)
        new_string = reduce(
            lambda s, match: s.replace(match.group(0), match.group(1)),
            pattern_sub.finditer(string),
            string,
        )

        for pattern in patterns:
            if re.match(pattern, new_string, re.I):
                resultado = re.sub(pattern, formatar_coluna, new_string, flags=re.I)
                rst.append(resultado)
                break
        else:
            rst.append(f'`{new_string.strip()}`')

    return rst


def to_column_info_arrow(schema: Schema) -> tuple[dict]:
    columns = []
    for field in schema:
        type_, precision, scale = get_athena_type(field.type)
        columns.append(
            {
                'Name': field.name,
                'Type': type_,
                'Precision': precision,
                'Scale': scale,
                'Nullable': 'NULLABLE' if field.nullable else 'NOT_NULL',
            }
        )
    return tuple(columns)


def get_athena_type(type_: DataType) -> tuple[str, int, int]:
    import pyarrow.lib as types

    if type_.id in [types.Type_BOOL]:
        return 'boolean', 0, 0
    elif type_.id in [types.Type_UINT8, types.Type_INT8]:
        return 'tinyint', 3, 0
    elif type_.id in [types.Type_UINT16, types.Type_INT16]:
        return 'smallint', 5, 0
    elif type_.id in [types.Type_UINT32, types.Type_INT32]:
        return 'integer', 10, 0
    elif type_.id in [types.Type_UINT64, types.Type_INT64]:
        return 'bigint', 19, 0
    elif type_.id in [types.Type_HALF_FLOAT, types.Type_FLOAT]:
        return 'float', 17, 0
    elif type_.id in [types.Type_DOUBLE]:
        return 'double', 17, 0
    elif type_.id in [types.Type_STRING, types.Type_LARGE_STRING]:
        return 'varchar', 2147483647, 0
    elif type_.id in [
        types.Type_BINARY,
        types.Type_FIXED_SIZE_BINARY,
        types.Type_LARGE_BINARY,
    ]:
        return 'varbinary', 1073741824, 0
    elif type_.id in [types.Type_DATE32, types.Type_DATE64]:
        return 'date', 0, 0
    elif type_.id == types.Type_TIMESTAMP:
        return 'timestamp', 3, 0
    elif type_.id in [types.Type_DECIMAL128, types.Decimal256Type]:
        type_ = cast(types.Decimal128Type, type_)
        return 'decimal', type_.precision, type_.scale
    elif type_.id in [
        types.Type_LIST,
        types.Type_FIXED_SIZE_LIST,
        types.Type_LARGE_LIST,
    ]:
        return 'array', 0, 0
    elif type_.id in [types.Type_STRUCT]:
        return 'row', 0, 0
    elif type_.id in [types.Type_MAP]:
        return 'map', 0, 0
    else:
        return 'string', 2147483647, 0


def convert_df_athena(col: pd.Series) -> str:
    col_type = pd.api.types.infer_dtype(col, skipna=True)

    if col_type == 'datetime64' or col_type == 'datetime':
        return 'TIMESTAMP'

    elif col_type == 'timedelta':
        return 'INT'

    elif col_type == 'timedelta64':
        return 'BIGINT'

    elif col_type == 'floating':
        dtype_float = ['float16', 'float32']
        if col.dtype in dtype_float:
            return 'FLOAT'
        else:
            return 'DOUBLE'

    elif col_type == 'integer':
        dtype_ints = ['int8', 'int16', 'int32', 'uint8', 'uint16', 'uint32']
        if col.dtype in dtype_ints:
            return 'INT'
        else:
            return 'BIGINT'

    elif col_type == 'boolean':
        return 'BOOLEAN'

    elif col_type == 'date':
        return 'DATE'

    elif col_type == 'bytes':
        return 'BINARY'

    elif col_type in ['complex', 'time']:
        raise ValueError(f'Data type `{col_type}` is not supported')

    return 'STRING'


def map_convert_df_athena(df: pd.DataFrame):
    return [(c, convert_df_athena(df[c])) for c in df.columns]


def convert_tp_duckdb(col_type: str) -> str:
    col_type = col_type.upper()

    if col_type in ('BIT', 'BLOB'):
        return 'BINARY'

    if col_type.startswith('TIMESTAMP'):
        return 'TIMESTAMP'

    if col_type.startswith('TIME'):
        return 'TIME'

    if col_type in ('UBIGINT', 'UHUGEINT', 'HUGEINT'):
        return 'BIGINT'

    if col_type in (
        'INTEGER',
        'UINTEGER',
        'USMALLINT',
        'SMALLINT',
        'UTINYINT',
        'TINYINT',
    ):
        return 'INT'

    if col_type in ('UUID', 'VARCHAR', 'ENUM', 'UNION'):
        return 'STRING'

    if col_type == 'LIST':
        return 'ARRAY'

    return col_type


def map_convert_duckdb_athena(con: DuckDBPyConnection, file: list[str] | str):
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

    return [(col, convert_tp_duckdb(tep)) for col, tep in rst]


def map_convert_duckdb_athena_pandas_arrow(
    con: DuckDBPyConnection, data: pd.DataFrame | pa.Table
):
    stmt = """FROM (DESCRIBE data)
    select column_name, column_type
    """

    rst = con.sql(stmt).fetchall()

    return [(col, convert_tp_duckdb(tep)) for col, tep in rst]


def strtobool(val):
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return 1
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    else:
        raise ValueError(f'invalid truth value {val!r}')


def _to_date(varchar_value: str | None) -> date | None:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, '%Y-%m-%d').date()


def _to_datetime(varchar_value: str | None) -> datetime | None:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, '%Y-%m-%d %H:%M:%S.%f')


def _to_time(varchar_value: str | None) -> time | None:
    if varchar_value is None:
        return None
    return datetime.strptime(varchar_value, '%H:%M:%S.%f').time()


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
    return binascii.a2b_hex(''.join(varchar_value.split(' ')))


def _to_json(varchar_value: str | None) -> Any | None:
    if varchar_value is None:
        return None
    return json.loads(varchar_value)


def _to_default(varchar_value: str | None) -> str | None:
    return varchar_value


MAP_CONVERT = {
    'boolean': _to_boolean,
    'tinyint': _to_int,
    'smallint': _to_int,
    'integer': _to_int,
    'bigint': _to_int,
    'float': _to_float,
    'real': _to_float,
    'double': _to_float,
    'char': _to_default,
    'varchar': _to_default,
    'string': _to_default,
    'timestamp': _to_datetime,
    'date': _to_date,
    'time': _to_time,
    'varbinary': _to_binary,
    'array': _to_default,
    'map': _to_default,
    'row': _to_default,
    'decimal': _to_decimal,
    'json': _to_json,
}
