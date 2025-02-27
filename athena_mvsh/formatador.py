from datetime import datetime, date
from decimal import Decimal


def escape_presto(val: str) -> str:
    escaped = val.replace("'", "''")
    return f"'{escaped}'"


def escape_hive(val: str) -> str:
    escaped = (
        val.replace('\\', '\\\\')
        .replace("'", "\\'")
        .replace('\r', '\\r')
        .replace('\n', '\\n')
        .replace('\t', '\\t')
    )
    return f"'{escaped}'"


def get_value_format(v):
    if isinstance(v, type(None)):
        return 'null'

    if isinstance(v, bool):
        return str(v)

    if isinstance(v, datetime):
        return f"TIMESTAMP '{v:%Y-%m-%d %H:%M:%S.%f}'"

    if isinstance(v, date):
        return f"DATE '{v:%Y-%m-%d}'"

    if isinstance(v, str):
        return escape_presto(v)

    # @experimental
    if isinstance(v, (list, set, tuple)):
        return ','.join(f'{val!r}' for val in v)

    if isinstance(v, Decimal):
        value = escape_presto(f'{v:f}')
        return f"DECIMAL {value}"

    if isinstance(v, (int, float)):
        return v


def cast_format(consulta: str, *args, **kwargs) -> str:
    if args:
        args = list(args)
        for p, arg in enumerate(args):
            args[p] = get_value_format(arg)

    if kwargs:
        for k, v in kwargs.items():
            kwargs[k] = get_value_format(v)

    return consulta.format(*args, **kwargs)
