from datetime import datetime, date
from decimal import Decimal


def get_value_format(v):
    if isinstance(v, type(None)):
        return "null"
            
    if isinstance(v, bool):
        return str(v)

    if isinstance(v, date):
        return f"""DATE '{v:%Y-%m-%d}'"""

    if isinstance(v, datetime):
        return f"""TIMESTAMP '{v:%Y-%m-%d %H:%M:%S.%f}'"""

    if isinstance(v, str):
        value = v.replace("'", "''")
        return f"'{value}'"

    if isinstance(v, Decimal):
        value = f"{v:f}"
        value = value.replace("'", "''")
        return f"DECIMAL {value}"

    if isinstance(v, (int, float)):
        return v


def cast_format(
    consulta: str, 
    *args, 
    **kwargs
) -> str:
    
    if args:
        args = list(args)
        for p, arg in enumerate(args):
            args[p] = get_value_format(arg)
            
    if kwargs:
        for k, v in kwargs.items():
            kwargs[k] = get_value_format(v)

    return consulta.format(*args, **kwargs)