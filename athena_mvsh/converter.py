import binascii
import json
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any


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