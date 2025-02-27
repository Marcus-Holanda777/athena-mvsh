from athena_mvsh.formatador import cast_format
from datetime import datetime, date
from decimal import Decimal
import textwrap


def test_delete():
    esperado = textwrap.dedent(
        """
        DELETE FROM "teste"."teste"
        WHERE num = 10
        and periodo BETWEEN TIMESTAMP '2025-01-01 00:00:00.000000' and TIMESTAMP '2025-12-31 23:59:59.999999'
        and nome is null
        """
    ).strip()

    rst = cast_format(
        textwrap.dedent(
            """
            DELETE FROM "teste"."teste"
            WHERE num = {}
            and periodo BETWEEN {} and {}
            and nome is {}
            """
        ).strip(),
        10,
        datetime(2025, 1, 1),
        datetime(2025, 12, 31, 23, 59, 59, 999999),
        None,
    )

    assert rst == esperado


def test_insert():
    esperado = textwrap.dedent(
        """
        INSERT INTO "teste"."teste"
        VALUES (10, DATE '2025-01-01', null, 1.1, True)
        """
    ).strip()

    rst = cast_format(
        textwrap.dedent(
            """
            INSERT INTO "teste"."teste"
            VALUES ({}, {}, {}, {}, {})
            """
        ).strip(),
        10,
        date(2025, 1, 1),
        None,
        1.1,
        True,
    )

    assert rst == esperado


def test_select():
    esperado = textwrap.dedent(
        """
        SELECT *
        FROM "teste"."teste"
        WHERE num = 10
        and periodo BETWEEN TIMESTAMP '2025-01-01 00:00:00.000000' and TIMESTAMP '2025-12-31 23:59:59.999999'
        and nome is null
        and valor > DECIMAL '1.000'
        and sobre = 'nada'
        """
    ).strip()

    rst = cast_format(
        textwrap.dedent(
            """
            SELECT *
            FROM "teste"."teste"
            WHERE num = {}
            and periodo BETWEEN {} and {}
            and nome is {}
            and valor > {}
            and sobre = {}
            """
        ).strip(),
        10,
        datetime(2025, 1, 1),
        datetime(2025, 12, 31, 23, 59, 59, 999999),
        None,
        Decimal('1.000'),
        'nada',
    )

    assert rst == esperado
