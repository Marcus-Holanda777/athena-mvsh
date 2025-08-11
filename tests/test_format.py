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


def test_list():
    esperado = textwrap.dedent(
        """
        SELECT *
        FROM "teste"."teste"
        WHERE num in(10,20)
        and periodo in(TIMESTAMP '2025-01-01 00:00:00.000000',TIMESTAMP '2025-12-31 23:59:59.999999')
        and nome in(null,null)
        and valor in(DECIMAL '1.000',DECIMAL '2.000')
        and sobre in('tp','sp')
        and floats in(1.100000,2.200000)
        and date in(DATE '2021-01-01',DATE '2022-01-01')
        """
    ).strip()

    rst = cast_format(
        textwrap.dedent(
            """
            SELECT *
            FROM "teste"."teste"
            WHERE num in({})
            and periodo in({})
            and nome in({})
            and valor in({})
            and sobre in({})
            and floats in({})
            and date in({})
            """
        ).strip(),
        [10, 20],
        [datetime(2025, 1, 1), datetime(2025, 12, 31, 23, 59, 59, 999999)],
        [None, None],
        [Decimal('1.000'), Decimal('2.000')],
        ['tp', 'sp'],
        [1.1, 2.2],
        [date(2021, 1, 1), date(2022, 1, 1)],
    )

    assert rst == esperado


def test_return_cast():
    entrada = [1, 2, 3, 4, 5]
    entrada_dict = {'num': [1, 2, 3, 4, 5]}

    __ = cast_format(
        textwrap.dedent(
            """
            SELECT *
            FROM "teste"."teste"
            WHERE num in({})
            """
        ).strip(),
        entrada,
    )

    __ = cast_format(
        textwrap.dedent(
            """
            SELECT *
            FROM "teste"."teste"
            WHERE teste in({num})
            """
        ).strip(),
        **entrada_dict,
    )

    assert all([[1, 2, 3, 4, 5] == entrada, {'num': [1, 2, 3, 4, 5]} == entrada_dict])
