from pytest import mark
from athena_mvsh.converter import partition_func_iceberg


@mark.parametrize(
    'coluna,esperada',
    [
        (
            ['month(coluna)', 'year(coluna)', 'day(coluna)', 'hour(coluna)'],
            ['month(`coluna`)', 'year(`coluna`)', 'day(`coluna`)', 'hour(`coluna`)'],
        ),
        (
            ['MONTH(coluna)', 'YEAR(coluna)', 'DAY(coluna)', 'HOUR(coluna)'],
            ['MONTH(`coluna`)', 'YEAR(`coluna`)', 'DAY(`coluna`)', 'HOUR(`coluna`)'],
        ),
        (
            [
                ' coluna ',
                'coluna',
                '"coluna"',
                '`coluna`',
                'YEAR( coluna )',
                'day(`coluna `)',
                'HOUR(" coluna")',
                'month("`coluna`")',
            ],
            [
                '`coluna`',
                '`coluna`',
                '`coluna`',
                '`coluna`',
                'YEAR(`coluna`)',
                'day(`coluna`)',
                'HOUR(`coluna`)',
                'month(`coluna`)',
            ],
        ),
    ],
)
def test_partition_period(coluna, esperada):
    colunas = partition_func_iceberg(coluna)
    assert esperada == colunas


@mark.parametrize(
    'coluna,esperada',
    [
        (
            [
                'bucket(10, coluna)',
                'bucket(500,coluna)',
                'bucket(2, "coluna")',
                'bucket(12, `coluna`)',
                'bucket( 3000 ,"`coluna`")',
            ],
            [
                'bucket(10, `coluna`)',
                'bucket(500, `coluna`)',
                'bucket(2, `coluna`)',
                'bucket(12, `coluna`)',
                'bucket(3000, `coluna`)',
            ],
        ),
    ],
)
def test_partition_buckets(coluna, esperada):
    colunas = partition_func_iceberg(coluna)
    assert esperada == colunas


@mark.parametrize(
    'coluna,esperada',
    [
        (
            [
                'truncate(10, coluna)',
                'truncate(500,coluna)',
                'truncate(2, "coluna")',
                'truncate(12, `coluna`)',
                'truncate( 3000 ,"`coluna`")',
            ],
            [
                'truncate(10, `coluna`)',
                'truncate(500, `coluna`)',
                'truncate(2, `coluna`)',
                'truncate(12, `coluna`)',
                'truncate(3000, `coluna`)',
            ],
        ),
    ],
)
def test_partition_truncate(coluna, esperada):
    colunas = partition_func_iceberg(coluna)
    assert esperada == colunas
