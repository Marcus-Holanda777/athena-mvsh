import pandas as pd
from athena_mvsh.converter import map_convert_df_athena
from pytest import mark


@mark.parametrize(
    'esperado',
    [
        (
            [
                ('64', 'BIGINT'),
                ('32', 'INT'),
                ('16', 'INT'),
                ('8', 'INT'),
                ('U64', 'BIGINT'),
                ('U32', 'INT'),
                ('U16', 'INT'),
                ('U8', 'INT'),
                ('BOOL', 'BOOLEAN'),
                ('TEXTO', 'STRING'),
                ('TIMESTAMP', 'TIMESTAMP'),
                ('TIMEDELTA', 'BIGINT'),
                ('F16', 'FLOAT'),
                ('F32', 'FLOAT'),
                ('F64', 'DOUBLE'),
            ]
        )
    ],
)
def test_types_pandas(esperado):
    df = pd.DataFrame(
        {
            '64': [1, 2, 3],
            '32': [1, 2, 3],
            '16': [1, 2, 3],
            '8': [1, 2, 3],
            'U64': [1, 2, 3],
            'U32': [1, 2, 3],
            'U16': [1, 2, 3],
            'U8': [1, 2, 3],
            'BOOL': [False, True, False],
            'TEXTO': ['A', 'B', 'C'],
            'TIMESTAMP': pd.to_datetime(['2023-01-01', '2023-02-02', '2023-03-03']),
            'TIMEDELTA': pd.to_timedelta(['1 days', '2 days', '3 days']),
            'F16': [1.0, 2.0, 3.0],
            'F32': [1.0, 2.0, 3.0],
            'F64': [1.0, 2.0, 3.0],
        }
    ).astype(
        {
            '64': 'int64',
            '32': 'int32',
            '16': 'int16',
            '8': 'int8',
            'U64': 'uint64',
            'U32': 'uint32',
            'U16': 'uint16',
            'U8': 'uint8',
            'F16': 'float16',
            'F32': 'float32',
            'F64': 'float64',
        }
    )

    saida = map_convert_df_athena(df)

    assert saida == esperado
