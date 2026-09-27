import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from athena_mvsh.converter import select_timestamp_micros
from pytest import mark


def test_select_converte_somente_timestamp_ns():
    colunas = [
        ('id', 'INTEGER'),
        ('criado_em', 'TIMESTAMP_NS'),
        ('alterado_em', 'TIMESTAMP'),
        ('dia', 'DATE'),
    ]

    assert select_timestamp_micros(colunas) == (
        '"id", CAST("criado_em" AS TIMESTAMP) AS "criado_em", "alterado_em", "dia"'
    )


def test_select_escapa_aspas_no_nome_da_coluna():
    assert select_timestamp_micros([('a"b', 'timestamp_ns')]) == (
        'CAST("a""b" AS TIMESTAMP) AS "a""b"'
    )


def _copy(con, relation, destino, output=None):
    # Mesmo SELECT usado no __create_table_external. `output` precisa estar neste
    # escopo: o DuckDB encontra o DataFrame pelo nome da variavel local.
    columns = con.sql(f'DESCRIBE SELECT * FROM {relation}').fetchall()
    select = select_timestamp_micros([(n, t) for n, t, *_ in columns])
    con.sql(
        f"COPY (SELECT {select} FROM {relation}) TO '{destino}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )


@mark.parametrize('origem', ['pandas', 'parquet'])
def test_copy_grava_timestamp_em_microssegundos(tmp_path, origem):
    valores = pd.to_datetime(
        ['2024-01-02 03:04:05.123456789', '2024-12-31 23:59:59.999999900', None]
    )
    output = pd.DataFrame({'id': [1, 2, 3], 'criado_em': valores})
    con = duckdb.connect()

    if origem == 'pandas':
        relation = 'output'
    else:
        entrada = tmp_path / 'entrada.parquet'
        pq.write_table(
            pa.Table.from_pandas(output, preserve_index=False),
            entrada,
            coerce_timestamps=None,
        )
        assert pq.read_schema(entrada).field('criado_em').type == pa.timestamp('ns')
        relation = f'(from read_parquet({str(entrada)!r}))'

    destino = (tmp_path / 'saida.parquet').as_posix()
    _copy(con, relation, destino, output)

    tabela = pq.read_table(destino)
    assert tabela.schema.field('criado_em').type == pa.timestamp('us')
    assert tabela.column('criado_em').to_pylist()[:2] == [
        pd.Timestamp('2024-01-02 03:04:05.123456').to_pydatetime(),
        pd.Timestamp('2024-12-31 23:59:59.999999').to_pydatetime(),
    ]
    assert tabela.column('criado_em').to_pylist()[2] is None
    assert tabela.column('id').to_pylist() == [1, 2, 3]
