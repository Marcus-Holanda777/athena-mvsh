from pytest import mark
from athena_mvsh.utils import query_is_ddl


@mark.parametrize(
    'query,esperada',
    [
        (
            'CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING)',
            True,
        ),
        (
            'SELECT * FROM test_table',
            False,
        ),
        (
            'DROP TABLE IF EXISTS test_table',
            True,
        ),
        (
            'ALTER TABLE test_table ADD COLUMN age INT',
            True,
        ),
        (
            'INSERT INTO test_table VALUES (1, "John")',
            True,
        ),
        (
            'with temp AS (SELECT * FROM test_table) SELECT * FROM temp',
            False,
        ),
        (
            'OPTIMIZE [db_name.]table_name REWRITE DATA USING BIN_PACK',
            True,
        ),
        (
            'VACUUM [db_name.]table_name',
            True,
        ),
        (
            'update test_table SET name = "Doe" WHERE id = 1',
            True,
        ),
        (
            'UNLOAD (SELECT name FROM table) TO "s3://b/m/" WITH ( prop = "expr")',
            False,
        ),
    ],
)
def test_query(query, esperada):
    """
    Testa a funcao query_is_ddl para verificar se uma consulta é DDL ou não.
    """
    assert query_is_ddl(query) == esperada
