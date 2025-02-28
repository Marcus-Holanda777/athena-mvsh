"""
A classe Athena estende a funcionalidade de `CursorIterator` para executar consultas
SQL no Athena, gerenciar os resultados e fornecer métodos para exportar os dados
para diversos formatos (CSV, Parquet, Iceberg, etc.).

Ela permite a execução de consultas SQL, a obtenção dos resultados de forma iterativa
e a conversão desses resultados para estruturas de dados como Pandas DataFrame,
Parquet ou Arrow. A classe também oferece métodos para escrever dados de volta ao banco
de dados ou armazená-los em formatos como Parquet e Iceberg, com suporte para operações
de particionamento e compressão.

O tipo de cursor utilizado pode ser um dos seguintes:
    - **CursorPython**: Para operações com Python.
    - **CursorParquet**: Para operações com dados Parquet em integração com Pyarrow.
    - **CursorParquetDuckdb**: Para operações com dados Parquet em integração com DuckDB.

Métodos:
    - **execute**: Executa uma consulta SQL, com parâmetros opcionais.
    - **fetchone**: Retorna a próxima linha do resultado.
    - **fetchall**: Retorna todas as linhas do resultado.
    - **fetchmany**: Retorna um número especificado de linhas.
    - **to_arrow**: Converte os resultados para um formato Arrow.
    - **to_parquet**: Converte os resultados para o formato Parquet.
    - **to_csv**: Converte os resultados para um arquivo CSV.
    - **to_create_table_db**: Cria uma tabela no banco de dados usando os resultados.
    - **to_partition_create_table_db**: Cria uma tabela particionada no banco de dados.
    - **to_insert_table_db**: Insere dados em uma tabela do banco de dados.
    - **write_dataframe**: Escreve um DataFrame em uma tabela no banco de dados.
    - **write_arrow**: Escreve um Table Arrow em uma tabela no banco de dados.
    - **write_parquet**: Escreve dados em formato Parquet no banco de dados.
    - **write_table_iceberg**: Escreve dados em formato Iceberg no banco de dados.
    - **merge_table_iceberg**: Realiza uma operação de merge em uma tabela Iceberg.
    - **to_pandas**: Converte os resultados para um DataFrame Pandas.
    - **close**: Libera os recursos utilizados pela consulta.
    - **__enter__** / **__exit__**: Suporte para contexto (gerenciamento de recursos).

Exceções:
    - ProgrammingError: Lançada quando um método não é implementado para o tipo de cursor atual.

Exemplo de uso:
    Supondo que o cursor seja uma instância válida de um dos tipos: CursorPython, CursorParquet ou CursorParquetDuckdb

    ```python
    from athena_mvsh import CursorPython, CursorParquet, CursorParquetDuckdb

    cursor = CursorPython(...)  # ou CursorParquet(...) ou CursorParquetDuckdb(...)
    ```

    Criando uma instância da classe Athena
    ```python
    with Athena(cursor) as athena:
        # Executando uma consulta SQL
        athena.execute("SELECT * FROM sales_data WHERE region = 'US'")

        # Obtendo os resultados
        results = athena.fetchall()
        for row in results:
            print(row)

        # Convertendo os resultados para um DataFrame Pandas
        df = athena.to_pandas()
        print(df.head())

        # Exportando os resultados para um arquivo CSV
        athena.to_csv("sales_data_us.csv", delimiter=";", include_header=True)

        # Escrevendo dados em uma tabela no banco de dados
        athena.write_parquet(
            file="sales_data.parquet",
            table_name="sales_table",
            schema="public"
        )
    ```
"""

from athena_mvsh.dbathena import DBAthena
from athena_mvsh.cursores import (
    CursorIterator,
    CursorBaseParquet,
    CursorParquetDuckdb,
    CursorPython,
    CursorParquet,
)
import pyarrow as pa
import pyarrow.csv as csv_arrow
from athena_mvsh.error import ProgrammingError
import pandas as pd
import os
from itertools import islice
from athena_mvsh.formatador import cast_format
from athena_mvsh.utils import query_is_ddl
from pathlib import Path
from typing import Literal


WORKERS = min([4, os.cpu_count()])


class Athena(CursorIterator):
    """
    Classe responsável por executar consultas SQL no AWS Athena ou em cursores alternativos,
    como CursorParquetDuckdb, CursorPython ou CursorParquet. A classe suporta gerenciamento
    automático de recursos com o uso de um gerenciador de contexto.

    Attributes:
        cursor (CursorParquetDuckdb | CursorPython | CursorParquet):
            Instância do cursor utilizado para executar consultas.
        row_cursor: Armazena o resultado da execução da query.
    """

    def __init__(self, cursor: DBAthena) -> None:
        """
        Inicializa a classe Athena com um cursor, permitindo a execução de consultas SQL.

        Args:
            cursor (CursorParquetDuckdb | CursorPython | CursorParquet):
                Instância do cursor que define o backend para execução das queries.
        """

        self.cursor = cursor
        self.row_cursor = None

    def execute(
        self,
        query: str,
        parameters: tuple | dict = None,
        *,
        result_reuse_enable: bool = False,
    ):
        """
        Executa uma consulta SQL no backend configurado pelo cursor.

        Args:
            query (str): A string da consulta SQL a ser executada.
            parameters (tuple | dict, optional): Parâmetros para a consulta.
                - Se for uma tupla, é interpretada como argumentos posicionais.
                - Se for um dicionário, é interpretado como argumentos nomeados.
                - Padrão é None.
            result_reuse_enable (bool, optional): Habilita a reutilização de resultados da consulta
                armazenados em cache (se suportado pelo cursor). Padrão é False.

        Retorno:
            self: A instância atual da classe Athena, permitindo chamadas encadeadas.

        Notes:
            - A classe é compatível com os cursores:
              * `CursorParquetDuckdb`
              * `CursorPython`
              * `CursorParquet`
            - Cada cursor deve ser inicializado com os seguintes parâmetros obrigatórios:
                * `s3_staging_dir` (str): Diretório no Amazon S3 usado como área de staging.
                * `schema_name` (str, optional): Nome do schema do banco de dados.
                * `catalog_name` (str, optional): Nome do catálogo no Athena.
                * `poll_interval` (float, optional): Intervalo em segundos entre verificações de consulta. Padrão: 1.0.
                * `result_reuse_enable` (bool, optional): Habilita reutilização de resultados.

            - Caso as configurações do AWS CLI não estejam disponíveis, os parâmetros opcionais
              para autenticação podem ser passados como **kwargs** ao instanciar os cursores:
                * `region_name` (str): Região da AWS.
                * `aws_access_key_id` (str): Chave de acesso da AWS.
                * `aws_secret_access_key` (str): Chave secreta de acesso da AWS.

            - Se a consulta for um comando DDL (ex.: CREATE, ALTER, DROP), o método retorna o resultado
              de `fetchone()` imediatamente.

        Exemplo:
            ```python
            from athena_mvsh import Athena, CursorParquetDuckdb
            ```

            Inicializando CursorParquetDuckdb com credenciais AWS
            ```python
            cursor = CursorParquetDuckdb(
                s3_staging_dir="s3://my-bucket/staging/",
                schema_name="default",
                catalog_name="awsdatacatalog",
                region_name="us-west-2",
                aws_access_key_id="my-access-key",
                aws_secret_access_key="my-secret-key"
            )

            with Athena(cursor) as athena:
                athena.execute("SELECT * FROM example_table").fetchall()
            ```

            Inicializando CursorPython sem credenciais adicionais (usa AWS CLI configurado)
            ```python
            cursor = CursorPython(
                s3_staging_dir="s3://my-bucket/staging/"
             )
            with Athena(cursor) as athena:
                athena.execute("SELECT COUNT(*) FROM parquet_data")
            ```

            Inicializando CursorParquet com credenciais
            ```python
            cursor = CursorParquet(
                s3_staging_dir="s3://my-bucket/staging/",
                schema_name="analytics",
                region_name="us-west-1",
                aws_access_key_id="my-access-key",
                aws_secret_access_key="my-secret-key"
            )

            with Athena(cursor) as athena:
                result = athena.execute("SELECT * FROM parquet_table").fetchone()
            print(result)
            (1000,)
            ```
        """

        if parameters:
            args = parameters if isinstance(parameters, tuple) else tuple()
            kwargs = parameters if isinstance(parameters, dict) else dict()
            query = cast_format(query, *args, **kwargs)

        self.row_cursor = self.cursor.execute(query, result_reuse_enable)

        self.query = query
        self.result_reuse_enable = result_reuse_enable

        if query_is_ddl(query):
            return self.fetchone()

        return self

    @property
    def description(self) -> list[tuple] | None:
        """
        Retorna os metadados da consulta executada.

        Esta propriedade retorna uma lista de tuplas que representam as colunas
        da consulta executada. Cada tupla contém as seguintes informações:
            - Nome da coluna
            - Tipo da coluna
            - (Valor não utilizado, sempre `None`)
            - (Valor não utilizado, sempre `None`)
            - Precisão da coluna
            - Escala da coluna
            - Se a coluna pode ser nula (`Nullable`)

        Retorno:
            list[tuple] | None: Uma lista de tuplas contendo os metadados de cada
            coluna ou `None` se os metadados não estiverem disponíveis.

        Exemplo:
            ```python
            cursor = CursorPython(...)
            with Athena(cursor) as athena:
                athena.execute("SELECT id, name FROM users")
                print(athena.description)
            [('id', 'INTEGER', None, None, 10, 0, True), ('name', 'STRING', None, None, None, None, True)]
            ```
        """

        return self.cursor.description()

    @property
    def rowcount(self) -> int:
        """
        Retorna o número total de registros afetados pela execução da consulta.

        Este método recupera o número de registros afetados pela última execução de consulta,
        como determinado pelo cursor utilizado. O valor é fornecido pela implementação do cursor,
        seguindo as especificações da DB API Python.

        Returns:
            int: O número total de registros afetados pela última execução de consulta.

        Notes:
            - O valor retornado pode variar dependendo do tipo de consulta executada (como `SELECT`, `INSERT`, `UPDATE`, `DELETE`).
            - O método `rowcount` depende da implementação do cursor específico. Por exemplo, cursors como `CursorPython`, `CursorParquetDuckdb` ou outros tipos podem ter diferentes formas de calcular os registros afetados.
            - Se a consulta não afetar nenhum registro, o valor retornado será `-1`.

        Exemplo:
            ```python
            from athena_mvsh import Athena, CursorPython

            cursor = CursorPython(...)

            with Athena(cursor) as athena:
                athena.execute("UPDATE tabela SET coluna = 'valor' WHERE condicao = 'verdadeira'")
                print(f"Registros afetados: {athena_cursor.rowcount}")
            ```
        """

        return self.cursor.rowcount()

    def fetchone(self):
        """
        Retorna a próxima linha do cursor.

        Este método tenta obter a próxima linha do cursor. Se houver uma linha disponível,
        ela é retornada. Caso contrário, retorna `None` quando o cursor atingir o final
        dos resultados.

        Retorno:
            tuple | None: Retorna a próxima linha como uma tupla ou `None` se não houver mais linhas.

        Exemplo:
            ```python
            cursor = CursorPython(...)
            with Athena(cursor) as athena:
                athena.execute("SELECT id, name FROM users")
                print(athena.fetchone())  # Exemplo de uma linha retornada: (1, 'Alice')
                print(athena.fetchone())  # Exemplo de uma linha retornada: (2, 'Bob')
                print(athena.fetchone())  # Exemplo de retorno `None`, se não houver mais dados
            ```
        """

        try:
            row = next(self.row_cursor)
        except StopIteration:
            return None
        else:
            return row

    def fetchall(self) -> list:
        """
        Retorna todas as linhas do cursor como uma lista.

        Este método converte todas as linhas disponíveis no cursor em uma lista de
        tuplas, onde cada tupla representa uma linha do resultado da consulta executada.

        Retorno:
            list: Uma lista de tuplas representando todas as linhas retornadas pela consulta.

        Exemplo:
            ```python
            cursor = CursorPython(...)
            with Athena(cursor) as athena:
                athena.execute("SELECT id, name FROM users")
                rows = athena.fetchall()
                print(rows)  # Exemplo: [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]
            ```
        """

        return list(self.row_cursor)

    def fetchmany(self, size: int = 1):
        """
        Retorna um número especificado de linhas do cursor.

        Este método retorna até `size` linhas do cursor, como uma lista de tuplas.
        Se o número de linhas disponíveis for menor que o valor de `size`, ele retorna
        o número de linhas restantes. O valor padrão de `size` é 1.

        Args:
            size (int, opcional): O número de linhas a serem retornadas. O valor padrão é 1.

        Retorno:
            list: Uma lista de tuplas representando as linhas retornadas pelo cursor.

        Exemplo:
            ```python
            cursor = CursorPython(...)
            with Athena(cursor) as athena:
                athena.execute("SELECT id, name FROM users")
                rows = athena.fetchmany(2)
                print(rows)  # Exemplo: [(1, 'Alice'), (2, 'Bob')]
            ```
        """

        return list(islice(self.row_cursor, size))

    def to_arrow(self) -> pa.Table:
        """
        Converte os resultados da consulta para um `pa.Table` (PyArrow Table).

        Este método converte os dados retornados pela consulta executada em um
        objeto `pa.Table` (tabela do PyArrow). A conversão só é realizada se o
        cursor utilizado for uma instância de `CursorParquet` ou `CursorParquetDuckdb`.
        Caso contrário, uma exceção `ProgrammingError` será levantada.

        Exceções:
            ProgrammingError: Se o cursor não for uma instância de `CursorParquet`
            ou `CursorParquetDuckdb`.

        Retorno:
            pa.Table: Um objeto `pa.Table` contendo os resultados da consulta.

        Exemplo:
            ```python
            cursor = CursorParquet(...)
            with Athena(cursor) as athena:
                athena.execute("SELECT id, name FROM users")
                table = athena.to_arrow()
                print(table)  # Exemplo de saída: <pyarrow.table>
            ```
        """

        if not isinstance(self.cursor, CursorBaseParquet):
            raise ProgrammingError('Function not implemented for cursor !')

        return self.cursor.to_arrow(self.query, self.result_reuse_enable)

    def to_parquet(self, *args, **kwargs) -> None:
        """
        Converte os resultados da consulta para o formato Parquet.

        Este método converte os dados retornados pela consulta executada em um arquivo
        Parquet. A conversão é realizada apenas se o cursor utilizado for do tipo
        `CursorParquet` ou `CursorParquetDuckdb`. Dependendo do tipo do cursor, o método
        chama a função `to_parquet` correspondente.

        - Se o cursor for uma instância de `CursorParquetDuckdb`, o método chamará diretamente
        o método `to_parquet` do cursor, passando os argumentos fornecidos para ele.

        - Para o cursor `CursorParquet`, os dados são primeiro convertidos para um `pa.Table`
        e então são gravados como Parquet.

        Args:
            *args: Argumentos adicionais passados para a função `to_parquet` do cursor.
                - Para detalhes sobre os argumentos esperados, consulte a documentação do
                `DuckDB` e `PyArrow`:
                    - DuckDB: https://duckdb.org/docs
                    - PyArrow: https://arrow.apache.org/docs/python/

            **kwargs: Argumentos de palavra-chave adicionais passados para a função
            `to_parquet` do cursor. Consulte a documentação do `DuckDB` e `PyArrow` para
            mais detalhes sobre os argumentos aceitos por essas funções.

        Exceções:
            ProgrammingError: Se o cursor não for uma instância de `CursorParquet` ou
            `CursorParquetDuckdb`.

        Retorno:
            None: Este método não retorna valor, mas realiza a conversão para o formato Parquet.

        Exemplo:
            ```python
            cursor = CursorParquet(...)
            with Athena(cursor) as athena:
                athena.execute("SELECT id, name FROM users")
                athena.to_parquet('/path/to/file.parquet')
            ```
        """

        if not isinstance(self.cursor, CursorBaseParquet):
            raise ProgrammingError('Function not implemented for cursor !')

        if isinstance(self.cursor, CursorParquetDuckdb):
            self.cursor.to_parquet(
                self.query, self.result_reuse_enable, *args, **kwargs
            )
            return

        tbl = self.to_arrow()
        args = (tbl,) + args
        self.cursor.to_parquet(*args, **kwargs)

    def to_csv(
        self, output_file: str, delimiter: str = ';', include_header: bool = True
    ) -> None:
        """
        Converte os resultados da consulta para um arquivo CSV.

        Este método converte os dados retornados pela consulta executada e os salva em um arquivo
        CSV. A conversão é realizada apenas se o cursor utilizado for do tipo
        `CursorParquet` ou `CursorParquetDuckdb`. Dependendo do tipo do cursor, o método
        chama a função `to_csv` correspondente.

        - Se o cursor for uma instância de `CursorParquetDuckdb`, o método chamará diretamente
        o método `to_csv` do cursor, passando os parâmetros apropriados para ele.

        - Para o cursor `CursorParquet`, os dados são primeiro convertidos para um `pa.Table`
        e então salvos como CSV, com opções de escrita personalizáveis.

        Args:
            output_file (str): O caminho do arquivo CSV de saída.
            delimiter (str, opcional): O delimitador a ser usado no arquivo CSV. O valor padrão é `';'`.
            include_header (bool, opcional): Indica se o cabeçalho deve ser incluído no CSV. O valor padrão é `True`.

        Exceções:
            ProgrammingError: Se o cursor não for uma instância de `CursorParquet` ou `CursorParquetDuckdb`.

        Retorno:
            None: Este método não retorna valor, mas realiza a conversão para o formato CSV.

        Exemplo:
            ```python
            cursor = CursorParquet(...)
            with Athena(cursor) as athena:
                athena.execute("SELECT id, name FROM users")
                athena.to_csv('/path/to/file.csv')
            ```
        """

        if not isinstance(self.cursor, CursorBaseParquet):
            raise ProgrammingError('Function not implemented for cursor !')

        kwargs = {}
        if isinstance(self.cursor, CursorParquetDuckdb):
            kwargs |= {'header': include_header, 'sep': delimiter}
            args = (output_file,)

            self.cursor.to_csv(self.query, self.result_reuse_enable, *args, **kwargs)
            return

        options = csv_arrow.WriteOptions(
            delimiter=delimiter,
            include_header=include_header,
            quoting_style='all_valid',
        )

        kwargs['write_options'] = options
        tbl = self.to_arrow()
        args = (tbl, output_file)

        self.cursor.to_csv(*args, **kwargs)

    def to_create_table_db(
        self, table_name: str, *, database: str = 'db.duckdb'
    ) -> None:
        """
        Cria uma tabela no banco de dados DuckDB com base nos resultados da consulta executada.

        Este método cria uma tabela no banco de dados DuckDB especificado usando os dados retornados
        pela consulta executada. A tabela será criada com o nome fornecido como `table_name`.

        O método só pode ser utilizado com o cursor do tipo `CursorParquetDuckdb`. O nome do banco
        de dados pode ser especificado como um argumento, e se não fornecido, o banco de dados padrão
        será `'db.duckdb'`.

        Args:
            table_name (str): O nome da tabela a ser criada no banco de dados.
            database (str, opcional): O nome do banco de dados onde a tabela será criada. O valor padrão é `'db.duckdb'`.

        Exceções:
            ProgrammingError: Se o cursor não for uma instância de `CursorParquetDuckdb`.

        Retorno:
            None: Este método não retorna valor, mas cria a tabela no banco de dados.

        Exemplo:
            ```python
            cursor = CursorParquetDuckdb(...)
            with Athena(cursor) as athena:
                athena.execute("SELECT id, name FROM users")
                athena.to_create_table_db('users_table')
            ```
        """

        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')

        self.cursor.to_create_table_db(
            database, self.query, self.result_reuse_enable, table_name=table_name
        )

    def to_partition_create_table_db(
        self, table_name: str, *, database: str = 'db.duckdb', workers: int = WORKERS
    ) -> None:
        """
        Lê arquivos Parquet gerados a partir da consulta executada e insere os dados na tabela do banco de dados DuckDB.

        Este método executa uma consulta no Athena para gerar arquivos Parquet que são armazenados no S3.
        Após a consulta, os arquivos Parquet são lidos e os dados são inseridos na tabela especificada no DuckDB.
        A inserção dos dados é paralelizada utilizando múltiplos trabalhadores (threads) para otimizar o desempenho.

        O método realiza os seguintes passos:
        - Executa a consulta fornecida no Athena para gerar os arquivos Parquet.
        - Descarrega os arquivos Parquet do S3 a partir do manifest gerado pela consulta.
        - Para cada arquivo Parquet, insere os dados na tabela especificada no DuckDB, utilizando
        `ThreadPoolExecutor` para paralelizar as inserções.

        Args:
            table_name (str): O nome da tabela onde os dados serão inseridos no DuckDB.
            database (str, opcional): O nome do banco de dados DuckDB. O valor padrão é 'db.duckdb'.
            workers (int, opcional): O número de trabalhadores (threads) a serem usados para paralelizar a inserção dos arquivos Parquet. O valor padrão é `WORKERS`.

        Exceções:
            ProgrammingError: Se o cursor não for do tipo `CursorParquetDuckdb`.
            Exception: Se ocorrer um erro durante a execução da inserção dos dados.

        Retorno:
            None: O método não retorna valor, mas insere os dados na tabela do banco de dados DuckDB.

        Exemplo:
            ```python
            cursor = CursorParquetDuckdb(...)
            with Athena(cursor) as athena:
                athena.to_partition_create_table_db(
                    table_name='target_table',
                    database='db.duckdb',
                    workers=4
                )
            ```
        """

        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')

        self.cursor.to_partition_create_table_db(
            database,
            self.query,
            workers,
            self.result_reuse_enable,
            table_name=table_name,
        )

    def to_insert_table_db(
        self, table_name: str, *, database: str = 'db.duckdb'
    ) -> None:
        """
        Insere os resultados de uma consulta executada no Amazon Athena em uma tabela no DuckDB.

        Esta função utiliza o cursor específico da biblioteca, `CursorParquetDuckdb`, para transferir
        os dados resultantes de uma consulta executada no Amazon Athena para uma tabela no banco de dados
        DuckDB. Caso o cursor utilizado não seja compatível, um erro será lançado.

        Args:
            table_name (str): O nome da tabela de destino no DuckDB onde os dados serão inseridos.
            database (str, optional): O caminho ou nome do banco de dados DuckDB onde a tabela será criada
                ou atualizada. O valor padrão é `'db.duckdb'`.

        Exceções:
            ProgrammingError: Se a função for chamada com um cursor incompatível.

        Exemplo:
            ```python
            from athena_mvsh import Athena, CursorParquetDuckdb

            cursor = CursorParquetDuckdb(...)

            with Athena(cursor) as athena:
                athena.to_insert_table_db(
                    table_name="tabela_destino",
                    database="meu_banco.duckdb"
                )
            ```
        """

        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')

        self.cursor.to_insert_table_db(
            database, self.query, self.result_reuse_enable, table_name=table_name
        )

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        location: str = None,
        partitions: list[str] = None,
        catalog_name: str = 'awsdatacatalog',
        compression: Literal['GZIP', 'SNAPPY', 'ZSTD'] = 'ZSTD',
    ) -> None:
        """
        Escreve um DataFrame pandas em uma tabela externa no Athena usando o DuckDB.

        Este método utiliza o DuckDB para escrever um DataFrame pandas como uma tabela externa no Amazon Athena,
        permitindo especificar o esquema, particionamento e compressão dos dados. Caso o cursor utilizado não seja
        compatível, um erro será lançado.

        Args:
            df (pd.DataFrame): O DataFrame pandas que será escrito na tabela externa no Athena.
            table_name (str): O nome da tabela externa de destino no Athena.
            schema (str): O esquema onde a tabela será criada ou atualizada no Athena.
            location (str, optional): O local onde os dados serão armazenados (geralmente um bucket S3), se aplicável.
            partitions (list[str], optional): Lista de colunas para particionamento dos dados na tabela.
            catalog_name (str, optional): O nome do catálogo de dados a ser utilizado. O valor padrão é `'awsdatacatalog'`.
            compression (Literal['ZSTD', 'SNAPPY', 'GZIP'], optional): O algoritmo de compressão a ser utilizado nos dados. O valor padrão é `'ZSTD'`.

        Exceções:
            ProgrammingError: Se a função for chamada com um cursor incompatível.

        Detalhes:
            - O DataFrame pandas fornecido será convertido e escrito como uma tabela externa no Athena.
            - A opção de particionamento permite distribuir os dados em várias pastas ou arquivos, facilitando o gerenciamento de grandes volumes de dados no S3.
            - A compressão `ZSTD` pode ser usada para reduzir o tamanho do arquivo gerado, com outras opções como `SNAPPY` também sendo suportadas.
            - O catálogo de dados `awsdatacatalog` pode ser alterado para refletir configurações específicas do ambiente de execução.

        Exemplo:
            ```python
            import pandas as pd
            from athena_mvsh import Athena, CursorParquetDuckdb

            cursor = CursorParquetDuckdb(...)

            # Exemplo de DataFrame
            df = pd.DataFrame({
                "coluna1": [1, 2, 3],
                "coluna2": ["A", "B", "C"]
            })

            # Escreve o DataFrame em uma tabela externa no Athena
            with Athena(cursor) as athena:
                athena.write_dataframe(
                    df,
                    table_name="tabela_destino",
                    schema="meu_esquema",
                    location="s3://meu-bucket/dados/",
                    partitions=["coluna1"],
                    catalog_name="meu_catalogo",
                    compression="ZSTD"
                )
            ```

        Notas:
            - Certifique-se de que o cursor utilizado seja instância de `CursorParquetDuckdb`.
            - O particionamento é útil para grandes volumes de dados, permitindo consultas mais eficientes no Athena.
            - A compressão pode ser ajustada para balancear entre desempenho e tamanho do arquivo.
        """

        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')

        self.cursor.write_dataframe(
            df, table_name, schema, location, partitions, catalog_name, compression
        )

    def write_arrow(
        self,
        tbl: pa.Table,
        table_name: str,
        schema: str,
        location: str = None,
        partitions: list[str] = None,
        catalog_name: str = 'awsdatacatalog',
        compression: Literal['GZIP', 'SNAPPY', 'ZSTD'] = 'ZSTD',
    ) -> None:
        """
        Escreve um Table Arrow em uma tabela externa no Athena usando o DuckDB.

        Este método utiliza o DuckDB para escrever um Table Arrow como uma tabela externa no Amazon Athena,
        permitindo especificar o esquema, particionamento e compressão dos dados. Caso o cursor utilizado não seja
        compatível, um erro será lançado.

        Args:
            tbl (pa.Table): O Table Arrow que será escrito na tabela externa no Athena.
            table_name (str): O nome da tabela externa de destino no Athena.
            schema (str): O esquema onde a tabela será criada ou atualizada no Athena.
            location (str, optional): O local onde os dados serão armazenados (geralmente um bucket S3), se aplicável.
            partitions (list[str], optional): Lista de colunas para particionamento dos dados na tabela.
            catalog_name (str, optional): O nome do catálogo de dados a ser utilizado. O valor padrão é `'awsdatacatalog'`.
            compression (Literal['ZSTD', 'SNAPPY', 'GZIP'], optional): O algoritmo de compressão a ser utilizado nos dados. O valor padrão é `'ZSTD'`.

        Exceções:
            ProgrammingError: Se a função for chamada com um cursor incompatível.

        Detalhes:
            - O Table Arrow fornecido será convertido e escrito como uma tabela externa no Athena.
            - A opção de particionamento permite distribuir os dados em várias pastas ou arquivos, facilitando o gerenciamento de grandes volumes de dados no S3.
            - A compressão `ZSTD` pode ser usada para reduzir o tamanho do arquivo gerado, com outras opções como `SNAPPY` também sendo suportadas.
            - O catálogo de dados `awsdatacatalog` pode ser alterado para refletir configurações específicas do ambiente de execução.

        Exemplo:
            ```python
            import pandas as pd
            from athena_mvsh import Athena, CursorParquetDuckdb

            cursor = CursorParquetDuckdb(...)

            # Exemplo de DataFrame
            df = pd.DataFrame({
                "coluna1": [1, 2, 3],
                "coluna2": ["A", "B", "C"]
            })

            # Escreve o Table Arrow em uma tabela externa no Athena
            with Athena(cursor) as athena:
                athena.write_arrow(
                    tbl,
                    table_name="tabela_destino",
                    schema="meu_esquema",
                    location="s3://meu-bucket/dados/",
                    partitions=["coluna1"],
                    catalog_name="meu_catalogo",
                    compression="ZSTD"
                )
            ```

        Notas:
            - Certifique-se de que o cursor utilizado seja instância de `CursorParquetDuckdb`.
            - O particionamento é útil para grandes volumes de dados, permitindo consultas mais eficientes no Athena.
            - A compressão pode ser ajustada para balancear entre desempenho e tamanho do arquivo.
        """

        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')

        self.cursor.write_arrow(
            tbl, table_name, schema, location, partitions, catalog_name, compression
        )

    def write_parquet(
        self,
        file: list[str | Path] | str | Path,
        table_name: str,
        schema: str,
        location: str = None,
        partitions: list[str] = None,
        catalog_name: str = 'awsdatacatalog',
        compression: Literal['GZIP', 'SNAPPY', 'ZSTD'] = 'ZSTD',
    ) -> None:
        """
        Cria uma tabela externa no Athena a partir de um ou vários arquivos Parquet.

        Este método utiliza o DuckDB para criar uma tabela externa no Amazon Athena a partir de arquivos Parquet,
        permitindo especificar o esquema, particionamento e compressão dos dados. Caso o cursor utilizado não seja
        compatível, um erro será lançado.

        Args:
            file (list[str | Path] | str | Path): Um ou vários caminhos para arquivos Parquet ou diretórios
                contendo arquivos Parquet. Pode ser uma lista ou uma única string/Path.
            table_name (str): O nome da tabela externa a ser criada no Athena.
            schema (str): O esquema onde a tabela será criada ou atualizada no Athena.
            location (str, optional): O local no S3 onde os arquivos Parquet serão armazenados, se aplicável.
            partitions (list[str], optional): Lista de colunas para particionamento dos dados na tabela.
            catalog_name (str, optional): O nome do catálogo de dados a ser utilizado. O valor padrão é `'awsdatacatalog'`.
            compression (Literal['ZSTD', 'SNAPPY', 'GZIP'], optional): O algoritmo de compressão a ser utilizado nos dados. O valor padrão é `'ZSTD'`.

        Exceções:
            ProgrammingError: Se a função for chamada com um cursor incompatível.

        Detalhes:
            - O(s) arquivo(s) Parquet fornecido(s) serão usados para criar uma tabela externa no Athena.
            - A opção de particionamento permite distribuir os dados em várias pastas ou arquivos no S3, facilitando o gerenciamento de grandes volumes de dados.
            - A compressão `ZSTD` pode ser usada para reduzir o tamanho dos arquivos Parquet gerados, com outras opções como `SNAPPY` também sendo suportadas.
            - O catálogo de dados `awsdatacatalog` pode ser alterado para refletir configurações específicas do ambiente de execução.

        Exemplo:
            ```python
            from pathlib import Path
            from athena_mvsh import Athena, CursorParquetDuckdb

            cursor = CursorParquetDuckdb(...)

            # Caminho para os arquivos Parquet
            arquivos_parquet = Path("/caminho/para/arquivos/*.parquet")

            # Cria uma tabela externa no Athena a partir dos arquivos Parquet
            with Athena(cursor) as athena:
                athena.write_parquet(
                    file=arquivos_parquet,
                    table_name="tabela_destino",
                    schema="meu_esquema",
                    location="s3://meu-bucket/dados/",
                    partitions=["coluna1"],
                    catalog_name="meu_catalogo"
                )
            ```

        Notas:
            - Certifique-se de que o cursor utilizado seja instância de `CursorParquetDuckdb`.
            - O particionamento é útil para grandes volumes de dados, permitindo consultas mais eficientes no Athena.
            - A compressão pode ser ajustada para balancear entre desempenho e tamanho do arquivo.

        """

        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')

        self.cursor.write_parquet(
            file, table_name, schema, location, partitions, catalog_name, compression
        )

    def write_table_iceberg(
        self,
        data: pd.DataFrame | list[str | Path] | str | Path | pa.Table,
        table_name: str,
        schema: str,
        location: str = None,
        partitions: list[str] = None,
        catalog_name: str = 'awsdatacatalog',
        compression: Literal['ZSTD', 'SNAPPY', 'GZIP'] = 'ZSTD',
        if_exists: Literal['replace', 'append'] = 'replace',
    ) -> None:
        """
        Cria ou insere dados em uma tabela Iceberg no Athena a partir de um DataFrame pandas, Table Arrow ou arquivos Parquet.

        Este método usa o DuckDB para criar ou inserir dados em uma tabela Iceberg no Amazon Athena,
        permitindo que os dados sejam inseridos a partir de um DataFrame pandas, Table Arrow ou múltiplos arquivos Parquet.
        A operação pode substituir a tabela existente ou adicionar novos dados, dependendo do valor de `if_exists`.

        Args:
            data (pd.DataFrame | list[str | Path] | str | Path | pa.Table): Dados a serem inseridos ou criados na tabela Iceberg.
                Pode ser um DataFrame pandas, Table Arrow ou um ou mais arquivos Parquet (caminhos em formato string ou Path).
            table_name (str): O nome da tabela Iceberg a ser criada ou inserida no Athena.
            schema (str): O esquema onde a tabela Iceberg será criada ou atualizada no Athena.
            location (str, optional): O local no S3 onde os dados serão armazenados, se aplicável.
            partitions (list[str], optional): Lista de colunas para particionamento dos dados na tabela.
            catalog_name (str, optional): O nome do catálogo de dados a ser utilizado. O valor padrão é `'awsdatacatalog'`.
            compression (Literal['ZSTD', 'SNAPPY', 'GZIP'], optional): O algoritmo de compressão a ser utilizado nos dados. O valor padrão é `'ZSTD'`.
            if_exists (Literal['replace', 'append'], optional): Define se os dados devem substituir a tabela existente
                (`'replace'`) ou ser adicionados (`'append'`). O valor padrão é `'replace'`.

        Exceções:
            ProgrammingError: Se a função for chamada com um cursor incompatível.

        Detalhes:
            - A tabela Iceberg será criada ou atualizada no Athena com os dados fornecidos.
            - O particionamento permite distribuir os dados em várias pastas ou arquivos no S3, facilitando o gerenciamento de grandes volumes de dados.
            - A compressão `ZSTD` pode ser usada para reduzir o tamanho dos arquivos Parquet gerados, com outras opções como `GZIP` também sendo suportadas.
            - O catálogo de dados `awsdatacatalog` pode ser alterado para refletir configurações específicas do ambiente de execução.
            - O parâmetro `if_exists` controla se os dados devem substituir ou ser adicionados à tabela existente.

        Exemplo:
            ```python
            import pandas as pd
            from athena_mvsh import Athena, CursorParquetDuckdb

            cursor = CursorParquetDuckdb(...)

            # Exemplo de DataFrame
            df = pd.DataFrame({
                "coluna1": [1, 2, 3],
                "coluna2": ["A", "B", "C"]
            })

            # Cria ou insere dados na tabela Iceberg no Athena
            with Athena(cursor) as athena:
                athena.write_table_iceberg(
                    data=df,
                    table_name="tabela_iceberg",
                    schema="meu_esquema",
                    location="s3://meu-bucket/dados/",
                    partitions=["coluna1"],
                    catalog_name="meu_catalogo",
                    if_exists="append"
                )
            ```

        Notas:
            - Certifique-se de que o cursor utilizado seja instância de `CursorParquetDuckdb`.
            - O particionamento é útil para grandes volumes de dados, permitindo consultas mais eficientes no Athena.
            - A compressão pode ser ajustada para balancear entre desempenho e tamanho do arquivo.
            - O parâmetro `if_exists` permite escolher entre substituir ou adicionar dados à tabela existente, controlando o comportamento da operação.
        """

        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')

        self.cursor.write_table_iceberg(
            data,
            table_name,
            schema,
            location,
            partitions,
            catalog_name,
            compression,
            if_exists,
        )

    def merge_table_iceberg(
        self,
        target_table: str,
        source_data: pd.DataFrame | list[str | Path] | str | Path | pa.Table,
        schema: str,
        predicate: str,
        delete_condition: str = None,
        update_condition: str = None,
        insert_condition: str = None,
        alias: tuple = ('t', 's'),
        location: str = None,
        catalog_name: str = 'awsdatacatalog',
    ) -> None:
        """
        Executa um merge (UPSERT) em uma tabela Iceberg no Athena, utilizando uma tabela temporária criada no Athena.

        Este método usa o DuckDB para criar uma tabela temporária no Athena com os dados fornecidos e, em seguida,
        executa um merge (UPSERT) entre a tabela de destino e os dados da tabela temporária. A operação permite
        atualizar ou inserir registros na tabela de destino com base em uma condição de junção (predicate).
        Pode-se utilizar um DataFrame pandas, Table Arrow ou arquivos Parquet como dados de origem.

        Args:
            target_table (str): O nome da tabela de destino do tipo Iceberg no Athena, onde os dados serão mesclados.
            source_data (pd.DataFrame | list[str | Path] | str | Path | pa.Table): Dados de origem a serem mesclados com a tabela de destino.
                Pode ser um DataFrame pandas, Table Arrow ou um ou mais arquivos Parquet (caminhos em formato string ou Path).
            schema (str): O esquema onde a tabela Iceberg será manipulada no Athena.
            predicate (str): A condição de junção (predicate) que define a lógica de correspondência entre os dados de origem
                e os dados da tabela de destino para o merge (atualização ou inserção).
            delete_condition (str, opcional): Condição para excluir registros da tabela de destino. Se `None`, a cláusula `DELETE` não será aplicada.
            update_condition (str, opcional): Condição para atualizar registros existentes na tabela de destino. Se `None`, todos os registros correspondentes serão atualizados..
            insert_condition (str, opcional): Condição para inserir novos registros na tabela de destino. Se `None`, todos os registros correspondentes serão inseridos.
            alias (tuple, optional): Alias para as tabelas de origem e destino no merge. O valor padrão é `('t', 's')`,
                onde `'t'` é o alias da tabela de destino e `'s'` é o alias da tabela de origem.
            location (str, optional): O local no S3 onde os dados serão armazenados, se aplicável.
            catalog_name (str, optional): O nome do catálogo de dados a ser utilizado. O valor padrão é `'awsdatacatalog'`.

        Exceções:
            ProgrammingError: Se a função for chamada com um cursor incompatível.

        Detalhes:
            - O método cria uma **tabela temporária** no Athena com os dados fornecidos (DataFrame pandas, Table Arrow ou arquivos Parquet)
            e executa um merge (UPSERT) entre a tabela de destino (Iceberg) e a tabela temporária.
            - O `predicate` define a condição de junção para o merge, permitindo atualizar ou inserir registros na tabela de destino.
            - O parâmetro `alias` é utilizado para dar nomes alias às tabelas de origem e destino durante a execução do SQL gerado.
            - O parâmetro `catalog_name` permite configurar o catálogo de dados, sendo `'awsdatacatalog'` o valor padrão.

        Exemplo:
            ```python
            import pandas as pd
            from athena_mvsh import Athena, CursorParquetDuckdb

            cursor = CursorParquetDuckdb(...)

            # Exemplo de DataFrame
            df = pd.DataFrame({
                "id": [1, 2, 3],
                "nome": ["A", "B", "C"]
            })

            # Realiza um merge (UPSERT) na tabela Iceberg no Athena, utilizando uma tabela temporária
            with Athena(cursor) as athena:
                athena.merge_table_iceberg(
                    target_table="tabela_destino",
                    source_data=df,
                    schema="meu_esquema",
                    predicate="t.id = s.id",
                    alias=("t", "s"),
                    location="s3://meu-bucket/dados/",
                    catalog_name="meu_catalogo"
                )
            ```

        Notas:
            - Certifique-se de que o cursor utilizado seja instância de `CursorParquetDuckdb`.
            - O `predicate` deve ser uma expressão lógica válida para a junção dos dados da origem com os dados da tabela de destino.
            - A operação de merge cria uma tabela temporária no Athena para facilitar a execução do merge. O uso de uma tabela temporária pode otimizar o processo, especialmente quando os dados de origem são grandes.
        """

        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')

        self.cursor.merge_table_iceberg(
            target_table,
            source_data,
            schema,
            predicate,
            delete_condition,
            update_condition,
            insert_condition,
            alias,
            location,
            catalog_name,
        )

    def to_pandas(self, *args, **kwargs) -> pd.DataFrame:
        """
        Exporta os resultados de uma consulta para um DataFrame pandas.

        Este método converte os resultados da consulta executada em diferentes tipos de cursor para um DataFrame pandas.
        Ele suporta cursors do tipo `CursorParquetDuckdb`, `CursorPython` e `CursorParquet`, adaptando-se conforme o tipo de cursor utilizado.

        Args:
            *args: Argumentos adicionais que serão passados para o método `to_pandas` do cursor.
            **kwargs: Argumentos adicionais que serão passados para o método `to_pandas` do cursor.

        Retorno:
            pd.DataFrame: Um DataFrame pandas contendo os resultados da consulta executada.

        Exceções:
            ProgrammingError: Se o cursor não for do tipo compatível (nenhum dos tipos mencionados acima).

        Detalhes:
            - Se o cursor for do tipo `CursorParquetDuckdb`, o método utilizará o método `to_pandas` do cursor para exportar os resultados da consulta.
            - Se o cursor for do tipo `CursorPython`, os resultados da consulta serão obtidos via `fetchall()` e então convertidos para um DataFrame pandas com os nomes das colunas extraídos da descrição da consulta.
            - Se o cursor for do tipo `CursorParquet`, os resultados serão convertidos para o formato Arrow e, em seguida, para um DataFrame pandas utilizando o método apropriado.

        Exemplo:
            ```python
            from athena_mvsh import Athena, CursorPython

            cursor = CursorPython(...)

            with Athena(cursor) as athena:
                athena.execute("SELECT id, name FROM users")
                df = athena.to_pandas()

            print(df.head())
            ```

        Notas:
            - A função é adaptável para diferentes tipos de cursor e utiliza os métodos específicos de cada um para converter os resultados para um DataFrame pandas.
            - A compatibilidade do cursor é verificada antes de tentar acessar o método `to_pandas` específico de cada tipo de cursor.
            - A funcionalidade depende de cada tipo de cursor implementado (como `CursorParquetDuckdb`, `CursorPython` e `CursorParquet`).
        """

        if isinstance(self.cursor, CursorParquetDuckdb):
            return self.cursor.to_pandas(
                self.query, self.result_reuse_enable, *args, **kwargs
            )

        if isinstance(self.cursor, CursorPython):
            tbl = self.fetchall()
            kwargs |= {
                'columns': [c[0] for c in self.description],
                'data': tbl,
                'coerce_float': True,
            }
            return self.cursor.to_pandas(*args, **kwargs)

        if isinstance(self.cursor, CursorParquet):
            tbl = self.to_arrow()
            args = args + (tbl,)
            kwargs |= {'types_mapper': pd.ArrowDtype}
            return self.cursor.to_pandas(*args, **kwargs)

    def close(self):
        if self.row_cursor:
            self.row_cursor = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
