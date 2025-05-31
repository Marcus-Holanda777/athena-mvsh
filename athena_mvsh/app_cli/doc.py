import typer
from rich.markdown import Markdown
from rich.console import Console
from rich.table import Table
import os

app = typer.Typer()
terminal = Console()


def clear():
    os.system('cls' if os.name == 'nt' else 'clear')


def mostrar_paginas(markdown: str, linhas_por_pagina: int = 20):
    """
    Mostra o conteúdo do Markdown em páginas, com um número específico de linhas por página.
    """
    linhas = markdown.splitlines()
    total_linhas = len(linhas)

    for i in range(0, total_linhas, linhas_por_pagina):
        pagina = '\n'.join(linhas[0:i + linhas_por_pagina])
        terminal.print(Markdown(pagina))
        if i + linhas_por_pagina < total_linhas:
            input("\nPressione Enter para continuar...")
            clear()


def create_table_rich(title: str, rows: list[tuple]) -> Table:
    table = Table(title=f'\n{title}\n', show_lines=True, title_justify='left')

    table.add_column('Propriedade', style='cyan', no_wrap=True)
    table.add_column('Valor padrão', style='magenta')
    table.add_column('Descrição', style='green')

    for row in rows:
        table.add_row(*row)

    return table


@app.command(
    help='Comando para criar tabelas no Athena.',
    short_help='Criar tabelas no Athena',
    name='create-table',
)
def create_table(
    property: bool = typer.Option(
        False, '--property', '-p', help='Propriedade de table CREATE TABLE'
    ),
):
    mark = """
# CREATE TABLE

O comando `CREATE TABLE` é **exclusivo para tabelas do tipo Iceberg** no Amazon Athena.
Tabelas Iceberg são transacionais e oferecem recursos avançados como versionamento, 
atualizações incrementais, e evolução de schema.
Athena **não permite usar `CREATE TABLE` para formatos tradicionais** como CSV, JSON ou Parquet puro. 
Nestes casos, deve-se usar `CREATE EXTERNAL TABLE`.

**Sinopse:**

```sql
CREATE TABLE
  [db_name.]table_name (col_name data_type [COMMENT col_comment] [, ...] )
  [PARTITIONED BY (col_name | transform, ... )]
  LOCATION 's3://amzn-s3-demo-bucket/your-folder/'
  TBLPROPERTIES ( 'table_type' ='ICEBERG' [, property_name=property_value] )
```

**Exemplo básico:**

```sql
CREATE TABLE iceberg_table (
  id int,
  data string,
  category string) 
PARTITIONED BY (category, bucket(16,id)) 
LOCATION 's3://amzn-s3-demo-bucket/iceberg-folder' 
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='snappy',
  'optimize_rewrite_delete_file_threshold'='10'
)
```
"""

    terminal.print(Markdown(mark))

    if property:
        rows = [
            ('format', 'parquet', 'Formato de dados do arquivo.'),
            ('write_compression', 'snappy', 'Codec de compactação de arquivo.'),
            (
                'optimize_rewrite_data_file_threshold',
                '5',
                'Ignora reescrita se poucos arquivos de dados precisarem de otimização, reduzindo custo computacional.',
            ),
            (
                'optimize_rewrite_delete_file_threshold',
                '2',
                'Ignora reescrita se poucos arquivos de exclusão estiverem associados, acumulando mais antes de otimizar.',
            ),
            (
                'vacuum_min_snapshots_to_keep',
                '1',
                'Número mínimo de snapshots a serem retidos na ramificação principal de uma tabela.',
            ),
            (
                'vacuum_max_snapshot_age_seconds',
                '432 mil segundos',
                'Período máximo para reter os snapshots na ramificação principal.',
            ),
            (
                'vacuum_max_metadata_files_to_keep',
                '100',
                'O número máximo de arquivos de metadados anteriores a serem retidos na ramificação principal da tabela.',
            ),
        ]

        tbl = create_table_rich(
            'Propriedades de tabela [b green]CREATE TABLE[/b green] cláusula [b red]TBLPROPERTIES[/b red]:',
            rows,
        )
        terminal.print(tbl)


@app.command(
    help='Comando para criar tabelas no Athena a partir de uma consulta.',
    short_help='Criar tabelas no Athena a partir de uma consulta',
    name='create-table-as',
)
def create_table_as(
    property: bool = typer.Option(
        False, '--property', '-p', help='Propriedade de table CREATE TABLE AS'
    ),
):
    mark = """
# CREATE TABLE AS SELECT (CTAS)

O comando `CREATE TABLE AS SELECT` (CTAS) permite criar uma nova tabela no Amazon Athena.
Com base no resultado de uma consulta `SELECT`. 
Ele é útil para transformar e armazenar dados em um novo formato ou particionamento, 
além de otimizar consultas futuras.

**Sinopse:**

```sql
CREATE TABLE table_name
[ WITH ( property_name = expression [, ...] ) ]
AS query
[ WITH [ NO ] DATA ]
```

**Exemplo básico:**

Cria uma tabela do tipo `ICEBERG` a partir do resultado de uma consulta `SELECT`:

```sql
CREATE TABLE table_iceberg WITH (
  table_type = 'ICEBERG'
  is_external = False,
  location ='s3://amzn-s3-demo-bucket/tables/iceberg_table/',
  format = 'PARQUET',
  compression = 'ZSTD',
  partitioning = ARRAY['month(order_date)', 'country']
) AS 
SELECT 
  * 
FROM 
  table_name
)
"""

    terminal.print(Markdown(mark))

    if property:
        rows = [
            (
                'table_type',
                'hive',
                'Opcional. O padrão é `HIVE` (outra opção `ICEBERG`). Especifica o tipo de tabela da tabela resultante.',
            ),
            (
                'external_location',
                's3://amzn-s3-demo-bucket/',
                'Opcional. O local no qual o Athena salvará sua consulta CTAS no Amazon S3.',
            ),
            (
                'is_external',
                'true',
                'Opcional. Indica se a tabela corresponde a uma tabela externa. O padrão é true. Para tabelas do Iceberg, deve ser definido como “false”',
            ),
            (
                'location',
                's3://amzn-s3-demo-bucket/',
                'Obrigatório para tabelas do Iceberg. Especifica o local raiz da tabela do Iceberg que será criada a partir dos resultados da consulta.',
            ),
            (
                'field_delimiter',
                r'\001',
                r'Delimitador de campo para arquivos de texto (como CSV) deve ser um único caractere; se não for especificado, o padrão é \001. Delimitadores com vários caracteres não são permitidos em CTAS.',
            ),
            (
                'format',
                'parquet',
                'O formato de armazenamento dos resultados de consultas CTAS, como ORC, PARQUET, AVRO, JSON, ION ou TEXTFILE. Para tabelas do Iceberg, os formatos permitidos são ORC, PARQUET e AVRO.',
            ),
            ('write_compression', 'snappy', 'Codec de compactação de arquivo.'),
            (
                'compression_level',
                '3',
                'O nível de compressão a ser usado. Essa propriedade se aplica apenas à compressão ZSTD. Os valores possíveis são de 1 a 22. O valor padrão é 3.',
            ),
            (
                'bucketed_by',
                'ARRAY[ column_name[,…] ]',
                'Uma lista matriz de buckets para dados do bucket. Se omitida, o Athena não armazenará os dados dessa consulta em bucket. Essa propriedade não se aplica para tabelas do Iceberg.',
            ),
            (
                'bucket_count',
                '[ int ]',
                'O número de buckets para armazenar seus dados em um bucket. Se omitido, o Athena não armazenará os dados em bucket. Essa propriedade não se aplica para tabelas do Iceberg.',
            ),
            (
                'partitioned_by',
                'ARRAY[ col_name[,…] ]',
                'Opcional. Uma lista matriz de colunas pela qual a tabela CTAS será particionada. Essa propriedade não se aplica para tabelas do Iceberg.',
            ),
            (
                'partitioning',
                'ARRAY[ partition_transform, ... ]',
                'Opcional. Especifica o particionamento da tabela do Iceberg que será criada.',
            ),
            (
                'optimize_rewrite_min_data_file_size_bytes',
                '[ long ]',
                'Opcional. Arquivos menores que o valor especificado são incluídos para otimização. Essa propriedade se aplica apenas a tabelas do Iceberg.',
            ),
            (
                'optimize_rewrite_max_data_file_size_bytes',
                '[ long ]',
                'Arquivos maiores que o valor especificado são incluídos para otimização. Essa propriedade se aplica apenas a tabelas do Iceberg.',
            ),
            (
                'optimize_rewrite_data_file_threshold',
                '5',
                'Ignora reescrita se poucos arquivos de dados precisarem de otimização, reduzindo custo computacional.',
            ),
            (
                'optimize_rewrite_delete_file_threshold',
                '2',
                'Ignora reescrita se poucos arquivos de exclusão estiverem associados, acumulando mais antes de otimizar.',
            ),
            (
                'vacuum_min_snapshots_to_keep',
                '1',
                'Número mínimo de snapshots a serem retidos na ramificação principal de uma tabela.',
            ),
            (
                'vacuum_max_snapshot_age_seconds',
                '432 mil segundos',
                'Período máximo para reter os snapshots na ramificação principal.',
            ),
        ]

        tbl = create_table_rich(
            'Propriedades de tabela [b green]CREATE TABLE AS[/b green] cláusula [b red]WITH[/b red]:',
            rows,
        )
        terminal.print(tbl)


@app.command(
    help='Comandos para otimizar uma tabela do tipo iceberg no Athena.',
    short_help='Otimizar tabela do tipo iceberg no Athena',
    name='optimize-table',
)
def optimize_table():
    mark = """
# OPTIMIZE

Reescreve arquivos pequenos em blocos maiores para acelerar as consultas.

**Sinopse:**

```sql
OPTIMIZE [db_name.]table_name REWRITE DATA USING BIN_PACK
  [WHERE predicate]
```

**Exemplo básico:**

```sql
OPTIMIZE iceberg_table REWRITE DATA USING BIN_PACK
  WHERE category = 'c1'
```
  
# VACUUM

Remove arquivos não utilizados (como dados sobrescritos ou snapshots antigos).

**Sinopse:**

```sql
VACUUM [database_name.]target_table
```

**Exemplo básico:**

```sql
VACUUM iceberg_table
```

"""
    terminal.print(Markdown(mark))


@app.command(
    help='Grava os resultados da consulta de uma SELECT instrução no formato de dados especificado',
    short_help='Grava os resultados da consulta de uma SELECT instrução',
    name='unload',
)
def unload(
    property: bool = typer.Option(
        False, '--property', '-p', help='Propriedades UNLOAD'
    ),
):
    mark = """
# UNLOAD

O comando UNLOAD é usado para exportar os resultados de uma consulta SQL diretamente para um arquivo no Amazon S3, 
geralmente no formato Parquet ou CSV. Ele é ideal para mover dados de forma eficiente do Athena para uma área de data lake.

**Sinopse:**

```sql
UNLOAD (SELECT col_name[, ...] FROM old_table) 
TO 's3://amzn-s3-demo-bucket/my_folder/' 
WITH ( property_name = 'expression' [, ...] )
```

**Exemplo básico:**

```sql
UNLOAD (
  SELECT * FROM old_table
) 
TO 's3://amzn-s3-demo-bucket/' 
WITH (
  format = 'PARQUET', 
  compression = 'ZSTD', 
  compression_level = 4
)
```
"""

    terminal.print(Markdown(mark))
    if property:
        rows = [
            (
                'field_delimiter',
                r'\001',
                r'Opcional. Delimitador de campo para arquivos de texto (como CSV) deve ser um único caractere; se não for especificado, o padrão é \001. Delimitadores com vários caracteres não são permitidos.',
            ),
            (
                'format',
                '[ file_format ]',
                'Obrigatório. Especifica o formato de arquivo da saída. Os valores possíveis são ORC, PARQUET, AVRO, JSON ou TEXTFILE',
            ),
            (
                'compression',
                '[ zlib, gzip ]',
                'Opcional. Essa opção é específica aos formatos ORC e Parquet. Para ORC, o padrão é zlib, e para Parquet, o padrão é gzip.',
            ),
            (
                'compression_level',
                '3',
                'O nível de compressão a ser usado. Essa propriedade se aplica apenas à compressão ZSTD. Os valores possíveis são de 1 a 22. O valor padrão é 3.',
            ),
            (
                'partitioned_by',
                'ARRAY[ col_name[,…] ]',
                'Opcional. Uma lista matriz de colunas pela qual a saída é particionada.',
            ),
        ]

        tbl = create_table_rich(
            'Propriedades [b green]UNLOAD[/b green] cláusula [b red]WITH[/b red]:',
            rows,
        )
        terminal.print(tbl)


@app.command(
    help='Consultar metadados de tabelas do Iceberg',
    short_help='Consultar metadados tabelas Iceberg',
    name='iceberg-metadados',
)
def iceberg_metadados():
    mark = """
# ICEBERG - METADADOS

Em uma consulta `SELECT`, é possível usar as seguintes propriedades após **table_name** para consultar metadados de tabela do Iceberg:

- **$files**: mostra os arquivos de dados atuais de uma tabela.
- **$manifests**: mostra os manifestos do arquivo atual de uma tabela.
- **$history**: mostra o histórico de uma tabela.
- **$partitions**: mostra as partições atuais de uma tabela.
- **$snapshots**: mostra os snapshots de uma tabela.
- **$refs**: mostra as referências de uma tabela.

**Exemplo básico:**

A instrução a seguir lista os arquivos de uma tabela do Iceberg.

```sql
SELECT * FROM "dbname"."tablename$files"
```

A instrução a seguir lista os manifestos de uma tabela do Iceberg.

```sql
SELECT * FROM "dbname"."tablename$manifests" 
```

A instrução a seguir mostra o histórico de uma tabela do Iceberg.

```sql
SELECT * FROM "dbname"."tablename$history"
```

"""
    terminal.print(Markdown(mark))


@app.command(
    help='Comando para mesclar dados em uma tabela do Iceberg',
    short_help='Mesclar dados em uma tabela do Iceberg',
    name='merge-into',
)
def merge_into():
    mark = """
# MERGE INTO

Atualiza, exclui ou insere linhas de forma condicional em uma tabela do Apache Iceberg. 
Uma única instrução pode combinar ações de atualização, exclusão e inserção.

**Sinopse:**

```sql
MERGE INTO target_table [ [ AS ]  target_alias ]
USING { source_table | query } [ [ AS ] source_alias ]
ON search_condition
when_clause [...]
```

A `when_clause` corresponde a uma das seguintes:

- **WHEN MATCHED**: A linha correspondente foi encontrada na tabela de destino.
- **WHEN NOT MATCHED**: A linha correspondente não foi encontrada na tabela de destino.

```sql
WHEN MATCHED [ AND condition ]
    THEN DELETE
```

```sql
WHEN MATCHED [ AND condition ]
    THEN UPDATE SET ( column = expression [, ...] )
```

```sql
WHEN NOT MATCHED [ AND condition ]
    THEN INSERT (column_name[, column_name ...]) VALUES (expression, ...)
```

**Exemplo básico:**

O exemplo a seguir atualiza a tabela de destino `t` com as informações do cliente presentes na tabela de origem `s`. 
Para linhas de clientes na tabela `t` que têm linhas de clientes correspondentes na tabela `s`, o exemplo incrementa as aquisições na tabela `t`. 
Se a tabela `t` não corresponder a uma linha de cliente na tabela `s`, o exemplo irá inserir a linha de cliente da tabela `s` na tabela `t` .

```sql
MERGE INTO accounts t USING monthly_accounts_update s
ON (t.customer = s.customer)
WHEN MATCHED
    THEN UPDATE SET purchases = s.purchases + t.purchases
WHEN NOT MATCHED
    THEN INSERT (customer, purchases, address)
        VALUES(s.customer, s.purchases, s.address)
```
"""

    mostrar_paginas(mark, linhas_por_pagina=30)
