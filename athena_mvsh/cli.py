import typer
from rich.markdown import Markdown
from rich.console import Console
from typing import Optional
from athena_mvsh import __version__, __author__, __appname__

app = typer.Typer()
terminal = Console()


def version(value: bool):
    if value:
        terminal.print(f'Version: {__version__}, {__author__}, {__appname__}')
        raise typer.Exit()


@app.command(
    help='Comando para criar tabelas no Athena.',
    short_help='Criar tabelas no Athena',
    name='create-table'
)
def create_table(
    version: Optional[bool] = typer.Option(
        None, '--version', '-v', callback=version, is_eager=True
    ),
):
    mark = """
### 🛠️ CREATE TABLE no Athena

O comando `CREATE TABLE` é **exclusivo para tabelas do tipo Iceberg** no Amazon Athena. 
Tabelas Iceberg são transacionais e oferecem recursos avançados como versionamento, 
atualizações incrementais, e evolução de schema.
Athena **não permite usar `CREATE TABLE` para formatos tradicionais** como CSV, JSON ou Parquet puro. 
Nestes casos, deve-se usar `CREATE EXTERNAL TABLE`.

**Sinpse:**

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


@app.command(
    help='Comando para criar tabelas no Athena a partir de uma consulta.',
    short_help='Criar tabelas no Athena a partir de uma consulta',
    name='create-table-as' 
)
def create_table_as(
    version: Optional[bool] = typer.Option(
        None, '--version', '-v', callback=version, is_eager=True
    ),
):
    mark = """
### 🛠️ CREATE TABLE AS SELECT (CTAS) no Athena

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


def main():
    app(prog_name=__appname__)
