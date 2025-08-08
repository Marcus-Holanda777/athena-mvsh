import typer
from textual.app import App, ComposeResult
from textual.widgets import MarkdownViewer, Footer

app = typer.Typer()


DOC_MARKDOWN = """\
# CREATE TABLE

O comando `CREATE TABLE` é **exclusivo para tabelas do tipo Iceberg** no Amazon Athena.
Tabelas Iceberg são transacionais e oferecem recursos avançados como versionamento, 
atualizações incrementais, e evolução de schema.
Athena **não permite usar `CREATE TABLE` para formatos tradicionais** como CSV, JSON ou Parquet puro. 
Nestes casos, deve-se usar `CREATE EXTERNAL TABLE`.

## Sinopse

```sql
CREATE TABLE
  [db_name.]table_name (col_name data_type [COMMENT col_comment] [, ...] )
  [PARTITIONED BY (col_name | transform, ... )]
  LOCATION 's3://amzn-s3-demo-bucket/your-folder/'
  TBLPROPERTIES ( 'table_type' ='ICEBERG' [, property_name=property_value] )
```

## Exemplo

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

## Propriedades da tabela

| Propriedade     | Valor Padrao   | Descricao |
| --------------- | -------------- | --------- |
| format | parquet | Formato de dados do arquivo. |
| write_compression' | 'snappy' | Codec de compactação de arquivo. |
| optimize_rewrite_data_file_threshold | 5 | Ignora reescrita se poucos arquivos de dados precisarem de otimização, reduzindo custo computacional. |
| optimize_rewrite_delete_file_threshold | 2 | Ignora reescrita se poucos arquivos de exclusão estiverem associados, acumulando mais antes de otimizar. |
| vacuum_min_snapshots_to_keep | 1 | Número mínimo de snapshots a serem retidos na ramificação principal. |
| vacuum_max_snapshot_age_seconds | 432 mil segundos | Período máximo para reter os snapshots na ramificação principal. |
| vacuum_max_metadata_files_to_keep | 100 | Número máximo de arquivos de metadados anteriores a serem retidos na ramificação principal. |

---

# CREATE TABLE AS SELECT (CTAS)

O comando `CREATE TABLE AS SELECT` (CTAS) permite criar uma nova tabela no Amazon Athena.
Com base no resultado de uma consulta `SELECT`. 
Ele é útil para transformar e armazenar dados em um novo formato ou particionamento, 
além de otimizar consultas futuras.

## Sinopse

```sql
CREATE TABLE table_name
[ WITH ( property_name = expression [, ...] ) ]
AS query
[ WITH [ NO ] DATA ]
```

## Exemplo

Cria uma tabela do tipo `ICEBERG` a partir do resultado de uma consulta `SELECT`:

```sql
CREATE TABLE table_iceberg WITH (
  table_type = 'ICEBERG'
  is_external = False,
  location ='s3://amzn-s3-demo-bucket/tables/iceberg_table/',
  format = 'PARQUET',
  write_compression = 'ZSTD',
  partitioning = ARRAY['month(order_date)', 'country']
) AS 
SELECT 
  * 
FROM 
  table_name
)
```

## Propriedades do CTAS

| Propriedade     | Valor Padrao   | Descricao |
| --------------- | -------------- | --------- |
| table_type | hive | Opcional. O padrão é `HIVE` (outra opção `ICEBERG`). Especifica o tipo de tabela da tabela resultante. |
| external_location | s3://amzn-s3-demo-bucket/ | Opcional. O local no qual o Athena salvará sua consulta CTAS no Amazon S3. |
| is_external | true | Opcional. Indica se a tabela corresponde a uma tabela externa. O padrão é `true`. Para tabelas do Iceberg, deve ser definido como `false`. |
| location | s3://amzn-s3-demo-bucket/ | Obrigatório para tabelas do Iceberg. Especifica o local raiz da tabela do Iceberg que será criada a partir dos resultados da consulta. |
| field_delimiter | `\\001` | Delimitador de campo para arquivos de texto (como CSV) deve ser um único caractere; se não for especificado, o padrão é `\\001`. Delimitadores com vários caracteres não são permitidos em CTAS. |
| format | parquet | O formato de armazenamento dos resultados de consultas CTAS, como ORC, PARQUET, AVRO, JSON, ION ou TEXTFILE. Para tabelas do Iceberg, os formatos permitidos são ORC, PARQUET e AVRO. |
| write_compression | snappy | Codec de compactação de arquivo. Os valores válidos são `snappy`, `zstd`, `gzip`, `lzo` e `none`. O padrão é `snappy`. Para tabelas do Iceberg, os formatos permitidos são ORC, PARQUET e AVRO. |
| compression_level | 3 | O nível de compressão a ser usado. Essa propriedade se aplica apenas à compressão ZSTD. Os valores possíveis são de 1 a 22. O valor padrão é 3. |
| bucketed_by | ARRAY[ column_name[,…] ] | Uma lista matriz de buckets para dados do bucket. Se omitida, o Athena não armazenará os dados dessa consulta em bucket. Essa propriedade não se aplica para tabelas do Iceberg. |
| bucket_count | [ int ] | O número de buckets para armazenar seus dados em um bucket. Se omitido, o Athena não armazenará os dados em bucket. Essa propriedade não se aplica para tabelas do Iceberg. |
| partitioned_by | ARRAY[ col_name[,…] ] | Opcional. Uma lista matriz de colunas pela qual a tabela CTAS será particionada. Essa propriedade não se aplica para tabelas do Iceberg. |
| partitioning | ARRAY[ partition_transform, ... ] | Opcional. Especifica o particionamento da tabela do Iceberg que será criada. |
| optimize_rewrite_min_data_file_size_bytes | [ long ] | Opcional. Arquivos menores que o valor especificado são incluídos para otimização. Essa propriedade se aplica apenas a tabelas do Iceberg. |
| optimize_rewrite_max_data_file_size_bytes | [ long ] | Opcional. Arquivos maiores que o valor especificado são incluídos para otimização. Essa propriedade se aplica apenas a tabelas do Iceberg. |
| optimize_rewrite_data_file_threshold | 5 | Ignora reescrita se poucos arquivos de dados precisarem de otimização, reduzindo custo computacional. Essa propriedade se aplica apenas a tabelas do Iceberg. |
| optimize_rewrite_delete_file_threshold | 2 | Ignora reescrita se poucos arquivos de exclusão estiverem associados, acumulando mais antes de otimizar. Essa propriedade se aplica apenas a tabelas do Iceberg. |
| vacuum_min_snapshots_to_keep | 1 | Número mínimo de snapshots a serem retidos na ramificação principal de uma tabela. Essa propriedade se aplica apenas a tabelas do Iceberg. |
| vacuum_max_snapshot_age_seconds | 432 mil segundos | Período máximo para reter os snapshots na ramificação principal de uma tabela. Essa propriedade se aplica apenas a tabelas do Iceberg. |

---

# OPTIMIZE

Reescreve arquivos pequenos em blocos maiores para acelerar as consultas.

## Sinopse

```sql
OPTIMIZE [db_name.]table_name REWRITE DATA USING BIN_PACK
  [WHERE predicate]
```

## Exemplo

```sql
OPTIMIZE iceberg_table REWRITE DATA USING BIN_PACK
  WHERE category = 'c1'
```

---

# VACUUM

Remove arquivos não utilizados (como dados sobrescritos ou snapshots antigos).

## Sinopse

```sql
VACUUM [database_name.]target_table
```

## Exemplo

```sql
VACUUM iceberg_table
```

---

# UNLOAD

O comando UNLOAD é usado para exportar os resultados de uma consulta SQL diretamente para um arquivo no Amazon S3, 
geralmente no formato Parquet ou CSV. Ele é ideal para mover dados de forma eficiente do Athena para uma área de data lake.

## Sinopse

```sql
UNLOAD (SELECT col_name[, ...] FROM old_table) 
TO 's3://amzn-s3-demo-bucket/my_folder/' 
WITH ( property_name = 'expression' [, ...] )
```

## Exemplo

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

## Propriedades do UNLOAD

| Propriedade     | Valor Padrao   | Descricao |
| --------------- | -------------- | --------- |
| field_delimiter | `\\001` | Opcional. Delimitador de campo para arquivos de texto (como CSV) deve ser um único caractere; se não for especificado, o padrão é `\\001`. Delimitadores com vários caracteres não são permitidos. |
| format | parquet | Obrigatório. Especifica o formato de arquivo da saída. Os valores possíveis são ORC, PARQUET, AVRO, JSON ou TEXTFILE. |
| compression | [zlib, gzip] | Opcional. Essa opção é específica aos formatos ORC e Parquet. Para ORC, o padrão é zlib, e para Parquet, o padrão é gzip. |
| compression_level | 3 | Opcional. O nível de compressão a ser usado. Essa propriedade se aplica apenas à compressão ZSTD. Os valores possíveis são de 1 a 22. O valor padrão é 3. |
| partitioned_by | ARRAY[ col_name[,…] ] | Opcional. Uma lista matriz de colunas pela qual a saída é particionada. |

---

# MERGE INTO

Atualiza, exclui ou insere linhas de forma condicional em uma tabela do Apache Iceberg. 
Uma única instrução pode combinar ações de atualização, exclusão e inserção.

## Sinopse

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

## Exemplo

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

---

# ICEBERG - METADADOS

Em uma consulta `SELECT`, é possível usar as seguintes propriedades após *`table_name`* para consultar metadados de tabela do Iceberg:

- *`$files`*: mostra os arquivos de dados atuais de uma tabela.
- *`$manifests`*: mostra os manifestos do arquivo atual de uma tabela.
- *`$history`*: mostra o histórico de uma tabela.
- *`$partitions`*: mostra as partições atuais de uma tabela.
- *`$snapshots`*: mostra os snapshots de uma tabela.
- *`$refs`*: mostra as referências de uma tabela.

## Exemplo

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

---

"""


class MarkdownExampleApp(App):
    BINDINGS = [('ctrl+q', 'quit', 'SAIR'), ('d', 'toggle_dark', 'TEMA')]

    def compose(self) -> ComposeResult:
        markdown_viewer = MarkdownViewer(DOC_MARKDOWN, show_table_of_contents=True)
        markdown_viewer.code_indent_guides = False
        yield markdown_viewer
        yield Footer()


@app.command(
    name="doc",
    help="Exibe a documentação de comandos SQL do Athena em um terminal interativo.",
    short_help="Documentação de comandos SQL do Athena"
)
def doc():
    app = MarkdownExampleApp()
    app.run()
