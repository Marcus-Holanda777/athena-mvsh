# Athena-MVSH
[![PyPI](https://img.shields.io/pypi/v/athena-mvsh.svg)](https://pypi.org/project/athena-mvsh/)

A **athena-mvsh** é uma biblioteca Python projetada para facilitar a execução de consultas no **AWS Athena** com suporte ao uso de **pyarrow** e **DuckDB** para resultados e execução de consultas. A biblioteca oferece uma interface intuitiva, integrando com o Athena e outros formatos de dados, como Parquet, CSV, e Arrow, permitindo exportações e transformações de dados eficientes.

Além disso, **athena-mvsh** implementa partes da **DB API 2.0** (PEP 249), garantindo que a interação com o Athena seja simples e padronizada, seguindo as boas práticas de acesso a bancos de dados em Python.

## Características

- **Execução de consultas no Athena**: Execute consultas SQL no **AWS Athena** e manipule os resultados com facilidade.
- **Suporte a formatos de dados**: Converta os resultados para **Parquet**, **CSV**, **Arrow**, e **Pandas** DataFrames.
- **Conformidade com DB API 2.0 (PEP 249)**: A biblioteca segue várias convenções da DB API 2.0, como o uso de cursores, gerenciamento de exceções e execução de consultas.
- **Gerenciamento de Contexto**: Utilize a biblioteca com a sintaxe `with` para garantir a limpeza de recursos ao final de uma operação.

## Conformidade com DB API 2.0 (PEP 249)

A **athena-mvsh** foi projetada para implementar a interface padrão de acesso a bancos de dados em Python, seguindo as diretrizes da **DB API 2.0** (PEP 249). Abaixo estão alguns dos componentes implementados:

### 1. **Cursor**

O **Cursor** é responsável por interagir com o banco de dados, executar consultas e recuperar resultados. A biblioteca `athena-mvsh` implementa a classe `Athena`, que se comporta como um cursor:

- **Criação do Cursor**: O cursor é criado a partir de um objeto `DBAthena` (que pode ser um `CursorPython`, `CursorParquet`, ou `CursorParquetDuckdb`), e gerencia a execução de consultas no Athena.
- **Métodos principais**:
  - `execute(query, parameters=None)`: Executa uma consulta SQL.
  - `fetchone()`: Recupera a próxima linha de um resultado.
  - `fetchall()`: Recupera todas as linhas de um resultado.
  - `fetchmany(size)`: Recupera um número especificado de linhas.

### 2. **Execução de Consultas**

- **`execute(query, parameters=None)`**: Executa uma consulta SQL. Caso parâmetros sejam fornecidos, a biblioteca os passa de forma segura.
- **Consultas DML e DDL**: Suporte a consultas de manipulação de dados e definição de dados.

### 3. **Propriedades e Métodos do Cursor**

A `athena-mvsh` implementa métodos e propriedades como a `description` (descrição das colunas do resultado) e `rowcount` (número de linhas afetadas pela consulta), conforme esperado pela DB API 2.0:

- **`description`**: Retorna uma lista com informações sobre as colunas da consulta.
  ```python
  print(cursor.description)
  ```

- **`rowcount`**: Retorna o número de linhas afetadas pela consulta.
  ```python
  print(cursor.rowcount)
  ```

## Instalação

```bash
pip install athena-mvsh
```

## Credenciais

Para se conectar é preciso informar o `aws_access_key_id`, `aws_secret_access_key` o local de saida das consultas `s3_staging_dir` 
e a regiao do bucket `region_name`. Se você usar o arquivo de perfil padrão, não será necessário informar as credenciais.

```python
from athena_mvsh import (
    Athena,
    CursorPython
)

cursor = CursorPython(
    s3_staging_dir='s3://caminho-saida-consulta/',
    aws_access_key_id='KEY_ID',
    aws_secret_access_key='SECRET_KEY',
    region_name='us-east-1'
)
```

## Iteração do cursor

O cursor fornecido por esta biblioteca implementa o protocolo de iterator do Python, permitindo que você itere sobre os resultados da consulta linha por linha. Isso é útil para processar grandes conjuntos de dados sem precisar carregá-los completamente na memória.

```python

cursor = CursorPython(
  s3_staging_dir='s3://caminho-saida-consulta/'
)

with Athena(cursor) as athena:
    athena.execute("SELECT * FROM sales_data")
    for row in athena:
        print(row)
```

## Exemplo de Uso

A seguir, um exemplo de como utilizar a biblioteca **athena-mvsh** para executar uma consulta e manipular os resultados:

### 1. **Uso Básico**

```python
from athena_mvsh import Athena, CursorPython

cursor = CursorPython(
  s3_staging_dir='s3:/caminho-saida-consulta/'
)

with Athena(cursor) as athena:
    athena.execute("SELECT * FROM sales_data")
    print(athena.fetchall())
    print(athena.description)
    print(athena.rowcount)
```

### 2. **Reutilização de consultas**

O parâmetro `result_reuse_enable` é uma funcionalidade que habilita ou desabilita a reutilização de resultados de consultas previamente executadas no Amazon Athena. Essa abordagem reduz custos e melhora o desempenho ao aproveitar resultados armazenados em cache, desde que os dados subjacentes não tenham sido alterados.

```python
from athena_mvsh import Athena, CursorPython

cursor = CursorPython(
  s3_staging_dir='s3://caminho-saida-consulta/',
  result_reuse_enable=True
)

with Athena(cursor) as athena:
    athena.execute("SELECT * FROM sales_data")
    print(athena.fetchone())
```

## DuckDB consulta e transformação

A biblioteca suporta a execução de consultas no Amazon Athena utilizando o recurso UNLOAD para exportar os resultados diretamente em formato Parquet no Amazon S3. Em seguida, o DuckDB é utilizado para carregar e processar esses dados localmente, aproveitando as vantagens do formato Parquet para consultas analíticas rápidas e eficientes.

É possível criar tabelas externas e do tipo Iceberg diretamente a partir de DataFrames do pandas, Table Arrow ou arquivos Parquet. Essa funcionalidade permite que dados estruturados sejam facilmente integrados a ambientes analíticos, facilitando o uso em consultas SQL e outras operações analíticas

### Métodos disponíveis

- **to_arrow**: Converte os resultados para um formato Arrow.
- **to_pandas**: Converte os resultados para um DataFrame Pandas.
- **to_parquet**: Converte os resultados para o formato Parquet.
- **to_csv**: Converte os resultados para um arquivo CSV.
- **to_create_table_db**: Cria uma tabela no DuckDB usando os resultados.
- **to_partition_create_table_db**: Cria uma tabela no DuckDB inserindo os dados de forma incremental.
- **to_insert_table_db**: Insere dados em uma tabela do DuckDB.
- **write_dataframe**: Escreve um DataFrame em uma tabela externa no Athena.
- **write_arrow**: Escreve um Table Arrow em uma tabela externa no Athena.
- **write_parquet**: Escreve dados a partir de um ou mais arquivos `.parquet` em uma tabela externa no Athena.
- **write_table_iceberg**: Cria uma tabela do tipo `Iceberg` no Athena
- **merge_table_iceberg**: Realiza uma operação de merge em uma tabela `Iceberg`.

### 1. **Criando tabela no Duckdb a partir de uma consulta**

```python
from athena_mvsh import Athena, CursorParquetDuckdb

cursor = CursorParquetDuckdb(
    s3_staging_dir='s3://caminho-saida-consulta/'
)

with Athena(cursor) as athena:
    athena.execute("SELECT * FROM sales_data")
    athena.to_create_table_db('sales_table', database='db.duckdb')
```

### 2. **Criando tabela externa no Athena a partir de um DataFrame do pandas**

```python
import pandas as pd
from athena_mvsh import Athena, CursorParquetDuckdb

cursor = CursorParquetDuckdb(
    s3_staging_dir='s3://caminho-saida-consulta/'
)

df = pd.read_excel('caminho_plan.xlsx')

with Athena(cursor=cursor) as athena:
    athena.write_dataframe(
        df,
        schema='schema-tabela',
        table_name='nome_tabela',
        location=f's3://caminho-saida-tabela/tabela/nome_tabela/',
    )
```

### 3. **Criando tabela externa no Athena a partir de arquivos Parquet**

```python
import pandas as pd
from athena_mvsh import Athena, CursorParquetDuckdb

cursor = CursorParquetDuckdb(
    s3_staging_dir='s3://caminho-saida-consulta/'
)

with Athena(cursor=cursor) as athena:
    athena.write_parquet(
        'arquivo_parquet.parquet',
        schema='schema-tabela',
        table_name='nome_tabela',
        location=f's3://caminho-saida-tabela/tabela/nome_tabela/',
    )
```