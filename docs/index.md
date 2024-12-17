# Athena-MVSH

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

## Exemplo de Uso

A seguir, um exemplo de como utilizar a biblioteca **athena-mvsh** para executar uma consulta e manipular os resultados:

### 1. **Executando uma Consulta e Recuperando Resultados**

```python
from athena_mvsh import Athena, CursorPython  # ou CursorParquet, ou CursorParquetDuckdb

# Criando um cursor Athena
cursor = CursorPython(...)

# Usando o cursor dentro de um contexto
with Athena(cursor) as athena:
    # Executando a consulta SQL
    athena.execute("SELECT * FROM sales_data WHERE region = 'US'")

    # Usando fetchone() para obter uma linha
    row = athena.fetchone()
    print(row)  # Exemplo: ('US', 1000, '2024-01-01')

    # Usando fetchall() para obter todas as linhas
    rows = athena.fetchall()
    print(rows)  # Exemplo: [('US', 1000, '2024-01-01'), ('US', 1500, '2024-02-01')]

    # Usando fetchmany() para obter um número específico de linhas
    rows = athena.fetchmany(5)
    print(rows)  # Exemplo: [('US', 1000, '2024-01-01'), ('US', 1500, '2024-02-01')]

    # Convertendo os resultados para um Pandas DataFrame
    df = athena.to_pandas()
    print(df)
    # Exemplo de saída:
    #    region  sales     date
    # 0     US  1000  2024-01-01
    # 1     US  1500  2024-02-01
```