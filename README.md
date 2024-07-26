# Athena-mvsh

## O que é o Athena-mvsh ?

Athena-mvsh é um biblioteca python, que interage com o serviço `Amazon Athena`, que é um serviço de consulta interativa que permite usar SQL para analisar dados diretamente no Amazon S3.

Algumas regras da [DB API 2.0 (PEP 249)](https://www.python.org/dev/peps/pep-0249/) são implemetadas.

## Instalação

```bash
pip install "git+https://github.com/Marcus-Holanda777/athena-mvsh.git"
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
    s3_staging_dir='s3:/caminho-saida-consulta/',
    aws_access_key_id='KEY_ID',
    aws_secret_access_key='SECRET_KEY',
    region_name='us-east-1'
)

with Athena(cursor=cursor) as cliente:
    cliente.execute("SELECT 1")
    rst = cliente.fetchone()
```

## Como usar ?

Esta biblioteca Python facilita a consulta ao Amazon Athena, oferecendo suporte para três tipos de cursores: Python, Parquet e DuckDB. Com esta biblioteca, você pode executar consultas SQL no Amazon Athena e obter os resultados no formato desejado de maneira fácil e eficiente.

Funcionalidades
Consulta ao Amazon Athena: Execute consultas SQL no Amazon Athena.

Cursores Diversos:
- `CursorPython`: Retorna os resultados da consulta como objetos Python.
- `CursorParquet`: Retorna os resultados da consulta no formato Parquet.
- `CursorParquetDuckdb`: Retorna os resultados da consulta integrados ao DuckDB.