from athena_mvsh.cursores.cursores import CursorBaseParquet
import duckdb
import pyarrow as pa
from contextlib import contextmanager
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from functools import partial
from athena_mvsh.error import DatabaseError, ProgrammingError
from athena_mvsh.utils import parse_output_location, query_is_ddl
import uuid
from athena_mvsh.converter import (
    map_convert_df_athena,
    map_convert_duckdb_athena,
    partition_func_iceberg,
    map_convert_duckdb_athena_pandas_arrow,
)
import logging
from pathlib import Path
from typing import Literal


logger = logging.getLogger(__name__)

"""@Experimental
"""


class CursorParquetDuckdb(CursorBaseParquet):
    def __init__(
        self,
        s3_staging_dir: str,
        schema_name: str = None,
        catalog_name: str = None,
        poll_interval: float = 1,
        result_reuse_enable: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(
            s3_staging_dir,
            schema_name,
            catalog_name,
            poll_interval,
            result_reuse_enable,
            *args,
            **kwargs,
        )

    @contextmanager
    def __connect_duckdb(self, database: str = 'db.duckdb'):
        self.home_duckdb = 'duckdb_home'
        os.makedirs(self.home_duckdb, exist_ok=True)

        try:
            config = {
                'preserve_insertion_order': False,
                'threads': (os.cpu_count() or 1) * 5,
            }
            con = duckdb.connect(database, config=config)

            # diretorio de extensoes
            if os.path.isdir(self.home_duckdb):
                con.sql(f"SET home_directory='{self.home_duckdb}'")

            con.install_extension('httpfs')
            con.load_extension('httpfs')

            con.sql(f"""
                CREATE SECRET IF NOT EXISTS(
                   TYPE s3,
                   KEY_ID '{self.config['aws_access_key_id']}',
                   SECRET '{self.config['aws_secret_access_key']}',
                   REGION '{self.config['region_name']}'
            )
            """)
            yield con
        except:
            raise
        finally:
            con.close()

    def __read_duckdb(self):
        bucket_s3 = self.get_bucket_s3()
        *__, manifest = self.unload_location(bucket_s3)

        with self.__connect_duckdb() as con:
            view = con.read_parquet(manifest)
            while row := view.fetchone():
                yield row

    def __read_arrow(self):
        bucket_s3 = self.get_bucket_s3()
        *__, manifest = self.unload_location(bucket_s3)

        with self.__connect_duckdb() as con:
            view = con.read_parquet(manifest)
            return view.arrow()

    def __pre_execute(
        self, query: str, result_reuse_enable: bool = False, unload: bool = True
    ):
        if unload:
            query, __ = self.format_unload(query)

        id_exec = self.start_query_execution(query, result_reuse_enable)

        return id_exec

    def execute(self, query: str, result_reuse_enable: bool = False):
        unload = True
        if query_is_ddl(query):
            unload = False

        __ = self.__pre_execute(query, result_reuse_enable, unload=unload)

        try:
            yield from self.__read_duckdb()
        except Exception:
            return

    def to_arrow(self, query: str, result_reuse_enable: bool = False):
        __ = self.__pre_execute(query, result_reuse_enable)

        try:
            return self.__read_arrow()
        except Exception:
            return pa.Table.from_dict(dict())

    def to_parquet(
        self, query: str, result_reuse_enable: bool = False, *args, **kwargs
    ):
        __ = self.__pre_execute(query, result_reuse_enable)

        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb() as con:
                view = con.read_parquet(manifest)
                view.write_parquet(*args, **kwargs)

        except Exception:
            ...

    def to_csv(self, query: str, result_reuse_enable: bool = False, *args, **kwargs):
        __ = self.__pre_execute(query, result_reuse_enable)

        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb() as con:
                view = con.read_parquet(manifest)
                view.write_csv(*args, **kwargs)

        except Exception:
            ...

    def to_pandas(
        self, query: str, result_reuse_enable: bool = False, *args, **kwargs
    ) -> pd.DataFrame:
        __ = self.__pre_execute(query, result_reuse_enable)

        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb() as con:
                view = con.read_parquet(manifest)
                return view.df(*args, **kwargs)
        except Exception:
            return pd.DataFrame()

    def to_create_table_db(
        self,
        database: str,
        query: str,
        result_reuse_enable: bool = False,
        *args,
        **kwargs,
    ):
        __ = self.__pre_execute(query, result_reuse_enable)

        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb(database) as con:
                view = con.read_parquet(manifest)
                con.sql(f'DROP TABLE IF EXISTS {kwargs["table_name"]}')
                view.create(*args, **kwargs)

        except Exception:
            ...

    def to_partition_create_table_db(
        self,
        database: str,
        query: str,
        workers: int,
        result_reuse_enable: bool = False,
        *args,
        **kwargs,
    ):
        """Consulta com menor desempenho"""

        # NOTE: Inserir por arquivo
        def insert_part(con, file):
            cursor = con.cursor()
            cursor.sql(f"""
                INSERT INTO {kwargs['table_name']} 
                FROM read_parquet('{file}')
            """)
            return f'File read_insert: {os.path.basename(file)}'

        __ = self.__pre_execute(query, result_reuse_enable)

        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb(database) as con:
                start_file = manifest[0]
                view = con.read_parquet(start_file)
                con.sql(f'DROP TABLE IF EXISTS {kwargs["table_name"]}')
                view.create(*args, **kwargs)

                logger.info(f'Start create table: {kwargs["table_name"]}')

                # NOTE: Considerar arquivos apartir da segunda posicao
                rest_file = list(manifest[1:])

                logger.info(f'Length files: {len(rest_file)}')

                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures: list[Future] = []
                    fun_part = partial(insert_part, con)

                    for rst in rest_file:
                        futures.append(executor.submit(fun_part, rst))

                    for fut in as_completed(futures):
                        result = fut.result()

                        logger.info(result)

        except Exception:
            ...

    def to_insert_table_db(
        self, database: str, query: str, result_reuse_enable: bool = False, **kwargs
    ):
        # NOTE: Verifica se banco existe
        if not os.path.isfile(database):
            raise DatabaseError(f'Database {database} not exists !')

        # NOTE: Verifica se tabela existe
        with self.__connect_duckdb(database) as con:
            rst = con.execute(f"""
            select 1 from information_schema.tables
            where table_name = '{kwargs['table_name']}'
            """)

            ok = rst.fetchone()

            if not ok:
                raise DatabaseError(f'Table {kwargs["table_name"]} not exists !')

        __ = self.__pre_execute(query, result_reuse_enable)

        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb(database) as con:
                con.sql(f"""
                    INSERT INTO {kwargs['table_name']}
                    FROM read_parquet({manifest})
                """)

        except Exception:
            ...

    def __get_table_exists(self, catalog_name: str, schema: str, table_name: str):
        response_meta = self.get_table_metadata(
            catalog_name=catalog_name, database_name=schema, table_name=table_name
        )

        return response_meta

    def __delete_table(self, catalog_name: str, schema: str, table_name: str) -> None:
        response_meta = self.__get_table_exists(catalog_name, schema, table_name)

        if response_meta:
            # TODO: Deletar a tabela
            __ = self.__pre_execute(
                f"""
                DROP TABLE `{schema}`.`{table_name}`
                """,
                unload=False,
            )

            # TODO: Retornar bucket name
            location_table = response_meta['Parameters']['location']
            bucket_name, keys = parse_output_location(location_table)

            # TODO: Localizar e deletar o bucket associado a tabela
            bucket = self.get_bucket_resource(bucket_name)
            objects = bucket.objects.filter(Prefix=keys)

            if list(objects.limit(1)):
                objects.delete()

    def __create_table_external(
        self,
        schema: str,
        table_name: str,
        location: str,
        output: pd.DataFrame | list[str | Path] | str | Path | pa.Table,
        partitions: list[str] = None,
        compression: Literal['ZSTD', 'SNAPPY', 'GZIP'] = 'ZSTD',
    ):
        # NOTE: LER DATAFRAME DUCKDB ou PARQUET
        with self.__connect_duckdb() as db:
            s3_dir = f'{location}{uuid.uuid4()}/'
            s3_dir_file = f'{s3_dir}{uuid.uuid4()}.parquet'

            if isinstance(output, pd.DataFrame):
                cols_map = map_convert_df_athena(output)
            elif isinstance(output, pa.Table):
                cols_map = map_convert_duckdb_athena_pandas_arrow(db, output)

            else:
                # NOTE: Normaliza o caminho para o duckdb
                def normaliza_path(f):
                    return str(f).replace(os.sep, os.altsep)

                if isinstance(output, list):
                    output = list(map(normaliza_path, output))

                if isinstance(output, (str, Path)):
                    output = normaliza_path(output)

                cols_map = map_convert_duckdb_athena(db, output)

            parts_duck = parts_athena = ''

            if partitions:
                parts_duck = f"""
                , PARTITION_BY ({','.join(partitions)})'
                """

                parts_athena = f"""
                PARTITIONED BY (
                    {','.join([f'`{col}` {tipo}' for col, tipo in cols_map if col in partitions])}
                )
                """

            if isinstance(output, (pd.DataFrame, pa.Table)):
                db.sql(f"""
                COPY output 
                TO '{s3_dir if partitions else s3_dir_file}'
                (FORMAT PARQUET, COMPRESSION {compression}{parts_duck})
                """)
            else:
                db.sql(f"""
                COPY (from read_parquet({output!r})) 
                TO '{s3_dir if partitions else s3_dir_file}'
                (FORMAT PARQUET, COMPRESSION {compression}{parts_duck})
                """)

            if partitions is None:
                partitions = list()

            cols = ',\n'.join(
                [f'`{col}` {tipo}' for col, tipo in cols_map if col not in partitions]
            )

            stmt = f"""
                CREATE EXTERNAL TABLE `{schema}`.`{table_name}` (
                {cols}
                )
                {parts_athena}
                STORED AS PARQUET
                LOCATION '{s3_dir}'
                TBLPROPERTIES ('parquet.compress'='{compression}')
            """
            __ = self.__pre_execute(stmt, unload=False)

            if partitions:
                hive_parts = f"""MSCK REPAIR TABLE `{schema}`.`{table_name}`"""
                __ = self.__pre_execute(hive_parts, unload=False)

        return cols_map

    def __create_table_iceberg(
        self,
        schema: str,
        table_name: str,
        location: str,
        cols_map,
        partitions: list[str] = None,
        catalog_name: str = 'awsdatacatalog',
        compression: Literal['ZSTD', 'SNAPPY', 'GZIP'] = 'ZSTD',
        if_exists: Literal['replace', 'append'] = 'replace',
    ) -> None:
        if if_exists == 'replace':
            # NOTE: DELETAR SE EXISTIR
            self.__delete_table(catalog_name, schema, table_name)

            s3_dir = f'{location}{uuid.uuid4()}/'
            parts_athena = ''

            if partitions:
                parts_athena = f"""
                PARTITIONED BY (
                    {','.join(partition_func_iceberg(partitions))}
                )
                """

            if partitions is None:
                partitions = list()

            cols = ',\n'.join([f'`{col}` {tipo}' for col, tipo in cols_map])

            stmt = f"""
                CREATE TABLE `{schema}`.`{table_name}` (
                {cols}
                )
                {parts_athena}
                LOCATION '{s3_dir}'
                TBLPROPERTIES (
                    'table_type'='ICEBERG',
                    'format'='parquet',
                    'write_compression'='{compression}',
                    'optimize_rewrite_delete_file_threshold'='10'
                )
            """

            # TODO: Criar a tabela
            __ = self.__pre_execute(stmt, unload=False)

        stmt_insert = f"""
        INSERT INTO "{schema}"."{table_name}"
        SELECT * FROM "{schema}"."temp_{table_name}"
        """

        __ = self.__pre_execute(stmt_insert, unload=False)

        # TODO: Deletar tabela temporaria
        self.__delete_table(catalog_name, schema, f'temp_{table_name}')

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        location: str = None,
        partitions: list[str] = None,
        catalog_name: str = 'awsdatacatalog',
        compression: Literal['ZSTD', 'SNAPPY', 'GZIP'] = 'ZSTD',
    ) -> None:
        if not isinstance(df, pd.DataFrame):
            raise ProgrammingError("Parameter 'df' is not a dataframe |")

        if df.empty:
            raise ProgrammingError('Dataframe is empty |')

        if location:
            location = location if location.endswith('/') else location + '/'
        else:
            location = self.s3_staging_dir

        self.__delete_table(catalog_name, schema, table_name)

        # TODO: Criar tabela com o tipo correto
        self.__create_table_external(
            schema, table_name, location, df, partitions, compression
        )

    def write_arrow(
        self,
        tbl: pa.Table,
        table_name: str,
        schema: str,
        location: str = None,
        partitions: list[str] = None,
        catalog_name: str = 'awsdatacatalog',
        compression: Literal['ZSTD', 'SNAPPY', 'GZIP'] = 'ZSTD',
    ) -> None:
        if not isinstance(tbl, pa.Table):
            raise ProgrammingError("Parameter 'tbl' is not a Table Arrow |")

        if tbl.num_rows == 0:
            raise ProgrammingError('Table Arrow is empty |')

        if location:
            location = location if location.endswith('/') else location + '/'
        else:
            location = self.s3_staging_dir

        self.__delete_table(catalog_name, schema, table_name)

        self.__create_table_external(
            schema, table_name, location, tbl, partitions, compression
        )

    def write_parquet(
        self,
        file: list[str | Path] | str | Path,
        table_name: str,
        schema: str,
        location: str = None,
        partitions: list[str] = None,
        catalog_name: str = 'awsdatacatalog',
        compression: Literal['ZSTD', 'SNAPPY', 'GZIP'] = 'ZSTD',
    ) -> None:
        if location:
            location = location if location.endswith('/') else location + '/'
        else:
            location = self.s3_staging_dir

        # TODO: Verificar se tabela existe
        self.__delete_table(catalog_name, schema, table_name)

        # TODO: Criar tabela com o tipo correto
        self.__create_table_external(
            schema, table_name, location, file, partitions, compression
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
        # TODO: TABELA EXTERNA
        if location:
            location = location if location.endswith('/') else location + '/'
        else:
            location = self.s3_staging_dir

        temp_table_name = f'temp_{table_name}'

        self.__delete_table(catalog_name, schema, temp_table_name)

        cols_map = self.__create_table_external(schema, temp_table_name, location, data)

        # TODO: Tabela ICEBERG
        self.__create_table_iceberg(
            schema,
            table_name,
            location,
            cols_map,
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
        if location:
            location = location if location.endswith('/') else location + '/'
        else:
            location = self.s3_staging_dir

        temp_table_name = f'temp_{target_table}'

        self.__delete_table(catalog_name, schema, temp_table_name)

        cols_map = self.__create_table_external(
            schema, temp_table_name, location, source_data
        )

        # TODO: Criar a consulta do tipo MERGE
        cols = list(map(lambda col: f'"{col[0]}"', cols_map))
        target, source = alias
        update_cols = ', '.join(f'{col} = {source}.{col}' for col in cols)
        insert_cols = ', '.join(cols)
        values_cols = ', '.join(f'{source}.{col}' for col in cols)

        if delete_condition:
            merge_del = f"""
            WHEN MATCHED AND {delete_condition}
                THEN DELETE
            """

        if update_condition:
            conds_up = f'AND {update_condition}'

        if insert_condition:
            conds_ins = f'AND {insert_condition}'

        stmt = f"""
            MERGE INTO "{schema}"."{target_table}" AS {target}
            USING "{schema}"."{temp_table_name}" AS {source}
            ON ({predicate})
            {merge_del if delete_condition else ''}
            WHEN MATCHED {conds_up if update_condition else ''}
               THEN UPDATE SET {update_cols}
            WHEN NOT MATCHED {conds_ins if insert_condition else ''}
               THEN INSERT ({insert_cols}) VALUES ({values_cols})
        """

        # TODO: Executar consulta MERGE e deletar tabela temp
        __ = self.__pre_execute(stmt, unload=False)

        self.__delete_table(catalog_name, schema, temp_table_name)
