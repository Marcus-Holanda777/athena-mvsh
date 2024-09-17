from athena_mvsh.cursores.cursores import CursorBaseParquet
import duckdb
import pyarrow as pa
from contextlib import contextmanager
import os
import pandas as pd
from concurrent.futures import (
    ThreadPoolExecutor, 
    as_completed,
    Future
)
from functools import partial
from athena_mvsh.error import (
    DatabaseError, 
    ProgrammingError
)
from athena_mvsh.utils import (
    parse_output_location,
    query_is_ddl
)
import uuid
from athena_mvsh.converter import (
    map_convert_df_athena,
    map_convert_duckdb_athena
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
        **kwargs
    ) -> None:
        
        super().__init__(
            s3_staging_dir,
            schema_name, 
            catalog_name, 
            poll_interval, 
            result_reuse_enable, 
            *args, 
            **kwargs
        )
   
    @contextmanager
    def __connect_duckdb(
        self, 
        database: str = 'db.duckdb'
    ):
        try:
            config = {
                'preserve_insertion_order': False,
                'threads': (os.cpu_count() or 1) * 5
            }
            con = duckdb.connect(database, config=config)
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
        self, 
        query: str, 
        result_reuse_enable: bool = False,
        unload: bool = True
    ):
        if unload:
            query, __ = self.format_unload(query)

        id_exec = self.start_query_execution(
            query,
            result_reuse_enable
        )

        return id_exec
    
    def execute(
        self, 
        query: str, 
        result_reuse_enable: bool = False
    ):
        unload = True
        if query_is_ddl(query):
            unload = False

        __ = self.__pre_execute(
            query,
            result_reuse_enable,
            unload=unload
        )

        try:
            yield from self.__read_duckdb()
        except Exception:
            return
        
    def to_arrow(
        self,
        query: str, 
        result_reuse_enable: bool = False
    ):
        __ = self.__pre_execute(
            query,
            result_reuse_enable
        )
        
        try:
            return self.__read_arrow()
        except Exception:
            return pa.Table.from_dict(dict())

    def to_parquet(
        self,
        query: str,
        result_reuse_enable: bool = False,
        *args, 
        **kwargs
    ):
        __ = self.__pre_execute(
            query,
            result_reuse_enable
        )
        
        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb() as con:
                view = con.read_parquet(manifest)
                view.write_parquet(*args, **kwargs)

        except Exception as e:
            ...
    
    def to_csv(
        self,
        query: str,
        result_reuse_enable: bool = False,
        *args, 
        **kwargs
    ):
        __ = self.__pre_execute(
            query,
            result_reuse_enable
        )
        
        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb() as con:
                view = con.read_parquet(manifest)
                view.write_csv(*args, **kwargs)

        except Exception as e:
            ...

    def to_pandas(
        self,
        query: str, 
        result_reuse_enable: bool = False, 
        *args,
        **kwargs
    ) -> pd.DataFrame:
        
        __ = self.__pre_execute(
            query,
            result_reuse_enable
        )
        
        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb() as con:
                view = con.read_parquet(manifest)
                return view.df(*args, **kwargs)
        except Exception as e:
            return pd.DataFrame()

    def to_create_table_db(
        self,
        database: str,
        query: str,
        result_reuse_enable: bool = False, 
        *args, 
        **kwargs
    ):
        __ = self.__pre_execute(
            query,
            result_reuse_enable
        )
        
        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb(database) as con:
                view = con.read_parquet(manifest)
                con.sql(f"DROP TABLE IF EXISTS {kwargs['table_name']}")
                view.create(*args, **kwargs)

        except Exception as e:
            ...

    def to_partition_create_table_db(
        self,
        database: str,
        query: str, 
        workers: int,
        result_reuse_enable: bool = False,
        *args, 
        **kwargs
    ):
        """Consulta com menor desempenho
        """
        # NOTE: Inserir por arquivo
        def insert_part(con, file):
            cursor = con.cursor()
            cursor.sql(f"""
                INSERT INTO {kwargs['table_name']} 
                FROM read_parquet('{file}')
            """
            )
            return f'File read_insert: {os.path.basename(file)}'

        __ = self.__pre_execute(
            query,
            result_reuse_enable
        )
        
        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb(database) as con:
                start_file = manifest[0]
                view = con.read_parquet(start_file)
                con.sql(f"DROP TABLE IF EXISTS {kwargs['table_name']}")
                view.create(*args, **kwargs)

                logger.info(f"Start create table: {kwargs['table_name']}")
                
                # NOTE: Considerar arquivos apartir da segunda posicao
                rest_file = list(manifest[1:])
                
                logger.info(f"Length files: {len(rest_file)}")

                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures: list[Future] = []
                    fun_part = partial(insert_part, con)

                    for rst in rest_file:
                        futures.append(executor.submit(fun_part, rst))
                    
                    for fut in as_completed(futures):
                        result = fut.result()
                        
                        logger.info(result)

        except Exception as e:
            ...
        
    def to_insert_table_db(
        self,
        database: str,
        query: str,
        result_reuse_enable: bool = False,
        **kwargs
    ):
        # NOTE: Verifica se banco existe
        if not os.path.isfile(database):
            raise DatabaseError(f"Database {database} not exists !")
        
        # NOTE: Verifica se tabela existe
        with self.__connect_duckdb(database) as con:
            rst = con.execute(f"""
            select 1 from information_schema.tables
            where table_name = '{kwargs['table_name']}'
            """)
            
            ok = rst.fetchone()

            if not ok:
                raise DatabaseError(
                    f"Table {kwargs['table_name']} not exists !"
                )

        __ = self.__pre_execute(
            query,
            result_reuse_enable
        )
        
        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb(database) as con:
                con.sql(f"""
                    INSERT INTO {kwargs['table_name']}
                    FROM read_parquet({manifest})
                """)

        except Exception as e:
            ...

    def __get_table_exists(
        self,
        catalog_name: str,
        schema: str,
        table_name: str
    ):
        
        response_meta = self.get_table_metadata(
            catalog_name=catalog_name,
            database_name=schema,
            table_name=table_name
        )

        return response_meta

    
    def __delete_table(
        self, 
        catalog_name: str,
        schema: str, 
        table_name: str
    ) -> None:
        
        response_meta = self.__get_table_exists(
            catalog_name,
            schema,
            table_name
        )

        if response_meta:
            # TODO: Deletar a tabela
            __ = self.__pre_execute(
                f"""
                DROP TABLE `{schema}`.`{table_name}`
                """,
                unload=False
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
        output: pd.DataFrame | str | Path,
        partitions: list[str] = None,
        compression: str = 'GZIP'
    ):

        # NOTE: LER DATAFRAME DUCKDB ou PARQUET
        with self.__connect_duckdb() as db:
            s3_dir = f"{location}{uuid.uuid4()}/"
            s3_dir_file = f'{s3_dir}{uuid.uuid4()}.parquet.gzip'
            
            if isinstance(output, pd.DataFrame):
               cols_map = map_convert_df_athena(output)
            else:
                cols_map = map_convert_duckdb_athena(db, output)

            parts_duck = parts_athena = ''

            if partitions:
                parts_duck = f"""
                , PARTITION_BY ({','.join(partitions)}), FILE_EXTENSION 'parquet.gz'
                """

                parts_athena = f"""
                PARTITIONED BY (
                    {",".join([f"`{col}` {tipo}" for col, tipo in cols_map if col in partitions])}
                )
                """
            
            if isinstance(output, pd.DataFrame):
                db.sql(f"""
                COPY output 
                TO '{s3_dir if partitions else s3_dir_file}'
                (FORMAT PARQUET, COMPRESSION {compression}{parts_duck})
                """)
            else:
                db.sql(f"""
                COPY (from read_parquet('{str(output)}')) 
                TO '{s3_dir if partitions else s3_dir_file}'
                (FORMAT PARQUET, COMPRESSION {compression}{parts_duck})
                """)
            
            if partitions is None:
                partitions = list()

            cols = ',\n'.join(
                [
                    f"`{col}` {tipo}" 
                    for col, tipo in cols_map
                    if col not in partitions
                ]
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

            # TODO: Criar a tabela
            __ = self.__pre_execute(stmt, unload=False)
            
            # NOTE: O duckdb só particiona os dados no
            # estilo HIVE, como as particoes ja existem antes de criar a tabela
            # é preciso realizar um reparo
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
        compression: str = 'snappy',
        if_exists: Literal['replace', 'append'] = 'replace'
    ) -> None:
        
        if if_exists == 'replace':
            # NOTE: DELETAR SE EXISTIR
            self.__delete_table(
                catalog_name,
                schema,
                table_name
            )

            s3_dir = f"{location}{uuid.uuid4()}/"
            parts_athena = ''

            if partitions:
                parts_athena = f"""
                PARTITIONED BY (
                    {",".join([f"`{col}` {tipo}" for col, tipo in cols_map if col in partitions])}
                )
                """
                
            if partitions is None:
                partitions = list()

            # TODO: aterar tipos do athena -> iceberg
            for p, (col, tipo) in enumerate(cols_map):
                if tipo in ('INTEGER', 'TINYINT', 'SMALLINT'):
                    cols_map[p] = (col, 'INT')

                elif tipo == 'BIGINT':
                    cols_map[p] = (col, 'LONG')
                
                elif tipo == 'ARRAY':
                    cols_map[p] = (col, 'LIST')

                else:
                    cols_map[p] = (col, tipo)
                
            cols = ',\n'.join(
                [
                    f"`{col}` {tipo}"
                    for col, tipo in cols_map
                    if col not in partitions
                ]
            )

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
        self.__delete_table(
            catalog_name,
            schema,
            f'temp_{table_name}'
        )

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        location: str = None,
        partitions: list[str] = None,
        catalog_name: str = 'awsdatacatalog',
        compression: str = 'GZIP'
    ) -> None:
        
        if not isinstance(df, pd.DataFrame):
            raise ProgrammingError("Parameter 'df' is not a dataframe |")
        
        if df.empty:
            raise ProgrammingError("Dataframe is empty |")

        if location:
            location = (
                location if location.endswith('/')
                else 
                location + '/'
            )
        else:
            location = self.s3_staging_dir
        
        
        self.__delete_table(
            catalog_name,
            schema, 
            table_name
        )
        
        # TODO: Criar tabela com o tipo correto
        self.__create_table_external(
            schema, 
            table_name, 
            location,
            df,
            partitions,
            compression
        )

    def write_parquet(
        self,
        file: str | Path,
        table_name: str,
        schema: str,
        location: str = None,
        partitions: list[str] = None,
        catalog_name: str = 'awsdatacatalog',
        compression: str = 'GZIP'
    ) -> None:

        if location:
            location = (
                location if location.endswith('/')
                else 
                location + '/'
            )
        else:
            location = self.s3_staging_dir
        
        # TODO: Verificar se tabela existe
        self.__delete_table(
            catalog_name,
            schema,
            table_name
        )

        # TODO: Criar tabela com o tipo correto
        self.__create_table_external(
            schema,
            table_name,
            location,
            file,
            partitions,
            compression
        )
    

    def write_table_iceberg(
        self,
        file: str | Path,
        table_name: str,
        schema: str,
        location: str = None,
        partitions: list[str] = None,
        catalog_name: str = 'awsdatacatalog',
        compression: str = 'snappy',
        if_exists: Literal['replace', 'append'] = 'replace'
    ) -> None:
        
        """
        Criar uma tabela externa temporaria, que vai armazenar os dados do parquet
        Criar TABELA ICEBERG DO MESMO TIPO
        """
        
        # TODO: TABELA EXTERNA
        if location:
            location = (
                location if location.endswith('/')
                else 
                location + '/'
            )
        else:
            location = self.s3_staging_dir
        
        temp_table_name = f'temp_{table_name}'

        self.__delete_table(
            catalog_name,
            schema,
            temp_table_name
        )
        
        cols_map = self.__create_table_external(
            schema,
            temp_table_name,
            location,
            file,
            partitions,
            compression
        )

        # TODO: Tabela ICEBERG
        self.__create_table_iceberg(
            schema,
            table_name,
            location,
            cols_map,
            partitions,
            catalog_name,
            compression,
            if_exists
        )
