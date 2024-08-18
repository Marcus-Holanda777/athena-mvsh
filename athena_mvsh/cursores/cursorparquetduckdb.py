from athena_mvsh.cursores.cursores import CursorBaseParquet
import duckdb
import pyarrow as pa
from contextlib import contextmanager
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from athena_mvsh.error import DatabaseError
from athena_mvsh.utils import parse_output_location
import uuid
from athena_mvsh.converter import map_convert_df_athena


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

        id_exec = self.__pre_execute(
            query,
            result_reuse_enable
        )

        __ = self.pool(id_exec)
        
        yield from self.__read_duckdb()

    def to_parquet(
        self,
        query: str, 
        result_reuse_enable: bool = False,
        *args, 
        **kwargs
    ):
        id_exec = self.__pre_execute(
            query,
            result_reuse_enable
        )
        __ = self.pool(id_exec)
        
        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb() as con:
                view = con.read_parquet(manifest)
                view.write_parquet(*args, **kwargs)

        except Exception as e:
            ...

    def to_pandas(
        self,
        query: str, 
        result_reuse_enable: bool = False, 
        *args,
        **kwargs
    ) -> pd.DataFrame:
        
        id_exec = self.__pre_execute(
            query,
            result_reuse_enable
        )
        __ = self.pool(id_exec)
        
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
        id_exec = self.__pre_execute(
            query,
            result_reuse_enable
        )
        __ = self.pool(id_exec)
        
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
            return 1

        id_exec = self.__pre_execute(
            query,
            result_reuse_enable
        )
        __ = self.pool(id_exec)
        
        try:
            bucket_s3 = self.get_bucket_s3()
            *__, manifest = self.unload_location(bucket_s3)

            with self.__connect_duckdb(database) as con:
                start_file = manifest[0]
                view = con.read_parquet(start_file)
                con.sql(f"DROP TABLE IF EXISTS {kwargs['table_name']}")
                view.create(*args, **kwargs)
                
                # NOTE: Considerar arquivos apartir da segunda posicao
                rest_file = list(manifest[1:])
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    ___ = executor.map(partial(insert_part, con), rest_file)

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

        id_exec = self.__pre_execute(
            query,
            result_reuse_enable
        )

        __ = self.pool(id_exec)
        
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
    

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        location: str = None,
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
        response_meta = self.get_table_metadata(
            catalog_name=catalog_name,
            database_name=schema,
            table_name=table_name
        )

        if response_meta:
            # TODO: Deletar a tabela
            id_exec = self.__pre_execute(
                f"""
                DROP TABLE `{schema}`.`{table_name}`
                """,
                unload=False
            )
            ___ = self.pool(id_exec)
            
            # TODO: Retornar bucket name
            location_table = response_meta['Parameters']['location']
            bucket_name, keys = parse_output_location(location_table) 

            # TODO: Localizar e deletar o bucket associado a tabela
            bucket = self.get_bucket_resource(bucket_name)
            objects = bucket.objects.filter(Prefix=keys)

            if list(objects.limit(1)):
                objects.delete()

        # NOTE: LER DATAFRAME COM DUCKDB
        with self.__connect_duckdb() as db:
            view = db.from_df(df)
            s3_dir = f"{location}{uuid.uuid4()}/"
            s3_dir_file = f'{s3_dir}{uuid.uuid4()}.parquet'

            db.sql("""
                CREATE OR REPLACE TABLE temp_tbl
                AS from view
            """
            )

            db.sql(f"""
              COPY temp_tbl 
              TO '{s3_dir_file}' (FORMAT PARQUET)   
            """)
            
            cols = ',\n'.join(
                [
                    f"`{col}` {tipo}" 
                    for col, tipo in map_convert_df_athena(df)
                ]
            )

            stmt = f"""
                CREATE EXTERNAL TABLE `{schema}`.`{table_name}` (
                {cols}
                )
                STORED AS PARQUET
                LOCATION '{s3_dir}'
                TBLPROPERTIES ('parquet.compress'='{compression}')
            """

            # TODO: Deletar a tabela
            id_exec = self.__pre_execute(stmt, unload=False)
            ___ = self.pool(id_exec)