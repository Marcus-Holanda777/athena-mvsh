from cursores.cursores import CursorBaseParquet
import duckdb
import pyarrow as pa
from contextlib import contextmanager
import os


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
    def __connect_duckdb(self):
        try:
            config = {
                'preserve_insertion_order': False,
                'threads': os.cpu_count() * 5
            }
            con = duckdb.connect('db.duckdb', config=config)
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

    def __read_duckdb(self) -> pa.Table:

        """Experimental
        """

        bucket_s3 = self.get_bucket_s3()
        *__, manifest = self.unload_location(bucket_s3)

        with self.__connect_duckdb() as con:
            view = con.read_parquet(manifest)
            return view.arrow()
        
    def __pre_execute(
        self, 
        query: str, 
        result_reuse_enable: bool = False
    ):
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

        yield self.__read_duckdb()

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

        bucket_s3 = self.get_bucket_s3()
        *__, manifest = self.unload_location(bucket_s3)

        with self.__connect_duckdb() as con:
            view = con.read_parquet(manifest)
            view.write_parquet(*args, **kwargs)

    def to_create_table_db(
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

        bucket_s3 = self.get_bucket_s3()
        *__, manifest = self.unload_location(bucket_s3)

        with self.__connect_duckdb() as con:
            view = con.read_parquet(manifest)
            con.sql(f"DROP TABLE IF EXISTS {kwargs['table_name']}")
            view.create(*args, **kwargs)
            con.sql("DROP VIEW IF EXISTS view")