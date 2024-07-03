from cursores.cursores import CursorBaseParquet
import duckdb
import pyarrow as pa


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

    def __read_duckdb(self) -> pa.Table:

        """Experimental
        """

        bucket_s3 = self.get_bucket_s3()
        *__, manifest = self.unload_location(bucket_s3)

        with duckdb.connect('db.duckdb') as con:
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

            view = con.read_parquet(manifest)
            return view.arrow()
    
    def execute(
        self, 
        query: str, 
        result_reuse_enable: bool = False
    ):
        query, __ = self.format_unload(query)

        id_exec = self.start_query_execution(
            query,
            result_reuse_enable
        )

        __ = self.pool(id_exec)

        yield self.__read_duckdb()