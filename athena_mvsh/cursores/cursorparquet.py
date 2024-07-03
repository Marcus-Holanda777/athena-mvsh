from cursores.cursores import CursorBaseParquet
import pyarrow.fs as fs
import pyarrow as pa
import pyarrow.parquet as pq


class CursorParquet(CursorBaseParquet):
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
    
    def get_filesystem_fs(self):
        return fs.S3FileSystem(
            access_key=self.config['aws_access_key_id'],
            secret_key=self.config['aws_secret_access_key'],
            region=self.config['region_name']
        )

    def __read_parquet(self) -> pa.Table:
        bucket_s3 = self.get_bucket_s3()
        bucket, key, __ = self.unload_location(bucket_s3)

        fs_s3 = self.get_filesystem_fs()

        dataset = pq.ParquetDataset(
            f"{bucket}/{key}",
            filesystem=fs_s3
        )

        return dataset.read(use_threads=True)
        
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

        yield self.__read_parquet()