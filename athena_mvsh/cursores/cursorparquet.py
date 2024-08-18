from athena_mvsh.cursores.cursores import CursorBaseParquet
import pyarrow.fs as fs
import pyarrow as pa
import pyarrow.parquet as pq
from athena_mvsh.error import ProgrammingError
from itertools import filterfalse
import pandas as pd
from athena_mvsh.utils import query_is_ddl


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
        if not query_is_ddl(query):
            query, __ = self.format_unload(query)

        id_exec = self.start_query_execution(
            query,
            result_reuse_enable
        )

        __ = self.pool(id_exec)
        
        try:
            tbl = self.__read_parquet()
            iter_tbl = iter(tbl.to_batches(1))
                
            for rows in iter_tbl:
                [row] = rows.to_pylist()
                yield tuple(row.values())

        except Exception:
            return
    
    def to_arrow(
        self, 
        query: str, 
        result_reuse_enable: bool = False
    ) -> pa.Table:
        
        query, __ = self.format_unload(query)
        id_exec = self.start_query_execution(
            query,
            result_reuse_enable
        )

        __ = self.pool(id_exec)
        
        try:
            return self.__read_parquet()
        except Exception:
            return pa.Table.from_dict(dict())
    
    def to_parquet(
        self,
        *args,
        **kwargs
    ):
        pq.write_table(*args, **kwargs)
    
    def to_pandas(
        self,
        *args,
        **kwargs
    ) -> pd.DataFrame:
        
        conds = lambda x: isinstance(x, pa.Table)
        [tbl] = [*filter(conds, args)]
        args = tuple(filterfalse(conds, args))

        return tbl.to_pandas(*args, **kwargs)

    def to_create_table_db(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')
    
    def to_partition_create_table_db(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')
    
    def to_insert_table_db(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')
    
    def write_dataframe(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')