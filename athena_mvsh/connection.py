from athena_mvsh.dbathena import DBAthena
from athena_mvsh.cursores import (
    CursorIterator,
    CursorBaseParquet,
    CursorParquetDuckdb,
    CursorPython,
    CursorParquet
)
import pyarrow as pa
from athena_mvsh.error import ProgrammingError
import pandas as pd
import os
from itertools import islice
from athena_mvsh.formatador import cast_format
from athena_mvsh.utils import query_is_ddl


WORKERS = min([4, os.cpu_count()])


class Athena(CursorIterator):
    def __init__(self, cursor: DBAthena) -> None:
        self.cursor = cursor
        self.row_cursor = None

    def execute(
        self, 
        query: str,
        parameters: tuple | dict = None,
        *,
        result_reuse_enable: bool = False
    ):
        # NOTE: Recebendo parametros
        if parameters:
            args = parameters if isinstance(parameters, tuple) else tuple()
            kwargs = parameters if isinstance(parameters, dict) else dict()
            query = cast_format(query, *args, **kwargs)
        
        self.row_cursor = self.cursor.execute(
            query,
            result_reuse_enable
        )
        
        # NOTE: Usado pelo duckdb
        self.query = query
        self.result_reuse_enable = result_reuse_enable

        if query_is_ddl(query):
            return self.fetchone()

        return self
    
    def description(self):
        return self.cursor.description()
    
    def fetchone(self):
        try:
            row = next(self.row_cursor)
        except StopIteration:
            return None
        else:
            return row
    
    def fetchall(self) -> list | pa.Table:
        return list(self.row_cursor)
    
    def fetchmany(self, size: int = 1):
        return list(islice(self.row_cursor, size))
    
    def to_arrow(self) -> pa.Table:
        return self.cursor.to_arrow(self.query, self.result_reuse_enable)
    
    def to_parquet(self, *args, **kwargs) -> None:
        if not isinstance(self.cursor, CursorBaseParquet):
            raise ProgrammingError('Function not implemented for cursor !')
        
        if isinstance(self.cursor, CursorParquetDuckdb):
            self.cursor.to_parquet(
                self.query,
                self.result_reuse_enable,
                *args,
                **kwargs
            )
            return
        
        tbl = self.to_arrow()
        args = (tbl, ) + args
        self.cursor.to_parquet(*args, **kwargs)

    def to_create_table_db(
        self, 
        table_name: str,
        *,
        database: str = 'db.duckdb'
    ) -> None:
        
        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')
        
        self.cursor.to_create_table_db(
            database,
            self.query,
            self.result_reuse_enable,
            table_name=table_name
        )
    
    def to_partition_create_table_db(
        self, 
        table_name: str,
        *,
        database: str = 'db.duckdb',
        workers: int = WORKERS
    ) -> None:
        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')
        
        self.cursor.to_partition_create_table_db(
            database,
            self.query,
            workers,
            self.result_reuse_enable,
            table_name=table_name
        )
    
    def to_insert_table_db(
        self, 
        table_name: str,
        *,
        database: str = 'db.duckdb'
    ) -> None:
        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')
        
        self.cursor.to_insert_table_db(
            database,
            self.query,
            self.result_reuse_enable,
            table_name=table_name
        )
    
    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        location: str = None,
        catalog_name: str = 'awsdatacatalog',
        compression: str = 'GZIP'
    ) -> None:
        
        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')
        
        self.cursor.write_dataframe(
            df,
            table_name,
            schema,
            location,
            catalog_name,
            compression
        )
        
    def to_pandas(self, *args, **kwargs) -> pd.DataFrame:
        if isinstance(self.cursor, CursorParquetDuckdb):
            return self.cursor.to_pandas(
                self.query,
                self.result_reuse_enable,
                *args,
                **kwargs
            )
        
        # NOTE: Utiliza a mesma estrutura
        if isinstance(self.cursor, CursorPython):
            tbl = self.fetchall()
            kwargs |= {
                'columns': [c[0] for c in self.description()],
                'data': tbl,
                'coerce_float': True
            }
            return self.cursor.to_pandas(*args, **kwargs)
        
        if isinstance(self.cursor, CursorParquet):
           tbl = self.to_arrow()
           args = args + (tbl,)
           kwargs |= {'types_mapper': pd.ArrowDtype}
           return self.cursor.to_pandas(*args, **kwargs)
    
    def close(self):
        if self.row_cursor:
            self.row_cursor = None
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()