from athena_mvsh.dbathena import DBAthena
from athena_mvsh.cursores import (
    CursorIterator,
    CursorBaseParquet,
    CursorParquetDuckdb,
    CursorPython
)
import pyarrow as pa
from athena_mvsh.error import ProgrammingError
import pandas as pd
import os


WORKERS = min([4, os.cpu_count()])


class Athena(CursorIterator):
    def __init__(self, cursor: DBAthena) -> None:
        self.cursor = cursor
        self.row_cursor = None

    def execute(
        self, 
        query: str,
        result_reuse_enable: bool = False
    ):
        
        if not isinstance(self.cursor, CursorParquetDuckdb):
            self.row_cursor = self.cursor.execute(
                query,
                result_reuse_enable
            )
        else:
            # NOTE: Usado pelo duckdb
            self.query = query
            self.result_reuse_enable = result_reuse_enable

        return self
    
    def description(self):
        return self.cursor.description()
    
    def fetchone(self):
        if isinstance(self.cursor, CursorParquetDuckdb):
            self.row_cursor = self.cursor.execute(
               self. query,
               self.result_reuse_enable
            )

        try:
            row = next(self.row_cursor)
        except StopIteration:
            return None
        else:
            return row
    
    def fetchall(self) -> list | pa.Table:
        """Retorna uma `Table` ou lista de tuplas
        """
        if isinstance(self.cursor, CursorBaseParquet):
            return self.fetchone()
        return list(self.row_cursor)
    
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
        
        tbl = self.fetchall()
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

    def to_pandas(self, *args, **kwargs) -> pd.DataFrame:
        if isinstance(self.cursor, CursorParquetDuckdb):
            return self.cursor.to_pandas(
                self.query,
                self.result_reuse_enable,
                *args,
                **kwargs
            )
        
        # NOTE: Utiliza a mesma estrutura
        tbl = self.fetchall()
        if isinstance(self.cursor, CursorPython):
            kwargs |= {
                'columns': [c[0] for c in self.description()],
                'data': tbl,
                'coerce_float': True
            }
            return self.cursor.to_pandas(*args, **kwargs)
        
        if isinstance(self.cursor, CursorBaseParquet):
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