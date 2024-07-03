from dbathena import DBAthena
from cursores import (
    CursorIterator,
    CursorBaseParquet,
    CursorParquetDuckdb
)
import pyarrow as pa
from error import ProgrammingError
import pandas as pd


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
            self.query = query
            self.result_reuse_enable = result_reuse_enable

        return self
    
    def fetchone(self):
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
    
    def to_parquet(self, filename: str) -> None:
        if not isinstance(self.cursor, CursorBaseParquet):
            raise ProgrammingError('Function not implemented for cursor !')
        
        if isinstance(self.cursor, CursorParquetDuckdb):
            self.cursor.to_parquet(
                self.query,
                self.result_reuse_enable,
                file_name=filename,
                row_group_size=100_000
            )
            return
        
        tbl = self.fetchall()
        self.cursor.to_parquet(tbl, filename)

    def to_create_table_db(self, table_name: str) -> None:
        if not isinstance(self.cursor, CursorParquetDuckdb):
            raise ProgrammingError('Function not implemented for cursor !')
        
        self.cursor.to_create_table_db(
            self.query,
            self.result_reuse_enable,
            table_name=table_name
        )

    def to_pandas(self) -> pd.DataFrame:
        if not isinstance(self.cursor, CursorBaseParquet):
            raise ProgrammingError('Function not implemented for cursor !')
        
        tbl = self.fetchall()  
        return tbl.to_pandas(types_mapper=pd.ArrowDtype)
    
    def close(self):
        if self.row_cursor:
            self.row_cursor = None
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()