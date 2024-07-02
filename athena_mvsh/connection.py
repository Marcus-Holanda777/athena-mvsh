from dbathena import DBAthena
from cursores import (
    CursorIterator,
    CursorParquet
)
import pyarrow.parquet as pq
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
        
        self.row_cursor = self.cursor.execute(
            query, 
            result_reuse_enable
        )

        return self
    
    def fetchone(self):
        try:
            row = next(self.row_cursor)
        except StopIteration:
            return None
        else:
            return row
    
    def fetchall(self) -> list | pa.Table:
        if isinstance(self.cursor, CursorParquet):
            return self.fetchone()
        return list(self.row_cursor)
    
    def to_parquet(self, filename: str) -> None:
        if not isinstance(self.cursor, CursorParquet):
            raise ProgrammingError('Function not implemented for cursor !')
        
        tbl = self.fetchall()
        pq.write_table(tbl, filename)

    def to_pandas(self) -> pd.DataFrame:
        if not isinstance(self.cursor, CursorParquet):
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