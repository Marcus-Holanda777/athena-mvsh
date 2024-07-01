from dbathena import DBAthena
from cursores import (
    CursorInterator,
    CursorParquet
)
import pyarrow.parquet as pq


class Athena(CursorInterator):
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
    
    def fetchall(self) -> list:
        if isinstance(self.cursor, CursorParquet):
            return self.fetchone()
        return list(self.row_cursor)
    
    def to_parquet(self, filename: str):
        tbl = self.fetchall()
        pq.write_table(tbl, filename)
    
    def close(self):
        if self.row_cursor:
            self.row_cursor = None
        
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()