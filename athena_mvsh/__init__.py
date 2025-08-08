from athena_mvsh.connection import Athena
from athena_mvsh.cursores import CursorParquet, CursorParquetDuckdb, CursorPython

__version__ = '0.0.22'
__author__ = 'Marcus Holanda'
__appname__ = 'athena'

__all__ = ['Athena', 'CursorParquetDuckdb', 'CursorPython', 'CursorParquet']
