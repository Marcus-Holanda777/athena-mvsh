from athena_mvsh.dbathena import DBAthena
from typing import Generator, Any
from athena_mvsh.converter import MAP_CONVERT
from athena_mvsh.error import ProgrammingError
import pandas as pd


class CursorPython(DBAthena):
    def __init__(
        self,
        s3_staging_dir: str,
        schema_name: str = None,
        catalog_name: str = None,
        poll_interval: float = 1,
        result_reuse_enable: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(
            s3_staging_dir,
            schema_name,
            catalog_name,
            poll_interval,
            result_reuse_enable,
            *args,
            **kwargs,
        )

    def __get_rowcount(self, response) -> int:
        updatecount = response.get('UpdateCount', -1)
        if updatecount == 0:
            return -1
        return updatecount

    def __get_metadata(self, response) -> tuple:
        rst = response['ResultSet']
        metadata = rst.get('ResultSetMetadata')
        column_info = metadata.get('ColumnInfo')
        return tuple(column_info)

    def __get_rows_set(self, response) -> dict:
        return response['ResultSet']['Rows']

    def __get_rows_tuple(self, rows, offset) -> list[tuple]:
        return [
            tuple(
                [
                    MAP_CONVERT[meta.get('Type')](row.get('VarCharValue'))
                    for meta, row in zip(self.metadata, rows[i].get('Data', []))
                ]
            )
            for i in range(offset, len(rows))
        ]

    def description(self):
        if self.metadata is None:
            return None
        else:
            return [
                (
                    c['Name'],
                    c['Type'],
                    None,
                    None,
                    c['Precision'],
                    c['Scale'],
                    c['Nullable'],
                )
                for c in self.metadata
            ]

    def rowcount(self):
        return self.getrowcount

    def execute(
        self, query: str, result_reuse_enable: bool = False
    ) -> Generator[tuple, Any, None]:
        id_exec = self.start_query_execution(query, result_reuse_enable)

        data_response = {'QueryExecutionId': id_exec, 'MaxResults': self.MAX_RESULTS}

        self.token_next = None
        offset = 1

        while True:
            response = self.cliente.get_query_results(**data_response)

            self.token_next = response.get('NextToken', None)
            if offset == 1:
                self.metadata = self.__get_metadata(response)
                self.getrowcount = self.__get_rowcount(response)

            rows = self.__get_rows_set(response)

            # retornar total de registros do select

            self.getrowcount += len(rows)

            yield from self.__get_rows_tuple(rows, offset)

            if self.token_next is None:
                break
            else:
                data_response |= {'NextToken': self.token_next}
                offset = 0

    def to_arrow(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def to_parquet(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def to_csv(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def to_pandas(self, *args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame.from_records(*args, **kwargs)

    def to_create_table_db(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def to_partition_create_table_db(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def to_insert_table_db(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def write_dataframe(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def write_arrow(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def write_parquet(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def write_table_iceberg(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def merge_table_iceberg(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')
