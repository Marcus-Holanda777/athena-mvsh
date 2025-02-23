from athena_mvsh.cursores.cursores import CursorBaseParquet
import pyarrow.fs as fs
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv_arrow
from athena_mvsh.error import ProgrammingError
from itertools import filterfalse
import pandas as pd
from athena_mvsh.utils import query_is_ddl
from athena_mvsh.converter import to_column_info_arrow


class CursorParquet(CursorBaseParquet):
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

    def get_filesystem_fs(self):
        return fs.S3FileSystem(
            access_key=self.config['aws_access_key_id'],
            secret_key=self.config['aws_secret_access_key'],
            region=self.config['region_name'],
        )

    def rowcount(self):
        return self.getrowcount

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

    def __read_parquet(self) -> pa.Table:
        bucket_s3 = self.get_bucket_s3()
        bucket, key, __ = self.unload_location(bucket_s3)

        fs_s3 = self.get_filesystem_fs()

        dataset = pq.ParquetDataset(f'{bucket}/{key}', filesystem=fs_s3)

        # ADICIONAR OS METADADOS -- NESSA PARTE
        # add description
        self.metadata = to_column_info_arrow(dataset.schema)

        # add row count
        self.getrowcount = sum(
            tbl.num_rows
            for fragment in dataset.fragments
            for tbl in fragment.row_groups
        )

        return dataset.read(use_threads=True)

    def execute(self, query: str, result_reuse_enable: bool = False):
        if not query_is_ddl(query):
            query, __ = self.format_unload(query)

        __ = self.start_query_execution(query, result_reuse_enable)

        try:
            tbl = self.__read_parquet()
            iter_tbl = iter(tbl.to_batches(1))

            for rows in iter_tbl:
                [row] = rows.to_pylist()
                yield tuple(row.values())

        except Exception:
            return

    def to_arrow(self, query: str, result_reuse_enable: bool = False) -> pa.Table:
        query, __ = self.format_unload(query)
        __ = self.start_query_execution(query, result_reuse_enable)

        try:
            return self.__read_parquet()
        except Exception:
            return pa.Table.from_pydict(dict())

    def to_parquet(self, *args, **kwargs):
        pq.write_table(*args, **kwargs)

    def to_csv(self, *args, **kwargs):
        csv_arrow.write_csv(*args, **kwargs)

    def to_pandas(self, *args, **kwargs) -> pd.DataFrame:
        def conds(x):
            return isinstance(x, pa.Table)

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

    def write_arrow(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def write_parquet(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def write_table_iceberg(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')

    def merge_table_iceberg(self, *args, **kwargs):
        raise ProgrammingError('Function not implemented for cursor !')
