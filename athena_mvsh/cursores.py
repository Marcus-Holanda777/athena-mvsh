from dbathena import DBAthena
from converter import MAP_CONVERT
from typing import Generator, Any
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
from abc import ABC, abstractmethod
import boto3
from utils import parse_output_location
from datetime import (
    datetime,
    timezone
)
import uuid
import textwrap
from error import ProgrammingError


class CursorIterator(ABC):
    @abstractmethod
    def fetchone(self):
        ...
    
    @abstractmethod
    def fetchall(self):
        ...
    
    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        else:
            return row

    def __iter__(self):
        return self


class CursorParquet(DBAthena):
    FORMAT: str = 'PARQUET'
    COMPRESS: str = 'SNAPPY'

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

    def format_unload(self, query):
        local = self.s3_staging_dir

        now = datetime.now(timezone.utc).strftime("%Y%m%d")
        location = f"{local}unload/{now}/{str(uuid.uuid4())}/"
        quey = textwrap.dedent(
            f"""
                UNLOAD (
                \t{query.strip()}
                )
                TO '{location}'
                WITH (
                \tformat = '{self.FORMAT}',
                \tcompression = '{self.COMPRESS}'
                )
                """
        )

        return quey, location
    
    def get_manifest_local(self):
        match self.get_query_execution:
            case {"QueryExecution": {"Statistics": {"DataManifestLocation": msg}}}:
                return msg
            case __:
                raise ProgrammingError
    
    def get_bucket_s3(self):
        cliente_s3 = boto3.client(
            's3',
            aws_access_key_id=self.config['aws_access_key_id'],
            aws_secret_access_key=self.config['aws_secret_access_key'],
            region_name=self.config['region_name']
        )

        data_manifest_local = self.get_manifest_local()
        bucket, key = parse_output_location(data_manifest_local)

        bucket_s3 = cliente_s3.get_object(
            Bucket=bucket,
            Key=key
        )

        return bucket_s3
    
    def unload_location(self, bucket_s3):
        manifest = bucket_s3["Body"].read().decode("utf-8").strip()
        manifest = manifest.split("\n") if manifest else []

        _unload_location = "/".join(manifest[0].split("/")[:-1]) + "/"
        bucket, key = parse_output_location(_unload_location)

        return bucket, key, manifest

    
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
    
    def __read_duckdb(
        self, 
        name_table = 'teste'
    ):
        """Experimental
        """

        import duckdb

        bucket_s3 = self.get_bucket_s3()
        *__, manifest = self.unload_location(bucket_s3)

        with duckdb.connect('db.duckdb') as con:
            con.install_extension('httpfs')
            con.load_extension('httpfs')

            con.sql(f"""
            CREATE SECRET IF NOT EXISTS(
              TYPE s3,
              KEY_ID '{self.config['aws_access_key_id']}',
              SECRET '{self.config['aws_secret_access_key']}',
              REGION '{self.config['region_name']}'
            )
            """)

            view = con.read_parquet(manifest)
            view.create(name_table)

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
        

class CursorPython(DBAthena):
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
                    MAP_CONVERT[meta.get('Type')](row.get("VarCharValue"))
                    for meta, row in zip(self.metadata, rows[i].get("Data", []))
                ]
            )
            for i in range(offset, len(rows))
        ]

    
    def execute(
        self, 
        query: str,
        result_reuse_enable: bool = False
    ) -> Generator[tuple, Any, None]:
        
        id_exec = self.start_query_execution(
            query,
            result_reuse_enable
        )

        data_response = {
            "QueryExecutionId": id_exec,
            "MaxResults": self.MAX_RESULTS
        }
        
        self.token_next = None
        offset = 1
        
        while True:
            response = (
                self.cliente.get_query_results(
                    **data_response
                )
            )

            self.token_next = response.get("NextToken", None)
            if offset == 1:
               self.metadata = self.__get_metadata(response)
            
            rows = self.__get_rows_set(response)
            yield from self.__get_rows_tuple(rows, offset)

            if self.token_next is None:
                break
            else:
                data_response |= {'NextToken': self.token_next}
                offset = 0