from athena_mvsh.dbathena import DBAthena
from abc import ABC, abstractmethod
import boto3
from athena_mvsh.utils import parse_output_location
from datetime import datetime, timezone
import uuid
import textwrap
from athena_mvsh.error import ProgrammingError


class CursorIterator(ABC):
    @abstractmethod
    def fetchone(self): ...

    @abstractmethod
    def fetchall(self): ...

    @abstractmethod
    def fetchmany(self, size): ...

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        else:
            return row

    def __iter__(self):
        return self


class CursorBaseParquet(DBAthena):
    FORMAT: str = 'PARQUET'
    COMPRESS: str = 'ZSTD'

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

    def format_unload(self, query):
        local = self.s3_staging_dir

        now = datetime.now(timezone.utc).strftime('%Y%m%d')
        location = f'{local}unload/{now}/{str(uuid.uuid4())}/'
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
            case {'QueryExecution': {'Statistics': {'DataManifestLocation': location}}}:
                return location
            case __:
                raise ProgrammingError('Data location does not exist')

    def get_bucket_s3(self):
        cliente_s3 = boto3.client(
            's3',
            aws_access_key_id=self.config['aws_access_key_id'],
            aws_secret_access_key=self.config['aws_secret_access_key'],
            region_name=self.config['region_name'],
        )

        data_manifest_local = self.get_manifest_local()
        bucket, key = parse_output_location(data_manifest_local)

        bucket_s3 = cliente_s3.get_object(Bucket=bucket, Key=key)

        return bucket_s3

    def get_bucket_resource(self, bucket_name: str):
        bucket = boto3.resource(
            's3',
            aws_access_key_id=self.config['aws_access_key_id'],
            aws_secret_access_key=self.config['aws_secret_access_key'],
            region_name=self.config['region_name'],
        )

        return bucket.Bucket(bucket_name)

    def unload_location(self, bucket_s3):
        manifest = bucket_s3['Body'].read().decode('utf-8').strip()
        manifest = manifest.split('\n') if manifest else []

        _unload_location = '/'.join(manifest[0].split('/')[:-1]) + '/'
        bucket, key = parse_output_location(_unload_location)

        return bucket, key, manifest
