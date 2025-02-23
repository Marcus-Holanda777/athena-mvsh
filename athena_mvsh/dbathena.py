from abc import ABC, abstractmethod
import boto3
from enum import Enum
from time import sleep
from athena_mvsh.error import DatabaseError
import logging
from athena_mvsh.utils import logs_print


logger = logging.getLogger(__name__)


class AthenaStatus(str, Enum):
    STATE_QUEUED = 'QUEUED'
    STATE_RUNNING = 'RUNNING'
    STATE_SUCCEEDED = 'SUCCEEDED'
    STATE_FAILED = 'FAILED'
    STATE_CANCELLED = 'CANCELLED'


class DBAthena(ABC):
    MAX_RESULTS = 1_000
    MAX_RESULTS_TABLES = 50
    RESULT_SET_REUSE = 60

    KWARGS_CLIENT = set(['region_name', 'aws_access_key_id', 'aws_secret_access_key'])

    def __init__(
        self,
        s3_staging_dir: str,
        schema_name: str = None,
        catalog_name: str = None,
        poll_interval: float = 1.0,
        result_reuse_enable: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__()
        self.s3_staging_dir = s3_staging_dir
        self.schema_name = schema_name
        self.catalog_name = catalog_name
        self.poll_interval = poll_interval
        self.result_reuse_enable = result_reuse_enable
        self.token_next = None
        self.metadata = None
        self.getrowcount = -1
        self.get_query_execution = None
        self.statement_type = None
        self.substatement_type = None
        self.__kwargs = {**kwargs}
        self.cliente = boto3.client('athena', *args, **kwargs)
        self.config = self.__create_config()

    def __create_config(self) -> dict:
        data = {k: v for k, v in self.__kwargs.items() if k in self.KWARGS_CLIENT}

        if 'aws_access_key_id' not in data.keys():
            creds = self.cliente._get_credentials()
            data['aws_access_key_id'] = creds.access_key
            data['aws_secret_access_key'] = creds.secret_key

        if 'region_name' not in data.keys():
            config = self.cliente._client_config
            data['region_name'] = config.region_name

        return data

    def pool(self, id_executation) -> str:
        """Espera a requisicao até o status 'SUCCEEDED'
        em intervalos definito no atributo 'pool_interval'
        """

        while True:
            response = self.cliente.get_query_execution(QueryExecutionId=id_executation)

            match response:
                case {'QueryExecution': {'Status': {'State': status}}}:
                    if status in [
                        AthenaStatus.STATE_SUCCEEDED,
                        AthenaStatus.STATE_FAILED,
                        AthenaStatus.STATE_CANCELLED,
                    ]:
                        if status in [
                            AthenaStatus.STATE_FAILED,
                            AthenaStatus.STATE_CANCELLED,
                        ]:
                            raise DatabaseError(response)
                        break

            sleep(self.poll_interval)

        # NOTE: RETORNA A RESPOSTA DA CONEXAO
        # PARA CONFIGURACOES FUTURAS
        # SEMPRE ATUALIZAR
        self.get_query_execution = response

        # NOTE: STATEMENT and KIND da consulta
        query_temp = response['QueryExecution']
        self.statement_type = query_temp.get('StatementType')
        self.substatement_type = query_temp.get('SubstatementType')

        # NOTE: Print LOGS
        logs_print(query_temp, logger)

        return id_executation

    def start_query_execution(
        self,
        query: str,
        result_reuse_enable: bool = False,
    ) -> str:
        """Executa as instruções de consulta SQL contidas no
        parametro 'query'

        return: uma string com o id da consulta
        """

        data_response = {
            'QueryString': query,
            'ResultConfiguration': {'OutputLocation': self.s3_staging_dir},
        }

        if self.schema_name or self.catalog_name:
            data_response |= {'QueryExecutionContext': {}}

        if self.schema_name:
            data_response['QueryExecutionContext'] |= {'Database': self.schema_name}

        if self.catalog_name:
            data_response['QueryExecutionContext'] |= {'Catalog': self.catalog_name}

        if self.result_reuse_enable or result_reuse_enable:
            reuse_conf = {
                'Enabled': self.result_reuse_enable
                if self.result_reuse_enable
                else result_reuse_enable,
                'MaxAgeInMinutes': self.RESULT_SET_REUSE,
            }

            data_response['ResultReuseConfiguration'] = {
                'ResultReuseByAgeConfiguration': reuse_conf
            }

        response = self.cliente.start_query_execution(**data_response)

        id_exec = self.pool(response['QueryExecutionId'])

        return id_exec

    def get_table_metadata(
        self,
        catalog_name: str,
        database_name: str,
        table_name: str,
        work_group: str = None,
    ) -> dict:
        try:
            data_response = dict(
                CatalogName=catalog_name,
                DatabaseName=database_name,
                TableName=table_name,
            )

            if work_group:
                data_response['WorkGroup'] = work_group

            response = self.cliente.get_table_metadata(**data_response)
        except Exception:
            return dict()
        else:
            return response['TableMetadata']

    def list_table_metadata(self, catalog_name: str, database_name: str):
        data_response = dict(
            CatalogName=catalog_name,
            DatabaseName=database_name,
            MaxResults=self.MAX_RESULTS_TABLES,
        )

        while True:
            response = self.cliente.list_table_metadata(**data_response)

            yield from response['TableMetadataList']

            token = response.get('NextToken', None)
            if token is None:
                break
            else:
                data_response |= {'NextToken': token}

    @abstractmethod
    def execute(self, query: str, result_reuse_enable: bool = False): ...

    @abstractmethod
    def to_arrow(self, *args, **kwargs): ...

    @abstractmethod
    def to_parquet(self, *args, **kwargs): ...

    @abstractmethod
    def to_csv(self, *args, **kwargs): ...

    @abstractmethod
    def to_pandas(self, *args, **kwargs): ...

    @abstractmethod
    def to_create_table_db(self, *args, **kwargs): ...

    @abstractmethod
    def to_partition_create_table_db(self, *args, **kwargs): ...

    @abstractmethod
    def to_insert_table_db(self, *args, **kwargs): ...

    @abstractmethod
    def write_dataframe(self, *args, **kwargs): ...

    @abstractmethod
    def write_arrow(self, *args, **kwargs): ...

    @abstractmethod
    def write_parquet(self, *args, **kwargs): ...

    @abstractmethod
    def write_table_iceberg(self, *args, **kwargs): ...

    @abstractmethod
    def merge_table_iceberg(self, *args, **kwargs): ...
