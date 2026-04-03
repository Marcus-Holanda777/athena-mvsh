from __future__ import annotations


class Error(Exception): ...


class DatabaseError(Error):
    def __init__(self, response: dict | str) -> None:
        self.response = response
        self.__set_error()

    def __set_error(self):
        if not isinstance(self.response, dict):
            self.args = (self.response,)
        else:
            query_execution = self.response.get('QueryExecution', {})
            status = query_execution.get('Status', {})
            dict_error = status.get('AthenaError')

            if dict_error and isinstance(dict_error, dict):
                self.args = (*dict_error.items(),)
            else:
                self.args = ('Failed to connect to database !',)


class OperationalError(DatabaseError): ...


class ProgrammingError(DatabaseError): ...
