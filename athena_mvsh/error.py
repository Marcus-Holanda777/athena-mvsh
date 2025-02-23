class Error(Exception): ...


class DatabaseError(Error):
    def __init__(self, response: dict | str) -> None:
        self.response = response
        self.__set_error()

    def __set_error(self):
        if not isinstance(self.response, dict):
            self.args = (self.response,)
        else:
            match self.response:
                case {'QueryExecution': {'Status': {'AthenaError': dict_error}}}:
                    self.args = (*dict_error.items(),)
                case __:
                    self.args = ('Failed to connect to database !',)


class OperationalError(DatabaseError): ...


class ProgrammingError(DatabaseError): ...
