class RunnerException(SystemExit):
    def __init__(self, message="", *args):
        self.message = message
        super().__init__(*args)


class InvalidFunctionArgumentsException(RuntimeError):
    pass
