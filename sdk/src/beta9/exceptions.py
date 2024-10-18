class RunnerException(SystemExit):
    def __init__(self, message="", *args):
        self.message = message
        self.code = 1
        super().__init__(*args)


class InvalidFunctionArgumentsError(RuntimeError):
    def __init__(self):
        super().__init__("Invalid function arguments")


class FunctionSetResultError(RunnerException):
    def __init__(self):
        super().__init__("Unable to set function result")


class TaskStartError(RunnerException):
    def __init__(self):
        super().__init__("Unable to start task")


class TaskEndError(RunnerException):
    def __init__(self):
        super().__init__("Unable to end task")


class InvalidRunnerEnvironmentError(RunnerException):
    def __init__(self):
        super().__init__("Invalid runner environment")
