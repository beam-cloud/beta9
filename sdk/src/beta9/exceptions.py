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


class CreatePresignedUrlError(RuntimeError):
    def __init__(self, message: str):
        self.message = message
        super().__init__(f"Unable to create presigned URL: {message}")


class CreateMultipartUploadError(RuntimeError):
    def __init__(self, message: str):
        self.message = message
        super().__init__(f"Unable to create multipart upload: {message}")


class CompleteMultipartUploadError(RuntimeError):
    def __init__(self, message: str):
        self.message = message
        super().__init__(f"Unable to complete multipart upload: {message}")


class UploadPartError(RuntimeError):
    def __init__(self, part_number: int, message: str):
        self.message = message
        self.part_number = part_number
        super().__init__(f"Unable to upload part: {part_number=} {message=}")


class DownloadChunkError(RuntimeError):
    def __init__(self, number: int, start: int, end: int, message: str):
        self.message = message
        self.number = number
        self.start = start
        self.end = end
        super().__init__(f"Unable to download chunk: {number=} {start=} {end=} {message=}")


class RetryableError(Exception):
    def __init__(self, tries: int, message: str):
        self.message = message
        super().__init__(f"Retryable error after {tries} tries: {message}")


class GetFileSizeError(RuntimeError):
    def __init__(self, status_code: int, message: str):
        self.message = message
        self.status_code = status_code
        super().__init__(f"Unable to get file size: {status_code=} {message=}")


class ListPathError(RuntimeError):
    def __init__(self, path: str, message: str):
        self.message = message.capitalize() if message else ""
        self.path = path
        super().__init__(f"Unable to list path: {path=} {message=}")


class StatPathError(RuntimeError):
    def __init__(self, path: str, message: str):
        self.message = message.capitalize() if message else ""
        self.path = path
        super().__init__(f"Unable to stat path: {path=} {message=}")


class TaskNotFoundError(RuntimeError):
    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"Task not found: {task_id}")


class WorkspaceNotFoundError(RuntimeError):
    def __init__(self, workspace_id: str):
        self.workspace_id = workspace_id
        super().__init__(f"Workspace not found: {workspace_id}")


class StubNotFoundError(RuntimeError):
    def __init__(self, stub_id: str):
        self.stub_id = stub_id
        super().__init__(f"Stub not found: {stub_id=}")


class DeploymentNotFoundError(RuntimeError):
    def __init__(self, deployment_id: str):
        self.deployment_id = deployment_id
        super().__init__(f"Deployment not found: {deployment_id=}")


class VolumeUploadError(RuntimeError):
    def __init__(self, message: str):
        self.message = message
        super().__init__(f"Unable to upload volume: {message}")


class SandboxConnectionError(RuntimeError):
    def __init__(self, message: str):
        self.message = message
        super().__init__(f"Unable to connect to sandbox: {message}")


class SandboxProcessError(RuntimeError):
    def __init__(self, message: str):
        self.message = message
        super().__init__(f"Unable to launch sandbox process: {message}")


class SandboxFileSystemError(RuntimeError):
    def __init__(self, message: str):
        self.message = message
        super().__init__(f"Unable to perform sandbox filesystem operation: {message}")
