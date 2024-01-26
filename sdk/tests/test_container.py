from unittest import TestCase
from unittest.mock import MagicMock, AsyncMock

from beta9 import Image
from beta9.clients.container import ContainerServiceStub, CommandExecutionRequest, CommandExecutionResponse
from beta9.abstractions.container import Container

class AsyncIterator:
    def __init__(self, seq):
        self.iter = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration
        
class TestContainer(TestCase):
    def test_execute_command(self):
        c = Container(cpu=1.0, memory=128, image=Image(python_version="python3.8", python_packages=["numpy"]))
        mock_stub = MagicMock()
        c.container_stub = mock_stub

        self.assertEqual(c.image.python_version, "python3.8")
        self.assertEqual(c.cpu, 1)
        self.assertEqual(c.memory, 128)
    
    def test_run(self):
        c = Container(cpu=1.0, memory=128, image=Image(python_version="python3.8", python_packages=["numpy"]))
        mock_stub = MagicMock()
        c.container_stub = mock_stub
        c.prepare_runtime = MagicMock(return_value=True)
        c.syncer = MagicMock()

        mock_stub.execute_command = MagicMock(return_value=AsyncIterator([CommandExecutionResponse(done=True, exit_code=0, result="")]))

        ec = c.run(["ls"])
        mock_stub.execute_command.assert_called_once_with(stub_id=c.stub_id, command="ls".encode())
        self.assertEqual(ec, 0)

    async def test_run_async(self):
        c = Container(cpu=1.0, memory=128, image=Image(python_version="python3.8", python_packages=["numpy"]))
        mock_stub = AsyncMock()
        c.container_stub = mock_stub
        c.prepare_runtime = AsyncMock(return_value=True)
        c.syncer = AsyncMock()

        mock_stub.execute_command = AsyncMock(return_value=AsyncIterator([CommandExecutionResponse(done=True, exit_code=0, result="")]))
        ec = await c.run_async(["ls"])
        mock_stub.execute_command.assert_called_once_with(stub_id=c.stub_id, command="ls".encode())
        self.assertEqual(ec, 0)