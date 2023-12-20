from typing import List, Optional, Union

from grpclib.client import Channel

from beam.abstractions.base import BaseAbstraction, GatewayConfig, get_gateway_config
from beam.clients.image import BuildImageResponse, ImageServiceStub, VerifyImageBuildResponse
from beam.terminal import Terminal
from beam.type import (
    PythonVersion,
)


class Image(BaseAbstraction):
    """
    Defines a custom container image that your code will run in.
    """

    def __init__(
        self,
        python_version: Union[PythonVersion, str] = PythonVersion.Python38,
        python_packages: Union[List[str], str] = [],
        commands: List[str] = [],
        base_image: Optional[str] = None,
    ):
        """
        Creates an Image instance.

        An Image object encapsulates the configuration of a custom container image
        that will be used as the runtime environment for executing tasks.

        Parameters:
            python_version (Union[PythonVersion, str]):
                The Python version to be used in the image. Default is
                [PythonVersion.Python38](#pythonversion).
            python_packages (Union[List[str], str]):
                A list of Python packages to install in the container image. Alternatively, a string
                containing a path to a requirements.txt can be provided. Default is [].
            commands (List[str]):
                A list of shell commands to run when building your container image. These commands
                can be used for setting up the environment, installing dependencies, etc.
                Default is [].
            base_image (Optional[str]):
                A custom base image to replace the default ubuntu20.04 image used in your container.
                For example: docker.io/library/ubuntu:20.04
                This image must contain a valid python executable that matches the version specified
                in python_version (i.e. python3.8, python3.9, etc)
                Default is None.
        """
        super().__init__()

        self.python_version = python_version
        self.python_packages = python_packages
        self.commands = commands
        self.base_image = base_image
        self.base_image_creds = None

        config: GatewayConfig = get_gateway_config()
        self.channel: Channel = Channel(
            host=config.host,
            port=config.port,
            ssl=True if config.port == 443 else False,
        )
        self.stub: ImageServiceStub = ImageServiceStub(self.channel)

    def exists(self) -> bool:
        r: VerifyImageBuildResponse = self.run_sync(
            self.stub.verify_image_build(
                python_packages=self.python_packages,
                python_version=self.python_version,
                commands=self.commands,
                force_rebuild=False,
                existing_image_uri=self.base_image,
            )
        )
        return r.exists

    def build(self) -> bool:
        Terminal.header("Building image")

        if self.exists():
            Terminal.header("Using cached image")
            return True

        async def _build_async() -> BuildImageResponse:
            last_response: Union[None, BuildImageResponse] = None

            async for r in self.stub.build_image(
                python_packages=self.python_packages,
                python_version=self.python_version,
                commands=self.commands,
                existing_image_uri=self.base_image,
            ):
                Terminal.detail(r.msg)

                if r.done:
                    last_response = r
                    break

            return last_response

        with Terminal.progress("Working..."):
            last_response: BuildImageResponse = self.loop.run_until_complete(_build_async())

        if not last_response.success:
            Terminal.error("Build failed ☠️")
            return False

        Terminal.header("Build complete 🎉")
        return True

    def __del__(self):
        self.channel.close()

    def remote(self):
        raise NotImplementedError
