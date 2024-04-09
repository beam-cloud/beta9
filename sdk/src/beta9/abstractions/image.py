from typing import List, NamedTuple, Optional, Tuple, Union

from .. import terminal
from ..abstractions.base import BaseAbstraction
from ..clients.image import (
    BuildImageRequest,
    BuildImageResponse,
    ImageServiceStub,
    VerifyImageBuildRequest,
    VerifyImageBuildResponse,
)
from ..type import (
    PythonVersion,
)


class ImageBuildResult(NamedTuple):
    success: bool = False
    image_id: str = ""


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
        self.stub: ImageServiceStub = ImageServiceStub(self.channel)

    def exists(self) -> Tuple[bool, ImageBuildResult]:
        r: VerifyImageBuildResponse = self.run_sync(
            self.stub.verify_image_build(
                VerifyImageBuildRequest(
                    python_packages=self.python_packages,
                    python_version=self.python_version,
                    commands=self.commands,
                    force_rebuild=False,
                    existing_image_uri=self.base_image,
                )
            )
        )
        return (r.exists, ImageBuildResult(success=r.exists, image_id=r.image_id))

    def build(self) -> ImageBuildResult:
        terminal.header("Building image")

        exists, exists_response = self.exists()
        if exists:
            terminal.header("Using cached image")
            return ImageBuildResult(success=True, image_id=exists_response.image_id)

        async def _build_async() -> BuildImageResponse:
            last_response = BuildImageResponse()

            async for r in self.stub.build_image(
                BuildImageRequest(
                    python_packages=self.python_packages,
                    python_version=self.python_version,
                    commands=self.commands,
                    existing_image_uri=self.base_image,
                )
            ):
                terminal.detail(r.msg)

                if r.done:
                    last_response = r
                    break

            return last_response

        with terminal.progress("Working..."):
            last_response: BuildImageResponse = self.loop.run_until_complete(_build_async())

        if not last_response.success:
            terminal.error("Build failed ❌")
            return ImageBuildResult(success=False)

        terminal.header("Build complete 🎉")
        return ImageBuildResult(success=True, image_id=last_response.image_id)
