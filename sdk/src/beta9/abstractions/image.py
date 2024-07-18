import os
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Sequence, Tuple, TypedDict, Union

from .. import env, terminal
from ..abstractions.base import BaseAbstraction
from ..clients.image import (
    BuildImageRequest,
    BuildImageResponse,
    ImageServiceStub,
    VerifyImageBuildRequest,
    VerifyImageBuildResponse,
)
from ..type import PythonVersion

try:
    from typing import TypeAlias
except ImportError:
    from typing_extensions import TypeAlias


class ImageBuildResult(NamedTuple):
    success: bool = False
    image_id: str = ""


class ImageCredentialValueNotFound(Exception):
    def __init__(self, key_name: str, *args: object) -> None:
        super().__init__(*args)
        self.key_name = key_name

    def __str__(self) -> str:
        return f"Did not find the environment variable {self.key_name}. Did you forget to set it?"


class AWSCredentials(TypedDict, total=False):
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_SESSION_TOKEN: str
    AWS_REGION: str


ImageCredentials: TypeAlias = Union[AWSCredentials, Sequence[str]]


class Image(BaseAbstraction):
    """
    Defines a custom container image that your code will run in.
    """

    def __init__(
        self,
        python_version: Union[PythonVersion, str] = PythonVersion.Python310,
        python_packages: Union[List[str], str] = [],
        commands: List[str] = [],
        base_image: Optional[str] = None,
        base_image_creds: Optional[ImageCredentials] = None,
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
            base_image_creds (Optional[ImageCredentials]):
                A key/value pair or key sequence of environment variables that contain credentials to
                a private registry. When provided as a dict, you must supply the correct keys and values.
                When provided as a sequence, the keys are used to lookup the environment variable value
                for you. Currently only AWS ECR is supported and can be configured by setting the
                `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` and `AWS_REGION` keys.
                Default is None.

        Example:

            Using a custom private image from AWS ECR. By defining a sequence of AWS environment variable
            keys, the Image object will lookup the values automatically.

            ```python
            image = Image(
                base_image="111111111111.dkr.ecr.us-east-1.amazonaws.com/myapp:latest,
                base_image_creds=("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"),
            )
            @endpoint(image=image)
            def squared(i: int = 0) -> int:
                return i**2
            ```
        """
        super().__init__()

        if isinstance(python_packages, str):
            python_packages = self._load_requirements_file(python_packages)

        self.python_version = python_version
        self.python_packages = self._sanitize_python_packages(python_packages)
        self.commands = commands
        self.base_image = base_image or ""
        self.base_image_creds = base_image_creds or {}
        self._stub: Optional[ImageServiceStub] = None

    @property
    def stub(self) -> ImageServiceStub:
        if not self._stub:
            self._stub = ImageServiceStub(self.channel)
        return self._stub

    def _sanitize_python_packages(self, packages: List[str]) -> List[str]:
        return [p.replace(" ", "") for p in packages]

    def _load_requirements_file(self, path: str) -> List[str]:
        requirements_file = Path(path)

        if requirements_file.is_file():
            with open(requirements_file, "r") as f:
                contents = f.read()
                lines = contents.split("\n")
                lines = list(filter(lambda r: r != "", lines))
                return lines
        else:
            raise FileNotFoundError

    def exists(self) -> Tuple[bool, ImageBuildResult]:
        r: VerifyImageBuildResponse = self.stub.verify_image_build(
            VerifyImageBuildRequest(
                python_packages=self.python_packages,
                python_version=self.python_version,
                commands=self.commands,
                force_rebuild=False,
                existing_image_uri=self.base_image,
            )
        )

        return (r.exists, ImageBuildResult(success=r.exists, image_id=r.image_id))

    def build(self) -> ImageBuildResult:
        terminal.header("Building image")

        exists, exists_response = self.exists()
        if exists:
            terminal.header("Using cached image")
            return ImageBuildResult(success=True, image_id=exists_response.image_id)

        with terminal.progress("Working..."):
            last_response = BuildImageResponse(success=False)
            for r in self.stub.build_image(
                BuildImageRequest(
                    python_packages=self.python_packages,
                    python_version=self.python_version,
                    commands=self.commands,
                    existing_image_uri=self.base_image,
                    existing_image_creds=self.get_credentials_from_env(),
                )
            ):
                if r.msg != "":
                    terminal.detail(r.msg, end="")

                if r.done:
                    last_response = r
                    break

        if not last_response.success:
            terminal.error(f"Build failed: {last_response.msg} ❌")
            return ImageBuildResult(success=False)

        terminal.header("Build complete 🎉")
        return ImageBuildResult(success=True, image_id=last_response.image_id)

    def get_credentials_from_env(self) -> Dict[str, str]:
        if env.is_remote():
            return {}

        keys = (
            self.base_image_creds.keys()
            if isinstance(self.base_image_creds, dict)
            else self.base_image_creds
        )

        creds = {}
        for key in keys:
            if v := os.getenv(key):
                creds[key] = v
            else:
                raise ImageCredentialValueNotFound(key)
        return creds
