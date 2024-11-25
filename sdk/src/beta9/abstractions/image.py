import os
from pathlib import Path
from typing import Dict, List, Literal, NamedTuple, Optional, Sequence, Tuple, TypedDict, Union

from .. import env, terminal
from ..abstractions.base import BaseAbstraction
from ..clients.image import (
    BuildImageRequest,
    BuildImageResponse,
    BuildStep,
    ImageServiceStub,
    VerifyImageBuildRequest,
    VerifyImageBuildResponse,
)
from ..type import PythonVersion, PythonVersionAlias


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
    """Amazon Web Services credentials"""

    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_SESSION_TOKEN: str
    AWS_REGION: str


class GCPCredentials(TypedDict, total=False):
    """Google Cloud Platform credentials"""

    GCP_ACCESS_TOKEN: str


class DockerHubCredentials(TypedDict, total=False):
    """Docker Hub credentials"""

    DOCKERHUB_USERNAME: str
    DOCKERHUB_PASSWORD: str


class NGCCredentials(TypedDict, total=False):
    """NVIDIA GPU Cloud credentials"""

    NGC_API_KEY: str


ImageCredentialKeys = Literal[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_REGION",
    "DOCKERHUB_USERNAME",
    "DOCKERHUB_PASSWORD",
    "GCP_ACCESS_TOKEN",
    "NGC_API_KEY",
]

ImageCredentials = Union[
    AWSCredentials,
    DockerHubCredentials,
    GCPCredentials,
    NGCCredentials,
    ImageCredentialKeys,
]


class Image(BaseAbstraction):
    """
    Defines a custom container image that your code will run in.
    """

    def __init__(
        self,
        python_version: PythonVersionAlias = PythonVersion.Python310,
        python_packages: Union[List[str], str] = [],
        commands: List[str] = [],
        base_image: Optional[str] = None,
        base_image_creds: Optional[ImageCredentials] = None,
        env_vars: Optional[Union[str, List[str], Dict[str, str]]] = None,
    ):
        """
        Creates an Image instance.

        An Image object encapsulates the configuration of a custom container image
        that will be used as the runtime environment for executing tasks.

        If the `python_packages` variable is set, it will always run before `commands`.
        To control the order of execution, use the `add_commands` and `add_python_packages`
        methods. These will be executed in the order they are added.

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
                This can be a public or private image from Docker Hub, Amazon ECR, Google Cloud Artifact Registry, or
                NVIDIA GPU Cloud Registry. The formats for these registries are respectively `docker.io/my-org/my-image:0.1.0`,
                `111111111111.dkr.ecr.us-east-1.amazonaws.com/my-image:latest`,
                `us-east4-docker.pkg.dev/my-project/my-repo/my-image:0.1.0`, and `nvcr.io/my-org/my-repo:0.1.0`.
                Default is None.
            base_image_creds (Optional[ImageCredentials]):
                A key/value pair or key list of environment variables that contain credentials to
                a private registry. When provided as a dict, you must supply the correct keys and values.
                When provided as a list, the keys are used to lookup the environment variable value
                for you. Default is None.
            env_vars (Optional[Union[str, List[str], Dict[str, str]]):
                Adds environment variables to an image. These will be available when building the image
                and when the container is running. This can be a string, a list of strings, or a
                dictionary of strings. The string must be in the format of "KEY=VALUE". If a list of
                strings is provided, each element should be in the same format. Deafult is None.

        Example:

            Docker Hub

            To use a private image from Docker Hub, export your Docker Hub credentials.

            ```sh
            export DOCKERHUB_USERNAME=user123
            export DOCKERHUB_PASSWORD=pass123
            ```

            Then configure the Image object with those environment variables.

            ```python
            image = Image(
                python_version="python3.12",
                base_image="docker.io/my-org/my-image:0.1.0",
                base_image_creds=["DOCKERHUB_USERNAME", "DOCKERHUB_PASSWORD"],
            )

            @endpoint(image=image)
            def handler():
                pass
            ```

            Amazon Elastic Container Registry (ECR)

            To use a private image from Amazon ECR, export your AWS environment variables.
            Then configure the Image object with those environment variables.

            ```python
            image = Image(
                python_version="python3.12",
                base_image="111111111111.dkr.ecr.us-east-1.amazonaws.com/my-image:latest,
                base_image_creds=["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"],
            )

            @endpoint(image=image)
            def handler():
                pass
            ```

            Google Artifact Registry (GAR)

            To use a private image from Google Artifact Registry, export your access token.

            ```sh
            export GCP_ACCESS_TOKEN=$(gcloud auth print-access-token --project=my-project)
            ```

            Then configure the Image object to use the environment variable.

            ```python
            image = Image(
                python_version="python3.12",
                base_image="us-east4-docker.pkg.dev/my-project/my-repo/my-image:0.1.0",
                base_image_creds=["GCP_ACCESS_TOKEN"],
            )

            @endpoint(image=image)
            def handler():
                pass

            NVIDIA GPU Cloud (NGC)

            To use a private image from NVIDIA GPU Cloud, export your API key.

            ```sh
            export NGC_API_KEY=abc123
            ```

            Then configure the Image object to use the environment variable.

            ```python
            image = Image(
                python_version="python3.12",
                base_image="nvcr.io/nvidia/tensorrt:24.10-py3",
                base_image_creds=["NGC_API_KEY"],
            )

            @endpoint(image=image)
            def handler():
                pass
            ```
        """
        super().__init__()

        if isinstance(python_packages, str):
            python_packages = self._load_requirements_file(python_packages)

        self.python_version = python_version
        self.python_packages = self._sanitize_python_packages(python_packages)
        self.commands = commands
        self.build_steps = []
        self.base_image = base_image or ""
        self.base_image_creds = base_image_creds or {}
        self.env_vars = []
        self._stub: Optional[ImageServiceStub] = None

        self.with_envs(env_vars or [])

    @property
    def stub(self) -> ImageServiceStub:
        if not self._stub:
            self._stub = ImageServiceStub(self.channel)
        return self._stub

    def __eq__(self: "Image", other: "Image"):
        return (
            self.python_version == other.python_version
            and self.python_packages == other.python_packages
            and self.base_image == other.base_image
            and self.base_image_creds == other.base_image_creds
        )

    def __str__(self) -> str:
        return f"Python Version: {self.python_version}, Python Packages: {self.python_packages}, Base Image: {self.base_image}, Base Image Credentials: {self.base_image_creds}"

    def _sanitize_python_packages(self, packages: List[str]) -> List[str]:
        # https://pip.pypa.io/en/stable/reference/requirements-file-format/
        prefix_exceptions = ["--", "-"]
        sanitized = []
        for p in packages:
            if any(p.startswith(prefix) for prefix in prefix_exceptions):
                sanitized.append(p)
            elif p.startswith("#"):
                continue
            else:
                sanitized.append(p.replace(" ", ""))
        return sanitized

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
                build_steps=self.build_steps,
                force_rebuild=False,
                existing_image_uri=self.base_image,
                env_vars=self.env_vars,
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
                    build_steps=self.build_steps,
                    existing_image_uri=self.base_image,
                    existing_image_creds=self.get_credentials_from_env(),
                    env_vars=self.env_vars,
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

    def micromamba(self) -> "Image":
        """
        Use micromamba to manage python packages.
        """
        self.python_version = self.python_version.replace("python", "micromamba")
        return self

    def add_micromamba_packages(
        self, packages: Union[Sequence[str], str], channels: Optional[Sequence[str]] = []
    ) -> "Image":
        """
        Add micromamba packages that will be installed when building the image.

        These will be executed at the end of the image build and in the
        order they are added. If a single string is provided, it will be
        interpreted as a path to a requirements.txt file.

        Parameters:
            packages: The micromamba packages to add or the path to a requirements.txt file.
            channels: The micromamba channels to use.
        """
        # Error if micromamba is not enabled
        if not self.python_version.startswith("micromamba"):
            raise ValueError("Micromamba must be enabled to use this method.")

        # Check if we were given a .txt requirement file
        if isinstance(packages, str):
            packages = self._sanitize_python_packages(self._load_requirements_file(packages))

        for package in packages:
            self.build_steps.append(BuildStep(command=package, type="micromamba"))

        for channel in channels:
            self.build_steps.append(BuildStep(command=f"-c {channel}", type="micromamba"))

        return self

    def add_commands(self, commands: Sequence[str]) -> "Image":
        """
        Add shell commands that will be executed when building the image.

        These will be executed at the end of the image build and in the
        order they are added.

        Parameters:
            commands: The shell commands to execute.

        Returns:
            Image: The Image object.
        """
        for command in commands:
            self.build_steps.append(BuildStep(command=command, type="shell"))
        return self

    def add_python_packages(self, packages: Union[Sequence[str], str]) -> "Image":
        """
        Add python packages that will be installed when building the image.

        These will be executed at the end of the image build and in the
        order they are added.

        Parameters:
            packages: The Python packages to add or the path to a requirements.txt file. Valid package names are: numpy, pandas==2.2.2, etc.

        Returns:
            Image: The Image object.
        """

        if isinstance(packages, str):
            try:
                packages = self._sanitize_python_packages(self._load_requirements_file(packages))
            except FileNotFoundError:
                raise ValueError(
                    f"Could not find valid requirements.txt file at {packages}. Libraries must be specified as a list of valid package names or a path to a requirements.txt file."
                )

        for package in packages:
            self.build_steps.append(BuildStep(command=package, type="pip"))
        return self

    def with_envs(
        self, env_vars: Union[str, List[str], Dict[str, str]], clear: bool = False
    ) -> "Image":
        """
        Add environment variables to the image.

        These will be available when building the image and when the container is running.

        Parameters:
            env_vars: Environment variables. This can be a string, a list of strings, or a
                dictionary of strings. The string must be in the format of "KEY=VALUE". If a list of
                strings is provided, each element should be in the same format. Deafult is None.
            clear: Clear existing environment variables before adding the new ones.

        Returns:
            Image: The Image object.
        """
        if isinstance(env_vars, dict):
            env_vars = [f"{key}={value}" for key, value in env_vars.items()]
        elif isinstance(env_vars, str):
            env_vars = [env_vars]

        self.validate_env_vars(env_vars)

        if clear:
            self.env_vars.clear()

        self.env_vars.extend(env_vars)

        return self

    def validate_env_vars(self, env_vars: List[str]) -> None:
        for env_var in env_vars:
            key, sep, value = env_var.partition("=")
            if not sep:
                raise ValueError(f"Environment variable must contain '=': {env_var}")
            if not key:
                raise ValueError(f"Environment variable key cannot be empty: {env_var}")
            if not value:
                raise ValueError(f"Environment variable value cannot be empty: {env_var}")
            if "=" in value:
                raise ValueError(
                    f"Environment variable cannot contain multiple '=' characters: {env_var}"
                )
