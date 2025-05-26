import os
import sys
from pathlib import Path
from typing import Dict, List, Literal, NamedTuple, Optional, Sequence, Tuple, TypedDict, Union

from beta9.clients.gateway import GatewayServiceStub
from beta9.sync import FileSyncer

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
from ..env import is_notebook_env
from ..type import GpuType, GpuTypeAlias, PythonVersion, PythonVersionAlias

LOCAL_PYTHON_VERSION = f"python{sys.version_info.major}.{sys.version_info.minor}"


class ImageBuildResult(NamedTuple):
    success: bool = False
    image_id: str = ""
    python_version: str = ""


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
    List[ImageCredentialKeys],
]


def detected_python_version() -> PythonVersion:
    try:
        return PythonVersion(LOCAL_PYTHON_VERSION)
    except ValueError:
        return PythonVersion.Python3


class Image(BaseAbstraction):
    """
    Defines a custom container image that your code will run in.
    """

    def __init__(
        self,
        python_version: PythonVersionAlias = PythonVersion.Python3,
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
                The Python version to be used in the image. Default is set to [PythonVersion.Python3](#pythonversion).
                When using [PythonVersion.Python3](#pythonversion), whatever version of Python 3 exists in the image will be used.
                If none exists, Python 3.10 will be installed. When running in a notebook environment without a custom base image,
                the Python version detected in the local environment will be used if compatible. Otherwise, Python 3.10 will be installed.
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

            Custom Dockerfile

            To use a custom Dockerfile, you can use the `from_dockerfile` class method.
            This will build the image using your Dockerfile. You can set the docker build's
            context directory using the `context_dir` parameter.

            The context directory should contain all files referenced in your Dockerfile
            (like files being COPYed). If no context_dir is specified, the directory
            containing the Dockerfile will be used as the context.

            ```python
            # Basic usage - uses Dockerfile's directory as context
            image = Image.from_dockerfile("path/to/Dockerfile")

            # Specify a different context directory
            image = Image.from_dockerfile(
                "path/to/Dockerfile",
                context_dir="path/to/context"
            )

            # You can still chain additional commands and python packages
            image.add_commands(["echo 'Hello, World!'"]).add_python_packages(["numpy"])

            @endpoint(image=image)
            def handler():
                pass
            ```

            Building in a GPU environment

            By default, the image will be built on a CPU node. If you need to build on a GPU node,
            you can set the `gpu` parameter to the GPU type you need. This might be necessary if you
            are using a library or framework that will install differently depending on the availability
            of a GPU.

            ```python
            image = Image(
                python_version="python3.12",
            ).build_with_gpu("A10G")
            ```
        """
        super().__init__()
        self._gateway_stub: Optional[GatewayServiceStub] = None

        if isinstance(python_packages, str):
            python_packages = self._load_requirements_file(python_packages)

        # Only attempt to detect an appropriate default python version if we are in a notebook environment
        # and there is no base image (it might provide a required python version that we will have to detect)
        if is_notebook_env() and base_image is None:
            python_version = detected_python_version()

        self.python_version = python_version
        self.python_packages = self._sanitize_python_packages(python_packages)
        self.commands = commands
        self.build_steps = []
        self.base_image = base_image or ""
        self.base_image_creds = base_image_creds or {}
        self.env_vars = []
        self.secrets = []
        self._stub: Optional[ImageServiceStub] = None
        self.dockerfile = ""
        self.build_ctx_object = ""
        self.gpu = GpuType.NoGPU
        self.ignore_python = False

        self.with_envs(env_vars or [])

    @property
    def gateway_stub(self) -> GatewayServiceStub:
        if not self._gateway_stub:
            self._gateway_stub = GatewayServiceStub(self.channel)
        return self._gateway_stub

    @gateway_stub.setter
    def gateway_stub(self, value) -> None:
        self._gateway_stub = value

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

    @classmethod
    def from_dockerfile(cls, path: str, context_dir: Optional[str] = None) -> "Image":
        """
        Build the base image based on a Dockerfile.

        This method will sync the context directory and use the Dockerfile at the provided path to
        build the base image.

        Parameters:
            path: The path to the Dockerfile.
            context_dir: The directory to sync. If not provided, the directory of the Dockerfile will be used.

        Returns:
            Image: The Image object.
        """
        image = cls()
        if env.is_remote():
            return image

        if not context_dir:
            context_dir = os.path.dirname(path)

        syncer = FileSyncer(gateway_stub=image.gateway_stub, root_dir=context_dir)
        result = syncer.sync()
        if not result.success:
            raise ValueError("Failed to sync context directory.")

        image.build_ctx_object = result.object_id

        with open(path, "r") as f:
            dockerfile = f.read()
        image.dockerfile = dockerfile
        return image

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
                dockerfile=self.dockerfile,
                build_ctx_object=self.build_ctx_object,
                secrets=self.secrets,
                gpu=self.gpu,
                ignore_python=self.ignore_python,
            )
        )

        return (
            r.exists,
            ImageBuildResult(
                success=r.exists, image_id=r.image_id, python_version=self.python_version
            ),
        )

    def build(self) -> ImageBuildResult:
        terminal.header("Building image")

        if is_notebook_env():
            if LOCAL_PYTHON_VERSION != self.python_version:
                terminal.warn(
                    f"Local version {LOCAL_PYTHON_VERSION.value} differs from image version {self.python_version}. This may cause issues in your remote environment."
                )

        if self.base_image != "" and self.dockerfile != "":
            raise ValueError("Cannot use from_dockerfile and provide a custom base image.")

        exists, exists_response = self.exists()
        if exists:
            terminal.header("Using cached image")
            return ImageBuildResult(
                success=True,
                image_id=exists_response.image_id,
                python_version=exists_response.python_version,
            )

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
                    dockerfile=self.dockerfile,
                    build_ctx_object=self.build_ctx_object,
                    secrets=self.secrets,
                    gpu=self.gpu,
                    ignore_python=self.ignore_python,
                )
            ):
                if r.warning:
                    terminal.warn("WARNING: " + r.msg)
                elif r.msg != "" and not r.done:
                    terminal.detail(r.msg, end="")

                if r.done:
                    last_response = r
                    break

        if not last_response.success:
            terminal.error(str(last_response.msg).rstrip(), exit=False)
            return ImageBuildResult(success=False)

        terminal.header("Build complete 🎉")
        return ImageBuildResult(
            success=True,
            image_id=last_response.image_id,
            python_version=last_response.python_version,
        )

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
        if self.python_version == PythonVersion.Python3:
            self.python_version = PythonVersion.Python311

        self.python_version = self.python_version.replace("python", "micromamba")
        return self

    def add_micromamba_packages(
        self, packages: Union[Sequence[str], str], channels: Sequence[str] = []
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

    def with_secrets(self, secrets: List[str]) -> "Image":
        """
        Adds secrets stored in the platform to the build environment.

        Parameters:
            secrets: The secrets to add.

        Returns:
            Image: The Image object.
        """
        self.secrets.extend(secrets)
        return self

    def build_with_gpu(self, gpu: GpuTypeAlias) -> "Image":
        """
        Build the image on a GPU node.

        Parameters:
            gpu: The GPU type to use.

        Returns:
            Image: The Image object.
        """
        self.gpu = gpu
        return self
