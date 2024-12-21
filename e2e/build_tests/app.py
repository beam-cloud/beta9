import sys
import uuid

from beta9 import Image, function

numpy_310_img = (
    Image(python_version="python3.10")
    .add_python_packages(["numpy"])
    .add_commands([f"echo {uuid.uuid4()}"])
)
ffmpeg_img = Image().add_commands(
    ["apt-get update && apt-get install -y ffmpeg", f"echo {uuid.uuid4()}"]
)
nv_cuda_1222_img = Image(
    python_version="python3.10",
    base_image="nvidia/cuda:12.2.2-cudnn8-runtime-ubuntu22.04",
).add_commands([f"echo {uuid.uuid4()}"])
nv_cuda_1222_torch_img = (
    Image(
        python_version="python3.10",
        base_image="nvidia/cuda:12.2.2-cudnn8-runtime-ubuntu22.04",
    )
    .add_python_packages(["torch"])
    .add_commands([f"echo 1-{uuid.uuid4()}"])
)
nv_cuda_1231_ubuntu2004_img = Image(
    python_version="python3.11",
    commands=[
        "apt-get update -y && apt-get install -y git",
    ],
    base_image="docker.io/nvidia/cuda:12.3.1-runtime-ubuntu20.04",
)
torch_img = Image(
    base_image="pytorch/pytorch:2.3.1-cuda11.8-cudnn8-runtime",
    python_version="python3.11",
).add_commands(
    [
        "apt-get update -y",
        "apt-get install git -y",
        "apt-get install ffmpeg libsm6 libxext6 libgl1-mesa-glx -y",
    ]
)


@function(image=numpy_310_img)
def numpy_310():
    import numpy as np

    _ = np.array([0, 0, 0, 0])
    return "pass"


@function(image=ffmpeg_img)
def ffmpeg():
    return "pass"


@function(image=nv_cuda_1222_img)
def nv_cuda_1222():
    return "pass"


@function(image=nv_cuda_1222_torch_img)
def nv_cuda_1222_torch():
    import torch

    _ = torch.rand(5, 3)
    return "pass"


@function(image=nv_cuda_1231_ubuntu2004_img)
def nv_cuda_1231_ubuntu2004():
    return "pass"


@function(image=torch_img)
def torch_cuda():
    import torch

    _ = torch.rand(5, 3)
    return "pass"


if __name__ == "__main__":
    local = len(sys.argv) > 1 and sys.argv[1] == "local"

    if local:
        for version in ["python3.8", "python3.9", "python3.11", "python3.12"]:
            tmp_img = (
                Image(python_version=version)
                .add_python_packages(["numpy"])
                .add_commands([f"echo {uuid.uuid4()}"])
            )

            @function(image=tmp_img)
            def tmp_func():
                import numpy as np

                _ = np.array([0, 0, 0, 0])
                return "pass"

            assert tmp_func.remote() == "pass"

    assert numpy_310.remote() == "pass"
    assert ffmpeg.remote() == "pass"

    if not local:
        assert nv_cuda_1222.remote() == "pass"
        assert nv_cuda_1222_torch.remote() == "pass"
        assert nv_cuda_1231_ubuntu2004.remote() == "pass"
        assert torch_cuda.remote() == "pass"
