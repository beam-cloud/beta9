import uuid

from beta9 import Image, function

numpy_311_img = (
    Image(python_version="python3.11")
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
rapids_img = Image(base_image="rapidsai/rapidsai").add_commands(
    [f"echo {uuid.uuid4()}"]
)


@function(image=numpy_311_img)
def numpy_311():
    import numpy as np

    _ = np.array([0, 0, 0, 0])
    return "pass"


@function(image=ffmpeg_img)
def ffmpeg():
    return "pass"


@function(image=nv_cuda_1222_img)
def nv_cuda_1222_img():
    return "pass"


@function(image=nv_cuda_1222_torch_img)
def nv_cuda_1222_torch_img():
    import torch

    _ = torch.rand(5, 3)
    return "pass"


@function(image=rapids_img)
def rapids_img():
    return "pass"


if __name__ == "__main__":
    assert numpy_311.remote() == "pass"
    assert ffmpeg.remote() == "pass"
    assert nv_cuda_1222_img.remote() == "pass"
    assert nv_cuda_1222_torch_img.remote() == "pass"
    assert rapids_img.remote() == "pass"
