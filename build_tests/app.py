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


@function(image=numpy_310_img)
def numpy_310():
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


# take one optional argument "local" which defaults to False
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
        assert nv_cuda_1222_img.remote() == "pass"
        assert nv_cuda_1222_torch_img.remote() == "pass"
