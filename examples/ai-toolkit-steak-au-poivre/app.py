import os
from pathlib import Path

from beta9 import DurableDisk, Image, Pod, Volume

ROOT = Path(__file__).parent

# The empty-string override is useful when validating on serverless capacity:
# `BETA9_POOL= beta9 run app.py:pod`.
ON_DEMAND_POOL = os.getenv("BETA9_POOL", "ai-toolkit-steak")
GPU = os.getenv("BETA9_GPU", "A100-80")

workspace = DurableDisk(
    name="ai-toolkit-steak-model-cache",
    size="50Gi",
    mount_path="/workspace",
)

artifact_volume = Volume(
    name="ai-toolkit-steak-au-poivre",
    mount_path="/artifacts",
)

image = Image.from_dockerfile(
    str(ROOT / "Dockerfile"),
    context_dir=str(ROOT),
)

pod = Pod(
    app="ai-toolkit-steak-au-poivre",
    name="flux2-klein-steak-au-poivre",
    image=image,
    entrypoint=["python", "/app/example/run.py"],
    cpu=8,
    memory="64Gi",
    gpu=GPU,
    gpu_count=1,
    pool=ON_DEMAND_POOL or None,
    disks=[workspace],
    env={
        "HF_HOME": "/workspace/huggingface",
        # Prefer regular Hub downloads. The runner also validates cached Qwen
        # shards because some Hub versions may still use Xet reconstruction.
        "HF_HUB_DISABLE_XET": "1",
        "PYTHONUNBUFFERED": "1",
        "TRAIN_STEPS": "600",
        "WORKSPACE": "/workspace",
    },
    keep_warm_seconds=-1,
)

export = Pod(
    app="ai-toolkit-steak-au-poivre",
    name="export-steak-au-poivre-artifacts",
    image=image,
    entrypoint=["python", "/app/example/export.py"],
    cpu=2,
    memory="4Gi",
    pool=ON_DEMAND_POOL or None,
    volumes=[artifact_volume],
    disks=[
        DurableDisk(
            name=workspace.name,
            size=workspace.size,
            mount_path=workspace.mount_path,
            read_only=True,
        )
    ],
    env={
        "ARTIFACT_ROOT": "/artifacts",
        "WORKSPACE": "/workspace",
    },
    keep_warm_seconds=-1,
)
