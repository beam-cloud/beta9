# Fine-tune FLUX.2 Klein on steak au poivre

This example trains a LoRA for
[`black-forest-labs/FLUX.2-klein-base-4B`](https://huggingface.co/black-forest-labs/FLUX.2-klein-base-4B)
with [`ostris/ai-toolkit`](https://github.com/ostris/ai-toolkit), then generates three
1024px validation images with the trained adapter.

The training Pod uses a 50 GiB Beta9 `DurableDisk` as its only mounted storage.
The Hugging Face cache, Wikimedia Commons dataset, latent cache, checkpoints,
logs, and generated images all stay on block storage and are snapshotted when
the Pod exits. Restarts—and Pods scheduled on another node—restore that disk
instead of rebuilding state through a FUSE mount.

An optional, separate export Pod mounts the disk read-only and copies only the
result manifest, log, LoRA weights, samples, config, and dataset attribution to
a Beta9 `Volume`. This keeps GeeseFS completely out of the training hot path
while preserving convenient `beta9 cp` downloads.

## Run it

From this directory:

```bash
# Reserve one on-demand A100-80 for two hours in the pool used by app.py.
beta9 --context prod3 machine reserve \
  --gpu A100-80 \
  --name ai-toolkit-steak \
  --ttl 2h \
  --max-spend 6 \
  --yes

# Build the pinned AI Toolkit image and run the training Pod.
beta9 --context prod3 run app.py:pod

# Restore the disk in a fresh Pod and export final artifacts for download.
beta9 --context prod3 run app.py:export

# Stop billing as soon as the export completes.
beta9 --context prod3 machine release --pool ai-toolkit-steak --yes
```

If the current inventory has no on-demand offer, the exact same Pod can run on
prod3's serverless RTX 4090 capacity by disabling only the pool selection:

```bash
BETA9_POOL= beta9 --context prod3 run app.py:pod
```

The first run downloads the public base model and 16 openly licensed steak au
poivre images. It trains for 600 steps by default. To change the duration
without editing the example:

```bash
beta9 --context prod3 run app.py:pod --env TRAIN_STEPS=1000
```

The training Pod prints every generated sample and LoRA checkpoint on the
DurableDisk. After running the export Pod, inspect or download them through the
artifact Volume:

```bash
beta9 --context prod3 ls \
  ai-toolkit-steak-au-poivre/runs/steak_au_poivre_flux2_klein_4b_lora
beta9 --context prod3 cp \
  beta9://ai-toolkit-steak-au-poivre/runs/steak_au_poivre_flux2_klein_4b_lora/ \
  ./artifacts
```

The prompt trigger is `b9poivre`. Dataset attribution and source URLs are saved
to `dataset/sources.json` on both the disk and the exported Volume. Inspect the
disk and its snapshots with:

```bash
beta9 --context prod3 disk list
beta9 --context prod3 disk snapshots ai-toolkit-steak-model-cache
```

## Why this model

FLUX.2 Klein Base 4B is an undistilled, modern foundation model intended for
fine-tuning. It fits comfortably as a quantized LoRA on an H100, yet retains
the full training signal that distilled inference-only checkpoints discard. The
4B base is Apache-2.0 licensed and does not require gated Hugging Face access.

AI Toolkit is pinned to commit
`e00f3791e221eb65d87351224a7788906b361b89`; training and final sampling happen
inside that same environment to avoid LoRA format drift across Diffusers
versions.
