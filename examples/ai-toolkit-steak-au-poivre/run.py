from __future__ import annotations

import html
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import requests
import yaml
from PIL import Image
from safetensors import safe_open

AI_TOOLKIT_COMMIT = "e00f3791e221eb65d87351224a7788906b361b89"
MODEL = "black-forest-labs/FLUX.2-klein-base-4B"
RUN_NAME = "steak_au_poivre_flux2_klein_4b_lora"
TRIGGER = "b9poivre"
WIKIMEDIA_API = "https://commons.wikimedia.org/w/api.php"
HTTP_HEADERS = {
    "User-Agent": (
        "beam-beta9-ai-toolkit-example/1.0 "
        "(https://github.com/beam-cloud/beta9; support@beam.cloud)"
    ),
    "Referer": "https://commons.wikimedia.org/",
}

# A fixed, openly licensed subset makes the example repeatable while avoiding
# redistributing the source images in this repository.
SOURCE_TITLES = (
    "File:Filet de boeuf au poivre.png",
    "File:Food at the Restaurante Associação Agrícola de São Miguel 03.jpg",
    "File:Payard Pâtisserie & Bistro.jpg",
    "File:Steak au poivre (2010).jpg",
    "File:Steak au poivre - Italy.jpg",
    "File:Steak au poivre 2025.jpg",
    "File:Steak au poivre at Grilli Ribis.jpg",
    "File:Steak au poivre at Il Siciliano.jpg",
    "File:Steak au poivre at La Famiglia, Flamingo, Vantaa.jpg",
    "File:Steak au poivre at Persilja.jpg",
    "File:Steak au poivre at restaurant Teini in Turku.jpg",
    "File:Steak au poivre in Mo i Rana, Norway.jpg",
    "File:Steak Au Poivre.jpg",
    "File:Steak au poivre.jpg",
    "File:Steak Pimienta.jpg",
    "File:Steakaupoivre.jpg",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_html(value: str | None) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", value or "")).strip()


def metadata_value(metadata: dict[str, Any], key: str) -> str:
    value = metadata.get(key, {})
    return clean_html(value.get("value") if isinstance(value, dict) else str(value))


def http_get(url: str, **kwargs: Any) -> requests.Response:
    for attempt in range(5):
        response = requests.get(url, headers=HTTP_HEADERS, timeout=60, **kwargs)
        if response.status_code not in (429, 503):
            response.raise_for_status()
            return response

        retry_after = response.headers.get("Retry-After", "")
        delay = float(retry_after) if retry_after.isdigit() else 2**attempt
        print(f"Wikimedia returned {response.status_code}; retrying in {delay:g}s")
        time.sleep(delay)

    response.raise_for_status()
    raise AssertionError("unreachable")


def fetch_source_metadata() -> list[dict[str, Any]]:
    response = http_get(
        WIKIMEDIA_API,
        params={
            "action": "query",
            "format": "json",
            "formatversion": "2",
            "prop": "imageinfo",
            "iiprop": "url|mime|extmetadata",
            "redirects": "1",
            "titles": "|".join(SOURCE_TITLES),
        },
    )

    pages = response.json().get("query", {}).get("pages", [])
    sources: list[dict[str, Any]] = []
    for page in pages:
        image_info = page.get("imageinfo", [])
        if page.get("missing") or not image_info:
            continue
        info = image_info[0]
        metadata = info.get("extmetadata", {})
        sources.append(
            {
                "title": page["title"],
                "download_url": info["url"],
                "description_url": info["descriptionurl"],
                "mime": info.get("mime", ""),
                "artist": metadata_value(metadata, "Artist"),
                "license": metadata_value(metadata, "LicenseShortName"),
                "license_url": metadata_value(metadata, "LicenseUrl"),
                "credit": metadata_value(metadata, "Credit"),
            }
        )

    if len(sources) < 12:
        raise RuntimeError(f"Expected at least 12 source images, found {len(sources)}")
    return sorted(sources, key=lambda source: source["title"].casefold())


def caption_for(title: str) -> str:
    presentation = "restaurant presentation" if " at " in title.lower() else "plated presentation"
    return (
        f"{TRIGGER}, steak au poivre, pepper-crusted beef steak with creamy peppercorn "
        f"sauce, {presentation}, realistic food photography"
    )


def prepare_dataset(dataset_dir: Path) -> list[dict[str, Any]]:
    manifest_path = dataset_dir / "sources.json"
    existing_images = sorted(dataset_dir.glob("*.jpg"))
    if manifest_path.exists() and len(existing_images) >= 12:
        print(f"Reusing {len(existing_images)} prepared images from {dataset_dir}")
        return json.loads(manifest_path.read_text())

    dataset_dir.mkdir(parents=True, exist_ok=True)
    sources = fetch_source_metadata()
    prepared: list[dict[str, Any]] = []

    for index, source in enumerate(sources):
        stem = f"{index:02d}"
        image_path = dataset_dir / f"{stem}.jpg"
        caption_path = dataset_dir / f"{stem}.txt"

        if image_path.exists() and caption_path.exists():
            image = Image.open(image_path)
            image.load()
        else:
            response = http_get(source["download_url"])
            image = Image.open(BytesIO(response.content))
            image.load()

            if image.mode == "RGBA":
                background = Image.new("RGB", image.size, "white")
                background.paste(image, mask=image.getchannel("A"))
                image = background
            else:
                image = image.convert("RGB")

            image.thumbnail((1280, 1280), Image.Resampling.LANCZOS)
            image.save(image_path, format="JPEG", quality=92, optimize=True)
            caption_path.write_text(caption_for(source["title"]) + "\n")
            time.sleep(1)

        prepared.append(
            {
                **source,
                "file": image_path.name,
                "width": image.width,
                "height": image.height,
                "caption": caption_for(source["title"]),
            }
        )
        print(f"Prepared {image_path.name}: {source['title']} ({image.width}x{image.height})")

    manifest_path.write_text(json.dumps(prepared, indent=2, ensure_ascii=False) + "\n")
    return prepared


def clear_invalid_qwen_cache(cache_root: Path) -> None:
    snapshots = cache_root / "huggingface" / "hub" / "models--Qwen--Qwen3-4B" / "snapshots"
    for path in snapshots.glob("**/*.safetensors"):
        try:
            with safe_open(path, framework="pt"):
                pass
        except Exception as error:
            target = path.resolve()
            print(f"Removing incomplete cached model shard {path.name}: {error}")
            path.unlink(missing_ok=True)
            if target != path:
                target.unlink(missing_ok=True)


def resolved_config(
    workspace: Path,
    dataset_dir: Path,
    steps: int,
) -> Path:
    template_path = Path(__file__).with_name("train.yaml")
    config = yaml.safe_load(template_path.read_text())
    process = config["config"]["process"][0]
    process["training_folder"] = str(workspace / "runs")
    process["datasets"][0]["folder_path"] = str(dataset_dir)
    process["train"]["steps"] = steps
    process["save"]["save_every"] = max(100, steps // 2)

    config_text = yaml.safe_dump(config, sort_keys=False)
    config_path = workspace / "config" / "steak-au-poivre.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(config_text)
    return config_path


def collect_artifacts(workspace: Path) -> dict[str, Any]:
    run_dir = workspace / "runs" / RUN_NAME
    models = sorted(str(path) for path in run_dir.rglob("*.safetensors"))
    samples = sorted(
        str(path)
        for pattern in ("*.png", "*.jpg", "*.jpeg")
        for path in (run_dir / "samples").glob(pattern)
    )
    if not models:
        raise RuntimeError(f"Training completed but no LoRA weights were found under {run_dir}")
    if not samples:
        raise RuntimeError(f"Training completed but no generated samples were found under {run_dir}")
    return {
        "run_dir": str(run_dir),
        "models": models,
        "samples": samples,
    }


def main() -> None:
    workspace = Path(os.environ.get("WORKSPACE", "/workspace"))
    steps = int(os.environ.get("TRAIN_STEPS", "600"))
    if steps < 1:
        raise ValueError("TRAIN_STEPS must be positive")

    workspace.mkdir(parents=True, exist_ok=True)
    dataset = prepare_dataset(workspace / "dataset")
    clear_invalid_qwen_cache(workspace)
    shutil.rmtree(workspace / "dataset" / "_latent_cache", ignore_errors=True)
    config_path = resolved_config(workspace, workspace / "dataset", steps)
    shutil.rmtree(workspace / "runs" / RUN_NAME / "samples", ignore_errors=True)

    result_path = workspace / "result.json"
    log_path = workspace / "ai-toolkit.log"
    result: dict[str, Any] = {
        "status": "running",
        "started_at": utc_now(),
        "ai_toolkit_commit": AI_TOOLKIT_COMMIT,
        "base_model": MODEL,
        "trigger": TRIGGER,
        "train_steps": steps,
        "dataset_images": len(dataset),
        "storage": {
            "type": "durable_disk",
            "name": "ai-toolkit-steak-model-cache",
            "root": str(workspace),
        },
        "config": str(config_path),
        "log": str(log_path),
    }
    result_path.write_text(json.dumps(result, indent=2) + "\n")

    print(f"Training {MODEL} for {steps} steps with {len(dataset)} images")
    print(f"Resolved config: {config_path}")
    try:
        process = subprocess.Popen(
            [sys.executable, "/opt/ai-toolkit/run.py", str(config_path)],
            cwd="/opt/ai-toolkit",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        with log_path.open("w") as log_file:
            assert process.stdout is not None
            for line in process.stdout:
                print(line, end="", flush=True)
                log_file.write(line)
                log_file.flush()
        if process.wait() != 0:
            raise subprocess.CalledProcessError(process.returncode, process.args)
    except Exception as error:
        result.update(
            {
                "status": "error",
                "completed_at": utc_now(),
                "error": str(error),
            }
        )
        result_path.write_text(json.dumps(result, indent=2) + "\n")
        raise

    result.update(
        {
            "status": "complete",
            "completed_at": utc_now(),
            "artifacts": collect_artifacts(workspace),
        }
    )
    result_path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
