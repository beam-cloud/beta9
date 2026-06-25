import shlex
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .. import terminal
from ..abstractions.image import Image
from ..abstractions.pod import Pod
from ..abstractions.service import Service, command_from_dockerfile, dockerfile_instructions
from ..type import GpuType, LLMConfig, LLMTokenPressureAutoscaler
from .extraclick import env_vars_to_dict

LLM_APP_KIND = "llm_model"
LLM_SERVING_PROTOCOL = "openai"
LLM_DEFAULT_METRICS_PATH = "/metrics"

LLM_MODEL_ENV_KEYS = (
    "MODEL_ID",
    "MODEL",
    "HF_MODEL_ID",
    "HUGGINGFACE_MODEL_ID",
    "HUGGING_FACE_HUB_MODEL_ID",
    "VLLM_MODEL",
    "SGLANG_MODEL",
)
LLM_TOKENIZER_ENV_KEYS = ("TOKENIZER", "TOKENIZER_ID", "VLLM_TOKENIZER")
LLM_CONTEXT_ENV_KEYS = ("CONTEXT_LENGTH", "MAX_MODEL_LEN", "MAX_MODEL_LENGTH", "MAX_SEQ_LEN")
LLM_MODEL_FLAGS = ("--model", "--model-id", "--model_id", "--model-path", "--model_path")
LLM_SERVED_MODEL_FLAGS = ("--served-model-name", "--served_model_name")
LLM_CONTEXT_FLAGS = (
    "--max-model-len",
    "--max_model_len",
    "--context-length",
    "--context_length",
    "--max-seq-len",
    "--max_seq_len",
)
LLM_TOKENIZER_FLAGS = ("--tokenizer",)
LLM_ENGINE_MARKERS = (
    ("vllm", ("vllm", "vllm-openai")),
    ("sglang", ("sglang", "sgl-project")),
    ("tgi", ("text-generation-inference", "huggingface/tgi")),
    ("tensorrt-llm", ("tensorrt-llm", "trtllm")),
    ("llama.cpp", ("llama.cpp", "llama-server")),
)


@dataclass(frozen=True)
class LLMMetadataHints:
    engine: str = ""
    model_id: str = ""
    served_model_name: str = ""
    context_length: int = 0
    tokenizer: str = ""


def llm_requested(kwargs: Dict) -> bool:
    if kwargs.get("llm_enabled"):
        return True

    return any(
        kwargs.get(key) not in (None, "", 0)
        for key in (
            "llm_model_id",
            "llm_engine",
            "llm_served_model_name",
            "llm_context_length",
            "llm_tokenizer",
            "llm_metrics_path",
            "llm_slo_tier",
        )
    )


def _dockerfile_env(image: Optional[Image]) -> Dict[str, str]:
    dockerfile = getattr(image, "dockerfile", "") or ""
    env: Dict[str, str] = {}

    for instruction, value in dockerfile_instructions(dockerfile):
        if instruction not in ("ENV", "ARG"):
            continue

        try:
            tokens = shlex.split(value)
        except ValueError:
            tokens = value.split()

        if len(tokens) >= 2 and "=" not in tokens[0]:
            env[tokens[0]] = tokens[1]
            continue

        for token in tokens:
            key, sep, raw_value = token.partition("=")
            if sep and key:
                env[key] = raw_value

    return env


def _first_env(env: Dict[str, str], keys: Tuple[str, ...]) -> str:
    for key in keys:
        value = env.get(key)
        if value:
            return value
    return ""


def _command_tokens(*commands: Optional[List[str]]) -> List[str]:
    tokens: List[str] = []
    for command in commands:
        for token in command or []:
            token = str(token)
            tokens.append(token)
            if any(char.isspace() for char in token):
                try:
                    tokens.extend(shlex.split(token))
                except ValueError:
                    continue
    return tokens


def _command_flag_value(tokens: List[str], flags: Tuple[str, ...]) -> str:
    for index, token in enumerate(tokens):
        for flag in flags:
            if token == flag and index + 1 < len(tokens):
                value = tokens[index + 1]
                if not value.startswith("-"):
                    return value
            if token.startswith(f"{flag}="):
                return token.split("=", 1)[1]
    return ""


def _positive_int(value) -> int:
    if value in (None, ""):
        return 0

    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0

    return parsed if parsed > 0 else 0


def _image_engine_sources(image: Optional[Image]) -> List[str]:
    sources = [
        getattr(image, "base_image", "") or "",
        getattr(image, "dockerfile_path", "") or "",
    ]

    dockerfile = getattr(image, "dockerfile", "") or ""
    for instruction, value in dockerfile_instructions(dockerfile):
        if instruction == "FROM":
            sources.append(value)

    return sources


def _infer_llm_engine(image: Optional[Image], command_tokens: List[str]) -> str:
    source = " ".join([*_image_engine_sources(image), *command_tokens]).lower()

    for engine, markers in LLM_ENGINE_MARKERS:
        if any(marker in source for marker in markers):
            return engine
    return ""


def llm_metadata_hints(
    kwargs: Dict,
    image: Optional[Image],
    entrypoint: Optional[List[str]],
) -> LLMMetadataHints:
    env = _dockerfile_env(image)
    env.update(env_vars_to_dict(kwargs.get("env")))

    tokens = _command_tokens(entrypoint, command_from_dockerfile(image))
    return LLMMetadataHints(
        engine=kwargs.get("llm_engine") or _infer_llm_engine(image, tokens),
        model_id=kwargs.get("llm_model_id")
        or _first_env(env, LLM_MODEL_ENV_KEYS)
        or _command_flag_value(tokens, LLM_MODEL_FLAGS),
        served_model_name=kwargs.get("llm_served_model_name")
        or _command_flag_value(tokens, LLM_SERVED_MODEL_FLAGS),
        context_length=_positive_int(kwargs.get("llm_context_length"))
        or _positive_int(_first_env(env, LLM_CONTEXT_ENV_KEYS))
        or _positive_int(_command_flag_value(tokens, LLM_CONTEXT_FLAGS)),
        tokenizer=kwargs.get("llm_tokenizer")
        or _first_env(env, LLM_TOKENIZER_ENV_KEYS)
        or _command_flag_value(tokens, LLM_TOKENIZER_FLAGS),
    )


def llm_config_from_options(
    kwargs: Dict,
    image: Optional[Image] = None,
    entrypoint: Optional[List[str]] = None,
) -> Optional[LLMConfig]:
    if not llm_requested(kwargs):
        return None

    hints = llm_metadata_hints(kwargs, image, entrypoint)
    metrics_path = kwargs.get("llm_metrics_path") or ""
    if not metrics_path and hints.engine in {"vllm", "sglang", "tgi"}:
        metrics_path = LLM_DEFAULT_METRICS_PATH

    return LLMConfig(
        model_id=hints.model_id or "",
        engine=hints.engine or "",
        served_model_name=hints.served_model_name or "",
        context_length=hints.context_length,
        tokenizer=hints.tokenizer or "",
        metrics_path=metrics_path,
        slo_tier=kwargs.get("llm_slo_tier") or "",
    )


def service_llm_metadata(
    kwargs: Dict,
    image: Optional[Image] = None,
    entrypoint: Optional[List[str]] = None,
) -> Optional[Dict]:
    llm_config = llm_config_from_options(kwargs, image=image, entrypoint=entrypoint)
    if llm_config is None:
        return None

    return {
        "app_kind": LLM_APP_KIND,
        "serving_protocol": LLM_SERVING_PROTOCOL,
        "llm": llm_config,
    }


def _metadata_target(user_obj):
    return getattr(user_obj, "parent", user_obj)


def _enable_llm_autoscaler(target) -> None:
    autoscaler = getattr(target, "autoscaler", None)
    if autoscaler is None or isinstance(autoscaler, LLMTokenPressureAutoscaler):
        return

    target.autoscaler = LLMTokenPressureAutoscaler(
        min_containers=getattr(autoscaler, "min_containers", 0),
        max_containers=getattr(autoscaler, "max_containers", 1),
        tasks_per_container=getattr(autoscaler, "tasks_per_container", 1),
    )


def _ensure_llm_pool_gpu(target) -> None:
    if not getattr(target, "llm", None) or not getattr(target, "pool_config", None):
        return
    if getattr(target, "gpu", "") or getattr(target, "gpu_count", 0):
        return

    target.gpu = GpuType.Any
    target.gpu_count = 1


def apply_llm_metadata(user_obj, kwargs: Dict) -> bool:
    target = _metadata_target(user_obj)
    metadata = service_llm_metadata(
        kwargs,
        image=getattr(target, "image", None),
        entrypoint=getattr(target, "entrypoint", None),
    )
    if metadata is None:
        return True

    if not isinstance(target, (Pod, Service)):
        terminal.error("--llm is only supported for Pod and Service deployments.", exit=False)
        return False

    target.app_kind = metadata["app_kind"]
    target.serving_protocol = metadata["serving_protocol"]
    target.llm = metadata["llm"]
    _enable_llm_autoscaler(target)
    _ensure_llm_pool_gpu(target)
    return True
