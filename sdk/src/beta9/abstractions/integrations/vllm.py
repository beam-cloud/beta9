"""
beta9 deploy app.py:app

where app.py is just a simple file with the instance of the vllm app

# app.py
from beta9 import vllm

app = vllm(stub_args, vllm_args)

Then on deploy, we set this up as an ASGI app by having some getter that returns the FASTAPI instance.

app.get_fastapi_app()
"""

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Literal, Mapping, Optional, Tuple, Union

from ...abstractions.endpoint import ASGI, _CallableWrapper
from ...abstractions.image import Image
from ...config import ConfigContext
from ...type import GpuType, GpuTypeAlias
from ..volume import Volume


# vllm/engine/arg_utils.py:EngineArgs
@dataclass
class EngineConfig:
    model: str = "facebook/opt-125m"
    served_model_name: Optional[Union[str, List[str]]] = None
    tokenizer: Optional[str] = None
    task: str = "auto"
    skip_tokenizer_init: bool = False
    tokenizer_mode: str = "auto"
    chat_template_text_format: str = "string"
    trust_remote_code: bool = False
    download_dir: Optional[str] = None
    load_format: str = "auto"
    config_format: str = "auto"
    dtype: str = "auto"
    kv_cache_dtype: str = "auto"
    quantization_param_path: Optional[str] = None
    seed: int = 0
    max_model_len: Optional[int] = None
    worker_use_ray: bool = False
    # Note: Specifying a custom executor backend by passing a class
    # is intended for expert use only. The API may change without
    # notice.
    distributed_executor_backend: Optional[Union[str, Any]] = None
    pipeline_parallel_size: int = 1
    tensor_parallel_size: int = 1
    max_parallel_loading_workers: Optional[int] = None
    block_size: int = 16
    enable_prefix_caching: bool = False
    disable_sliding_window: bool = False
    use_v2_block_manager: bool = True
    swap_space: float = 4  # GiB
    cpu_offload_gb: float = 0  # GiB
    gpu_memory_utilization: float = 0.90
    max_num_batched_tokens: Optional[int] = None
    max_num_seqs: int = 256
    max_logprobs: int = 20  # Default value for OpenAI Chat Completions API
    disable_log_stats: bool = False
    revision: Optional[str] = None
    code_revision: Optional[str] = None
    rope_scaling: Optional[dict] = None
    rope_theta: Optional[float] = None
    tokenizer_revision: Optional[str] = None
    quantization: Optional[str] = None
    enforce_eager: Optional[bool] = None
    max_context_len_to_capture: Optional[int] = None
    max_seq_len_to_capture: int = 8192
    disable_custom_all_reduce: bool = False
    tokenizer_pool_size: int = 0
    # Note: Specifying a tokenizer pool by passing a class
    # is intended for expert use only. The API may change without
    # notice.
    tokenizer_pool_type: Union[str, Any] = "ray"
    tokenizer_pool_extra_config: Optional[dict] = None
    limit_mm_per_prompt: Optional[Mapping[str, int]] = None
    enable_lora: bool = False
    max_loras: int = 1
    max_lora_rank: int = 16
    enable_prompt_adapter: bool = False
    max_prompt_adapters: int = 1
    max_prompt_adapter_token: int = 0
    fully_sharded_loras: bool = False
    lora_extra_vocab_size: int = 256
    long_lora_scaling_factors: Optional[Tuple[float]] = None
    lora_dtype: Optional[Union[str, Any]] = "auto"
    max_cpu_loras: Optional[int] = None
    device: str = "auto"
    num_scheduler_steps: int = 1
    multi_step_stream_outputs: bool = True
    ray_workers_use_nsight: bool = False
    num_gpu_blocks_override: Optional[int] = None
    num_lookahead_slots: int = 0
    model_loader_extra_config: Optional[dict] = None
    ignore_patterns: Optional[Union[str, List[str]]] = None
    preemption_mode: Optional[str] = None

    scheduler_delay_factor: float = 0.0
    enable_chunked_prefill: Optional[bool] = None

    guided_decoding_backend: str = "outlines"
    # Speculative decoding configuration.
    speculative_model: Optional[str] = None
    speculative_model_quantization: Optional[str] = None
    speculative_draft_tensor_parallel_size: Optional[int] = None
    num_speculative_tokens: Optional[int] = None
    speculative_disable_mqa_scorer: Optional[bool] = False
    speculative_max_model_len: Optional[int] = None
    speculative_disable_by_batch_size: Optional[int] = None
    ngram_prompt_lookup_max: Optional[int] = None
    ngram_prompt_lookup_min: Optional[int] = None
    spec_decoding_acceptance_method: str = "rejection_sampler"
    typical_acceptance_sampler_posterior_threshold: Optional[float] = None
    typical_acceptance_sampler_posterior_alpha: Optional[float] = None
    qlora_adapter_name_or_path: Optional[str] = None
    disable_logprobs_during_spec_decoding: Optional[bool] = None

    otlp_traces_endpoint: Optional[str] = None
    collect_detailed_traces: Optional[str] = None
    disable_async_output_proc: bool = False
    override_neuron_config: Optional[Dict[str, Any]] = None
    mm_processor_kwargs: Optional[Dict[str, Any]] = None
    scheduling_policy: Literal["fcfs", "priority"] = "fcfs"
    disable_log_requests: bool = False


class VLLM(ASGI):
    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        image: Image = Image().add_python_packages(["vllm", "fastapi"]),
        workers: int = 1,
        concurrent_requests: int = 1,
        keep_warm_seconds: float = 10.0,
        max_pending_tasks: int = 100,
        timeout: int = 3600,
        authorized: bool = True,
        name: Optional[str] = None,
        # vLLM engine config
        engine_config: EngineConfig = EngineConfig(),
        # vLLM args
        response_role: Optional[str] = None,
        lora_modules: Optional[List[str]] = None,
        prompt_adapters: Optional[List[str]] = None,
        chat_template: Optional[str] = None,
        return_tokens_as_token_ids: bool = False,
        enable_auto_tools: bool = False,
        tool_call_parser: Optional[str] = None,
        disable_log_stats: bool = False,
    ):
        # ASGI initialization
        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            image=image,
            workers=workers,
            concurrent_requests=concurrent_requests,
            keep_warm_seconds=keep_warm_seconds,
            max_pending_tasks=max_pending_tasks,
            timeout=timeout,
            authorized=authorized,
            name=name,
            volumes=[Volume(name="vllm-cache", mount_path="/vllm-cache")],
        )

        self.engine_config = engine_config
        self.vllm_args = SimpleNamespace(
            model=engine_config.model,
            response_role=response_role,
            lora_modules=lora_modules,
            prompt_adapters=prompt_adapters,
            chat_template=chat_template,
            return_tokens_as_token_ids=return_tokens_as_token_ids,
            enable_auto_tools=enable_auto_tools,
            tool_call_parser=tool_call_parser,
            disable_log_stats=disable_log_stats,
        )

    def deploy(
        self,
        name: Optional[str] = None,
        context: Optional[ConfigContext] = None,
        invocation_details_func: Optional[Callable[..., None]] = None,
        **invocation_details_options: Any,
    ) -> bool:
        # Create the vllm app using the configuration
        def _create_app():
            import asyncio

            from fastapi import FastAPI
            from vllm.engine.arg_utils import AsyncEngineArgs
            from vllm.entrypoints.openai.api_server import (
                build_async_engine_client_from_engine_args,
                init_app_state,
            )

            engine_args = AsyncEngineArgs.from_cli_args(self.engine_config)
            engine_client = build_async_engine_client_from_engine_args(engine_args)
            app = FastAPI()
            model_config = asyncio.run(engine_args.create_model_config())

            init_app_state(
                engine_client,
                model_config,
                app.state,
                self.vllm_args,
            )

            return self.app

        return _CallableWrapper(_create_app, self).deploy(
            name=name,
            context=context,
            invocation_details_func=invocation_details_func,
            **invocation_details_options,
        )
