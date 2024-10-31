import os
import threading
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Literal, Mapping, Optional, Tuple, Union

from ... import terminal
from ...abstractions.base.runner import ASGI_DEPLOYMENT_STUB_TYPE, ASGI_SERVE_STUB_TYPE
from ...abstractions.endpoint import ASGI
from ...abstractions.image import Image
from ...abstractions.volume import Volume
from ...channel import with_grpc_error_handling
from ...clients.endpoint import (
    EndpointServeKeepAliveRequest,
    StartEndpointServeRequest,
    StartEndpointServeResponse,
    StopEndpointServeRequest,
)
from ...clients.gateway import DeployStubRequest, DeployStubResponse
from ...config import ConfigContext
from ...type import GpuType, GpuTypeAlias


# vllm/engine/arg_utils.py:EngineArgs
@dataclass
class VLLMEngineConfig:
    """
    The configuration for the vLLM engine. For more information, see the vllm documentation:
    https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html#command-line-arguments-for-the-server
    Each of these arguments corresponds to a command line argument for the vllm server.
    """

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
    """
    vllm is a wrapper around the vLLM library that allows you to deploy it as an ASGI app.

    Parameters:
        cpu (Union[int, float, str]):
            The number of CPU cores allocated to the container. Default is 1.0.
        memory (Union[int, str]):
            The amount of memory allocated to the container. It should be specified in
            MiB, or as a string with units (e.g. "1Gi"). Default is 128 MiB.
        gpu (Union[GpuType, str]):
            The type or name of the GPU device to be used for GPU-accelerated tasks. If not
            applicable or no GPU required, leave it empty. Default is [GpuType.NoGPU](#gputype).
        image (Union[Image, dict]):
            The container image used for the task execution. If you override this, it must include
            the vllm package and the fastapi package.
        workers (int):
            The number of workers to run in the container. Default is 1.
        concurrent_requests (int):
            The maximum number of concurrent requests to handle. Default is 1.
        keep_warm_seconds (float):
            The number of seconds to keep the container warm after the last request. Default is 10.0.
        max_pending_tasks (int):
            The maximum number of pending tasks to allow in the container. Default is 100.
        timeout (int):
            The maximum number of seconds to wait for the container to start. Default is 3600.
        authorized (bool):
            Whether the endpoints require authorization. Default is True.
        name (str):
            The name of the container. Default is none, which means you must provide it during deployment.
        engine_config (EngineConfig):
            The configuration for the vLLM engine.
        response_role (str):
            The role of the response.
        lora_modules (List[str]):
            The LoRA modules to use.
        prompt_adapters (List[str]):
            The prompt adapters to use.
        chat_template (str):
            The chat template to use.
        return_tokens_as_token_ids (bool):
            Whether to return tokens as token ids.
        enable_auto_tools (bool):
            Whether to enable auto tools.
        enable_auto_tool_choice (bool):
            Whether to enable auto tool choice.
        tool_call_parser (str):
            The tool call parser to use.
        disable_log_stats (bool):
            Whether to disable log stats.
        disable_log_requests (bool):
            Whether to disable log requests.
        max_log_len (Optional[int]):
            The maximum length of the log.

    Example:
        ```python
        from beta9 import integrations

        e = integrations.EngineConfig()
        e.device = "cpu"

        vllm_app = integrations.VLLM(name="vllm-abstraction-1", engine_config=e)
        ```
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        image: Image = Image(python_version="python3.11").add_python_packages(["fastapi", "vllm"]),
        workers: int = 1,
        concurrent_requests: int = 1,
        keep_warm_seconds: float = 10.0,
        max_pending_tasks: int = 100,
        timeout: int = 3600,
        authorized: bool = True,
        name: Optional[str] = None,
        # vLLM engine config
        engine_config: VLLMEngineConfig = VLLMEngineConfig(),
        # vLLM args
        response_role: Optional[str] = None,
        lora_modules: Optional[List[str]] = None,
        prompt_adapters: Optional[List[str]] = None,
        chat_template: Optional[str] = None,
        return_tokens_as_token_ids: bool = False,
        enable_auto_tools: bool = False,
        enable_auto_tool_choice: bool = False,
        tool_call_parser: Optional[str] = None,
        disable_log_stats: bool = False,
        disable_log_requests: bool = False,
        max_log_len: Optional[int] = None,
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
            served_model_name=engine_config.served_model_name,
            disable_log_requests=disable_log_requests,
            max_log_len=max_log_len,
            response_role=response_role,
            lora_modules=lora_modules,
            prompt_adapters=prompt_adapters,
            chat_template=chat_template,
            return_tokens_as_token_ids=return_tokens_as_token_ids,
            enable_auto_tool_choice=enable_auto_tool_choice,
            enable_auto_tools=enable_auto_tools,
            tool_call_parser=tool_call_parser,
            disable_log_stats=disable_log_stats,
        )

    def __name__(self) -> str:
        return self.name or "vllm"

    def set_handler(self, handler: str):
        self.handler = handler

    def func(self, *args: Any, **kwargs: Any):
        pass

    def __call__(self, *args: Any, **kwargs: Any):
        import asyncio

        from fastapi import FastAPI
        from vllm.engine.arg_utils import AsyncEngineArgs
        from vllm.engine.async_llm_engine import AsyncLLMEngine
        from vllm.entrypoints.openai.api_server import (
            init_app_state,
            router,
        )
        from vllm.usage.usage_lib import UsageContext

        engine_args = AsyncEngineArgs.from_cli_args(self.engine_config)
        engine_client = AsyncLLMEngine.from_engine_args(
            engine_args, usage_context=UsageContext.OPENAI_API_SERVER
        )
        app = FastAPI()
        app.include_router(router)

        model_config = asyncio.run(engine_client.get_model_config())
        init_app_state(
            engine_client,
            model_config,
            app.state,
            self.vllm_args,
        )

        return app

    def deploy(
        self,
        name: Optional[str] = None,
        context: Optional[ConfigContext] = None,
        invocation_details_func: Optional[Callable[..., None]] = None,
        **invocation_details_options: Any,
    ) -> bool:
        self.name = name or self.name
        if not self.name:
            terminal.error(
                "You must specify an app name (either in the decorator or via the --name argument)."
            )

        if context is not None:
            self.config_context = context

        if not self.prepare_runtime(
            stub_type=ASGI_DEPLOYMENT_STUB_TYPE,
            force_create_stub=True,
        ):
            return False

        terminal.header("Deploying")
        deploy_response: DeployStubResponse = self.gateway_stub.deploy_stub(
            DeployStubRequest(stub_id=self.stub_id, name=self.name)
        )

        self.deployment_id = deploy_response.deployment_id
        if deploy_response.ok:
            terminal.header("Deployed 🎉")
            if invocation_details_func:
                invocation_details_func(**invocation_details_options)
            else:
                self.print_invocation_snippet(**invocation_details_options)

        return deploy_response.ok

    @with_grpc_error_handling
    def serve(self, timeout: int = 0, url_type: str = ""):
        if not self.prepare_runtime(stub_type=ASGI_SERVE_STUB_TYPE, force_create_stub=True):
            return False

        try:
            with terminal.progress("Serving endpoint..."):
                self.print_invocation_snippet(url_type=url_type)

                return self._serve(dir=os.getcwd(), object_id=self.object_id, timeout=timeout)

        except KeyboardInterrupt:
            self._handle_serve_interrupt()

    def _handle_serve_interrupt(self) -> None:
        terminal.header("Stopping serve container")
        self.endpoint_stub.stop_endpoint_serve(StopEndpointServeRequest(stub_id=self.stub_id))
        terminal.print("Goodbye 👋")
        os._exit(0)  # kills all threads immediately

    def _serve(self, *, dir: str, object_id: str, timeout: int = 0):
        def notify(*_, **__):
            self.endpoint_stub.endpoint_serve_keep_alive(
                EndpointServeKeepAliveRequest(
                    stub_id=self.stub_id,
                    timeout=timeout,
                )
            )

        threading.Thread(
            target=self.sync_dir_to_workspace,
            kwargs={"dir": dir, "object_id": object_id, "on_event": notify},
            daemon=True,
        ).start()

        r: Optional[StartEndpointServeResponse] = None
        for r in self.endpoint_stub.start_endpoint_serve(
            StartEndpointServeRequest(
                stub_id=self.stub_id,
                timeout=timeout,
            )
        ):
            if r.output != "":
                terminal.detail(r.output, end="")

            if r.done or r.exit_code != 0:
                break

        if r is None or not r.done or r.exit_code != 0:
            terminal.error("Serve container failed ❌")

        terminal.warn("VLLM serve timed out. Container has been stopped.")
