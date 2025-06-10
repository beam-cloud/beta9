import os
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Literal, Mapping, Optional, Tuple, Type, Union

from ... import terminal
from ...abstractions.base.container import Container
from ...abstractions.base.runner import ASGI_DEPLOYMENT_STUB_TYPE, ASGI_SERVE_STUB_TYPE
from ...abstractions.endpoint import ASGI
from ...abstractions.image import Image
from ...abstractions.volume import CloudBucket, Volume
from ...channel import with_grpc_error_handling
from ...clients.endpoint import StartEndpointServeRequest, StartEndpointServeResponse
from ...clients.gateway import DeployStubRequest, DeployStubResponse
from ...config import ConfigContext
from ...type import Autoscaler, GpuType, GpuTypeAlias, QueueDepthAutoscaler

DEFAULT_VLLM_CACHE_DIR = "./vllm_cache"
DEFAULT_VLLM_CACHE_ROOT = "./vllm_cache_root"


# vllm/engine/arg_utils.py:EngineArgs
@dataclass
class VLLMArgs:
    """
    The configuration for the vLLM engine. For more information, see the vllm documentation:
    https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html#command-line-arguments-for-the-server
    Each of these arguments corresponds to a command line argument for the vllm server.
    """

    # Args for init_app_state
    model: str = "facebook/opt-125m"
    max_log_len: Optional[int] = None
    chat_template: Optional[str] = None
    lora_modules: Optional[List[str]] = None
    prompt_adapters: Optional[List[str]] = None
    response_role: Optional[str] = "assistant"
    chat_template_content_format: str = "auto"
    return_tokens_as_token_ids: bool = False
    enable_auto_tool_choice: bool = False
    tool_call_parser: Optional[str] = None
    reasoning_parser: Optional[str] = None
    enable_prompt_tokens_details: bool = False
    enable_server_load_tracking: bool = False
    chat_template_url: Optional[str] = None
    tool_parser_plugin: Optional[str] = None

    # Args for AsyncEngineArgs
    skip_tokenizer_init: bool = False
    tokenizer_mode: str = "auto"
    task: str = "auto"
    trust_remote_code: bool = False
    download_dir: Optional[str] = DEFAULT_VLLM_CACHE_DIR
    load_format: str = "auto"
    config_format: str = "auto"
    dtype: str = "auto"
    kv_cache_dtype: str = "auto"
    seed: int = 0
    max_model_len: Optional[int] = None
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
    max_logprobs: int = 20
    revision: Optional[str] = None
    code_revision: Optional[str] = None
    rope_scaling: Optional[dict] = None
    rope_theta: Optional[float] = None
    tokenizer_revision: Optional[str] = None
    quantization: Optional[str] = None
    enforce_eager: Optional[bool] = None
    max_seq_len_to_capture: int = 8192
    disable_custom_all_reduce: bool = False
    tokenizer_pool_size: int = 0
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
    qlora_adapter_name_or_path: Optional[str] = None
    otlp_traces_endpoint: Optional[str] = None
    collect_detailed_traces: Optional[str] = None
    disable_async_output_proc: bool = False
    override_neuron_config: Optional[Dict[str, Any]] = None
    override_pooler_config: Optional[Any] = None
    mm_processor_kwargs: Optional[Dict[str, Any]] = None
    scheduling_policy: Literal["fcfs", "priority"] = "fcfs"
    disable_log_requests: bool = False
    kv_transfer_config: Optional[Any] = None
    logits_processor_pattern: Optional[str] = None
    max_long_partial_prefills: Optional[int] = 1
    disable_mm_preprocessor_cache: bool = False
    allowed_local_media_path: str = ""
    disable_log_stats: bool = False
    enable_expert_parallel: bool = False
    prefix_caching_hash_algo: str = "builtin"
    enable_lora_bias: bool = False
    use_tqdm_on_load: bool = False
    model_impl: str = "auto"
    worker_cls: str = "auto"
    additional_config: Optional[Dict[str, Any]] = None
    hf_token: Optional[Union[bool, str]] = None
    worker_extension_cls: str = ""
    served_model_name: Optional[Union[str, List[str]]] = None
    override_generation_config: Optional[Dict[str, Any]] = None
    show_hidden_metrics_for_version: Optional[str] = None
    enable_reasoning: Optional[bool] = None
    scheduler_cls: Union[str, Type[object]] = "vllm.core.scheduler.Scheduler"
    tokenizer: Optional[str] = None
    generation_config: Optional[str] = "auto"
    hf_overrides: Optional[Any] = None
    long_prefill_token_threshold: Optional[int] = 0
    max_num_partial_prefills: Optional[int] = 1
    calculate_kv_scales: Optional[bool] = None
    reasoning_parser: Optional[str] = None
    speculative_config: Optional[Dict[str, Any]] = None
    disable_cascade_attn: bool = False
    enable_sleep_mode: bool = False
    hf_config_path: Optional[str] = None
    disable_chunked_mm_input: bool = False
    data_parallel_size: int = 1
    compilation_config: Optional[Any] = None
    vllm_cache_root: Optional[str] = DEFAULT_VLLM_CACHE_ROOT


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
            The container image used for the task execution. Whatever you pass here will have an additional `add_python_packages` call
            with `["fastapi", "vllm", "huggingface_hub"]` added to it to ensure that we can run vLLM in the container.
        vllm_version (str):
            The version of vLLM that will be installed from PyPI. As the configuration of the vLLM engine depends on the version of vLLM, using a non-default vllm_version might require subclassing VLLMArgs in order to add the missing configuration options. Default is version 0.8.4.
        huggingface_hub_version (str):
            The version of huggingface_hub that will be installed from PyPI. Different versions of vLLM require different versions of huggingface_hub, thus using a non-default vLLM version might require using a non-default version of huggingface_hub.  Default is version 0.30.2.
        workers (int):
            The number of workers to run in the container. Default is 1.
        concurrent_requests (int):
            The maximum number of concurrent requests to handle. Default is 1.
        keep_warm_seconds (int):
            The number of seconds to keep the container warm after the last request. Default is 60.
        max_pending_tasks (int):
            The maximum number of pending tasks to allow in the container. Default is 100.
        timeout (int):
            The maximum number of seconds to wait for the container to start. Default is 3600.
        authorized (bool):
            Whether the endpoints require authorization. Default is True.
        name (str):
            The name of the container. Default is none, which means you must provide it during deployment.
        volumes (List[Union[Volume, CloudBucket]]):
            The volumes and/or cloud buckets to mount into the container. Default is a single volume named "vllm_cache" mounted to "./vllm_cache".
            It is used as the download directory for vLLM models.
        secrets (List[str]):
            The secrets to pass to the container. If you need huggingface authentication to download models, you should set HF_TOKEN in the secrets.
        autoscaler (Autoscaler):
            The autoscaler to use. Default is a queue depth autoscaler.
        vllm_args (VLLMArgs):
            The arguments for the vLLM model.

    Example:
        ```python
        from beta9 import integrations

        e = integrations.VLLMArgs()
        e.device = "cpu"
        e.chat_template = "./chatml.jinja"

        vllm_app = integrations.VLLM(name="vllm-abstraction-1", vllm_args=e)
        ```
    """

    def __init__(
        self,
        cpu: Union[int, float, str] = 1.0,
        memory: Union[int, str] = 128,
        gpu: Union[GpuTypeAlias, List[GpuTypeAlias]] = GpuType.NoGPU,
        gpu_count: int = 0,
        image: Image = Image(python_version="python3.11"),
        vllm_version: str = "0.8.4",
        huggingface_hub_version: str = "0.30.2",
        workers: int = 1,
        concurrent_requests: int = 1,
        keep_warm_seconds: int = 60,
        max_pending_tasks: int = 100,
        timeout: int = 3600,
        authorized: bool = True,
        name: Optional[str] = None,
        volumes: Optional[List[Union[Volume, CloudBucket]]] = [],
        secrets: Optional[List[str]] = None,
        autoscaler: Autoscaler = QueueDepthAutoscaler(),
        vllm_args: VLLMArgs = VLLMArgs(),
    ):
        if vllm_args.download_dir == DEFAULT_VLLM_CACHE_DIR:
            # Add default vllm cache volume to preserve it if custom volumes are specified for chat templates
            volumes.append(Volume(name="vllm_cache", mount_path=DEFAULT_VLLM_CACHE_DIR))

        volumes.append(Volume(name="vllm_cache_root", mount_path=vllm_args.vllm_cache_root))

        image = image.add_python_packages(
            [
                "fastapi",
                "numpy",
                f"vllm=={vllm_version}",
                f"huggingface_hub=={huggingface_hub_version}",
            ]
        )

        super().__init__(
            cpu=cpu,
            memory=memory,
            gpu=gpu,
            gpu_count=gpu_count,
            image=image,
            workers=workers,
            concurrent_requests=concurrent_requests,
            keep_warm_seconds=keep_warm_seconds,
            max_pending_tasks=max_pending_tasks,
            timeout=timeout,
            authorized=authorized,
            name=name,
            volumes=volumes,
            secrets=secrets,
            autoscaler=autoscaler,
        )

        self.chat_template_url = vllm_args.chat_template_url
        self.engine_args = vllm_args
        self.app_args = SimpleNamespace(
            model=vllm_args.model,
            served_model_name=vllm_args.served_model_name,
            disable_log_requests=vllm_args.disable_log_requests,
            max_log_len=vllm_args.max_log_len,
            disable_log_stats=vllm_args.disable_log_stats,
            chat_template=vllm_args.chat_template,
            lora_modules=vllm_args.lora_modules,
            prompt_adapters=vllm_args.prompt_adapters,
            response_role=vllm_args.response_role,
            chat_template_content_format=vllm_args.chat_template_content_format,
            return_tokens_as_token_ids=vllm_args.return_tokens_as_token_ids,
            enable_auto_tool_choice=vllm_args.enable_auto_tool_choice,
            tool_call_parser=vllm_args.tool_call_parser,
            enable_reasoning=vllm_args.enable_reasoning,
            reasoning_parser=vllm_args.reasoning_parser,
            enable_prompt_tokens_details=vllm_args.enable_prompt_tokens_details,
            enable_server_load_tracking=vllm_args.enable_server_load_tracking,
        )

    def __name__(self) -> str:
        return self.name or "vllm"

    def set_handler(self, handler: str):
        self.handler = handler

    def func(self, *args: Any, **kwargs: Any):
        pass

    def __call__(self, *args: Any, **kwargs: Any):
        import asyncio

        import vllm.entrypoints.openai.api_server as api_server
        from fastapi import FastAPI
        from vllm.engine.arg_utils import AsyncEngineArgs
        from vllm.engine.async_llm_engine import AsyncLLMEngine
        from vllm.entrypoints.openai.tool_parsers import ToolParserManager
        from vllm.usage.usage_lib import UsageContext

        if self.engine_args.vllm_cache_root:
            os.environ["VLLM_CACHE_ROOT"] = self.engine_args.vllm_cache_root

        if self.chat_template_url:
            import requests

            chat_template_filename = self.chat_template_url.split("/")[-1]

            if not os.path.exists(f"{self.engine_args.download_dir}/{chat_template_filename}"):
                response = requests.get(self.chat_template_url)
                with open(
                    f"{self.engine_args.download_dir}/{chat_template_filename}", "wb"
                ) as file:
                    file.write(response.content)

            self.app_args.chat_template = (
                f"{self.engine_args.download_dir}/{chat_template_filename}"
            )

        if "HF_TOKEN" in os.environ:
            hf_token = os.environ["HF_TOKEN"]
            import huggingface_hub

            huggingface_hub.login(hf_token)

        if self.engine_args.tool_parser_plugin and len(self.engine_args.tool_parser_plugin) > 3:
            ToolParserManager.import_tool_parser(self.engine_args.tool_parser_plugin)

        valide_tool_parses = ToolParserManager.tool_parsers.keys()
        if (
            self.engine_args.enable_auto_tool_choice
            and self.engine_args.tool_call_parser not in valide_tool_parses
        ):
            raise KeyError(
                f"invalid tool call parser: {self.engine_args.tool_call_parser} "
                f"(chose from {{ {','.join(valide_tool_parses)} }})"
            )

        app = FastAPI()

        @app.get("/health")
        async def health_check():
            return {"status": "healthy"}

        app.include_router(api_server.router)

        engine_args = AsyncEngineArgs.from_cli_args(self.engine_args)

        engine_client = AsyncLLMEngine.from_engine_args(
            engine_args, usage_context=UsageContext.OPENAI_API_SERVER
        )

        model_config = asyncio.run(engine_client.get_model_config())
        asyncio.run(
            api_server.init_app_state(
                engine_client,
                model_config,
                app.state,
                self.app_args,
            )
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

        if (
            self.engine_args.download_dir != DEFAULT_VLLM_CACHE_DIR
            and self.engine_args.download_dir not in [v.mount_path for v in self.volumes]
        ):
            terminal.error(
                "The engine's download directory must match a mount path in the volumes list."
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
        stub_type = ASGI_SERVE_STUB_TYPE

        if not self.prepare_runtime(func=self.func, stub_type=stub_type, force_create_stub=True):
            return False

        try:
            with terminal.progress("Serving endpoint..."):
                self.parent.print_invocation_snippet(url_type=url_type)

                return self._serve(dir=os.getcwd(), timeout=timeout)
        except KeyboardInterrupt:
            terminal.header("Stopping serve container")
            terminal.print("Goodbye 👋")
            os._exit(0)  # kills all threads immediately

    def _serve(self, *, dir: str, timeout: int = 0):
        r: StartEndpointServeResponse = self.endpoint_stub.start_endpoint_serve(
            StartEndpointServeRequest(
                stub_id=self.stub_id,
                timeout=timeout,
            )
        )
        if not r.ok:
            return terminal.error(r.error_msg)

        container = Container(container_id=r.container_id)
        container.attach(container_id=r.container_id, sync_dir=dir)
