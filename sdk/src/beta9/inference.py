"""
Beta9 Inference Module

Simple API for calling inference endpoints in a Beta9 cluster.
Routes requests to nodes running Ollama (Mac/MPS) or vLLM/SGLang (Linux/CUDA).

Example:
    from beta9 import inference

    # Chat completion
    result = inference.chat(
        model="llama3.2",
        messages=[{"role": "user", "content": "Hello!"}]
    )
    print(result.content)

    # Simple text generation
    result = inference.generate(
        model="llama3.2",
        prompt="Once upon a time"
    )
    print(result.content)

    # Embeddings
    embedding = inference.embed(
        model="nomic-embed-text",
        input="Hello world"
    )
    print(embedding.embedding)

    # List available models
    models = inference.list_models()
    for m in models:
        print(f"{m.name}: {m.load_state}")
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

import httpx

# Default inference endpoint (Beta9 Gateway)
DEFAULT_INFERENCE_HOST = os.getenv("BETA9_INFERENCE_HOST", "localhost")
DEFAULT_INFERENCE_PORT = int(os.getenv("BETA9_INFERENCE_PORT", "1994"))  # Gateway HTTP port


@dataclass
class ChatMessage:
    """A chat message."""

    role: str  # "system", "user", "assistant"
    content: str

    def to_dict(self) -> Dict[str, str]:
        return {"role": self.role, "content": self.content}


@dataclass
class InferenceOptions:
    """Optional inference parameters."""

    temperature: float = 0.7
    max_tokens: Optional[int] = None
    stream: bool = False
    top_p: float = 1.0
    frequency_penalty: float = 0.0
    presence_penalty: float = 0.0


@dataclass
class InferenceResult:
    """Result from an inference call."""

    model: str
    content: str
    finish_reason: Optional[str] = None
    usage: Optional[Dict[str, int]] = None
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass
class EmbeddingResult:
    """Result from an embedding call."""

    model: str
    embedding: List[float] = field(default_factory=list)  # Legacy: First embedding if batch
    embeddings: List[List[float]] = field(default_factory=list)  # New: All embeddings
    usage: Optional[Dict[str, int]] = None
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass
class ModelInfo:
    """Information about a loaded model."""

    name: str
    load_state: str  # "idle", "loading", "ready", "error"
    size_gb: float = 0.0
    last_used: Optional[str] = None
    error: Optional[str] = None


class InferenceClient:
    """Client for Beta9 inference endpoints."""

    def __init__(
        self,
        host: str = DEFAULT_INFERENCE_HOST,
        port: int = DEFAULT_INFERENCE_PORT,
        token: Optional[str] = None,
        timeout: float = 300.0,  # 5 minute timeout for slow model loads
    ):
        self.base_url = f"http://{host}:{port}"
        self.timeout = timeout
        
        # Prioritize explicit token, then env var
        self.token = token or os.getenv("BETA9_INFERENCE_TOKEN")
        
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
            
        self._client = httpx.Client(timeout=timeout, headers=headers)

    def chat(
        self,
        model: str,
        messages: List[Union[ChatMessage, Dict[str, str]]],
        options: Optional[InferenceOptions] = None,
    ) -> InferenceResult:
        """
        Send a chat completion request.

        Args:
            model: Model name (e.g., "llama3.2", "codellama:13b")
            messages: List of chat messages
            options: Optional inference parameters

        Returns:
            InferenceResult with the model's response
        """
        if options is None:
            options = InferenceOptions()

        # Convert ChatMessage objects to dicts
        msg_list = []
        for m in messages:
            if isinstance(m, ChatMessage):
                msg_list.append(m.to_dict())
            else:
                msg_list.append(m)

        payload = {
            "model": model,
            "messages": msg_list,
            "stream": options.stream,
            "options": {
                "temperature": options.temperature,
                "top_p": options.top_p,
            },
        }
        if options.max_tokens is not None:
            payload["options"]["num_predict"] = options.max_tokens
        if options.frequency_penalty != 0:
            payload["options"]["frequency_penalty"] = options.frequency_penalty
        if options.presence_penalty != 0:
            payload["options"]["presence_penalty"] = options.presence_penalty

        try:
            resp = self._client.post(f"{self.base_url}/api/chat", json=payload)
            resp.raise_for_status()
            data = resp.json()

            return InferenceResult(
                model=model,
                content=data.get("message", {}).get("content", ""),
                finish_reason=data.get("done_reason"),
                usage={
                    "prompt_tokens": data.get("prompt_eval_count", 0),
                    "completion_tokens": data.get("eval_count", 0),
                },
            )
        except httpx.HTTPStatusError as e:
            return InferenceResult(
                model=model,
                content="",
                error=f"HTTP {e.response.status_code}: {e.response.text}",
            )
        except Exception as e:
            return InferenceResult(
                model=model,
                content="",
                error=str(e),
            )

    def generate(
        self,
        model: str,
        prompt: str,
        options: Optional[InferenceOptions] = None,
    ) -> InferenceResult:
        """
        Send a text generation request.

        Args:
            model: Model name
            prompt: Text prompt
            options: Optional inference parameters

        Returns:
            InferenceResult with the generated text
        """
        if options is None:
            options = InferenceOptions()

        payload = {
            "model": model,
            "prompt": prompt,
            "stream": options.stream,
        }

        # Add all options to payload
        payload["options"] = {
            "temperature": options.temperature,
            "top_p": options.top_p,
            "frequency_penalty": options.frequency_penalty,
            "presence_penalty": options.presence_penalty,
        }
        if options.max_tokens is not None:
            payload["options"]["num_predict"] = options.max_tokens

        try:
            resp = self._client.post(f"{self.base_url}/api/generate", json=payload)
            resp.raise_for_status()
            data = resp.json()

            return InferenceResult(
                model=model,
                content=data.get("response", ""),
                finish_reason=data.get("done_reason"),
                usage={
                    "prompt_tokens": data.get("prompt_eval_count", 0),
                    "completion_tokens": data.get("eval_count", 0),
                },
            )
        except httpx.HTTPStatusError as e:
            return InferenceResult(
                model=model,
                content="",
                error=f"HTTP {e.response.status_code}: {e.response.text}",
            )
        except Exception as e:
            return InferenceResult(
                model=model,
                content="",
                error=str(e),
            )

    def embed(
        self,
        model: str,
        input: Union[str, List[str]],
    ) -> EmbeddingResult:
        """
        Generate embeddings for text.

        Args:
            model: Embedding model name (e.g., "nomic-embed-text")
            input: Text or list of texts to embed

        Returns:
            EmbeddingResult with the embedding vector(s)
        """
        # Handle single string
        if isinstance(input, str):
            input = [input]

        payload = {
            "model": model,
            "input": input,
        }

        try:
            resp = self._client.post(f"{self.base_url}/api/embed", json=payload)
            resp.raise_for_status()
            data = resp.json()

            # Ollama returns embeddings in different formats depending on version
            embeddings = data.get("embeddings", [])
            if len(embeddings) == 0:
                # Fallback for older Ollama versions
                single = data.get("embedding", [])
                embeddings = [single] if single else []

            # Return all embeddings for batch input
            return EmbeddingResult(
                model=model,
                embedding=embeddings[0] if len(embeddings) == 1 else embeddings,
                usage={
                    "prompt_tokens": data.get("prompt_eval_count", 0),
                },
            )
        except httpx.HTTPStatusError as e:
            return EmbeddingResult(
                model=model,
                error=f"HTTP {e.response.status_code}: {e.response.text}",
            )
        except Exception as e:
            return EmbeddingResult(
                model=model,
                error=str(e),
            )

    def list_models(self) -> List[ModelInfo]:
        """
        List available models on the inference server.

        Returns:
            List of ModelInfo objects
        """
        try:
            resp = self._client.get(f"{self.base_url}/api/tags")
            resp.raise_for_status()
            data = resp.json()

            models = []
            for m in data.get("models", []):
                size_bytes = m.get("size", 0)
                size_gb = size_bytes / (1024 * 1024 * 1024) if size_bytes else 0

                models.append(
                    ModelInfo(
                        name=m.get("name", ""),
                        load_state="ready",  # If it's in tags, it's available
                        size_gb=round(size_gb, 2),
                        last_used=m.get("modified_at"),
                    )
                )
            return models
        except Exception:
            return []

    def load_model(self, model: str, keep_alive: int = -1) -> bool:
        """
        Pre-load a model into memory.

        Args:
            model: Model name to load
            keep_alive: How long to keep model in memory (-1 = forever, 0 = unload immediately)

        Returns:
            True if successful
        """
        payload = {
            "model": model,
            "prompt": "",
            "keep_alive": keep_alive,
        }

        try:
            resp = self._client.post(f"{self.base_url}/api/generate", json=payload)
            resp.raise_for_status()
            return True
        except Exception:
            return False

    def unload_model(self, model: str) -> bool:
        """
        Unload a model from memory.

        Args:
            model: Model name to unload

        Returns:
            True if successful
        """
        return self.load_model(model, keep_alive=0)

    def health(self) -> bool:
        """Check if the inference server is healthy."""
        try:
            resp = self._client.get(f"{self.base_url}/api/tags")
            return resp.status_code == 200
        except Exception:
            return False


# Global default client
_default_client: Optional[InferenceClient] = None


def get_client() -> InferenceClient:
    """Get the default inference client."""
    global _default_client
    if _default_client is None:
        _default_client = InferenceClient()
    return _default_client


def configure(
    host: str = DEFAULT_INFERENCE_HOST,
    port: int = DEFAULT_INFERENCE_PORT,
    token: Optional[str] = None,
    timeout: float = 300.0,
) -> None:
    """
    Configure the default inference client.

    Args:
        host: Inference server hostname
        port: Inference server port
        token: Authentication token (optional)
        timeout: Request timeout in seconds
    """
    global _default_client
    # Close previous client to prevent socket leaks
    if _default_client is not None:
        try:
            _default_client._client.close()
        except Exception:
            pass
    _default_client = InferenceClient(host=host, port=port, token=token, timeout=timeout)


# Convenience functions using the default client


def chat(
    model: str,
    messages: List[Union[ChatMessage, Dict[str, str]]],
    options: Optional[InferenceOptions] = None,
) -> InferenceResult:
    """Send a chat completion request using the default client."""
    return get_client().chat(model, messages, options)


def generate(
    model: str,
    prompt: str,
    options: Optional[InferenceOptions] = None,
) -> InferenceResult:
    """Send a text generation request using the default client."""
    return get_client().generate(model, prompt, options)


def embed(
    model: str,
    input: Union[str, List[str]],
) -> EmbeddingResult:
    """Generate embeddings using the default client."""
    return get_client().embed(model, input)


def list_models() -> List[ModelInfo]:
    """List available models using the default client."""
    return get_client().list_models()


def load_model(model: str, keep_alive: int = -1) -> bool:
    """Pre-load a model using the default client."""
    return get_client().load_model(model, keep_alive)


def unload_model(model: str) -> bool:
    """Unload a model using the default client."""
    return get_client().unload_model(model)


def health() -> bool:
    """Check server health using the default client."""
    return get_client().health()
