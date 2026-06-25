import argparse
import time
import uuid
from threading import Lock
from typing import Any

import torch
import uvicorn
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from transformers import AutoModelForCausalLM, AutoTokenizer


MODEL = None
TOKENIZER = None
MODEL_ID = ""
SERVED_MODEL_NAME = ""
MODEL_LOCK = Lock()


class CompletionRequest(BaseModel):
    model: str | None = None
    prompt: str | list[str] = ""
    max_tokens: int = Field(default=32, ge=1, le=128)
    temperature: float = Field(default=0.0, ge=0.0, le=2.0)


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatCompletionRequest(BaseModel):
    model: str | None = None
    messages: list[ChatMessage]
    max_tokens: int = Field(default=32, ge=1, le=128)
    temperature: float = Field(default=0.0, ge=0.0, le=2.0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", default=8000, type=int)
    parser.add_argument("--model", default="sshleifer/tiny-gpt2")
    parser.add_argument("--served-model-name", default="")
    parser.add_argument("--max-model-len", default=1024, type=int)
    return parser.parse_args()


def load_model(model_id: str) -> None:
    global MODEL, TOKENIZER
    TOKENIZER = AutoTokenizer.from_pretrained(model_id)
    if TOKENIZER.pad_token_id is None:
        TOKENIZER.pad_token = TOKENIZER.eos_token
    MODEL = AutoModelForCausalLM.from_pretrained(model_id)
    MODEL.eval()


def generate_text(prompt: str, max_tokens: int, temperature: float) -> tuple[str, dict[str, int]]:
    assert MODEL is not None
    assert TOKENIZER is not None
    inputs = TOKENIZER(prompt, return_tensors="pt", truncation=True, max_length=1024)
    prompt_tokens = int(inputs["input_ids"].shape[-1])
    with MODEL_LOCK, torch.inference_mode():
        output = MODEL.generate(
            **inputs,
            max_new_tokens=max_tokens,
            do_sample=temperature > 0,
            temperature=max(temperature, 0.01),
            pad_token_id=TOKENIZER.eos_token_id,
        )
    completion_ids = output[0][prompt_tokens:]
    text = TOKENIZER.decode(completion_ids, skip_special_tokens=True)
    completion_tokens = int(completion_ids.shape[-1])
    usage = {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
    }
    return text.strip(), usage


app = FastAPI()
started_at = time.time()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/server_info")
def server_info() -> dict[str, Any]:
    return {
        "engine": "beam-smoke-vllm-shim",
        "model": MODEL_ID,
        "served_model_name": SERVED_MODEL_NAME,
    }


@app.get("/get_model_info")
def model_info() -> dict[str, str]:
    return {"model": MODEL_ID, "served_model_name": SERVED_MODEL_NAME}


@app.get("/metrics", response_class=PlainTextResponse)
def metrics() -> str:
    uptime = max(time.time() - started_at, 0)
    return f"beam_llm_smoke_uptime_seconds {uptime:.3f}\n"


@app.get("/v1/models")
def models() -> dict[str, Any]:
    return {
        "object": "list",
        "data": [
            {
                "id": SERVED_MODEL_NAME,
                "object": "model",
                "created": int(started_at),
                "owned_by": "beam",
            }
        ],
    }


@app.post("/v1/completions")
def completions(request: CompletionRequest) -> dict[str, Any]:
    prompt = request.prompt[0] if isinstance(request.prompt, list) else request.prompt
    text, usage = generate_text(prompt, request.max_tokens, request.temperature)
    return {
        "id": f"cmpl-{uuid.uuid4().hex}",
        "object": "text_completion",
        "created": int(time.time()),
        "model": request.model or SERVED_MODEL_NAME,
        "choices": [{"index": 0, "text": text, "finish_reason": "length"}],
        "usage": usage,
    }


@app.post("/v1/chat/completions")
def chat_completions(request: ChatCompletionRequest) -> dict[str, Any]:
    prompt = "\n".join(f"{message.role}: {message.content}" for message in request.messages)
    text, usage = generate_text(prompt + "\nassistant:", request.max_tokens, request.temperature)
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": request.model or SERVED_MODEL_NAME,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "length",
            }
        ],
        "usage": usage,
    }


def main() -> None:
    global MODEL_ID, SERVED_MODEL_NAME
    args = parse_args()
    MODEL_ID = args.model
    SERVED_MODEL_NAME = args.served_model_name or args.model
    load_model(MODEL_ID)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
