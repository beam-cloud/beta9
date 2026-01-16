# Beta9 - Distributed GPU Compute Platform

**A fork of [beam-cloud/beta9](https://github.com/beam-cloud/beta9) for [Agentosaurus](https://agentosaurus.com)**

Beta9 is an open-source distributed GPU compute platform enabling serverless AI workloads across heterogeneous hardware. This fork extends the original with support for external GPU workers, Apple Silicon (MPS) inference, and a unified control API for inference lifecycle management.

## Project Status

**Work in Progress** - This fork is under active development as the compute infrastructure layer for Agentosaurus, a platform focused on democratizing GPU access for climate research and AI workloads within the European Union.

## Vision

Build a distributed GPU compute platform that:

- **Brings Your Own GPU**: Connect any machine (cloud VMs, workstations, Mac Studios) to a unified compute pool
- **Supports Heterogeneous Hardware**: NVIDIA CUDA, Apple MPS, AMD ROCm (planned), Intel Arc (planned)
- **Enables Serverless by Default**: Scale to zero, pay only for compute time used
- **Maintains EU Data Sovereignty**: All data and compute remains within European infrastructure
- **Prioritizes Carbon Efficiency**: Verified renewable energy usage with transparent carbon reporting

## Architecture

```
                          TAILSCALE MESH VPN
            (Encrypted overlay network - 100.x.x.x addressing)
    ┌─────────────────────────────────────────────────────────────┐
    │                                                             │
    ▼                           ▼                           ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   GATEWAY       │     │   WORKER 1      │     │   WORKER 2      │
│   (OCI Cloud)   │     │   (Mac MPS)     │     │   (NVIDIA GPU)  │
│                 │     │                 │     │                 │
│  ┌───────────┐  │     │  ┌───────────┐  │     │  ┌───────────┐  │
│  │ Gateway   │◄─┼─────┼──│ b9agent   │  │     │  │ b9agent   │  │
│  │ :1993/94  │  │     │  │ :9999     │  │     │  │ :9999     │  │
│  └───────────┘  │     │  └─────┬─────┘  │     │  └─────┬─────┘  │
│        │        │     │        │        │     │        │        │
│        ▼        │     │        ▼        │     │        ▼        │
│  ┌───────────┐  │     │  ┌───────────┐  │     │  ┌───────────┐  │
│  │ k3s API   │◄─┼─────┼──│ Ollama    │  │     │  │ vLLM      │  │
│  │ Scheduler │  │     │  │ :11434    │  │     │  │ :8000     │  │
│  └───────────┘  │     │  └───────────┘  │     │  └───────────┘  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
     Control Plane           MPS Inference         CUDA Inference
```

## Key Features

### External Worker Support

Connect machines outside your Kubernetes cluster to participate in distributed job execution:

- HTTP-based machine registration with the gateway
- Keepalive heartbeats to maintain worker pool membership
- Tailscale mesh VPN for secure machine-to-machine connectivity
- Real-time TUI dashboard showing worker status and job execution

### Apple Silicon (MPS) Inference

Native support for Apple Silicon GPUs via Metal Performance Shaders:

- Ollama-based inference server management
- Automatic Tailscale IP binding for mesh accessibility
- Control API for inference lifecycle (start/stop/pull models)
- Model pull progress streaming to TUI logs

### Control API

HTTP control server (port 9999) for external management:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/inference/start` | POST | Start Ollama inference server |
| `/inference/stop` | POST | Stop inference server |
| `/inference/status` | GET | Get inference server status |
| `/inference/pull` | POST | Pull model with progress logging |
| `/status` | GET | Get agent status |
| `/health` | GET | Health check |

### TUI Dashboard

Real-time terminal interface showing:

```
╔══ Beta9 Agent: 1c1b50c8 ═══════════════════════════════════════════════╗
║ Status: READY │ Gateway: http://100.72.101.23:1994 │ Pool: external    ║
║ CPU: 28.1% │ Memory: 78.1% │ GPUs: 0 │ Last Heartbeat: 25s ago         ║
╠════════════════════════════════════════════════════════════════════════╣
║ WORKER PODS                                                            ║
╟────────────────────────────────────────────────────────────────────────╢
║ No jobs yet                                                            ║
╠════════════════════════════════════════════════════════════════════════╣
║ INFERENCE                                                              ║
╟────────────────────────────────────────────────────────────────────────╢
║ Status: running │ Endpoint: 100.100.74.117:11434                       ║
║ Models: gemma3:1b                                                      ║
╠════════════════════════════════════════════════════════════════════════╣
║ LOGS                                                                   ║
╟────────────────────────────────────────────────────────────────────────╢
║ 10:15:23 Control API listening on :9999                                ║
║ 10:15:24 Inference: starting Ollama...                                 ║
║ 10:15:26 Inference: ready on :11434                                    ║
╚════════════════════════════════════════════════════════════════════════╝
Press Ctrl+C to quit
```

## Quick Start

### Prerequisites

- Go 1.21+ (for building the agent)
- Tailscale account and network
- Ollama (for Mac inference)

### 1. Build the Agent

```bash
cd backend/beta9
go build ./cmd/b9agent/...
```

### 2. Initialize Configuration

```bash
./b9agent init \
  --gateway <GATEWAY_TAILSCALE_IP>:1994 \
  --token <MACHINE_TOKEN> \
  --pool external
```

### 3. Start the Agent

```bash
./b9agent
```

### 4. Test Inference (from remote machine)

```bash
# Test inference pipeline
TEST_MODEL=llama3.2 ./backend/remote_servers/scripts/dgpu/test_inference.sh
```

## Configuration

Agent configuration is stored in `~/.b9agent/config.yaml`:

```yaml
gateway:
  host: "100.72.101.23"
  port: 1994
machine:
  id: "1c1b50c8"
  token: "<machine-token>"
  hostname: "100.100.74.117"
pool: "external"
k3s:
  token: "<k3s-bearer-token>"
```

## Python SDK

The inference module provides a lightweight client for inference endpoints:

```python
from beta9 import inference

# Configure endpoint
inference.configure(host="100.100.74.117", port=11434)

# Chat completion
result = inference.chat(
    model="llama3.2",
    messages=[{"role": "user", "content": "Hello!"}]
)
print(result.content)

# Text generation
result = inference.generate(
    model="llama3.2",
    prompt="Once upon a time"
)

# Embeddings
embedding = inference.embed(
    model="nomic-embed-text",
    input="Hello world"
)

# List models
models = inference.list_models()
```

## Testing

Run the inference test suite:

```bash
# Default model (llama3.2)
./backend/remote_servers/scripts/dgpu/test_inference.sh

# Custom model
TEST_MODEL=gemma3:1b ./backend/remote_servers/scripts/dgpu/test_inference.sh

# Custom host
BETA9_INFERENCE_HOST=100.100.74.117 ./backend/remote_servers/scripts/dgpu/test_inference.sh
```

Test output:

```
[0/6] Sending start-inference command to agent... ✓
[1/6] Testing health endpoint...                  ✓
[2/6] Checking model availability...              ✓
[3/6] Testing chat via curl...                    ✓
[4/6] Testing Python SDK...                       ✓
[5/6] Testing latency (3 requests)...             ✓
[6/6] Stopping inference server...                ✓
```

## Roadmap

### Phase 1: Foundation (Current)

- [x] External worker registration and keepalive
- [x] Apple Silicon (MPS) inference via Ollama
- [x] Control API for inference lifecycle
- [x] TUI dashboard with inference status and logs
- [x] Python SDK for inference

### Phase 2: Multi-Backend Inference

- [ ] vLLM integration for NVIDIA GPUs
- [ ] SGLang integration for structured outputs
- [ ] Model routing based on hardware capabilities
- [ ] Automatic model format conversion (GGUF/Safetensors)

### Phase 3: Production Hardening

- [ ] Prometheus metrics export
- [ ] Carbon footprint tracking
- [ ] Rate limiting and quotas
- [ ] Multi-tenant isolation

### Phase 4: Enterprise Features

- [ ] EU AI Act compliance reporting
- [ ] Model provenance tracking
- [ ] Audit logging
- [ ] SSO integration

## Project Structure

```
beta9/
├── cmd/
│   └── b9agent/           # Go agent binary
│       └── main.go
├── pkg/
│   └── agent/
│       ├── agent.go       # Agent lifecycle management
│       ├── control.go     # HTTP control API
│       ├── inference.go   # OllamaManager and inference types
│       ├── state.go       # Agent state for TUI
│       └── tui.go         # Terminal UI rendering
├── sdk/
│   └── src/
│       └── beta9/
│           └── inference.py  # Python inference SDK
└── docs/
    └── external-workers/     # External worker documentation
```

## Related Projects

- **[Agentosaurus](https://agentosaurus.com)** - Organization discovery platform and parent project
- **[FlowState](https://github.com/Wingie/flowstate-agents)** - AI presentation system using this compute layer
- **[beam-cloud/beta9](https://github.com/beam-cloud/beta9)** - Upstream project

## License

This fork maintains the same AGPL-3.0 license as the upstream beta9 project.

## Contributing

Contributions are welcome. Please open an issue to discuss proposed changes before submitting a pull request.

## Acknowledgments

- [Beam Cloud](https://beam.cloud) for the original beta9 project
- [Ollama](https://ollama.ai) for the inference server
- [Tailscale](https://tailscale.com) for the mesh VPN infrastructure

## Security & Networking

Beta9 uses **Tailscale** for secure mesh networking between components.

- **Network Isolation:** All traffic between Gateway, Workers, and Clients travels over an encrypted WireGuard mesh.
- **Endpoint Security:** 
  - Internal management endpoints (inference control, keepalives) are bound to `0.0.0.0` but are effectively protected because the nodes are only reachable via the private Tailscale network.
  - The Gateway uses `hostNetwork: true` to expose these services directly to the mesh.
  - **Note:** Do not expose the Gateway's ports (1993, 1994) to the public internet. Access should only be possible via the Tailscale mesh or a secure ingress.

## Known Limitations & Roadmap

The following security enhancements are planned for the upcoming **Security Epic** but are currently mitigated by the network architecture:

1.  **Inference Endpoint Auth (`beta9-b1j`)**: Direct inference endpoints (port 11434/8000) currently do not require per-request authentication.
    - *Mitigation*: These ports are only accessible within the encrypted Tailscale mesh. Workers are isolated from the public internet.
2.  **RBAC Scoping (`beta9-7aw`)**: The k3d manifest uses broad ClusterRole permissions for simplicity during beta.
    - *Mitigation*: The cluster is intended for single-tenant use. Scoped RBAC profiles will be introduced in Phase 3.
3.  **Token Binding (`beta9-bx0`)**: Keepalive tokens are not strictly bound to machine IDs in the current implementation.
    - *Mitigation*: Tokens are secret and transmitted over encrypted channels. Token binding will be enforced in the next auth refactor.
