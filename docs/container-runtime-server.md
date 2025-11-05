# Container Runtime Server

## Overview

The `ContainerRuntimeServer` is a unified, runtime-agnostic server implementation that provides container management services via gRPC. It replaces the previous `RunCServer` and works seamlessly with any OCI-compatible container runtime (runc, gVisor, etc.).

## Architecture

### Design Principles

1. **Runtime Agnostic**: The server doesn't know or care which runtime is being used. It delegates all container operations to the runtime associated with each container instance.

2. **Per-Container Runtime**: Each container stores a reference to its runtime (`runtime.Runtime`), allowing different containers on the same worker to use different runtimes.

3. **Backward Compatible**: The server implements the existing `RunCService` gRPC interface, ensuring no breaking changes to clients.

4. **Unified Implementation**: All container operations (exec, kill, status, checkpoint, sandbox, etc.) are handled in a single server implementation.

## Key Components

### ContainerRuntimeServer

The main server struct that manages container operations:

```go
type ContainerRuntimeServer struct {
    baseConfigSpec          specs.Spec
    containerInstances      *common.SafeMap[*ContainerInstance]
    containerRepoClient     pb.ContainerRepositoryServiceClient
    containerNetworkManager *ContainerNetworkManager
    imageClient             *ImageClient
    port                    int
    podAddr                 string
    createCheckpoint        func(ctx context.Context, opts *CreateCheckpointOpts) error
    grpcServer              *grpc.Server
    mu                      sync.Mutex
}
```

### Runtime Resolution

The server uses a helper method to retrieve the runtime for each container:

```go
func (s *ContainerRuntimeServer) getContainerRuntime(containerID string) (runtime.Runtime, error)
```

This method:
1. Looks up the container instance
2. Returns the runtime associated with that container
3. Returns an error if the container or runtime is not found

## Core Operations

### Container Lifecycle

- **RunCKill**: Terminates and removes a container using its configured runtime
- **RunCExec**: Executes commands inside a running container
- **RunCStatus**: Returns the current state of a container
- **RunCStreamLogs**: Streams container logs in real-time

### Checkpoint/Restore

- **RunCCheckpoint**: Creates a checkpoint of a container
  - Automatically checks runtime capabilities
  - Returns an error if the runtime doesn't support checkpointing
  - Works seamlessly with runc (CRIU support) and gracefully degrades with gVisor

### Image Management

- **RunCArchive**: Archives a container's filesystem as an image
  - Runtime-agnostic implementation
  - Uses the container's runtime to check state
  - Preserves all filesystem changes from the overlay

### Workspace Sync

- **RunCSyncWorkspace**: Synchronizes workspace files
  - Supports write, delete, and move operations
  - Works with the container's filesystem regardless of runtime

### Sandbox Operations

The server provides comprehensive sandbox functionality:

#### Process Management
- **RunCSandboxExec**: Execute commands in the container
- **RunCSandboxStatus**: Get process status
- **RunCSandboxKill**: Terminate a process
- **RunCSandboxListProcesses**: List all running processes

#### File System Operations
- **RunCSandboxUploadFile**: Upload files to container
- **RunCSandboxDownloadFile**: Download files from container
- **RunCSandboxDeleteFile**: Delete files in container
- **RunCSandboxStatFile**: Get file information
- **RunCSandboxListFiles**: List directory contents
- **RunCSandboxCreateDirectory**: Create directories
- **RunCSandboxDeleteDirectory**: Delete directories

#### Search and Replace
- **RunCSandboxFindInFiles**: Search for patterns using ripgrep
- **RunCSandboxReplaceInFiles**: Replace patterns in files

#### Networking
- **RunCSandboxExposePort**: Dynamically expose container ports
- **RunCSandboxListExposedPorts**: List all exposed ports

## Runtime Capability Gating

The server respects runtime capabilities for feature gating:

### Checkpoint Support

```go
if instance.Runtime != nil && !instance.Runtime.Capabilities().CheckpointRestore {
    return &pb.RunCCheckpointResponse{
        Ok:       false,
        ErrorMsg: fmt.Sprintf("Runtime %s does not support checkpoint/restore", instance.Runtime.Name()),
    }, nil
}
```

### Future Extensions

As more capabilities are added to the `runtime.Capabilities` struct, the server can gate additional features:

- GPU operations (already handled at worker level)
- OOM event monitoring
- Network namespace operations
- Device injection (CDI)

## Migration from RunCServer

The refactoring involved:

1. **Renaming**: `RunCServer` → `ContainerRuntimeServer`
2. **Removing Direct Runtime Calls**: All direct `go-runc` calls were replaced with `runtime.Runtime` interface calls
3. **Runtime Retrieval**: Added `getContainerRuntime()` to fetch the container's runtime
4. **Capability Checks**: Added runtime capability checks for features like checkpointing

### Before (RunCServer)

```go
// Direct runc usage
state, err := s.runcHandle.State(ctx, in.ContainerId)
```

### After (ContainerRuntimeServer)

```go
// Runtime interface usage
rt, err := s.getContainerRuntime(in.ContainerId)
if err != nil {
    return nil, err
}
state, err := rt.State(ctx, in.ContainerId)
```

## Benefits

1. **Flexibility**: Easy to add new runtimes without touching server code
2. **Per-Container Runtimes**: Different containers can use different runtimes
3. **Clean Abstraction**: Server logic is separated from runtime implementation
4. **Testability**: Runtime interface can be mocked for testing
5. **Graceful Degradation**: Features automatically degrade based on runtime capabilities

## Usage

### Initialization

```go
server, err := NewContainerRuntimeServer(&ContainerRuntimeServerOpts{
    PodAddr:                 podAddr,
    ContainerInstances:      containerInstances,
    ImageClient:             imageClient,
    ContainerRepoClient:     containerRepoClient,
    ContainerNetworkManager: containerNetworkManager,
    CreateCheckpoint:        checkpointFunc,
})
if err != nil {
    return err
}

if err := server.Start(); err != nil {
    return err
}
```

### Container Runtime Selection

The runtime for each container is selected at container creation time (in `Worker.RunContainer`):

```go
selectedRuntime := s.selectRuntime(request)

containerInstance := &ContainerInstance{
    Runtime:   selectedRuntime,
    // ... other fields
}
```

The server then uses this stored runtime for all operations on that container.

## Best Practices

1. **Always Check Runtime**: Use `getContainerRuntime()` at the start of any operation
2. **Respect Capabilities**: Check `runtime.Capabilities()` before attempting operations
3. **Graceful Errors**: Return user-friendly errors when capabilities are missing
4. **Log Runtime Selection**: Log which runtime is being used for debugging
5. **Handle Nil Runtime**: Always handle cases where runtime might not be set

## Future Improvements

1. **Runtime Metrics**: Add metrics for runtime-specific operations
2. **Runtime Health Checks**: Periodic checks for runtime availability
3. **Dynamic Runtime Switching**: Support changing runtime for stopped containers
4. **Runtime Configuration**: Per-request runtime preferences via API
5. **Multi-Runtime Pools**: Workers that support multiple runtimes simultaneously

## Troubleshooting

### Container Not Found

```
Error: container not found: <container_id>
```

**Cause**: Container instance doesn't exist or was already removed.

**Solution**: Check that the container was successfully created and is still running.

### Runtime Not Initialized

```
Error: container runtime not initialized for: <container_id>
```

**Cause**: Container instance exists but has no associated runtime.

**Solution**: This indicates a bug in container creation. Check worker logs during container startup.

### Capability Not Supported

```
Error: Runtime gvisor does not support checkpoint/restore
```

**Cause**: Attempting an operation not supported by the container's runtime.

**Solution**: This is expected behavior. Use a different runtime (runc) for operations requiring this capability.

## Related Documentation

- [Container Runtime Configuration](./container-runtime-configuration.md) - Worker pool runtime settings
- [gVisor Dockerfile Changes](./gvisor-dockerfile-changes.md) - Building with gVisor support
- [Runtime Package](../pkg/runtime/README.md) - Runtime interface and implementations
