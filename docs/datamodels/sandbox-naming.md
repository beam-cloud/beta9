# Sandbox Naming

The sandbox APIs use two identifiers:

- App namespace: groups related sandbox stubs.
- Container ID: identifies one running sandbox instance.

The backend does not persist a user-facing sandbox display name today.

## Current Behavior

In the Python SDK, this looks like a sandbox name:

```python
sandbox = Sandbox(name="test", image=Image())
sb = sandbox.create()
```

But `name="test"` does not label the running sandbox. It selects the app
namespace for the reusable sandbox stub. The live sandbox still gets a generated
`container_id`.

Backend flow today:

- `GetOrCreateStubRequest.name` is the stub name.
- `GetOrCreateStubRequest.app_name` is the app namespace.
- If `app_name` is empty, the backend falls back to `app_name = name`.
- `CreatePodRequest` only accepts `stub_id`, `image_id`, and `checkpoint_id`.
- `ContainerState` does not store a user-facing sandbox display name.

The backend supports app and stub grouping. It does not support a per-sandbox
display name.

## Problem

Users read `name` as "the sandbox I am creating." The SDK uses it as "the app
namespace for this sandbox."

This causes:

- Multiple created sandboxes appear to share the same user-provided name.
- The actual sandbox identity is only the generated container ID.
- There is no backend field where SDKs can send a per-sandbox display name.

## Target API

Keep app/stub semantics. Add a separate field for sandbox instance names.

New SDK calls:

```python
sandbox = Sandbox(app="test", sandbox_name="debug-run-001", image=Image())
```

```go
sandbox, err := client.CreateSandbox(ctx, beam.SandboxConfig{
    App:         "test",
    SandboxName: "debug-run-001",
    Image:       beam.NewImage(),
})
```

Semantics:

- `app`: app namespace/grouping.
- `sandbox_name`: display name for the created sandbox instance.
- `name`: retained as a backward-compatible alias for `app` in sandbox APIs.
- Stub name remains an implementation detail for sandboxes.

## Backend Work

Add a per-sandbox display-name path:

- Add `sandbox_name` or `display_name` to `CreatePodRequest`.
- Add `DisplayName` to `ContainerRequest`.
- Add `DisplayName` to `ContainerState` and `Container`.
- Persist the field in Redis container state.
- Return it from container list/status APIs.
- Include it in container events/log metadata where useful.

## Compatibility

Do not repurpose existing `name` to mean sandbox instance name. Existing code
like this should continue to group sandboxes under app namespace `test`:

```python
Sandbox(name="test")
```

New docs and examples should prefer:

```python
Sandbox(app="test")
Sandbox(app="test", sandbox_name="debug-run-001")
```
