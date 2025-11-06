# Quick Reference - gVisor + GPU Support

## On Worker Pod

### List Containers
```bash
runsc list                    # List all gVisor containers
runsc-list                    # Alias
runsc-ps                      # Alias
```

### Inspect Container
```bash
runsc state <container-id>    # Get state JSON
runsc ps <container-id>       # List processes
runsc events <container-id>   # Stream events
```

### Container Operations
```bash
runsc kill <container-id> SIGTERM   # Kill container
runsc delete <container-id>         # Delete container
```

### Debug
```bash
runsc --debug list            # Verbose output
runsc.real list               # Bypass wrapper (raw binary)
```

---

## Environment Variables

Set in worker pod automatically:
```bash
GVISOR_ROOT=/run/gvisor       # Where runsc stores state
GVISOR_PLATFORM=systrap       # Platform for execution
```

---

## GPU Testing

### Check GPU Available (in container)
```bash
nvidia-smi
ls -la /dev/nvidia*
```

### Quick CUDA Test (in container)
```python
import torch
assert torch.cuda.is_available()
print(f"GPU: {torch.cuda.get_device_name(0)}")
```

### Verify nvproxy Active (on worker)
```bash
kubectl logs <worker-pod> | grep nvproxy
ps aux | grep -- --nvproxy=true
```

---

## Configuration

### Worker Pool Config
```yaml
worker:
  pools:
    gpu:
      runtime: nvidia              # Pod RuntimeClass
      containerRuntime: gvisor     # Container runtime
      containerRuntimeConfig:
        gvisorPlatform: systrap
        gvisorRoot: /run/gvisor
      gpuType: A100
```

---

## Files Reference

| File | Purpose |
|------|---------|
| `/usr/local/bin/runsc` | Wrapper script (auto-adds flags) |
| `/usr/local/bin/runsc.real` | Real runsc binary |
| `/run/gvisor/` | Container state directory |

---

## Common Issues

### "No containers" when running `runsc list`
✅ **Fixed!** The wrapper now automatically uses correct `--root` flag.

### GPU not detected in container
- Check pod has `RuntimeClass: nvidia`
- Check pod requests `nvidia.com/gpu: 1`
- Check host has NVIDIA driver
- Check container image has CUDA libraries

### Want to use raw runsc
```bash
runsc.real [args...]          # Bypass wrapper
runsc-raw [args...]           # Alias
```

---

## Quick Verification

```bash
# On worker pod
runsc list                    # Should show containers
echo $GVISOR_ROOT            # Should be /run/gvisor
echo $GVISOR_PLATFORM        # Should be systrap

# In GPU container
nvidia-smi                    # Should show GPU
python -c "import torch; print(torch.cuda.is_available())"  # Should be True
```

---

## Documentation

- **Complete Implementation**: `IMPLEMENTATION_COMPLETE.md`
- **Wrapper Usage**: `docs/runsc-wrapper-usage.md`
- **GPU/nvproxy**: `docs/gvisor-nvproxy-implementation.md`
- **GPU Requirements**: `docs/nvproxy-requirements.md`
- **Troubleshooting**: `docs/troubleshooting-runsc-list.md`
- **Verification**: `VERIFICATION_GUIDE.md`

---

## Summary

✅ **runsc commands**: Work automatically without flags  
✅ **GPU support**: Automatic nvproxy detection and activation  
✅ **No extra setup**: Worker image has everything needed  
✅ **Documentation**: Comprehensive guides for all features  

Ready to deploy and test! 🚀
