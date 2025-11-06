#!/bin/bash
# verify-nvproxy-setup.sh
# Quick script to verify nvproxy is ready to use

set -e

echo "════════════════════════════════════════════════════════════════"
echo "🔍 Verifying nvproxy Setup"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_mark="${GREEN}✅${NC}"
cross_mark="${RED}❌${NC}"
warning_mark="${YELLOW}⚠️${NC}"

# Check 1: runsc binary exists
echo "1. Checking runsc binary..."
if command -v runsc &> /dev/null; then
    echo -e "${check_mark} runsc found: $(which runsc)"
    RUNSC_VERSION=$(runsc --version 2>&1 | head -n1)
    echo "   Version: $RUNSC_VERSION"
else
    echo -e "${cross_mark} runsc not found in PATH"
    exit 1
fi

echo ""

# Check 2: runsc version supports nvproxy
echo "2. Checking nvproxy support..."
if runsc help 2>&1 | grep -q "nvproxy"; then
    echo -e "${check_mark} nvproxy flag available in runsc"
else
    echo -e "${cross_mark} nvproxy not supported in this runsc version"
    echo "   Upgrade to a recent version (2023+)"
    exit 1
fi

echo ""

# Check 3: GPU devices available (runtime check)
echo "3. Checking GPU devices..."
if ls /dev/nvidia* &> /dev/null; then
    echo -e "${check_mark} NVIDIA devices found:"
    ls -la /dev/nvidia* | grep -E 'nvidia[0-9]|nvidiactl|nvidia-uvm' | awk '{print "   "$NF}'
else
    echo -e "${warning_mark} No NVIDIA devices found"
    echo "   This is OK in worker image - devices are injected by K8s at runtime"
    echo "   Ensure worker pod has RuntimeClass: nvidia"
fi

echo ""

# Check 4: NVIDIA driver version (runtime check)
echo "4. Checking NVIDIA driver..."
if [ -e /proc/driver/nvidia/version ]; then
    DRIVER_VERSION=$(cat /proc/driver/nvidia/version | head -n1)
    echo -e "${check_mark} NVIDIA driver installed:"
    echo "   $DRIVER_VERSION"
else
    echo -e "${warning_mark} NVIDIA driver not found"
    echo "   This is OK in worker image - driver is on the host"
    echo "   Verify host nodes have NVIDIA driver installed"
fi

echo ""

# Check 5: Test runsc with nvproxy flag
echo "5. Testing runsc --nvproxy flag..."
if runsc --nvproxy=true --version &> /dev/null; then
    echo -e "${check_mark} runsc accepts --nvproxy flag"
else
    echo -e "${cross_mark} runsc doesn't accept --nvproxy flag"
    exit 1
fi

echo ""

# Check 6: Verify gVisor platform support
echo "6. Checking gVisor platforms..."
PLATFORMS=$(runsc help 2>&1 | grep -A 10 "platform string" | grep -E "(systrap|kvm|ptrace)" || echo "")
if [ -n "$PLATFORMS" ]; then
    echo -e "${check_mark} Available platforms:"
    echo "$PLATFORMS" | sed 's/^/   /'
else
    echo -e "${warning_mark} Could not detect platform options"
fi

echo ""

# Summary
echo "════════════════════════════════════════════════════════════════"
echo "📊 Summary"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Worker Image Setup:"
echo -e "  ${check_mark} runsc with nvproxy support: READY"
echo ""
echo "Runtime Requirements (verified at pod runtime):"
echo "  • Worker pod must have RuntimeClass: nvidia"
echo "  • Worker pod must request nvidia.com/gpu resource"
echo "  • Host nodes must have NVIDIA driver installed"
echo ""
echo "User Container Requirements:"
echo "  • User images must include NVIDIA libraries (libcuda.so, etc.)"
echo "  • Typically from nvidia/cuda or pytorch/pytorch base images"
echo ""
echo -e "${GREEN}✅ Worker image is ready for nvproxy!${NC}"
echo ""
echo "Next steps:"
echo "  1. Deploy worker pod with RuntimeClass: nvidia"
echo "  2. Configure worker pool: containerRuntime: gvisor"
echo "  3. Run GPU workload with user image containing CUDA libraries"
echo ""
echo "════════════════════════════════════════════════════════════════"
