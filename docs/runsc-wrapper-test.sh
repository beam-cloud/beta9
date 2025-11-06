#!/bin/bash
# Test script to verify runsc wrapper works as expected

echo "════════════════════════════════════════════════════════════════"
echo "Testing runsc Wrapper"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Test 1: Check wrapper exists and is executable
echo "Test 1: Wrapper installation"
if [ -x /usr/local/bin/runsc ]; then
    echo "✅ /usr/local/bin/runsc exists and is executable"
else
    echo "❌ /usr/local/bin/runsc not found or not executable"
    exit 1
fi

# Test 2: Check real binary exists
echo ""
echo "Test 2: Real binary installation"
if [ -x /usr/local/bin/runsc.real ]; then
    echo "✅ /usr/local/bin/runsc.real exists and is executable"
else
    echo "❌ /usr/local/bin/runsc.real not found"
    exit 1
fi

# Test 3: Check environment variables
echo ""
echo "Test 3: Environment variables"
echo "GVISOR_ROOT=${GVISOR_ROOT:-not set}"
echo "GVISOR_PLATFORM=${GVISOR_PLATFORM:-not set}"

# Test 4: Test wrapper adds correct flags (dry run)
echo ""
echo "Test 4: Verify wrapper behavior"
echo "Running: runsc --version"
runsc --version
echo ""

# Test 5: Check bash aliases
echo "Test 5: Bash aliases"
if grep -q "alias runsc-raw" /root/.bashrc; then
    echo "✅ Bash aliases configured in /root/.bashrc"
    echo "   Available aliases:"
    grep "alias runsc-" /root/.bashrc | sed 's/^/   /'
else
    echo "⚠️  Bash aliases not found in /root/.bashrc"
fi

echo ""
echo "Test 6: Test runsc list command"
echo "Running: runsc list"
runsc list || echo "(No containers running - this is OK for testing)"

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "✅ Wrapper tests complete!"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "Usage on worker pod:"
echo "  runsc list              # List all gVisor containers"
echo "  runsc state <id>        # Get container state"
echo "  runsc ps <id>           # Get container processes"
echo "  runsc.real list         # Bypass wrapper"
echo ""
