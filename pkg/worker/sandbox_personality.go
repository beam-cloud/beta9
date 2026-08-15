package worker

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

const maxSandboxIdentityFileSize = 256

// shouldNormalizeSandboxRoot limits cosmetic root filesystem changes to cold
// sandbox starts. Warm restores must see the filesystem exactly as it was when
// the process state was captured.
func shouldNormalizeSandboxRoot(request *types.ContainerRequest, restoringRuntimeCheckpoint bool) bool {
	return request != nil &&
		!restoringRuntimeCheckpoint &&
		request.Stub.Type.Kind() == types.StubTypeSandbox
}

// normalizeSandboxRoot initializes conventional machine identity files. All
// access is confined beneath rootPath, and user-managed values and unusual file
// types are preserved.
func normalizeSandboxRoot(rootPath, hostname string) error {
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return err
	}
	defer root.Close()

	var errs []error
	machineID, created, err := ensureSandboxMachineID(root)
	if err != nil {
		errs = append(errs, fmt.Errorf("initialize machine id: %w", err))
	} else if created {
		if err := syncSandboxDBusMachineID(root, machineID); err != nil {
			errs = append(errs, fmt.Errorf("synchronize dbus machine id: %w", err))
		}
	}

	if err := writeSandboxHostname(root, hostname); err != nil {
		errs = append(errs, fmt.Errorf("write hostname: %w", err))
	}

	return errors.Join(errs...)
}

func ensureSandboxMachineID(root *os.Root) ([]byte, bool, error) {
	info, err := root.Lstat("etc/machine-id")
	if err == nil {
		if !info.Mode().IsRegular() || info.Size() > 0 {
			return nil, false, nil
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, false, err
	}
	if machineID, ok, err := existingSandboxDBusMachineID(root); err != nil {
		return nil, false, err
	} else if ok {
		if err := root.WriteFile("etc/machine-id", machineID, 0444); err != nil {
			return nil, false, err
		}
		return machineID, false, nil
	}

	randomID := make([]byte, 16)
	if _, err := rand.Read(randomID); err != nil {
		return nil, false, err
	}
	machineID := []byte(hex.EncodeToString(randomID) + "\n")
	if err := root.WriteFile("etc/machine-id", machineID, 0444); err != nil {
		return nil, false, err
	}
	return machineID, true, nil
}

func existingSandboxDBusMachineID(root *os.Root) ([]byte, bool, error) {
	info, err := root.Lstat("var/lib/dbus/machine-id")
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return nil, false, nil
	}
	contents, ok, err := readSandboxIdentityFile(root, "var/lib/dbus/machine-id", info)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	value := strings.TrimSpace(string(contents))
	if len(value) != 32 {
		return nil, false, nil
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return nil, false, nil
		}
	}
	return []byte(value + "\n"), true, nil
}

func readSandboxIdentityFile(root *os.Root, path string, info fs.FileInfo) ([]byte, bool, error) {
	if info.Size() > maxSandboxIdentityFileSize {
		return nil, false, nil
	}
	file, err := root.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, maxSandboxIdentityFileSize+1))
	if err != nil {
		return nil, false, err
	}
	if len(contents) > maxSandboxIdentityFileSize {
		return nil, false, nil
	}
	return contents, true, nil
}

func syncSandboxDBusMachineID(root *os.Root, machineID []byte) error {
	info, err := root.Lstat("var/lib/dbus/machine-id")
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return nil
	}
	if info.Size() > 0 {
		return nil
	}
	return root.WriteFile("var/lib/dbus/machine-id", machineID, 0444)
}

func writeSandboxHostname(root *os.Root, hostname string) error {
	if hostname == "" {
		hostname = "localhost"
	}
	info, err := root.Lstat("etc/hostname")
	if err == nil {
		if !info.Mode().IsRegular() {
			return nil
		}
		contents, ok, err := readSandboxIdentityFile(root, "etc/hostname", info)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		current := strings.TrimSpace(string(contents))
		switch current {
		case "", "localhost", "localhost.localdomain", "debuerreotype", "runc", "runsc":
		default:
			return nil
		}
	}
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return root.WriteFile("etc/hostname", []byte(hostname+"\n"), 0644)
}
