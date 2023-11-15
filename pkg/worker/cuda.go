package worker

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/opencontainers/runtime-spec/specs-go"
)

var (
	defaultContainerCudaVersion string   = "12.2"
	defaultContainerPath        []string = []string{"/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin"}
	defaultContainerLibrary     []string = []string{"/usr/lib/x86_64-linux-gnu", "/usr/lib/worker/x86_64-linux-gnu", "/usr/local/nvidia/lib64"}
)

type ContainerCudaManager struct {
}

func NewContainerCudaManager() *ContainerCudaManager {
	return &ContainerCudaManager{}
}

func (c *ContainerCudaManager) InjectCudaEnvVars(env []string, options *ContainerOptions) ([]string, bool) {
	existingCudaFound := false
	cudaEnvVarNames := []string{
		"NVIDIA_DRIVER_CAPABILITIES",
		"NVIDIA_REQUIRE_CUDA",
		"NVARCH",
		"NV_CUDA_COMPAT_PACKAGE",
		"NV_CUDA_CUDART_VERSION",
		"NVIDIA_VISIBLE_DEVICES",
		"CUDA_VERSION",
		"GPU_TYPE",
	}

	initialEnvVars := make(map[string]string)
	if options.InitialSpec != nil {
		for _, m := range options.InitialSpec.Process.Env {

			// Only split on the first "=" in the env var
			// incase the value has any "=" in it
			splitVar := strings.SplitN(m, "=", 2)
			if len(splitVar) < 2 {
				continue
			}

			name := splitVar[0]
			value := splitVar[1]
			initialEnvVars[name] = value
		}
	}

	cudaVersion := defaultContainerCudaVersion
	existingCudaVersion, existingCudaFound := initialEnvVars["CUDA_VERSION"]
	if existingCudaFound {
		splitVersion := strings.Split(existingCudaVersion, ".")
		if len(splitVersion) >= 2 {
			major := splitVersion[0]
			minor := splitVersion[1]

			formattedVersion := major + "." + minor

			log.Printf("found existing cuda version in container image: %s (formatted: %s)\n", existingCudaVersion, formattedVersion)

			cudaVersion = formattedVersion
			existingCudaFound = true
		}
	}

	var cudaEnvVars []string
	for _, key := range cudaEnvVarNames {
		cudaEnvVarValue := os.Getenv(key)

		if existingCudaFound {
			if value, exists := initialEnvVars[key]; exists {
				cudaEnvVarValue = value
			} else {
				continue
			}
		}

		cudaEnvVars = append(cudaEnvVars, fmt.Sprintf("%s=%s", key, cudaEnvVarValue))
	}

	env = append(env, cudaEnvVars...)

	env = append(env,
		fmt.Sprintf("PATH=%s:/usr/local/cuda-%s/bin:$PATH",
			strings.Join(defaultContainerPath, ":"),
			cudaVersion))

	env = append(env,
		fmt.Sprintf("LD_LIBRARY_PATH=%s:/usr/local/cuda-%s/targets/x86_64-linux/lib:$LD_LIBRARY_PATH",
			strings.Join(defaultContainerLibrary, ":"),
			cudaVersion))

	return env, existingCudaFound
}

func (c *ContainerCudaManager) InjectCudaMounts(mounts []specs.Mount) []specs.Mount {
	cudaPaths := []string{fmt.Sprintf("/usr/local/cuda-%s", defaultContainerCudaVersion), "/usr/local/nvidia/lib64"}

	for _, path := range cudaPaths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			continue
		}

		mounts = append(mounts, []specs.Mount{
			{
				Type:        "bind",
				Source:      path,
				Destination: path,
				Options: []string{
					"rbind",
					"rprivate",
					"nosuid",
					"nodev",
					"rw",
				},
			},
		}...)
	}

	return mounts
}
