package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	blobcache "github.com/beam-cloud/blobcache-v2/pkg"
	"github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
	"github.com/beam-cloud/clip/pkg/storage"
	runc "github.com/beam-cloud/go-runc"
	"github.com/containers/buildah"
	"github.com/containers/buildah/define"
	"github.com/containers/buildah/imagebuildah"
	bstorage "github.com/containers/storage"
	"github.com/containers/storage/pkg/unshare"
	"github.com/hanwen/go-fuse/v2/fuse"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/opencontainers/umoci"
	"github.com/opencontainers/umoci/oci/cas/dir"
	"github.com/opencontainers/umoci/oci/casext"
	"github.com/opencontainers/umoci/oci/layer"
	"github.com/pkg/errors"

	"github.com/beam-cloud/beta9/pkg/abstractions/image"
	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	types "github.com/beam-cloud/beta9/pkg/types"
)

const (
	imagePullCommand string = "skopeo"
	imageBundlePath  string = "/dev/shm/images"
)

var (
	baseImageCachePath string = "/images/cache"
	baseImageMountPath string = "/images/mnt/%s"
)

var requiredContainerDirectories []string = []string{"/workspace", "/volumes"}

func getImageCachePath() string {
	path := baseImageCachePath

	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(path, 0755)
	}

	return path
}

func getImageMountPath(workerId string) string {
	path := fmt.Sprintf(baseImageMountPath, workerId)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(path, 0755)
	}

	return path
}

type ImageClient struct {
	registry           *common.ImageRegistry
	cacheClient        *blobcache.BlobCacheClient
	imageCachePath     string
	imageMountPath     string
	imageBundlePath    string
	pullCommand        string
	pDeathSignal       syscall.Signal
	mountedFuseServers *common.SafeMap[*fuse.Server]
	commandTimeout     int
	debug              bool
	creds              string
	config             types.AppConfig
	workerId           string
	workerRepo         repository.WorkerRepository
	logger             *ContainerLogger
}

func NewImageClient(config types.AppConfig, workerId string, workerRepo repository.WorkerRepository, fileCacheManager *FileCacheManager) (*ImageClient, error) {
	registry, err := common.NewImageRegistry(config.ImageService)
	if err != nil {
		return nil, err
	}

	c := &ImageClient{
		config:             config,
		registry:           registry,
		cacheClient:        fileCacheManager.GetClient(),
		imageBundlePath:    imageBundlePath,
		imageCachePath:     getImageCachePath(),
		imageMountPath:     getImageMountPath(workerId),
		pullCommand:        imagePullCommand,
		commandTimeout:     -1,
		debug:              false,
		creds:              "",
		workerId:           workerId,
		workerRepo:         workerRepo,
		mountedFuseServers: common.NewSafeMap[*fuse.Server](),
		logger:             &ContainerLogger{},
	}

	err = os.MkdirAll(c.imageBundlePath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *ImageClient) PullLazy(request *types.ContainerRequest) error {
	imageId := request.ImageId
	isBuildContainer := strings.HasPrefix(request.ContainerId, types.BuildContainerPrefix)

	c.logger.Log(request.ContainerId, request.StubId, "Loading image: %s", imageId)

	localCachePath := fmt.Sprintf("%s/%s.cache", c.imageCachePath, imageId)
	if !c.config.ImageService.LocalCacheEnabled && !isBuildContainer {
		localCachePath = ""
	}

	// If we have a valid cache client, attempt to cache entirety of the image
	// in memory (in a nearby region). If a remote cache is available, this supercedes
	// the local cache - which is basically just downloading the image to disk
	startTime := time.Now()

	if c.cacheClient != nil && !isBuildContainer {
		sourcePath := fmt.Sprintf("images/%s.clip", imageId)
		sourceOffset := int64(0)

		// If the image archive is already cached in memory (in blobcache), then we can use that as the local cache path
		baseBlobFsContentPath := fmt.Sprintf("%s/%s", baseFileCachePath, sourcePath)
		if _, err := os.Stat(baseBlobFsContentPath); err == nil {
			localCachePath = baseBlobFsContentPath
		} else {
			c.logger.Log(request.ContainerId, request.StubId, "image <%s> not found in cache, caching nearby", imageId)

			// Otherwise, lets cache it in a nearby blobcache host
			_, err := c.cacheClient.StoreContentFromSource(sourcePath, sourceOffset)
			if err == nil {
				localCachePath = baseBlobFsContentPath
			} else {
				c.logger.Log(request.ContainerId, request.StubId, "unable to cache image nearby <%s>: %v\n", imageId, err)
			}
		}
	}

	elapsed := time.Since(startTime)
	c.logger.Log(request.ContainerId, request.StubId, "Loaded image <%s>, took: %s", imageId, elapsed)

	remoteArchivePath := fmt.Sprintf("%s/%s.%s", c.imageCachePath, imageId, c.registry.ImageFileExtension)
	if _, err := os.Stat(remoteArchivePath); err != nil {
		err = c.registry.Pull(context.TODO(), remoteArchivePath, imageId)
		if err != nil {
			return err
		}
	}

	var mountOptions *clip.MountOptions = &clip.MountOptions{
		ArchivePath:           remoteArchivePath,
		MountPoint:            fmt.Sprintf("%s/%s", c.imageMountPath, imageId),
		Verbose:               false,
		CachePath:             localCachePath,
		ContentCache:          c.cacheClient,
		ContentCacheAvailable: c.cacheClient != nil,
		Credentials: storage.ClipStorageCredentials{
			S3: &storage.S3ClipStorageCredentials{
				AccessKey: c.config.ImageService.Registries.S3.AccessKey,
				SecretKey: c.config.ImageService.Registries.S3.SecretKey,
			},
		},
	}

	// Check if a fuse server exists for this imageId
	_, mounted := c.mountedFuseServers.Get(imageId)
	if mounted {
		return nil
	}

	// Get lock on image mount
	err := c.workerRepo.SetImagePullLock(c.workerId, imageId)
	if err != nil {
		return err
	}
	defer c.workerRepo.RemoveImagePullLock(c.workerId, imageId)

	startServer, _, server, err := clip.MountArchive(*mountOptions)
	if err != nil {
		return err
	}

	err = startServer()
	if err != nil {
		return err
	}

	c.mountedFuseServers.Set(imageId, server)
	return nil
}

func (c *ImageClient) Cleanup() error {
	c.mountedFuseServers.Range(func(imageId string, server *fuse.Server) bool {
		log.Printf("Un-mounting image: %s\n", imageId)
		server.Unmount()
		return true // Continue iteration
	})

	log.Println("Cleaning up blobfs image cache:", c.imageCachePath)
	if c.config.BlobCache.BlobFs.Enabled && c.cacheClient != nil {
		err := c.cacheClient.Cleanup()
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *ImageClient) InspectAndVerifyImage(ctx context.Context, sourceImage string, creds string) error {
	args := []string{"inspect", fmt.Sprintf("docker://%s", sourceImage)}

	args = append(args, c.inspectArgs(creds)...)
	cmd := exec.CommandContext(ctx, c.pullCommand, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	output, err := exec.CommandContext(ctx, c.pullCommand, args...).Output()
	if err != nil {
		return &types.ExitCodeError{
			ExitCode: types.WorkerContainerExitCodeInvalidCustomImage,
		}
	}

	var imageInfo map[string]interface{}
	err = json.Unmarshal(output, &imageInfo)
	if err != nil {
		return err
	}

	if imageInfo["Architecture"] != runtime.GOARCH {
		return &types.ExitCodeError{
			ExitCode: types.WorkerContainerExitCodeIncorrectImageArch,
		}
	}

	if imageInfo["Os"] != runtime.GOOS {
		return &types.ExitCodeError{
			ExitCode: types.WorkerContainerExitCodeIncorrectImageOs,
		}
	}

	return nil
}

func (c *ImageClient) BuildAndArchiveImage(ctx context.Context, dockerfile string, bundlePath string, imageId string) error {
	buildDir, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}
	defer os.RemoveAll(buildDir)
	tempDockerFile := filepath.Join(buildDir, "Dockerfile")
	f, err := os.Create(tempDockerFile)
	if err != nil {
		return err
	}
	fmt.Fprintf(f, dockerfile)
	f.Close()

	rootfsPath := filepath.Join(bundlePath, "rootfs")
	os.MkdirAll(rootfsPath, 0755)

	cmd := exec.Command("buildah", "build", "--format=oci", "--output="+rootfsPath, "--tag="+imageId+":latest", "-f", tempDockerFile, ".")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return err
	}

	// Run the push command to get the config.json and then write it to the bundle path
	cmd = exec.Command("buildah", "push", imageId+":latest", "oci:"+buildDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return err
	}

	manifestDigest, err := getManifest(buildDir)
	if err != nil {
		return err
	}

	configContent, err := getConfig(buildDir, manifestDigest)
	if err != nil {
		return err
	}

	configPath := filepath.Join(bundlePath, specBaseName)
	err = os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		return err
	}

	for _, dir := range requiredContainerDirectories {
		fullPath := filepath.Join(rootfsPath, dir)
		err := os.MkdirAll(fullPath, 0755)
		if err != nil {
			errors.Wrap(err, fmt.Sprintf("creating /%s directory", dir))
			return err
		}
	}

	// TODO: Figure out how to get this to work
	if false {
		if buildah.InitReexec() {
			return err
		}
		unshare.MaybeReexecUsingUserNamespace(false)

		storeOpts := bstorage.StoreOptions{
			GraphDriverName: "vfs",
			GraphRoot:       "/var/lib/containers/storage",
			RunRoot:         "/run/containers/storage",
		}

		buildStore, err := bstorage.GetStore(storeOpts)
		if err != nil {
			return err
		}
		defer buildStore.Shutdown(false)

		buildOpts := define.BuildOptions{
			NamespaceOptions: []define.NamespaceOption{
				{Name: string(specs.NetworkNamespace), Host: true},
			},
			ReportWriter: os.Stdout,
			BuildOutput:  rootfsPath,
			Isolation:    define.IsolationChroot,
		}

		_, _, err = imagebuildah.BuildDockerfiles(ctx, buildStore, buildOpts, tempDockerFile)
		if err != nil {
			return err
		}
	}

	// TODO: Not sure if we need this or not?
	// defer os.RemoveAll(bundlePath) (we do this in pull and archive but that is because it immediately pulls after the pullandarchive call succeeds)
	return c.Archive(ctx, bundlePath, imageId, nil)
}

func (c *ImageClient) PullAndArchiveImage(ctx context.Context, sourceImage string, imageId string, creds string) error {
	baseImage, err := image.ExtractImageNameAndTag(sourceImage)
	if err != nil {
		return err
	}

	if err := c.InspectAndVerifyImage(ctx, sourceImage, creds); err != nil {
		return err
	}

	baseTmpBundlePath := filepath.Join(c.imageBundlePath, baseImage.Repo)
	os.MkdirAll(baseTmpBundlePath, 0755)

	dest := fmt.Sprintf("oci:%s:%s", baseImage.Repo, baseImage.Tag)
	args := []string{"copy", fmt.Sprintf("docker://%s", sourceImage), dest}

	args = append(args, c.copyArgs(creds)...)
	cmd := exec.CommandContext(ctx, c.pullCommand, args...)
	cmd.Env = os.Environ()
	cmd.Dir = c.imageBundlePath
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	ec, err := c.startCommand(cmd)
	if err != nil {
		return err
	}

	status, err := runc.Monitor.Wait(cmd, ec)
	if err == nil && status != 0 {
		return fmt.Errorf("unable to copy image: %v", cmd.String())
	}

	tmpBundlePath := filepath.Join(baseTmpBundlePath, imageId)
	err = c.unpack(baseImage.Repo, baseImage.Tag, tmpBundlePath)
	if err != nil {
		return fmt.Errorf("unable to unpack image: %v", err)
	}

	defer os.RemoveAll(baseTmpBundlePath)

	return c.Archive(ctx, tmpBundlePath, imageId, nil)
}

func (c *ImageClient) startCommand(cmd *exec.Cmd) (chan runc.Exit, error) {
	if c.pDeathSignal != 0 {
		return runc.Monitor.StartLocked(cmd)
	}
	return runc.Monitor.Start(cmd)
}

func (c *ImageClient) copyArgs(creds string) (out []string) {
	if creds != "" {
		out = append(out, "--src-creds", creds)
	} else if creds == "" {
		out = append(out, "--src-no-creds")
	} else if c.creds != "" {
		out = append(out, "--src-creds", c.creds)
	}

	if c.commandTimeout > 0 {
		out = append(out, "--command-timeout", fmt.Sprintf("%d", c.commandTimeout))
	}

	if !c.config.ImageService.EnableTLS {
		out = append(out, []string{"--src-tls-verify=false", "--dest-tls-verify=false"}...)
	}

	if c.debug {
		out = append(out, "--debug")
	}

	return out
}

func (c *ImageClient) inspectArgs(creds string) (out []string) {
	if creds != "" {
		out = append(out, "--creds", creds)
	} else if creds == "" {
		out = append(out, "--no-creds")
	} else if c.creds != "" {
		out = append(out, "--creds", c.creds)
	}

	if c.commandTimeout > 0 {
		out = append(out, "--command-timeout", fmt.Sprintf("%d", c.commandTimeout))
	}

	if !c.config.ImageService.EnableTLS {
		out = append(out, []string{"--tls-verify=false"}...)
	}

	if c.debug {
		out = append(out, "--debug")
	}

	return out
}

func (c *ImageClient) unpack(baseImageName string, baseImageTag string, bundlePath string) error {
	var unpackOptions layer.UnpackOptions
	var meta umoci.Meta
	meta.Version = umoci.MetaVersion

	unpackOptions.KeepDirlinks = true
	unpackOptions.MapOptions = meta.MapOptions

	// Get a reference to the CAS.
	baseImagePath := fmt.Sprintf("%s/%s", c.imageBundlePath, baseImageName)
	engine, err := dir.Open(baseImagePath)
	if err != nil {
		return errors.Wrap(err, "open CAS "+baseImagePath)
	}
	defer engine.Close()

	engineExt := casext.NewEngine(engine)
	defer engineExt.Close()

	tmpBundlePath := filepath.Join(bundlePath + "_")
	err = umoci.Unpack(engineExt, baseImageTag, tmpBundlePath, unpackOptions)
	if err == nil {
		for _, dir := range requiredContainerDirectories {
			fullPath := filepath.Join(tmpBundlePath, "rootfs", dir)
			err := os.MkdirAll(fullPath, 0755)
			if err != nil {
				errors.Wrap(err, fmt.Sprintf("creating /%s directory", dir))
				return err
			}
		}

		return os.Rename(tmpBundlePath, bundlePath)
	}

	return err
}

// Generate and upload archived version of the image for distribution
func (c *ImageClient) Archive(ctx context.Context, bundlePath string, imageId string, progressChan chan int) error {
	startTime := time.Now()

	archiveName := fmt.Sprintf("%s.%s.tmp", imageId, c.registry.ImageFileExtension)
	archivePath := filepath.Join("/tmp", archiveName)

	defer func() {
		os.RemoveAll(archivePath)
	}()

	var err error = nil
	switch c.config.ImageService.RegistryStore {
	case common.S3ImageRegistryStore:
		err = clip.CreateAndUploadArchive(clip.CreateOptions{
			InputPath:  bundlePath,
			OutputPath: archivePath,
			Credentials: storage.ClipStorageCredentials{
				S3: &storage.S3ClipStorageCredentials{
					AccessKey: c.config.ImageService.Registries.S3.AccessKey,
					SecretKey: c.config.ImageService.Registries.S3.SecretKey,
				},
			},
			ProgressChan: progressChan,
		}, &clipCommon.S3StorageInfo{
			Bucket:   c.config.ImageService.Registries.S3.BucketName,
			Region:   c.config.ImageService.Registries.S3.Region,
			Endpoint: c.config.ImageService.Registries.S3.Endpoint,
			Key:      fmt.Sprintf("%s.clip", imageId),
		})
	case common.LocalImageRegistryStore:
		err = clip.CreateArchive(clip.CreateOptions{
			InputPath:  bundlePath,
			OutputPath: archivePath,
		})
	}

	if err != nil {
		log.Printf("Unable to create archive: %v\n", err)
		return err
	}
	log.Printf("Container <%v> archive took %v\n", imageId, time.Since(startTime))

	// Push the archive to a registry
	startTime = time.Now()
	err = c.registry.Push(ctx, archivePath, imageId)
	if err != nil {
		log.Printf("Failed to push image <%v>: %v\n", imageId, err)
		return err
	}

	log.Printf("Image <%v> push took %v\n", imageId, time.Since(startTime))
	return nil
}

func getManifest(buildDir string) (map[string]interface{}, error) {
	indexPath := filepath.Join(buildDir, "index.json")
	index, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, err
	}

	var indexData map[string]interface{}
	err = json.Unmarshal(index, &indexData)
	if err != nil {
		return nil, err
	}

	manifests := indexData["manifests"].([]interface{})

	if len(manifests) == 0 {
		return nil, errors.New("no manifests found in index.json")
	}

	manifestEntry := manifests[0].(map[string]interface{})
	digest, ok := manifestEntry["digest"].(string)
	if !ok {
		return nil, errors.New("digest not found in manifest")
	}

	if strings.HasPrefix(digest, "sha256:") {
		digest = strings.TrimPrefix(digest, "sha256:")
	}

	manifestPath := filepath.Join(buildDir, "blobs", "sha256", digest)
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}

	var manifestData map[string]interface{}
	err = json.Unmarshal(manifest, &manifestData)
	if err != nil {
		return nil, err
	}

	err = checkMediaType(v1.MediaTypeImageManifest, manifestData)
	if err != nil {
		return nil, err
	}

	return manifestData, nil
}

func getConfig(buildDir string, manifestData map[string]interface{}) ([]byte, error) {
	config, ok := manifestData["config"].(map[string]interface{})
	if !ok {
		return nil, errors.New("config not found in manifest")
	}

	err := checkMediaType(v1.MediaTypeImageConfig, config)
	if err != nil {
		return nil, err
	}

	digest, ok := config["digest"].(string)
	if !ok {
		return nil, errors.New("digest not found in config")
	}

	if strings.HasPrefix(digest, "sha256:") {
		digest = strings.TrimPrefix(digest, "sha256:")
	}

	configPath := filepath.Join(buildDir, "blobs", "sha256", digest)
	configContent, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	return configContent, nil
}

func checkMediaType(expectedMediaType string, config map[string]interface{}) error {
	mediaType, ok := config["mediaType"].(string)
	if !ok {
		return errors.New("media type not found in config")
	}

	if mediaType != expectedMediaType {
		return errors.New(fmt.Sprintf("media type %s is not %s", mediaType, expectedMediaType))
	}
	return nil
}
