package vm

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// The enhanced cache of a workspace is a disk that every runner of the
// workspace starts with under its image, read-only (types.RootLayer). It
// holds the cache directories of the tools that jobs run, so a job finds
// there what an earlier job downloaded or built, and neither fetches nor
// builds it again. Nothing in a workflow changes for it.
//
// A job reads the cache whatever it is, and only the right job adds to it:
// one that GitHub started for a push to its repository's default branch, or
// on that branch's schedule, and that succeeded. Code that reached a default
// branch is code the account already runs, so nothing that a pull request or
// a fork could change ends up in what every other job of the workspace is
// given.
//
// One runner at a time is started to add to the cache. Its worker publishes
// what its job wrote as a new generation of the disk, which is a candidate:
// no runner gets it until promoteCaches finds that the job which ran on that
// runner was the right kind. A runner takes any job with its label, so the
// one it was started for says nothing about the one it ran.
const (
	// The cache holds no more than this. It takes the room of what is in it.
	runnerCacheSize = "64Gi"
	// A cache starts empty again when it is this old, which is what bounds
	// what it holds: entries that nothing uses any more are never removed
	// from it otherwise.
	runnerCacheLifetime = 7 * 24 * time.Hour
	// How long a runner that was started to add to the cache is waited for
	// before another is started to: longer than most jobs run.
	runnerCachePublisher = 15 * time.Minute
	// How long a candidate waits for GitHub to say how its job went.
	runnerCacheVerdict = 15 * time.Minute
	// How long a retired cache is kept: until no runner that started on it
	// is left, and no worker reads it any more.
	runnerCacheRetired = runnerLifetime + time.Hour
)

// runnerCachePaths are the directories of a runner that the cache holds:
// where Go, npm, pip and apt keep what they downloaded or built, on a
// runner as on one of GitHub's. Each names its entries by their content or
// by a version that never changes, so an entry is right for any job that
// looks for it, and one that a job does not look for costs it nothing.
var runnerCachePaths = []string{
	"/home/runner/.cache/go-build",
	"/home/runner/go/pkg/mod",
	"/home/runner/.npm/_cacache",
	"/home/runner/.cache/pip",
	"/var/cache/apt/archives",
}

// cacheLayer is the cache that the runner of a job starts with, or nil when
// its account has the cache turned off. The cache is a new one when the
// workspace has none, or when its last became too old. The runner keeps
// what its job adds if the job is on a default branch and no other runner
// is doing so.
func (r *githubRunners) cacheLayer(ctx context.Context, installation *types.GitHubRunnerInstallation, job types.GitHubRunnerJob) (*types.RootLayer, error) {
	if !installation.EnhancedCache {
		return nil, nil
	}

	if err := r.s.repo.RetireGitHubRunnerCache(ctx, job.WorkspaceID, runnerCacheLifetime); err != nil {
		return nil, err
	}

	cache, err := r.s.repo.CurrentGitHubRunnerCache(ctx, job.WorkspaceID, types.RootLayerDiskPrefix+randomHexID()[:16])
	if err != nil || cache == nil {
		return nil, err
	}

	layer := &types.RootLayer{DiskName: cache.DiskName, Size: runnerCacheSize, SnapshotID: cache.SnapshotID, Paths: runnerCachePaths}
	if !job.DefaultBranch {
		return layer, nil
	}

	layer.Publish, err = r.s.repo.ClaimGitHubRunnerCachePublisher(ctx, job.WorkspaceID, cache.DiskName, runnerCachePublisher)
	return layer, err
}

// promoteCaches gives the runners of each workspace the generation of its
// cache that a runner left, once the job that ran on that runner is known
// to be one whose additions can be trusted. A candidate of any other job is
// withdrawn.
func (r *githubRunners) promoteCaches(ctx context.Context) {
	candidates, err := r.s.repo.ListGitHubRunnerCacheCandidates(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("list GitHub runner cache candidates")
		return
	}

	for _, candidate := range candidates {
		outcome, err := r.settle(ctx, candidate)
		if err != nil {
			log.Warn().Err(err).Str("runner", candidate.Runner).Str("snapshot_id", candidate.SnapshotID).Msg("check a generation of a GitHub runner cache")
			continue
		}

		if outcome != "" {
			log.Info().Uint("workspace_id", candidate.WorkspaceID).Str("runner", candidate.Runner).Str("snapshot_id", candidate.SnapshotID).
				Msg("GitHub runner cache: " + outcome)
		}
	}
}

// settle promotes a candidate or withdraws it, and says which, or says
// nothing when it does neither yet.
func (r *githubRunners) settle(ctx context.Context, candidate types.GitHubRunnerCacheCandidate) (string, error) {
	trusted, refusal, err := r.trusts(ctx, candidate)
	switch {
	case err != nil:
		return "", err
	case trusted:
		// Another candidate of the same generation may have been first.
		promoted, err := r.s.repo.PromoteGitHubRunnerCache(ctx, candidate)
		if err != nil || !promoted {
			return "", err
		}

		return "runners start on a new generation", nil
	case refusal != "":
		return "withdrew a generation, as " + refusal, r.s.repo.RejectGitHubRunnerCacheCandidate(ctx, candidate, refusal)
	}

	return "", nil
}

// trusts reports whether what a candidate adds to its cache can be given to
// every runner of the workspace, or why not. It reports neither while GitHub
// may yet say how the job of the candidate's runner went.
func (r *githubRunners) trusts(ctx context.Context, candidate types.GitHubRunnerCacheCandidate) (bool, string, error) {
	job, err := r.s.repo.GetGitHubRunnerJobByRunner(ctx, candidate.WorkspaceID, candidate.Runner)
	if err != nil {
		return false, "", err
	}

	late := time.Since(candidate.CreatedAt) > runnerCacheVerdict
	switch {
	case job == nil || job.Status != types.GitHubJobCompleted:
		if late {
			return false, "GitHub did not say that the job of the runner finished", nil
		}

		return false, "", nil
	case job.Conclusion != "success":
		return false, "the job of the runner did not succeed", nil
	case !job.DefaultBranch || job.RunID == 0:
		return false, "the job of the runner was not on a default branch", nil
	}

	run, err := r.github.run(ctx, job.InstallationID, job.Repository, job.RunID)
	switch {
	case githubStatus(err) == http.StatusNotFound:
		return false, "GitHub does not have the run of the job", nil
	case err != nil && late:
		return false, "GitHub did not say what started the run of the job", nil
	case err != nil:
		return false, "", err
	}

	if !trustsRun(run, job.Repository) {
		return false, "the run of the job was not started by a push to the default branch of its repository, or on its schedule", nil
	}

	return true, "", nil
}

// trustsRun reports whether a workflow run of a repository ran code that
// its default branch holds: GitHub started it for a push to that branch, or
// on that branch's schedule. A pull request's run is neither, whichever
// branch it is from, and a run of a fork's branch that has the same name is
// of another repository.
func trustsRun(run *githubRun, repository string) bool {
	if run.Event != "push" && run.Event != "schedule" {
		return false
	}

	return run.DefaultBranch != "" && run.HeadBranch == run.DefaultBranch && strings.EqualFold(run.HeadRepository.FullName, repository)
}

// deleteCaches deletes the caches that were retired long enough ago: what
// is stored for each, and then its record.
func (r *githubRunners) deleteCaches(ctx context.Context) {
	caches, err := r.s.repo.ListRetiredGitHubRunnerCaches(ctx, runnerCacheRetired)
	if err != nil {
		log.Warn().Err(err).Msg("list retired GitHub runner caches")
		return
	}

	for _, cache := range caches {
		if err := r.deleteCache(ctx, cache); err != nil {
			log.Warn().Err(err).Uint("workspace_id", cache.WorkspaceID).Str("disk", cache.DiskName).Msg("delete a GitHub runner cache")
			continue
		}

		log.Info().Uint("workspace_id", cache.WorkspaceID).Str("disk", cache.DiskName).Msg("deleted a GitHub runner cache")
	}
}

func (r *githubRunners) deleteCache(ctx context.Context, cache types.GitHubRunnerCache) error {
	// Only a cache disk is ever deleted here, whatever its record says.
	if !strings.HasPrefix(cache.DiskName, types.RootLayerDiskPrefix) || types.SafeDurableDiskName(cache.DiskName) != cache.DiskName {
		return errors.New("not the name of a cache disk")
	}

	workspace, err := r.s.backend.GetWorkspace(ctx, cache.WorkspaceID)
	if err != nil {
		return err
	}

	// The slash keeps the disks whose names begin with this one's.
	if err := r.deleteObjects(ctx, workspace, path.Join(types.DefaultDurableDisksPrefix, cache.DiskName)+"/"); err != nil {
		return err
	}

	return r.s.repo.DeleteGitHubRunnerCache(ctx, cache.WorkspaceID, cache.DiskName)
}

// deleteObjects deletes what a workspace has stored under a prefix.
func (r *githubRunners) deleteObjects(ctx context.Context, workspace *types.Workspace, prefix string) error {
	if r.deleteStored != nil {
		return r.deleteStored(ctx, workspace, prefix)
	}

	if !workspace.StorageAvailable() {
		return nil
	}

	storage, err := clients.NewWorkspaceStorageClient(ctx, workspace.Name, workspace.Storage)
	if err != nil {
		return err
	}

	_, err = storage.DeleteWithPrefix(ctx, prefix)
	return err
}
