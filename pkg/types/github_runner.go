package types

import "time"

// The statuses of a GitHub Actions job, in the order it moves through them.
const (
	GitHubJobQueued     = "queued"
	GitHubJobInProgress = "in_progress"
	GitHubJobCompleted  = "completed"
)

// GitHubRunnerInstallation connects a GitHub account, through an installation
// of the GitHub App, to the workspace that runs its jobs and is billed for
// them. Runners launch with the Beam token that connected it.
type GitHubRunnerInstallation struct {
	ID          int64     `db:"id" json:"id"`
	WorkspaceID uint      `db:"workspace_id" json:"-"`
	TokenID     string    `db:"token_id" json:"-"`
	Account     string    `db:"account" json:"account"`
	AccountType string    `db:"account_type" json:"account_type"`
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
}

// GitHubRunnerJob is a workflow job that asked for a Beam runner.
//
// Runners are interchangeable, so a job has two of them: VM is the runner
// started for it, and RunnerName the one GitHub ran it on, which may have been
// started for another job with the same label. ProvisionAt is when to start
// the job's runner, and is unset once there is nothing left to start. Error
// is why the last attempt to start it failed.
type GitHubRunnerJob struct {
	ID             int64      `db:"id" json:"id"`
	InstallationID int64      `db:"installation_id" json:"-"`
	WorkspaceID    uint       `db:"workspace_id" json:"-"`
	Repository     string     `db:"repository" json:"repository"`
	Workflow       string     `db:"workflow" json:"workflow"`
	Name           string     `db:"name" json:"name"`
	URL            string     `db:"url" json:"url"`
	Label          string     `db:"label" json:"label"`
	Status         string     `db:"status" json:"status"`
	Conclusion     string     `db:"conclusion" json:"conclusion,omitempty"`
	RunnerName     string     `db:"runner_name" json:"runner_name,omitempty"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	StartedAt      *time.Time `db:"started_at" json:"started_at,omitempty"`
	CompletedAt    *time.Time `db:"completed_at" json:"completed_at,omitempty"`
	VM             string     `db:"vm" json:"-"`
	Attempts       int        `db:"attempts" json:"-"`
	ProvisionAt    *time.Time `db:"provision_at" json:"-"`
	Error          string     `db:"error" json:"error,omitempty"`
}

// GitHubRunner is what the lifecycle of a runner VM depends on: where it is
// registered, whether that account is still connected, and the status of the
// job GitHub gave it, which is empty while it waits for one.
type GitHubRunner struct {
	InstallationID int64  `db:"installation_id"`
	Repository     string `db:"repository"`
	Connected      bool   `db:"connected"`
	JobStatus      string `db:"job_status"`
}
